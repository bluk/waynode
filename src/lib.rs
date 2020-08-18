// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! BtDht is a library which can help build an application using the [BitTorrent][bittorrent]
//! [Distributed Hash Table][bep_0005].
//!
//! [bittorrent]: http://bittorrent.org/
//! [bep_0005]: http://bittorrent.org/beps/bep_0005.html

pub mod addr;
pub mod error;
pub mod krpc;
pub mod node;
pub(crate) mod routing;
pub(crate) mod transaction;

use crate::{
    addr::Addr,
    krpc::QueryArgs,
    node::remote::{RemoteNode, RemoteNodeId},
};
use bt_bencode::Value;
use serde_bytes::ByteBuf;
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::io::Write;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, PartialEq)]
struct OutboundMsg {
    tx_id: Option<transaction::Id>,
    addr: SocketAddr,
    remote_id: RemoteNodeId,
    msg_data: Vec<u8>,
}

impl OutboundMsg {
    fn into_transaction(self) -> Option<transaction::Transaction> {
        let remote_id = self.remote_id;
        let resolved_addr = self.addr;
        self.tx_id.map(|tx_id| transaction::Transaction {
            local_id: transaction::LocalId {
                id: tx_id,
                addr: resolved_addr,
            },
            remote_id,
            sent: Instant::now(),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
struct NodeToReplace {
    tx_local_id: transaction::LocalId,
    probe_node_id: RemoteNodeId,
    new_node: RemoteNode,
    timeout_count: u8,
}

#[derive(Debug, PartialEq)]
struct FindNodeOp {
    tx_local_ids: Vec<transaction::LocalId>,
    queried_remote_nodes: Vec<RemoteNodeId>,
    id_to_find: node::Id,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SendInfo {
    pub len: usize,
    pub addr: SocketAddr,
}

#[derive(Debug)]
pub struct InboundMsg {
    remote_id: RemoteNodeId,
    tx_local_id: Option<transaction::LocalId>,
    msg: Option<Value>,
    is_timeout: bool,
}

impl InboundMsg {
    pub fn remote_id(&self) -> &RemoteNodeId {
        &self.remote_id
    }

    pub fn tx_local_id(&self) -> Option<transaction::LocalId> {
        self.tx_local_id
    }

    pub fn msg(&self) -> &Option<Value> {
        &self.msg
    }

    pub fn is_timeout(&self) -> bool {
        self.is_timeout
    }
}

/// The configuration for the local DHT node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    /// Local node id
    pub id: node::Id,
    /// Client version identifier
    pub client_version: Option<ByteBuf>,
    /// The amount of time before a query without a response is considered timed out
    pub query_timeout: Duration,
    /// If the node is read only
    pub is_read_only_node: bool,
    /// The max amount of nodes in a routing table bucket
    pub max_node_count_per_bucket: usize,
}

/// The distributed hash table.
#[derive(Debug)]
pub struct Dht {
    config: Config,
    routing_table: routing::Table,
    probed_nodes: Vec<NodeToReplace>,

    find_node_ops: Vec<FindNodeOp>,

    transactions: VecDeque<transaction::Transaction>,
    next_transaction_id: transaction::Id,

    inbound_msgs: VecDeque<InboundMsg>,
    outbound_msgs: VecDeque<OutboundMsg>,
}

impl Dht {
    pub fn new_with_config(config: Config) -> Self {
        let max_node_count_per_bucket = config.max_node_count_per_bucket;
        let id = config.id;
        Self {
            config,
            routing_table: routing::Table::new(id, max_node_count_per_bucket),
            probed_nodes: Vec::new(),
            find_node_ops: Vec::new(),
            transactions: VecDeque::new(),
            next_transaction_id: transaction::Id(0),
            inbound_msgs: VecDeque::new(),
            outbound_msgs: VecDeque::new(),
        }
    }

    pub fn bootstrap<'a>(&mut self, bootstrap_nodes: &'a [RemoteNodeId]) {
        let neighbors = self
            .find_neighbors(self.config.id, bootstrap_nodes, true, None)
            .iter()
            .map(|&n| n.clone())
            .collect::<Vec<RemoteNodeId>>();
        let mut find_node_op = FindNodeOp {
            tx_local_ids: Vec::new(),
            queried_remote_nodes: Vec::new(),
            id_to_find: self.config.id,
        };
        for n in neighbors {
            use krpc::find_node::FindNodeQueryArgs;
            if let Ok(tx_local_id) = self.write_query(
                &FindNodeQueryArgs::new_with_id_and_target(self.config.id, self.config.id),
                &n,
            ) {
                find_node_op.tx_local_ids.push(tx_local_id);
                find_node_op.queried_remote_nodes.push(n);
            }
        }
        self.find_node_ops.push(find_node_op);
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn on_recv(&mut self, bytes: &[u8], addr: SocketAddr) -> Result<(), error::Error> {
        self.on_recv_with_now(bytes, addr, Instant::now())
    }

    fn on_recv_with_now(
        &mut self,
        bytes: &[u8],
        addr: SocketAddr,
        now: Instant,
    ) -> Result<(), error::Error> {
        use crate::krpc::{Kind, Msg};

        let value: Value = bt_bencode::from_slice(bytes)
            .map_err(|_| error::Error::CannotDeserializeKrpcMessage)?;
        if let Some(kind) = value.kind() {
            let transactions = &mut self.transactions;
            if let Some(transaction) = value
                .transaction_id()
                .and_then(|tx_id| {
                    transaction::Id::try_from(tx_id)
                        .map(|tx_id| transaction::LocalId { id: tx_id, addr })
                        .ok()
                })
                .and_then(|tx_local_id| transactions.iter().position(|t| t.local_id == tx_local_id))
                .and_then(|idx| transactions.swap_remove_back(idx))
            {
                match kind {
                    Kind::Response => {
                        use krpc::RespMsg;
                        let queried_node_id = RespMsg::queried_node_id(&value);
                        if transaction
                            .remote_id
                            .node_id
                            .and_then(|id| {
                                Some(queried_node_id.and_then(|q| Some(q == id)).unwrap_or(false))
                            })
                            .unwrap_or(true)
                        {
                            self.routing_table
                                .on_response_received(&transaction.remote_id);
                            self.probed_nodes
                                .retain(|n| n.tx_local_id != transaction.local_id);
                            self.add_node_to_table(&transaction.remote_id, Some(now), None);
                            if let Some(mut find_node_op) = self
                                .find_node_ops
                                .iter()
                                .position(|op| {
                                    op.tx_local_ids
                                        .iter()
                                        .any(|&tx_local_id| tx_local_id == transaction.local_id)
                                })
                                .map(|idx| self.find_node_ops.swap_remove(idx))
                            {
                                find_node_op
                                    .tx_local_ids
                                    .retain(|&tx_local_id| tx_local_id != transaction.local_id);
                                use krpc::find_node::FindNodeRespValues;
                                if let Some(values) = value.values() {
                                    if let Ok(response) = FindNodeRespValues::try_from(values) {
                                        if let Some(new_node_ids) =
                                            response.nodes().as_ref().map(|nodes| {
                                                nodes
                                                    .iter()
                                                    .filter(|n| {
                                                        !find_node_op
                                                            .queried_remote_nodes
                                                            .iter()
                                                            .any(|existing_n| {
                                                                existing_n
                                                                    .node_id
                                                                    .map(|e_nid| e_nid == n.id)
                                                                    .unwrap_or(false)
                                                                    || existing_n.addr
                                                                        == Addr::SocketAddr(
                                                                            SocketAddr::V4(n.addr),
                                                                        )
                                                            })
                                                    })
                                                    .map(|cn| RemoteNodeId {
                                                        addr: Addr::SocketAddr(SocketAddr::V4(
                                                            cn.addr,
                                                        )),
                                                        node_id: Some(cn.id),
                                                    })
                                                    .collect::<Vec<_>>()
                                            })
                                        {
                                            use krpc::find_node::FindNodeQueryArgs;
                                            for id in new_node_ids {
                                                if let Ok(tx_local_id) = self.write_query(
                                                    &FindNodeQueryArgs::new_with_id_and_target(
                                                        self.config.id,
                                                        find_node_op.id_to_find,
                                                    ),
                                                    &id,
                                                ) {
                                                    find_node_op.tx_local_ids.push(tx_local_id);
                                                    find_node_op
                                                        .queried_remote_nodes
                                                        .push(id.clone());
                                                }
                                            }
                                        }
                                    }
                                }
                                self.find_node_ops.push(find_node_op);
                            } else {
                                self.inbound_msgs.push_back(InboundMsg {
                                    remote_id: transaction.remote_id,
                                    tx_local_id: Some(transaction.local_id),
                                    msg: Some(value),
                                    is_timeout: false,
                                });
                            }
                        }
                    }
                    Kind::Error => {
                        self.routing_table.on_error_received(&transaction.remote_id);
                        self.probed_nodes
                            .retain(|n| n.tx_local_id != transaction.local_id);
                        self.add_node_to_table(&transaction.remote_id, None, None);
                        self.inbound_msgs.push_back(InboundMsg {
                            remote_id: transaction.remote_id,
                            tx_local_id: Some(transaction.local_id),
                            msg: Some(value),
                            is_timeout: false,
                        });
                    }
                    // unexpected
                    Kind::Query | Kind::Unknown(_) => {}
                }
            } else {
                match kind {
                    Kind::Query => {
                        use krpc::QueryMsg;
                        let querying_node_id = QueryMsg::querying_node_id(&value);
                        let remote_id = RemoteNodeId {
                            addr: Addr::SocketAddr(addr),
                            node_id: querying_node_id,
                        };
                        self.routing_table.on_query_received(&remote_id);
                        self.add_node_to_table(&remote_id, None, Some(now));
                        self.inbound_msgs.push_back(InboundMsg {
                            remote_id,
                            tx_local_id: None,
                            msg: Some(value),
                            is_timeout: false,
                        });
                    }
                    // unexpected
                    Kind::Response | Kind::Error | Kind::Unknown(_) => {}
                }
            }
        }
        Ok(())
    }

    fn add_node_to_table(
        &mut self,
        remote_id: &RemoteNodeId,
        last_response: Option<Instant>,
        last_query: Option<Instant>,
    ) {
        if let Some(id) = remote_id.node_id {
            if self.routing_table.contains(remote_id) {
                return;
            }

            if self
                .probed_nodes
                .iter()
                .find(|p| p.new_node.id == *remote_id)
                .is_some()
            {
                return;
            }

            let (bucket, is_last_bucket) = self.routing_table.find_bucket(&id);
            if !bucket.is_full() || is_last_bucket {
                self.routing_table.add(remote_id.clone(), None);
                return;
            }

            let bad_node_id = bucket.bad_nodes_remote_ids().next().map(|n| n.clone());
            if let Some(bad_node_id) = bad_node_id {
                self.routing_table
                    .add(remote_id.clone(), Some(&bad_node_id));
                return;
            }

            let questionable_node_remote_id = bucket
                .questionable_node_remote_ids()
                .filter(|n| {
                    self.probed_nodes
                        .iter()
                        .find(|p| p.probe_node_id == **n)
                        .is_none()
                })
                .next()
                .map(|n| n.clone());
            if let Some(questionable_node_remote_id) = questionable_node_remote_id {
                use krpc::ping::PingQueryArgs;
                if let Ok(tx_local_id) = self.write_query(
                    &PingQueryArgs::new_with_id(self.config.id),
                    &questionable_node_remote_id,
                ) {
                    self.probed_nodes.push(NodeToReplace {
                        tx_local_id,
                        probe_node_id: questionable_node_remote_id,
                        new_node: RemoteNode {
                            id: remote_id.clone(),
                            last_response,
                            last_query,
                            missing_responses: 0,
                        },
                        timeout_count: 0,
                    });
                }
            }
        }
    }

    pub fn read(&mut self) -> Option<InboundMsg> {
        self.inbound_msgs.pop_front()
    }

    pub fn timeout(&self) -> Option<Duration> {
        if let Some(earliest_sent) = self.transactions.iter().map(|t| t.sent).min() {
            let timeout = earliest_sent + self.config.query_timeout;
            let now = Instant::now();
            if now > timeout {
                Some(Duration::from_secs(0))
            } else {
                Some(now - timeout)
            }
        } else {
            None
        }
    }

    pub fn on_timeout(&mut self) {
        self.on_timeout_with_now(Instant::now())
    }

    fn on_timeout_with_now(&mut self, now: Instant) {
        let timeout = self.config.query_timeout;
        let mut reping: Vec<NodeToReplace> = vec![];

        for tx in self
            .transactions
            .iter()
            .filter(|tx| tx.sent + timeout <= now)
        {
            self.inbound_msgs.push_back(InboundMsg {
                remote_id: tx.remote_id.clone(),
                tx_local_id: Some(tx.local_id),
                msg: None,
                is_timeout: true,
            });

            self.routing_table.on_response_timeout(&tx.remote_id);
            if let Some(position) = self
                .probed_nodes
                .iter()
                .position(|p| p.tx_local_id == tx.local_id /* redundant: && p.probe_node_id == tx.remote_id */)
            {
                let mut probed_node = self.probed_nodes.swap_remove(position);
                probed_node.timeout_count += 1;
                if probed_node.timeout_count == 2 {
                    self.routing_table
                        .add(probed_node.new_node.id, Some(&probed_node.probe_node_id));
                } else {
                    reping.push(probed_node);
                }
            }
        }

        reping.into_iter().for_each(|mut p| {
            use krpc::ping::PingQueryArgs;
            if let Ok(tx_local_id) = self.write_query(
                &PingQueryArgs::new_with_id(self.config.id),
                &p.probe_node_id,
            ) {
                p.tx_local_id = tx_local_id;
                self.probed_nodes.push(p);
            }
        });

        self.transactions.retain(|tx| tx.sent + timeout > now);
    }

    #[inline]
    fn next_transaction_id(&mut self) -> transaction::Id {
        let transaction_id = self.next_transaction_id;
        self.next_transaction_id = self.next_transaction_id.next();
        transaction_id
    }

    pub fn write_query<T>(
        &mut self,
        args: &T,
        remote_id: &RemoteNodeId,
    ) -> Result<transaction::LocalId, error::Error>
    where
        T: QueryArgs,
    {
        let addr = remote_id.resolve_addr()?;
        let transaction_id = self.next_transaction_id();
        self.outbound_msgs.push_back(OutboundMsg {
            tx_id: Some(transaction_id.clone()),
            remote_id: remote_id.clone(),
            addr,
            msg_data: bt_bencode::to_vec(&krpc::ser::QueryMsg {
                a: Some(&args.to_value()),
                q: &ByteBuf::from(T::method_name()),
                t: &transaction_id.to_bytebuf(),
                v: self.config.client_version.as_ref(),
            })
            .map_err(|_| error::Error::CannotSerializeKrpcMessage)?,
        });
        Ok(transaction::LocalId {
            id: transaction_id,
            addr,
        })
    }

    pub fn write_resp(
        &mut self,
        transaction_id: &ByteBuf,
        resp: Option<Value>,
        remote_id: &RemoteNodeId,
    ) -> Result<(), error::Error> {
        let addr = remote_id.resolve_addr()?;
        self.outbound_msgs.push_back(OutboundMsg {
            tx_id: None,
            remote_id: remote_id.clone(),
            addr,
            msg_data: bt_bencode::to_vec(&krpc::ser::RespMsg {
                r: resp.as_ref(),
                t: &transaction_id,
                v: self.config.client_version.as_ref(),
            })
            .map_err(|_| error::Error::CannotSerializeKrpcMessage)?,
        });
        Ok(())
    }

    pub fn write_err(
        &mut self,
        transaction_id: &ByteBuf,
        details: Option<Value>,
        remote_id: &RemoteNodeId,
    ) -> Result<(), error::Error> {
        let addr = remote_id.resolve_addr()?;
        self.outbound_msgs.push_back(OutboundMsg {
            tx_id: None,
            remote_id: remote_id.clone(),
            addr,
            msg_data: bt_bencode::to_vec(&krpc::ser::ErrMsg {
                e: details.as_ref(),
                t: &transaction_id,
                v: self.config.client_version.as_ref(),
            })
            .map_err(|_| error::Error::CannotSerializeKrpcMessage)?,
        });
        Ok(())
    }

    pub fn send_to(&mut self, mut buf: &mut [u8]) -> Result<Option<SendInfo>, error::Error> {
        if let Some(out_msg) = self.outbound_msgs.pop_front() {
            buf.write_all(&out_msg.msg_data)
                .map_err(|_| error::Error::CannotSerializeKrpcMessage)?;
            let result = Some(SendInfo {
                len: out_msg.msg_data.len(),
                addr: out_msg.addr,
            });
            if let Some(tx) = out_msg.into_transaction() {
                self.transactions.push_back(tx);
            }
            Ok(result)
        } else {
            Ok(None)
        }
    }

    pub fn find_neighbors<'a>(
        &'a self,
        id: node::Id,
        bootstrap_nodes: &'a [RemoteNodeId],
        include_all_bootstrap_nodes: bool,
        want: Option<usize>,
    ) -> Vec<&'a RemoteNodeId> {
        self.routing_table.find_nearest_neighbor(
            id,
            bootstrap_nodes,
            include_all_bootstrap_nodes,
            want,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    use crate::krpc::{
        find_node::{FindNodeQueryArgs, METHOD_FIND_NODE},
        ping::{PingQueryArgs, METHOD_PING},
        Kind, Msg, QueryMsg,
    };

    fn new_config() -> Result<Config, error::Error> {
        Ok(Config {
            id: node::Id::rand()?,
            client_version: None,
            query_timeout: Duration::from_secs(60),
            is_read_only_node: true,
            max_node_count_per_bucket: 10,
        })
    }

    fn remote_addr() -> Addr {
        Addr::SocketAddr(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            6532,
        )))
    }

    fn node_id() -> node::Id {
        node::Id::rand().unwrap()
    }

    fn bootstrap_remote_addr() -> Addr {
        Addr::HostPort(String::from("127.0.0.1:6881"))
    }

    #[test]
    fn test_send_ping() -> Result<(), error::Error> {
        let id = node_id();
        let remote_addr = remote_addr();
        let remote_id = RemoteNodeId {
            addr: remote_addr.clone(),
            node_id: Some(id),
        };

        let args = PingQueryArgs::new_with_id(id);

        let mut dht: Dht = Dht::new_with_config(new_config()?);
        let tx_local_id = dht.write_query(&args, &remote_id).unwrap();

        let mut out: [u8; 65535] = [0; 65535];
        match dht.send_to(&mut out)? {
            Some(send_info) => {
                match remote_addr {
                    Addr::SocketAddr(socket_addr) => {
                        assert_eq!(send_info.addr, socket_addr);
                    }
                    _ => panic!(),
                }

                let filled_buf = &out[..send_info.len];
                let msg_sent: Value = bt_bencode::from_slice(filled_buf)
                    .map_err(|_| error::Error::CannotDeserializeKrpcMessage)?;
                assert_eq!(msg_sent.kind(), Some(Kind::Query));
                assert_eq!(msg_sent.method_name_str(), Some(METHOD_PING));
                assert_eq!(
                    msg_sent.transaction_id(),
                    Some(&tx_local_id.id.to_bytebuf())
                );

                Ok(())
            }
            None => panic!(),
        }
    }

    #[test]
    fn test_bootstrap() -> Result<(), error::Error> {
        let mut dht: Dht = Dht::new_with_config(new_config()?);
        let bootstrap_remote_addr = bootstrap_remote_addr();
        dht.bootstrap(&[RemoteNodeId {
            addr: bootstrap_remote_addr.clone(),
            node_id: None,
        }]);

        let mut out: [u8; 65535] = [0; 65535];
        match dht.send_to(&mut out)? {
            Some(send_info) => {
                match bootstrap_remote_addr.clone() {
                    Addr::HostPort(host_port) => {
                        use std::net::ToSocketAddrs;
                        let socket_addr: SocketAddr =
                            host_port.to_socket_addrs().unwrap().next().unwrap();
                        assert_eq!(send_info.addr, socket_addr);
                    }
                    _ => panic!(),
                }

                let find_node_op = dht.find_node_ops.first().unwrap();
                assert_eq!(
                    &RemoteNodeId {
                        addr: bootstrap_remote_addr.clone(),
                        node_id: None,
                    },
                    find_node_op.queried_remote_nodes.first().unwrap()
                );

                let filled_buf = &out[..send_info.len];
                let msg_sent: Value = bt_bencode::from_slice(filled_buf)
                    .map_err(|_| error::Error::CannotDeserializeKrpcMessage)?;
                assert_eq!(msg_sent.kind(), Some(Kind::Query));
                assert_eq!(msg_sent.method_name_str(), Some(METHOD_FIND_NODE));
                assert_eq!(
                    msg_sent.transaction_id(),
                    Some(&find_node_op.tx_local_ids.first().unwrap().id.to_bytebuf())
                );
                dbg!(&msg_sent);
                let find_node_query_args =
                    FindNodeQueryArgs::try_from(msg_sent.args().unwrap()).unwrap();
                assert_eq!(find_node_query_args.target(), &dht.config.id);
                assert_eq!(find_node_query_args.id(), &dht.config.id);

                Ok(())
            }
            None => panic!(),
        }
    }
}

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

#[macro_use]
extern crate log;

pub mod addr;
pub mod error;
pub(crate) mod find_node_op;
pub mod krpc;
pub(crate) mod msg_buffer;
pub mod node;
pub(crate) mod routing;
pub(crate) mod transaction;

use crate::{
    addr::Addr,
    find_node_op::FindNodeOp,
    krpc::{
        find_node::FindNodeQueryArgs, ping::PingQueryArgs, Kind, Msg, QueryArgs, QueryMsg, RespMsg,
    },
    msg_buffer::Buffer,
    msg_buffer::InboundMsg,
    node::remote::RemoteNodeId,
};
use bt_bencode::Value;
use serde_bytes::ByteBuf;
use std::io::Write;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
pub struct SendInfo {
    pub len: usize,
    pub addr: SocketAddr,
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
    tx_manager: transaction::Manager,
    msg_buffer: msg_buffer::Buffer,

    find_node_ops: Vec<FindNodeOp>,
}

impl Dht {
    pub fn new_with_config(config: Config) -> Self {
        let max_node_count_per_bucket = config.max_node_count_per_bucket;
        let id = config.id;
        Self {
            config,
            routing_table: routing::Table::new(id, max_node_count_per_bucket),
            tx_manager: transaction::Manager::new(),
            msg_buffer: Buffer::new(),
            find_node_ops: Vec::new(),
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn on_recv(&mut self, bytes: &[u8], addr: SocketAddr) -> Result<(), error::Error> {
        self.on_recv_with_now(bytes, addr, Instant::now())
    }

    fn find_find_node_op(&mut self, tx_local_id: transaction::LocalId) -> Option<FindNodeOp> {
        self.find_node_ops
            .iter()
            .position(|op| op.contains_tx_local_id(tx_local_id))
            .map(|idx| self.find_node_ops.swap_remove(idx))
    }

    fn handle_resp_or_err(
        &mut self,
        value: Value,
        tx: transaction::Transaction,
    ) -> Result<(), error::Error> {
        if let Some(mut find_node_op) = self.find_find_node_op(tx.local_id) {
            debug!(
                "transaction part of FindNodeOp. tx_local_ids.len()={:?} queried_remote_nodes.len()={:?}",
                find_node_op.tx_local_ids.len(),
                find_node_op.queried_remote_nodes.len(),
            );
            find_node_op.process_msg(self, tx, value);
            self.find_node_ops.push(find_node_op);
        } else {
            self.msg_buffer.push_inbound(InboundMsg {
                remote_id: tx.remote_id,
                tx_local_id: Some(tx.local_id),
                msg: Some(value),
                is_timeout: false,
            });
        }
        Ok(())
    }

    fn on_recv_with_now(
        &mut self,
        bytes: &[u8],
        addr: SocketAddr,
        now: Instant,
    ) -> Result<(), error::Error> {
        debug!("on_recv_with_now addr={}", addr);
        let value: Value = bt_bencode::from_slice(bytes)
            .map_err(|_| error::Error::CannotDeserializeKrpcMessage)?;
        if let Some(kind) = value.kind() {
            if let Some(tx) = value
                .tx_id()
                .and_then(|tx_id| self.tx_manager.find(tx_id, addr))
            {
                match kind {
                    Kind::Response => {
                        if tx.is_node_id_match(RespMsg::queried_node_id(&value)) {
                            self.routing_table
                                .on_msg_received(&tx.remote_id, &kind, now);
                            debug!("Received response for tx_local_id={:?}", tx.local_id);
                            self.handle_resp_or_err(value, tx)?;
                        }
                    }
                    Kind::Error => {
                        self.routing_table
                            .on_msg_received(&tx.remote_id, &kind, now);
                        debug!("Received error for tx_local_id={:?}", tx.local_id);
                        self.handle_resp_or_err(value, tx)?;
                    }
                    // unexpected
                    Kind::Query | Kind::Unknown(_) => {}
                }
            } else {
                match kind {
                    Kind::Query => {
                        debug!("Recieved query");
                        let querying_node_id = QueryMsg::querying_node_id(&value);
                        let remote_id = RemoteNodeId {
                            addr: Addr::SocketAddr(addr),
                            node_id: querying_node_id,
                        };
                        self.routing_table.on_msg_received(&remote_id, &kind, now);
                        self.msg_buffer.push_inbound(InboundMsg {
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

    fn ping_routing_table_questionable_nodes(&mut self, now: Instant) -> Result<(), error::Error> {
        for remote_id in self.routing_table.nodes_to_ping(now) {
            self.msg_buffer.write_query(
                &PingQueryArgs::new_with_id(self.config.id),
                &remote_id,
                &mut self.tx_manager,
                &self.config.client_version,
            )?;
        }
        Ok(())
    }

    pub fn on_recv_complete(&mut self) -> Result<(), error::Error> {
        self.on_recv_complete_with_now(Instant::now())
    }

    fn on_recv_complete_with_now(&mut self, now: Instant) -> Result<(), error::Error> {
        self.ping_routing_table_questionable_nodes(now)
    }

    pub fn read(&mut self) -> Option<InboundMsg> {
        self.msg_buffer.pop_inbound()
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.tx_manager.min_sent_instant().map(|earliest_sent| {
            let now = Instant::now();
            let timeout = earliest_sent + self.config.query_timeout;
            if now > timeout {
                Duration::from_secs(0)
            } else {
                timeout - now
            }
        })
    }

    pub fn on_timeout(&mut self) -> Result<(), error::Error> {
        self.on_timeout_with_now(Instant::now())
    }

    fn on_timeout_with_now(&mut self, now: Instant) -> Result<(), error::Error> {
        let timeout = self.config.query_timeout;

        if let Some(timed_out_txs) = self.tx_manager.timed_out_txs(timeout, now) {
            for tx in timed_out_txs {
                self.routing_table.on_resp_timeout(&tx.remote_id);
                self.msg_buffer.push_inbound(InboundMsg {
                    remote_id: tx.remote_id,
                    tx_local_id: Some(tx.local_id),
                    msg: None,
                    is_timeout: true,
                });
            }
        }

        self.ping_routing_table_questionable_nodes(now)?;

        Ok(())
    }

    pub fn write_query<T>(
        &mut self,
        args: &T,
        remote_id: &RemoteNodeId,
    ) -> Result<transaction::LocalId, error::Error>
    where
        T: QueryArgs + std::fmt::Debug,
    {
        self.msg_buffer.write_query(
            args,
            remote_id,
            &mut self.tx_manager,
            &self.config.client_version,
        )
    }

    pub fn write_resp(
        &mut self,
        transaction_id: &ByteBuf,
        resp: Option<Value>,
        remote_id: &RemoteNodeId,
    ) -> Result<(), error::Error> {
        self.msg_buffer
            .write_resp(transaction_id, resp, remote_id, &self.config.client_version)
    }

    pub fn write_err(
        &mut self,
        transaction_id: &ByteBuf,
        details: Option<Value>,
        remote_id: &RemoteNodeId,
    ) -> Result<(), error::Error> {
        self.msg_buffer.write_err(
            transaction_id,
            details,
            remote_id,
            &self.config.client_version,
        )
    }

    pub fn send_to(&mut self, mut buf: &mut [u8]) -> Result<Option<SendInfo>, error::Error> {
        if let Some(out_msg) = self.msg_buffer.pop_outbound() {
            buf.write_all(&out_msg.msg_data)
                .map_err(|_| error::Error::CannotSerializeKrpcMessage)?;
            let result = Some(SendInfo {
                len: out_msg.msg_data.len(),
                addr: out_msg.addr,
            });
            if let Some(tx) = out_msg.into_transaction() {
                self.tx_manager.on_send_to(tx);
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

    pub fn bootstrap<'a>(&mut self, bootstrap_nodes: &'a [RemoteNodeId]) {
        let neighbors = self
            .find_neighbors(self.config.id, bootstrap_nodes, true, None)
            .iter()
            .map(|&n| n.clone())
            .collect::<Vec<RemoteNodeId>>();
        let mut find_node_op = FindNodeOp::new_with_id(self.config.id);
        for n in neighbors {
            if let Ok(tx_local_id) = self.write_query(
                &FindNodeQueryArgs::new_with_id_and_target(self.config.id, self.config.id),
                &n,
            ) {
                find_node_op.add_tx_local_id(tx_local_id);
                find_node_op.add_queried_node_id(n);
            }
        }
        self.find_node_ops.push(find_node_op);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryFrom;
    use std::net::{Ipv4Addr, SocketAddrV4};

    use crate::krpc::{
        find_node::METHOD_FIND_NODE,
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
                assert_eq!(msg_sent.tx_id(), Some(&tx_local_id.id().to_bytebuf()));

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
                    msg_sent.tx_id(),
                    Some(&find_node_op.tx_local_ids.first().unwrap().id().to_bytebuf())
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

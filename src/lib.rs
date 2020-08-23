// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Sloppy is a library which can help build an application using the [BitTorrent][bittorrent]
//! [Distributed Hash Table][bep_0005].
//!
//! [bittorrent]: http://bittorrent.org/
//! [bep_0005]: http://bittorrent.org/beps/bep_0005.html

// TODO: Have a deadline timeout for doing a find_node on the local node
// TODO: Configuration option for IPv6 and IPv4 tables
// TODO: Configuration for whether node IDs are valid for IP
// TODO: Should process the responses if the queried_node_id returned is the same as config.local_id
// http://bittorrent.org/beps/bep_0005.html
// http://bittorrent.org/beps/bep_0043.html
// http://bittorrent.org/beps/bep_0044.html
// http://bittorrent.org/beps/bep_0045.html
// http://bittorrent.org/beps/bep_0046.html
// http://bittorrent.org/beps/bep_0051.html

#[macro_use]
extern crate log;

pub mod addr;
pub mod error;
pub(crate) mod find_node_op;
pub mod krpc;
pub mod msg_buffer;
pub mod node;
pub(crate) mod routing;
pub mod transaction;

use crate::{
    find_node_op::FindNodeOp,
    krpc::{Kind, Msg, QueryArgs, QueryMsg, RespMsg},
    msg_buffer::InboundMsg,
    node::AddrId,
};
use bt_bencode::Value;
use serde_bytes::ByteBuf;
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
    pub local_id: node::Id,
    /// Client version identifier
    pub client_version: Option<ByteBuf>,
    /// The default amount of time before a query without a response is considered timed out
    pub default_query_timeout: Duration,
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
    pub fn with_config(config: Config, existing_addr_ids: &[AddrId]) -> Result<Self, error::Error> {
        let max_node_count_per_bucket = config.max_node_count_per_bucket;
        let local_id = config.local_id;
        let client_version = config.client_version.clone();
        let mut dht = Self {
            config,
            routing_table: routing::Table::new(
                local_id,
                max_node_count_per_bucket,
                existing_addr_ids,
            ),
            tx_manager: transaction::Manager::new(),
            msg_buffer: msg_buffer::Buffer::with_client_version(client_version),
            find_node_ops: Vec::new(),
        };
        dht.routing_table.find_node(
            dht.config.local_id,
            &dht.config,
            &mut dht.tx_manager,
            &mut dht.msg_buffer,
            &mut dht.find_node_ops,
            &existing_addr_ids,
            Instant::now(),
        )?;
        Ok(dht)
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
                        let queried_node_id = RespMsg::queried_node_id(&value);
                        // TODO: Process result but don't add to routing table if queried_node_id
                        // is equal to self.config.local_id
                        if queried_node_id.is_some()
                            && queried_node_id != Some(self.config.local_id)
                            && tx.is_node_id_match(queried_node_id)
                        {
                            let routing_table_addr_id = if tx.addr_id.id().is_none() {
                                queried_node_id.map(|queried_node_id| {
                                    AddrId::with_addr_and_id(tx.addr_id.addr(), queried_node_id)
                                })
                            } else {
                                None
                            };
                            self.routing_table.on_msg_received(
                                routing_table_addr_id.unwrap_or(tx.addr_id),
                                &kind,
                                &self.config,
                                &mut self.tx_manager,
                                &mut self.msg_buffer,
                                now,
                            )?;
                            debug!("Received response for tx_id={:?}", tx.tx_id);
                            for op in &mut self.find_node_ops {
                                op.handle(
                                    &tx,
                                    find_node_op::Response::Resp(&value),
                                    &self.config,
                                    &mut self.tx_manager,
                                    &mut self.msg_buffer,
                                )?;
                            }
                            self.find_node_ops.retain(|op| !op.is_done());
                            self.msg_buffer.push_inbound(InboundMsg {
                                addr_id: tx.addr_id,
                                tx_id: Some(tx.tx_id),
                                msg: msg_buffer::Msg::Resp(value),
                            });
                        } else {
                            error!(
                        "Message did not match expected queried node id. tx={:?}, addr={} kind={:?} tx={:?} queried_node_id={:?} query_method_name={:?} querying_node_id={:?} client_version={:?} value={:?}",
                        tx,
                        addr,
                        kind,
                        value.tx_id(),
                        value.queried_node_id(),
                        value.method_name_str(),
                        value.querying_node_id(),
                        value.client_version_str(),
                        value
                    );
                            self.tx_manager.push(tx);
                        }
                    }
                    Kind::Error => {
                        self.routing_table.on_msg_received(
                            tx.addr_id,
                            &kind,
                            &self.config,
                            &mut self.tx_manager,
                            &mut self.msg_buffer,
                            now,
                        )?;
                        debug!("Received error for tx_local_id={:?}", tx.tx_id);
                        for op in &mut self.find_node_ops {
                            op.handle(
                                &tx,
                                find_node_op::Response::Error(&value),
                                &self.config,
                                &mut self.tx_manager,
                                &mut self.msg_buffer,
                            )?;
                        }
                        self.find_node_ops.retain(|op| !op.is_done());
                        self.msg_buffer.push_inbound(InboundMsg {
                            addr_id: tx.addr_id,
                            tx_id: Some(tx.tx_id),
                            msg: msg_buffer::Msg::Error(value),
                        });
                    }
                    // unexpected
                    Kind::Query | Kind::Unknown(_) => {
                        error!(
                        "Message kind not expected. tx={:?}, addr={} kind={:?} tx={:?} queried_node_id={:?} query_method_name={:?} querying_node_id={:?} client_version={:?} value={:?}",
                        tx,
                        addr,
                        kind,
                        value.tx_id(),
                        value.queried_node_id(),
                        value.method_name_str(),
                        value.querying_node_id(),
                        value.client_version_str(),
                        value
                    );
                        self.tx_manager.push(tx);
                    }
                }
            } else {
                match kind {
                    Kind::Query => {
                        debug!("Recieved query. addr={}", addr);
                        let addr_id = QueryMsg::querying_node_id(&value).map(|id| AddrId::with_addr_and_id(addr,id)).unwrap_or_else(|| AddrId::with_addr(addr));
                        self.routing_table.on_msg_received(addr_id, &kind, &self.config, &mut
                            self.tx_manager, &mut self.msg_buffer, now)?;
                        self.msg_buffer.push_inbound(InboundMsg {
                            addr_id,
                            tx_id: None,
                            msg: msg_buffer::Msg::Query(value),
                        });
                    }
                    // unexpected
                    Kind::Response | Kind::Error | Kind::Unknown(_) => error!(
                        "Unexpected no local tx message. addr={} kind={:?} tx={:?} queried_node_id={:?} query_method_name={:?} querying_node_id={:?} client_version={:?} value={:?}",
                        addr,
                        kind,
                        value.tx_id(),
                        value.queried_node_id(),
                        value.method_name_str(),
                        value.querying_node_id(),
                        value.client_version_str(),
                        value
                    ),
                }
            }
        } else {
            error!("bad message!!!!! from {}", addr);
        }
        debug!("handled on_recv_with_now");
        Ok(())
    }

    pub fn read(&mut self) -> Option<InboundMsg> {
        self.msg_buffer.pop_inbound()
    }

    pub fn write_query<T>(
        &mut self,
        args: &T,
        addr_id: AddrId,
        timeout: Option<Duration>,
    ) -> Result<transaction::Id, error::Error>
    where
        T: QueryArgs + std::fmt::Debug,
    {
        self.msg_buffer.write_query(
            args,
            addr_id,
            timeout.unwrap_or(self.config.default_query_timeout),
            &mut self.tx_manager,
        )
    }

    pub fn write_resp(
        &mut self,
        transaction_id: &ByteBuf,
        resp: Option<Value>,
        addr_id: AddrId,
    ) -> Result<(), error::Error> {
        self.msg_buffer.write_resp(transaction_id, resp, addr_id)
    }

    pub fn write_err(
        &mut self,
        transaction_id: &ByteBuf,
        details: Option<Value>,
        addr_id: AddrId,
    ) -> Result<(), error::Error> {
        self.msg_buffer.write_err(transaction_id, details, addr_id)
    }

    pub fn send_to(&mut self, mut buf: &mut [u8]) -> Result<Option<SendInfo>, error::Error> {
        if let Some(out_msg) = self.msg_buffer.pop_outbound() {
            use std::io::Write;
            buf.write_all(&out_msg.msg_data)
                .map_err(|_| error::Error::CannotSerializeKrpcMessage)?;
            let result = Some(SendInfo {
                len: out_msg.msg_data.len(),
                addr: out_msg.addr_id.addr(),
            });
            if let Some(tx) = out_msg.into_transaction() {
                self.tx_manager.push(tx);
            }
            Ok(result)
        } else {
            Ok(None)
        }
    }

    pub fn timeout(&self) -> Option<Duration> {
        [self.tx_manager.timeout(), self.routing_table.timeout()]
            .iter()
            .filter_map(|&deadline| deadline)
            .min()
            .map(|min_deadline| {
                let now = Instant::now();
                if now > min_deadline {
                    Duration::from_secs(0)
                } else {
                    min_deadline - now
                }
            })
    }

    pub fn on_timeout(&mut self) -> Result<(), error::Error> {
        self.on_timeout_with_now(Instant::now())
    }

    fn on_timeout_with_now(&mut self, now: Instant) -> Result<(), error::Error> {
        debug!("on_timeout_with_now now={:?}", now);
        if let Some(timed_out_txs) = self.tx_manager.timed_out_txs(now) {
            for tx in timed_out_txs {
                debug!("tx timed out: {:?}", tx);
                self.routing_table.on_resp_timeout(
                    tx.addr_id,
                    &self.config,
                    &mut self.tx_manager,
                    &mut self.msg_buffer,
                    now,
                )?;
                for op in &mut self.find_node_ops {
                    op.handle(
                        &tx,
                        find_node_op::Response::Timeout,
                        &self.config,
                        &mut self.tx_manager,
                        &mut self.msg_buffer,
                    )?;
                }
                self.find_node_ops.retain(|op| !op.is_done());
                self.msg_buffer.push_inbound(InboundMsg {
                    addr_id: tx.addr_id,
                    tx_id: Some(tx.tx_id),
                    msg: msg_buffer::Msg::Timeout,
                });
            }
        }

        self.routing_table.on_timeout(
            &self.config,
            &mut self.tx_manager,
            &mut self.msg_buffer,
            &mut self.find_node_ops,
            now,
        )?;

        debug!("remaining tx after timeout: {}", self.tx_manager.len());

        Ok(())
    }

    pub fn find_neighbors(&self, id: node::Id) -> impl Iterator<Item = &AddrId> {
        self.routing_table.find_neighbors(id, Instant::now())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use krpc::find_node::FindNodeQueryArgs;
    use std::convert::TryFrom;
    use std::net::{Ipv4Addr, SocketAddrV4};

    use crate::krpc::{
        find_node::METHOD_FIND_NODE,
        ping::{PingQueryArgs, METHOD_PING},
        Kind, Msg, QueryMsg,
    };

    fn new_config() -> Result<Config, error::Error> {
        Ok(Config {
            local_id: node::Id::rand()?,
            client_version: None,
            default_query_timeout: Duration::from_secs(60),
            is_read_only_node: true,
            max_node_count_per_bucket: 10,
        })
    }

    fn remote_addr() -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 6532))
    }

    fn node_id() -> node::Id {
        node::Id::rand().unwrap()
    }

    fn bootstrap_remote_addr() -> SocketAddr {
        use std::net::ToSocketAddrs;
        "127.0.0.1:6881".to_socket_addrs().unwrap().next().unwrap()
    }

    #[test]
    fn test_send_ping() -> Result<(), error::Error> {
        let id = node_id();
        let remote_addr = remote_addr();
        let addr_id = AddrId::with_addr_and_id(remote_addr, id);

        let args = PingQueryArgs::with_id(id);

        let mut dht: Dht = Dht::with_config(new_config()?, &[])?;
        let tx_id = dht.write_query(&args, addr_id, None).unwrap();

        let mut out: [u8; 65535] = [0; 65535];
        match dht.send_to(&mut out)? {
            Some(send_info) => {
                assert_eq!(send_info.addr, remote_addr);

                let filled_buf = &out[..send_info.len];
                let msg_sent: Value = bt_bencode::from_slice(filled_buf)
                    .map_err(|_| error::Error::CannotDeserializeKrpcMessage)?;
                assert_eq!(msg_sent.kind(), Some(Kind::Query));
                assert_eq!(msg_sent.method_name_str(), Some(METHOD_PING));
                assert_eq!(msg_sent.tx_id(), Some(&tx_id.to_bytebuf()));

                Ok(())
            }
            None => panic!(),
        }
    }

    #[test]
    fn test_bootstrap() -> Result<(), error::Error> {
        let bootstrap_remote_addr = bootstrap_remote_addr();
        let mut dht: Dht =
            Dht::with_config(new_config()?, &[AddrId::with_addr(bootstrap_remote_addr)])?;

        let mut out: [u8; 65535] = [0; 65535];
        match dht.send_to(&mut out)? {
            Some(send_info) => {
                assert_eq!(send_info.addr, bootstrap_remote_addr);

                let filled_buf = &out[..send_info.len];
                let msg_sent: Value = bt_bencode::from_slice(filled_buf)
                    .map_err(|_| error::Error::CannotDeserializeKrpcMessage)?;
                assert_eq!(msg_sent.kind(), Some(Kind::Query));
                assert_eq!(msg_sent.method_name_str(), Some(METHOD_FIND_NODE));
                let find_node_query_args =
                    FindNodeQueryArgs::try_from(msg_sent.args().unwrap()).unwrap();
                assert_eq!(find_node_query_args.target(), &dht.config.local_id);
                assert_eq!(find_node_query_args.id(), &dht.config.local_id);

                Ok(())
            }
            None => panic!(),
        }
    }
}

pub use node::Id as NodeId;

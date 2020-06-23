// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! BtDht is a library which can help build an application using the [BitTorrent][bittorent]
//! [Distributed Hash Table][bep_0005].
//!
//! [bittorrent]: http://bittorrent.org/
//! [bep_0005]: http://bittorrent.org/beps/bep_0005.html

pub(crate) mod addr;
pub mod config;
pub mod error;
pub mod krpc;
pub mod node;
pub(crate) mod routing;
pub(crate) mod transaction;

use bt_bencode::Value;
use serde_bytes::ByteBuf;
use std::collections::VecDeque;
use std::io::Write;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct Dht {
    config: config::Config,
    ipv4_routing_table: routing::Table,
    ipv6_routing_table: routing::Table,
    transactions: VecDeque<transaction::Transaction>,
    next_transaction_id: transaction::Id,

    inbound_msgs: VecDeque<InboundMsg>,
    outbound_msgs: VecDeque<OutboundMsg>,
}

#[derive(Clone, Debug, PartialEq)]
struct OutboundMsg {
    transaction_id: ByteBuf,
    remote_id: node::remote::RemoteNodeId,
    msg: krpc::Msg,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InboundMsg {
    pub remote_id: Option<node::remote::RemoteNodeId>,
    pub addr: SocketAddr,
    pub msg: Value,
}

impl Dht {
    pub fn new(config: config::Config) -> Self {
        let max_node_count_per_bucket = config.max_node_count_per_bucket;
        let ipv4_id = config.ipv4_id;
        let ipv6_id = config.ipv6_id;
        Self {
            config,
            ipv4_routing_table: routing::Table::new(ipv4_id, max_node_count_per_bucket),
            ipv6_routing_table: routing::Table::new(ipv6_id, max_node_count_per_bucket),
            transactions: VecDeque::new(),
            next_transaction_id: transaction::Id(0),

            outbound_msgs: VecDeque::new(),
            inbound_msgs: VecDeque::new(),
        }
    }

    pub fn on_recv(&mut self, bytes: &[u8], addr: SocketAddr) -> Result<(), error::Error> {
        use crate::krpc::{Kind, KrpcMsg};
        use crate::node::remote::RemoteNodeId;

        let value: Value = bt_bencode::from_slice(bytes)
            .map_err(|_| error::Error::CannotDeserializeKrpcMessage)?;
        if let Some(kind) = value.kind() {
            let table = match addr {
                SocketAddr::V4(_) => &mut self.ipv4_routing_table,
                SocketAddr::V6(_) => &mut self.ipv6_routing_table,
            };
            let transactions = &mut self.transactions;
            if let Some(transaction) = value
                .transaction_id()
                .and_then(|id| {
                    transactions
                        .iter()
                        .position(|t| t.id == id && t.resolved_addr == addr)
                })
                .and_then(|idx| transactions.swap_remove_back(idx))
            {
                match kind {
                    Kind::Response => {
                        use krpc::KrpcRespMsg;
                        let queried_node_id = KrpcRespMsg::queried_node_id(&value);
                        if transaction
                            .remote_id
                            .node_id
                            .and_then(|id| {
                                Some(queried_node_id.and_then(|q| Some(q == id)).unwrap_or(false))
                            })
                            .unwrap_or(true)
                        {
                            table.on_response_received(&transaction.remote_id);
                            self.inbound_msgs.push_back(InboundMsg {
                                remote_id: Some(transaction.remote_id),
                                addr,
                                msg: value,
                            });
                        }
                    }
                    Kind::Error => {
                        table.on_error_received(&transaction.remote_id);
                        self.inbound_msgs.push_back(InboundMsg {
                            remote_id: Some(transaction.remote_id),
                            addr,
                            msg: value,
                        });
                    }
                    // unexpected
                    Kind::Query | Kind::Unknown(_) => {}
                }
            } else {
                match kind {
                    Kind::Query => {
                        use krpc::KrpcQueryMsg;
                        let querying_node_id = KrpcQueryMsg::querying_node_id(&value);
                        let remote_id = RemoteNodeId {
                            addr: node::remote::RemoteAddr::SocketAddr(addr),
                            node_id: querying_node_id,
                        };
                        table.on_query_received(&remote_id);
                        self.inbound_msgs.push_back(InboundMsg {
                            remote_id: Some(remote_id),
                            addr,
                            msg: value,
                        });
                    }
                    // unexpected
                    Kind::Response | Kind::Error | Kind::Unknown(_) => {}
                }
            }
        }
        Ok(())
    }

    pub fn on_recv_complete(&mut self) {}

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
        // Go through the transactions and declare timeouts
        // Go through the routing table and flip bits if necessary
    }

    #[inline]
    fn next_transaction_id(&mut self) -> transaction::Id {
        let transaction_id = self.next_transaction_id.0;
        let (next_id, _) = transaction_id.overflowing_add(1);
        self.next_transaction_id.0 = next_id;
        transaction::Id(transaction_id)
    }

    pub fn write_query(
        &mut self,
        method_name: ByteBuf,
        args: Option<Value>,
        remote_id: node::remote::RemoteNodeId,
    ) -> ByteBuf {
        let transaction_id = ByteBuf::from(self.next_transaction_id().0.to_be_bytes());
        self.outbound_msgs.push_back(OutboundMsg {
            transaction_id: transaction_id.clone(),
            remote_id,
            msg: krpc::Msg::Query(krpc::QueryMsg {
                a: args,
                q: method_name,
                t: transaction_id.clone(),
                v: Some(self.config.client_version.clone()),
            }),
        });
        transaction_id
    }

    pub fn write_resp(
        &mut self,
        transaction_id: ByteBuf,
        resp: Option<Value>,
        remote_id: node::remote::RemoteNodeId,
    ) {
        self.outbound_msgs.push_back(OutboundMsg {
            transaction_id: transaction_id.clone(),
            remote_id,
            msg: krpc::Msg::Response(krpc::RespMsg {
                r: resp,
                t: transaction_id.clone(),
                v: Some(self.config.client_version.clone()),
            }),
        });
    }

    pub fn write_err(
        &mut self,
        transaction_id: ByteBuf,
        details: Option<Value>,
        remote_id: node::remote::RemoteNodeId,
    ) {
        self.outbound_msgs.push_back(OutboundMsg {
            transaction_id: transaction_id.clone(),
            remote_id,
            msg: krpc::Msg::Error(krpc::ErrMsg {
                e: details,
                t: transaction_id.clone(),
                v: Some(self.config.client_version.clone()),
            }),
        });
    }

    pub fn send_to(
        &mut self,
        mut buf: &mut [u8],
    ) -> Result<Option<(usize, SocketAddr)>, error::Error> {
        if let Some(out_msg) = self.outbound_msgs.pop_front() {
            let addr = match &out_msg.remote_id.addr {
                node::remote::RemoteAddr::SocketAddr(s) => *s,
                node::remote::RemoteAddr::HostPort(s) => {
                    use std::net::ToSocketAddrs;
                    s.to_socket_addrs()
                        .map_err(|_| error::Error::CannotResolveSocketAddr)?
                        .next()
                        .ok_or(error::Error::CannotResolveSocketAddr)?
                }
            };
            let OutboundMsg {
                transaction_id,
                remote_id,
                msg,
            } = out_msg;
            let transaction = transaction::Transaction {
                id: transaction_id,
                remote_id,
                resolved_addr: addr,
                sent: Instant::now(),
            };
            self.transactions.push_back(transaction);
            match msg {
                krpc::Msg::Response(msg) => {
                    let bytes = bt_bencode::to_vec(&msg)
                        .map_err(|_| error::Error::CannotSerializeKrpcMessage)?;
                    buf.write_all(&bytes)
                        .map_err(|_| error::Error::CannotSerializeKrpcMessage)?;
                    return Ok(Some((bytes.len(), addr)));
                }
                krpc::Msg::Error(msg) => {
                    let bytes = bt_bencode::to_vec(&msg)
                        .map_err(|_| error::Error::CannotSerializeKrpcMessage)?;
                    buf.write_all(&bytes)
                        .map_err(|_| error::Error::CannotSerializeKrpcMessage)?;
                    return Ok(Some((bytes.len(), addr)));
                }
                krpc::Msg::Query(msg) => {
                    let bytes = bt_bencode::to_vec(&msg)
                        .map_err(|_| error::Error::CannotSerializeKrpcMessage)?;
                    buf.write_all(&bytes)
                        .map_err(|_| error::Error::CannotSerializeKrpcMessage)?;
                    return Ok(Some((bytes.len(), addr)));
                }
            }
        } else {
            Ok(None)
        }
    }
}

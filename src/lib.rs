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

pub(crate) mod addr;
pub mod error;
pub mod krpc;
pub mod node;
pub(crate) mod routing;
pub(crate) mod transaction;

use crate::{krpc::QueryArgs, node::Id};
use bt_bencode::Value;
use serde_bytes::ByteBuf;
use std::collections::VecDeque;
use std::io::Write;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, PartialEq)]
struct OutboundMsg {
    transaction_id: Option<ByteBuf>,
    remote_id: node::remote::RemoteNodeId,
    msg_data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SendInfo {
    pub len: usize,
    pub addr: SocketAddr,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InboundMsg {
    pub remote_id: Option<node::remote::RemoteNodeId>,
    pub addr: SocketAddr,
    pub msg: Value,
}

impl InboundMsg {
    pub fn return_remote_id(&self) -> node::remote::RemoteNodeId {
        self.remote_id
            .as_ref()
            .map(|r| r.clone())
            .unwrap_or_else(|| node::remote::RemoteNodeId {
                addr: node::remote::RemoteAddr::SocketAddr(self.addr),
                node_id: None,
            })
    }
}

/// The configuration for the local DHT node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    /// Local node id
    pub id: Id,
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
            transactions: VecDeque::new(),
            next_transaction_id: transaction::Id(0),
            inbound_msgs: VecDeque::new(),
            outbound_msgs: VecDeque::new(),
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn on_recv(&mut self, bytes: &[u8], addr: SocketAddr) -> Result<(), error::Error> {
        use crate::krpc::{Kind, Msg};
        use crate::node::remote::RemoteNodeId;

        let value: Value = bt_bencode::from_slice(bytes)
            .map_err(|_| error::Error::CannotDeserializeKrpcMessage)?;
        if let Some(kind) = value.kind() {
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
                            self.inbound_msgs.push_back(InboundMsg {
                                remote_id: Some(transaction.remote_id),
                                addr,
                                msg: value,
                            });
                        }
                    }
                    Kind::Error => {
                        self.routing_table.on_error_received(&transaction.remote_id);
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
                        use krpc::QueryMsg;
                        let querying_node_id = QueryMsg::querying_node_id(&value);
                        let remote_id = RemoteNodeId {
                            addr: node::remote::RemoteAddr::SocketAddr(addr),
                            node_id: querying_node_id,
                        };
                        self.routing_table.on_query_received(&remote_id);
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

    // pub fn on_recv_complete(&mut self) {}

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
        unimplemented!();
    }

    #[inline]
    fn next_transaction_id(&mut self) -> transaction::Id {
        let transaction_id = self.next_transaction_id.0;
        let (next_id, _) = transaction_id.overflowing_add(1);
        self.next_transaction_id.0 = next_id;
        transaction::Id(transaction_id)
    }

    pub fn write_query<T>(
        &mut self,
        args: &T,
        remote_id: node::remote::RemoteNodeId,
    ) -> Result<ByteBuf, error::Error>
    where
        T: QueryArgs,
    {
        let transaction_id = ByteBuf::from(self.next_transaction_id().0.to_be_bytes());
        self.outbound_msgs.push_back(OutboundMsg {
            remote_id,
            msg_data: bt_bencode::to_vec(&krpc::ser::QueryMsg {
                a: Some(&args.as_value()),
                q: &ByteBuf::from(T::method_name()),
                t: &transaction_id,
                v: self.config.client_version.as_ref(),
            })
            .map_err(|_| error::Error::CannotSerializeKrpcMessage)?,
            transaction_id: Some(transaction_id.clone()),
        });

        Ok(transaction_id)
    }

    pub fn write_resp(
        &mut self,
        transaction_id: &ByteBuf,
        resp: Option<Value>,
        remote_id: node::remote::RemoteNodeId,
    ) -> Result<(), error::Error> {
        self.outbound_msgs.push_back(OutboundMsg {
            transaction_id: None,
            remote_id,
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
        transaction_id: ByteBuf,
        details: Option<Value>,
        remote_id: node::remote::RemoteNodeId,
    ) -> Result<(), error::Error> {
        self.outbound_msgs.push_back(OutboundMsg {
            transaction_id: None,
            remote_id,
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
                msg_data,
            } = out_msg;
            if let Some(transaction_id) = transaction_id {
                let transaction = transaction::Transaction {
                    id: transaction_id,
                    remote_id,
                    resolved_addr: addr,
                    sent: Instant::now(),
                };
                self.transactions.push_back(transaction);
            }
            buf.write_all(&msg_data)
                .map_err(|_| error::Error::CannotSerializeKrpcMessage)?;
            Ok(Some(SendInfo {
                len: msg_data.len(),
                addr,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn find_neighbors<'a>(
        &'a self,
        id: node::Id,
        bootstrap_nodes: &'a [node::remote::RemoteNodeId],
        include_all_bootstrap_nodes: bool,
        want: Option<usize>,
    ) -> Vec<&'a node::remote::RemoteNodeId> {
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

    use crate::krpc::{
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

    fn remote_addr() -> SocketAddr {
        use std::net::{Ipv4Addr, SocketAddrV4};

        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 6532))
    }

    fn node_id() -> node::Id {
        node::Id::rand().unwrap()
    }

    #[test]
    fn test_send_ping() -> Result<(), error::Error> {
        let id = node_id();
        let remote_addr = remote_addr();
        let remote_id = node::remote::RemoteNodeId {
            addr: node::remote::RemoteAddr::SocketAddr(remote_addr),
            node_id: Some(id.clone()),
        };

        let args = PingQueryArgs::new_with_id(id);

        let mut dht: Dht = Dht::new_with_config(new_config()?);
        let tx_id = dht.write_query(&args, remote_id).unwrap();

        let mut out: [u8; 65535] = [0; 65535];
        match dht.send_to(&mut out)? {
            Some(send_info) => {
                assert_eq!(send_info.addr, remote_addr);

                let filled_buf = &out[..send_info.len];
                let msg_sent: Value = bt_bencode::from_slice(filled_buf)
                    .map_err(|_| error::Error::CannotDeserializeKrpcMessage)?;
                assert_eq!(msg_sent.kind(), Some(Kind::Query));
                assert_eq!(msg_sent.method_name_str(), Some(METHOD_PING));
                assert_eq!(msg_sent.transaction_id(), Some(&tx_id));

                Ok(())
            }
            None => panic!(),
        }
    }
}

// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Sloppy is a library which can help build an application using the [`BitTorrent`][bittorrent]
//! [Distributed Hash Table][bep_0005].
//!
//! # Features
//!
//! | BEP                  | Status |
//! | ---------------------|--------|
//! | [BEP 0005][bep_0005] | WIP    |
//! | [BEP 0032][bep_0032] | -      |
//! | [BEP 0033][bep_0033] | -      |
//! | [BEP 0043][bep_0043] | -      |
//! | [BEP 0044][bep_0044] | -      |
//! | [BEP 0045][bep_0045] | -      |
//! | [BEP 0046][bep_0046] | -      |
//! | [BEP 0051][bep_0051] | -      |
//!
//! [bittorrent]: http://bittorrent.org/
//! [bep_0005]: http://bittorrent.org/beps/bep_0005.html
//! [bep_0032]: http://bittorrent.org/beps/bep_0032.html
//! [bep_0033]: http://bittorrent.org/beps/bep_0033.html
//! [bep_0043]: http://bittorrent.org/beps/bep_0043.html
//! [bep_0044]: http://bittorrent.org/beps/bep_0044.html
//! [bep_0045]: http://bittorrent.org/beps/bep_0045.html
//! [bep_0046]: http://bittorrent.org/beps/bep_0046.html
//! [bep_0051]: http://bittorrent.org/beps/bep_0051.html

// TODO: Configuration for whether node IDs are valid for IP

#[macro_use]
extern crate log;

pub(crate) mod find_node_op;
pub mod krpc;
pub(crate) mod msg_buffer;
pub(crate) mod routing;

use crate::find_node_op::FindNodeOp;

use bt_bencode::Value;
use cloudburst::dht::{
    krpc::{
        transaction::{self, Transactions},
        Error, ErrorVal, QueryArgs, QueryMsg, RespMsg, RespVal, Ty,
    },
    node::{AddrId, AddrOptId, Id, LocalId},
};
use std::{
    net::{SocketAddr, SocketAddrV4, SocketAddrV6},
    time::{Duration, Instant},
};

/// Events related to KRPC messages including responses, errors, queries, and timeouts.
#[derive(Clone, Debug, PartialEq)]
pub enum MsgEvent {
    Resp(Value),
    Error(Value),
    Query(Value),
    Timeout,
}

/// A deserialized message event with the relevant node information and local
/// transaction identifier.
#[derive(Clone, Debug)]
pub struct ReadEvent {
    addr_opt_id: AddrOptId<SocketAddr>,
    tx_id: Option<transaction::Id>,
    msg: MsgEvent,
}

impl ReadEvent {
    /// Returns the relevant node's network address and optional Id.
    #[must_use]
    pub fn addr_opt_id(&self) -> AddrOptId<SocketAddr> {
        self.addr_opt_id
    }

    /// Returns the relevant local transaction Id if the event is related to a query sent by the local node.
    #[must_use]
    pub fn tx_id(&self) -> Option<transaction::Id> {
        self.tx_id
    }

    /// Returns the message event which may contain a query, response, error, or timeout.
    #[must_use]
    pub fn msg(&self) -> &MsgEvent {
        &self.msg
    }
}

/// Information about the data to send.
#[derive(Clone, Copy, Debug)]
pub struct SendInfo {
    /// The length of the buffer filled with bytes to send.
    pub len: usize,
    /// The socket address to send the data to.
    pub addr: SocketAddr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// The types of addresses supported.
pub enum SupportedAddr {
    Ipv4,
    Ipv6,
    Ipv4AndIpv6,
}

/// The configuration for the local DHT node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    /// Local node id
    local_id: LocalId,
    /// Client version identifier
    client_version: Option<Vec<u8>>,
    /// The default amount of time before a query without a response is considered timed out
    default_query_timeout: Duration,
    /// If the node is read only
    is_read_only_node: bool,
    /// If responses from queried nodes are strictly checked for expected node ID
    is_response_queried_node_id_strictly_checked: bool,
    /// The types of socket addresses supported.
    supported_addr: SupportedAddr,
}

impl Config {
    /// Instantiate a new config with a node's local Id.
    pub fn new<I>(id: I) -> Self
    where
        I: Into<LocalId>,
    {
        Self {
            local_id: id.into(),
            client_version: None,
            default_query_timeout: Duration::from_secs(60),
            is_read_only_node: false,
            is_response_queried_node_id_strictly_checked: true,
            supported_addr: SupportedAddr::Ipv4AndIpv6,
        }
    }

    /// Returns the node's local Id.
    #[must_use]
    pub fn local_id(&self) -> LocalId {
        self.local_id
    }

    /// Sets the node's local Id.
    pub fn set_local_id<I>(&mut self, id: I)
    where
        I: Into<LocalId>,
    {
        self.local_id = id.into();
    }

    /// Returns the client version.
    #[must_use]
    pub fn client_version(&self) -> Option<&[u8]> {
        self.client_version.as_deref()
    }

    /// Sets the client version.
    pub fn set_client_version<I>(&mut self, client_version: I)
    where
        I: Into<Option<Vec<u8>>>,
    {
        self.client_version = client_version.into();
    }

    /// Returns the default query timeout.
    #[must_use]
    pub fn default_query_timeout(&self) -> Duration {
        self.default_query_timeout
    }

    /// Sets the default query timeout.
    pub fn set_default_query_timeout(&mut self, default_query_timeout: Duration) {
        self.default_query_timeout = default_query_timeout;
    }

    /// Returns true if the node is read only, false otherwise.
    #[must_use]
    pub fn is_read_only_node(&self) -> bool {
        self.is_read_only_node
    }

    /// Set to true if the node is read only, false otherwise.
    pub fn set_is_read_only_node(&mut self, is_read_only_node: bool) {
        self.is_read_only_node = is_read_only_node;
    }

    /// Returns true if responses from queried nodes are strictly checked for the expected node Id, false otherwise.
    #[must_use]
    pub fn is_response_queried_node_id_strictly_checked(&self) -> bool {
        self.is_response_queried_node_id_strictly_checked
    }

    /// Set to true if the responses from queried nodes are strictly checked for the expected node Id.
    pub fn set_is_response_queried_node_id_strictly_checked(
        &mut self,
        is_response_queried_node_id_strictly_checked: bool,
    ) {
        self.is_response_queried_node_id_strictly_checked =
            is_response_queried_node_id_strictly_checked;
    }

    /// Returns the supported address types.
    #[must_use]
    pub fn supported_addr(&self) -> SupportedAddr {
        self.supported_addr
    }

    /// Sets the supported address types.
    pub fn set_supported_addr(&mut self, supported_addr: SupportedAddr) {
        self.supported_addr = supported_addr;
    }
}

/// The distributed hash table.
#[derive(Debug)]
pub struct Node {
    config: Config,
    routing_table: routing::RoutingTable<transaction::Id, std::time::Instant>,
    tx_manager: Transactions<transaction::Id, std::net::SocketAddr, std::time::Instant>,
    msg_buffer: msg_buffer::Buffer<transaction::Id>,

    find_node_ops: Vec<FindNodeOp>,
}

impl Node {
    /// Instantiates a new node.
    pub fn new<'a, A, B, R>(
        config: Config,
        addr_ids: A,
        bootstrap_socket_addrs: B,
        rng: &mut R,
    ) -> Result<Self, Error>
    where
        A: IntoIterator<Item = &'a AddrId<SocketAddr>>,
        B: IntoIterator<Item = SocketAddr>,
        R: rand::Rng,
    {
        let local_id = Id::from(config.local_id);
        let now = Instant::now();

        let mut routing_table = match config.supported_addr {
            SupportedAddr::Ipv4 => routing::RoutingTable::Ipv4(routing::Table::new(local_id, now)),
            SupportedAddr::Ipv6 => routing::RoutingTable::Ipv6(routing::Table::new(local_id, now)),
            SupportedAddr::Ipv4AndIpv6 => routing::RoutingTable::Ipv4AndIpv6(
                routing::Table::new(local_id, now),
                routing::Table::new(local_id, now),
            ),
        };
        routing_table.try_insert_addr_ids(addr_ids, now);

        let mut dht = Self {
            config,
            routing_table,
            tx_manager: Transactions::default(),
            msg_buffer: msg_buffer::Buffer::new(),
            find_node_ops: Vec::new(),
        };
        dht.routing_table.find_node(
            Id::from(dht.config.local_id),
            &dht.config,
            &mut dht.tx_manager,
            &mut dht.msg_buffer,
            &mut dht.find_node_ops,
            bootstrap_socket_addrs,
            rng,
            now,
        )?;
        Ok(dht)
    }

    /// Returns the config.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn on_recv<R>(&mut self, bytes: &[u8], addr: SocketAddr, rng: &mut R) -> Result<(), Error>
    where
        R: rand::Rng,
    {
        self.on_recv_with_now(bytes, addr, rng, Instant::now())
    }

    fn on_recv_with_now<R>(
        &mut self,
        bytes: &[u8],
        addr: SocketAddr,
        rng: &mut R,
        now: Instant,
    ) -> Result<(), Error>
    where
        R: rand::Rng,
    {
        use cloudburst::dht::krpc::Msg as KrpcMsg;
        use core::convert::TryFrom;

        debug!("on_recv_with_now addr={}", addr);
        let value: Value = bt_bencode::from_slice(bytes)?;
        if let Some(kind) = value.ty() {
            if let Some(tx) = value
                .tx_id()
                .and_then(|tx_id| transaction::Id::try_from(tx_id).ok())
                .and_then(|tx_id| self.tx_manager.remove(&tx_id, &addr))
            {
                match kind {
                    Ty::Response => {
                        let queried_node_id = RespMsg::queried_node_id(&value);
                        let is_response_queried_id_valid =
                            tx.addr_opt_id.id().map_or(true, |expected_node_id| {
                                queried_node_id == Some(expected_node_id)
                            });
                        if is_response_queried_id_valid
                            || (!self.config.is_response_queried_node_id_strictly_checked
                                && queried_node_id == Some(Id::from(self.config.local_id)))
                        {
                            if is_response_queried_id_valid {
                                if let Some(node_id) = tx.addr_opt_id.id().or(queried_node_id) {
                                    self.routing_table.on_msg_received(
                                        AddrId::new(addr, node_id),
                                        &kind,
                                        &self.config,
                                        &mut self.tx_manager,
                                        &mut self.msg_buffer,
                                        rng,
                                        now,
                                    )?;
                                }
                            }
                            for op in &mut self.find_node_ops {
                                op.handle(
                                    &tx,
                                    find_node_op::Response::Resp(&value),
                                    &self.config,
                                    &mut self.tx_manager,
                                    &mut self.msg_buffer,
                                    rng,
                                )?;
                            }
                            self.find_node_ops.retain(|op| !op.is_done());
                            self.msg_buffer.push_inbound(ReadEvent {
                                addr_opt_id: tx.addr_opt_id,
                                tx_id: Some(tx.tx_id),
                                msg: MsgEvent::Resp(value),
                            });
                        } else {
                            self.tx_manager.insert(tx);
                        }
                    }
                    Ty::Error => {
                        if let Some(node_id) = tx.addr_opt_id.id() {
                            self.routing_table.on_msg_received(
                                AddrId::new(*tx.addr_opt_id.addr(), node_id),
                                &kind,
                                &self.config,
                                &mut self.tx_manager,
                                &mut self.msg_buffer,
                                rng,
                                now,
                            )?;
                        }
                        debug!("Received error for tx_local_id={:?}", tx.tx_id);
                        for op in &mut self.find_node_ops {
                            op.handle(
                                &tx,
                                find_node_op::Response::Error(&value),
                                &self.config,
                                &mut self.tx_manager,
                                &mut self.msg_buffer,
                                rng,
                            )?;
                        }
                        self.find_node_ops.retain(|op| !op.is_done());
                        self.msg_buffer.push_inbound(ReadEvent {
                            addr_opt_id: tx.addr_opt_id,
                            tx_id: Some(tx.tx_id),
                            msg: MsgEvent::Error(value),
                        });
                    }
                    // unexpected
                    Ty::Query | Ty::Unknown(_) => {
                        self.tx_manager.insert(tx);
                    }
                    _ => {
                        todo!()
                    }
                }
            } else {
                match kind {
                    Ty::Query => {
                        debug!("Recieved query. addr={}", addr);
                        let querying_node_id = QueryMsg::querying_node_id(&value);
                        let addr_opt_id = AddrOptId::new(addr, querying_node_id);
                        if let Some(node_id) = querying_node_id {
                            self.routing_table.on_msg_received(AddrId::new(addr, node_id), &kind, &self.config, &mut
                            self.tx_manager, &mut self.msg_buffer, rng, now)?;
                        }

                        self.msg_buffer.push_inbound(ReadEvent {
                            addr_opt_id,
                            tx_id: None,
                            msg: MsgEvent::Query(value),
                        });
                    }
                    // unexpected
                    Ty::Response | Ty::Error | Ty::Unknown(_) => error!(
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
                    _ => {
                        todo!()
                    }
                }
            }
        } else {
            error!("bad message!!!!! from {}", addr);
        }
        debug!("handled on_recv_with_now");
        Ok(())
    }

    pub fn read(&mut self) -> Option<ReadEvent> {
        self.msg_buffer.pop_inbound()
    }

    pub fn write_query<A, T>(
        &mut self,
        tx_id: transaction::Id,
        args: &T,
        addr_opt_id: A,
        timeout: Option<Duration>,
    ) -> Result<(), Error>
    where
        T: QueryArgs,
        A: Into<AddrOptId<SocketAddr>>,
    {
        self.msg_buffer.write_query(
            tx_id,
            args,
            addr_opt_id,
            timeout.unwrap_or(self.config.default_query_timeout),
            self.config.client_version(),
        )
    }

    pub fn write_resp<A, T>(
        &mut self,
        transaction_id: &[u8],
        resp: Option<T>,
        addr_opt_id: A,
    ) -> Result<(), Error>
    where
        T: RespVal,
        A: Into<AddrOptId<SocketAddr>>,
    {
        self.msg_buffer.write_resp(
            transaction_id,
            resp,
            addr_opt_id,
            self.config.client_version(),
        )
    }

    pub fn write_err<A, T>(
        &mut self,
        transaction_id: &[u8],
        details: &T,
        addr_opt_id: A,
    ) -> Result<(), Error>
    where
        T: ErrorVal,
        A: Into<AddrOptId<SocketAddr>>,
    {
        self.msg_buffer.write_err(
            transaction_id,
            details,
            addr_opt_id,
            self.config.client_version(),
        )
    }

    pub fn send_to(&mut self, mut buf: &mut [u8]) -> Result<Option<SendInfo>, std::io::Error> {
        if let Some(out_msg) = self.msg_buffer.pop_outbound() {
            use std::io::Write;
            buf.write_all(&out_msg.msg_data)?;
            let result = Some(SendInfo {
                len: out_msg.msg_data.len(),
                addr: *out_msg.addr_opt_id.addr(),
            });
            if let Some(tx) = out_msg.into_transaction() {
                self.tx_manager.insert(tx);
            }
            Ok(result)
        } else {
            Ok(None)
        }
    }

    #[must_use]
    pub fn timeout(&self) -> Option<Duration> {
        [self.tx_manager.min_timeout(), self.routing_table.timeout()]
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

    pub fn on_timeout<R>(&mut self, rng: &mut R) -> Result<(), Error>
    where
        R: rand::Rng,
    {
        self.on_timeout_with_now(rng, Instant::now())
    }

    fn on_timeout_with_now<R>(&mut self, rng: &mut R, now: Instant) -> Result<(), Error>
    where
        R: rand::Rng,
    {
        debug!("on_timeout_with_now now={:?}", now);
        if let Some(timed_out_txs) = self.tx_manager.timed_out_txs(&now) {
            for tx in timed_out_txs {
                if let Some(node_id) = tx.addr_opt_id.id() {
                    self.routing_table.on_resp_timeout(
                        AddrId::new(*tx.addr_opt_id.addr(), node_id),
                        &self.config,
                        &mut self.tx_manager,
                        &mut self.msg_buffer,
                        rng,
                        now,
                    )?;
                }

                for op in &mut self.find_node_ops {
                    op.handle(
                        &tx,
                        find_node_op::Response::Timeout,
                        &self.config,
                        &mut self.tx_manager,
                        &mut self.msg_buffer,
                        rng,
                    )?;
                }
                self.find_node_ops.retain(|op| !op.is_done());
                self.msg_buffer.push_inbound(ReadEvent {
                    addr_opt_id: tx.addr_opt_id,
                    tx_id: Some(tx.tx_id),
                    msg: MsgEvent::Timeout,
                });
            }
        }

        self.routing_table.on_timeout(
            &self.config,
            &mut self.tx_manager,
            &mut self.msg_buffer,
            &mut self.find_node_ops,
            rng,
            now,
        )?;

        debug!("remaining tx after timeout: {}", self.tx_manager.len());

        Ok(())
    }

    pub fn find_neighbors_ipv4(&self, id: Id) -> impl Iterator<Item = AddrId<SocketAddrV4>> {
        self.routing_table.find_neighbors_ipv4(id)
    }

    pub fn find_neighbors_ipv6(&self, id: Id) -> impl Iterator<Item = AddrId<SocketAddrV6>> {
        self.routing_table.find_neighbors_ipv6(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::{TryFrom, TryInto};
    use std::net::{Ipv4Addr, SocketAddrV4};

    use cloudburst::dht::krpc::{
        find_node::METHOD_FIND_NODE, ping::METHOD_PING, Msg, QueryArgs, QueryMsg, Ty,
    };

    fn new_config() -> Result<Config, rand::Error> {
        Ok(Config {
            local_id: LocalId::from(Id::rand(&mut rand::thread_rng())?),
            client_version: None,
            default_query_timeout: Duration::from_secs(60),
            is_read_only_node: true,
            is_response_queried_node_id_strictly_checked: true,
            supported_addr: SupportedAddr::Ipv4AndIpv6,
        })
    }

    fn remote_addr() -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 6532))
    }

    fn node_id() -> Id {
        Id::rand(&mut rand::thread_rng()).unwrap()
    }

    fn bootstrap_remote_addr() -> SocketAddr {
        use std::net::ToSocketAddrs;
        "127.0.0.1:6881".to_socket_addrs().unwrap().next().unwrap()
    }

    #[test]
    fn test_send_ping() -> Result<(), Error> {
        let local_id = LocalId::from(node_id());
        let id = node_id();
        let remote_addr = remote_addr();
        let addr_opt_id = AddrOptId::new(remote_addr, Some(id));

        let args = cloudburst::dht::krpc::ping::QueryArgs::new(local_id);

        let mut node: Node = Node::new(
            new_config().unwrap(),
            std::iter::empty(),
            std::iter::empty(),
            &mut rand::thread_rng(),
        )?;
        let tx_id = transaction::Id::rand(&mut rand::thread_rng()).unwrap();
        node.write_query(tx_id, &args, addr_opt_id, None).unwrap();

        let mut out: [u8; 65535] = [0; 65535];
        match node.send_to(&mut out).unwrap() {
            Some(send_info) => {
                assert_eq!(send_info.addr, remote_addr);

                let filled_buf = &out[..send_info.len];
                let msg_sent: Value = bt_bencode::from_slice(filled_buf)?;
                assert_eq!(msg_sent.ty(), Some(Ty::Query));
                assert_eq!(
                    msg_sent.method_name_str(),
                    Some(core::str::from_utf8(METHOD_PING).unwrap())
                );
                assert_eq!(
                    msg_sent.tx_id().and_then(|v| v.try_into().ok()),
                    Some(tx_id)
                );

                Ok(())
            }
            None => panic!(),
        }
    }

    #[test]
    fn test_bootstrap() -> Result<(), Error> {
        let bootstrap_remote_addr = bootstrap_remote_addr();
        let mut node: Node = Node::new(
            new_config().unwrap(),
            &[],
            vec![bootstrap_remote_addr],
            &mut rand::thread_rng(),
        )?;

        let mut out: [u8; 65535] = [0; 65535];
        match node.send_to(&mut out).unwrap() {
            Some(send_info) => {
                assert_eq!(send_info.addr, bootstrap_remote_addr);

                let filled_buf = &out[..send_info.len];
                let msg_sent: Value = bt_bencode::from_slice(filled_buf)?;
                assert_eq!(msg_sent.ty(), Some(Ty::Query));
                assert_eq!(
                    msg_sent.method_name_str(),
                    Some(core::str::from_utf8(METHOD_FIND_NODE).unwrap())
                );
                let find_node_query_args =
                    cloudburst::dht::krpc::find_node::QueryArgs::try_from(msg_sent.args().unwrap())
                        .unwrap();
                assert_eq!(
                    find_node_query_args.target(),
                    Id::from(node.config.local_id)
                );
                assert_eq!(find_node_query_args.id(), Id::from(node.config.local_id));

                Ok(())
            }
            None => panic!(),
        }
    }
}

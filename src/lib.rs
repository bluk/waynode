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

pub mod find_node_op;

use crate::find_node_op::FindNodeOp;

use bt_bencode::Value;
use cloudburst::dht::{
    krpc::{
        transaction::{self, Transaction, Transactions},
        Msg as KrpcMsg, QueryMsg, RespMsg, Ty,
    },
    node::{AddrId, AddrOptId, Id, LocalId},
    routing::Table,
};
use core::time::Duration;

use std::time::Instant;

const BUCKET_REFRESH_INTERVAL: Duration = Duration::from_secs(15 * 60);

/// Error for KRPC protocol.
#[derive(Debug)]
#[cfg_attr(feature = "std", derive(thiserror::Error))]
pub struct Error {
    #[cfg_attr(feature = "std", error(transparent))]
    kind: ErrorKind,
}

impl Error {
    #[must_use]
    pub fn is_bt_bencode_error(&self) -> bool {
        matches!(self.kind, ErrorKind::BtBencode(_))
    }

    fn unknown_msg_ty(value: Value) -> Self {
        Self {
            kind: ErrorKind::UnknownMsgTy(value),
        }
    }

    fn invalid_input(value: Value) -> Self {
        Self {
            kind: ErrorKind::InvalidInput(value),
        }
    }

    #[must_use]
    pub fn msg(&self) -> Option<&Value> {
        match &self.kind {
            ErrorKind::BtBencode(_) => None,
            ErrorKind::InvalidInput(value) | ErrorKind::UnknownMsgTy(value) => Some(value),
        }
    }
}

impl From<bt_bencode::Error> for Error {
    fn from(e: bt_bencode::Error) -> Self {
        Self {
            kind: ErrorKind::BtBencode(e),
        }
    }
}

#[cfg_attr(feature = "std", derive(thiserror::Error))]
#[derive(Debug)]
enum ErrorKind {
    BtBencode(
        #[cfg_attr(feature = "std", from)]
        #[cfg_attr(feature = "std", source)]
        #[cfg_attr(feature = "std", transparent)]
        bt_bencode::Error,
    ),
    UnknownMsgTy(Value),
    InvalidInput(Value),
}

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
pub struct ReadEvent<Addr> {
    addr_opt_id: AddrOptId<Addr>,
    tx_id: Option<transaction::Id>,
    msg: MsgEvent,
}

impl<Addr> ReadEvent<Addr> {
    /// Returns the relevant node's network address and optional Id.
    #[must_use]
    pub fn addr_opt_id(&self) -> &AddrOptId<Addr> {
        &self.addr_opt_id
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

const FIND_LOCAL_ID_INTERVAL: Duration = Duration::from_secs(15 * 60);

/// The distributed hash table.
#[derive(Debug)]
pub struct Node<Addr> {
    config: Config,
    routing_table: Table<Addr, transaction::Id, std::time::Instant>,
    find_pivot_id_deadline: Instant,
    tx_manager: Transactions<Addr, transaction::Id, std::time::Instant>,

    find_node_ops: Vec<FindNodeOp<Addr>>,
}

impl<Addr> Node<Addr>
where
    Addr: Clone + Copy + Ord,
{
    /// Instantiates a new node.
    pub fn new<A, B>(config: Config, addr_ids: A, bootstrap_socket_addrs: B) -> Self
    where
        Addr: Clone + Ord,
        A: IntoIterator<Item = AddrId<Addr>>,
        B: IntoIterator<Item = Addr>,
    {
        let local_id = Id::from(config.local_id);
        let now = Instant::now();

        let mut routing_table = Table::new(local_id, now);
        for addr_id in addr_ids {
            routing_table.try_insert(addr_id, now + BUCKET_REFRESH_INTERVAL, &now);
        }

        let mut dht = Self {
            config,
            routing_table,
            tx_manager: Transactions::default(),
            find_node_ops: Vec::new(),
            find_pivot_id_deadline: now + FIND_LOCAL_ID_INTERVAL,
        };
        dht.find_node_pivot(bootstrap_socket_addrs, now);
        dht
    }

    /// Returns the config.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Returns a transaction ID which can be used in the next query.
    ///
    /// The ID is not reserved until [`Self::insert_tx()`] is called.
    ///
    /// # Errors
    ///
    /// If a random number cannot be generated, an error will be returned.
    #[inline]
    pub fn next_tx_id<R>(&self, rng: &mut R) -> Result<transaction::Id, rand::Error>
    where
        R: rand::Rng,
    {
        // TODO: Fix this so it generates a random number a lot better
        loop {
            let tx_id = transaction::Id::rand(rng)?;
            if !self.tx_manager.contains_tx_id(&tx_id) {
                return Ok(tx_id);
            }
        }
    }

    /// Inserts [`Transaction`] information.
    ///
    /// When a query is sent, a `Transaction` should be inserted into the `Node`
    /// to track outstanding queries.
    ///
    /// When a message is received and processed via [`Node::on_recv()`], the
    /// message's transaction ID and inbound socket address is checked against
    /// existing `Transaction` data. If a matching `Transaction` exists, then
    /// the message is considered to be valid.
    pub fn insert_tx(&mut self, tx: Transaction<Addr, transaction::Id, Instant>) {
        self.tx_manager.insert(tx);
    }

    /// Processes a received message.
    ///
    /// When a message is received, use this callback method to process the data.
    /// If a message's transaction ID and inbound socket address matches
    /// existing `Transaction` data, then the message is considered valid.
    ///
    /// # Errors
    ///
    /// If the message is malformed, then an error is returned. If a response or
    /// error message does not have a matching `Transaction`, then the message is
    /// considered invalid and an error is returned.
    pub fn on_recv(&mut self, bytes: &[u8], addr: Addr) -> Result<ReadEvent<Addr>, Error>
    where
        Addr: Clone + PartialEq,
    {
        self.on_recv_with_now(bytes, addr, Instant::now())
    }

    fn on_recv_with_now(
        &mut self,
        bytes: &[u8],
        addr: Addr,
        now: Instant,
    ) -> Result<ReadEvent<Addr>, Error>
    where
        Addr: Clone + PartialEq,
    {
        let value: Value = bt_bencode::from_slice(bytes)?;

        if let Some(kind) = value.ty() {
            match kind {
                Ty::Response => {
                    if let Ok(tx) = self.tx_manager.on_recv_resp(
                        &addr,
                        &value,
                        self.config.is_response_queried_node_id_strictly_checked,
                        self.config.local_id,
                    ) {
                        if let Some(node_id) =
                            tx.addr_opt_id().id().or_else(|| value.queried_node_id())
                        {
                            self.routing_table.on_msg_received(
                                AddrId::new(addr, node_id),
                                &kind,
                                Some(tx.tx_id()),
                                now + BUCKET_REFRESH_INTERVAL,
                                &now,
                            );
                        }
                        for op in &mut self.find_node_ops {
                            op.handle(&tx, find_node_op::Response::Resp(&value));
                        }
                        self.find_node_ops.retain(|op| !op.is_done());

                        Ok(ReadEvent {
                            addr_opt_id: *tx.addr_opt_id(),
                            tx_id: None,
                            msg: MsgEvent::Resp(value),
                        })
                    } else {
                        Err(Error::invalid_input(value))
                    }
                }
                Ty::Error => {
                    if let Ok(tx) = self.tx_manager.on_recv_error(&addr, &value) {
                        if let Some(node_id) = tx.addr_opt_id().id() {
                            self.routing_table.on_msg_received(
                                AddrId::new(*tx.addr_opt_id().addr(), node_id),
                                &kind,
                                Some(tx.tx_id()),
                                now + BUCKET_REFRESH_INTERVAL,
                                &now,
                            );
                        }
                        debug!("Received error for tx_local_id={:?}", tx.tx_id());
                        for op in &mut self.find_node_ops {
                            op.handle(&tx, find_node_op::Response::Error(&value));
                        }
                        self.find_node_ops.retain(|op| !op.is_done());

                        Ok(ReadEvent {
                            addr_opt_id: *tx.addr_opt_id(),
                            tx_id: None,
                            msg: MsgEvent::Error(value),
                        })
                    } else {
                        Err(Error::invalid_input(value))
                    }
                }
                Ty::Query | Ty::Unknown(_) => {
                    let querying_node_id = QueryMsg::querying_node_id(&value);
                    let addr_opt_id = AddrOptId::new(addr, querying_node_id);
                    if let Some(node_id) = querying_node_id {
                        self.routing_table.on_msg_received(
                            AddrId::new(addr, node_id),
                            &kind,
                            None,
                            now + BUCKET_REFRESH_INTERVAL,
                            &now,
                        );
                    }

                    Ok(ReadEvent {
                        addr_opt_id,
                        tx_id: None,
                        msg: MsgEvent::Query(value),
                    })
                }
                _ => {
                    unreachable!()
                }
            }
        } else {
            Err(Error::unknown_msg_ty(value))
        }
    }

    /// Returns the next timeout deadline.
    ///
    /// When the timeout deadline has passed, the following methods should be called:
    ///
    /// * [`Node::on_timeout()`]
    /// * [`Node::find_timed_out_tx()`]
    /// * [`Node::find_node_to_ping_ipv4()`]
    /// * [`Node::find_node_to_ping_ipv6()`]
    ///
    /// The timeout deadline may change if other methods are called on this
    /// instance.
    #[must_use]
    pub fn timeout(&self) -> Option<Instant> {
        [self.tx_manager.min_timeout(), self.routing_table.timeout()]
            .iter()
            .filter_map(|&deadline| deadline)
            .min()
    }

    /// Processes timeout events.
    ///
    /// This method should be called after the deadline returned by [`Node::timeout()`].
    ///
    /// # Errors
    ///
    /// An error is returned in the random number generator cannot generate random data.
    pub fn on_timeout<R>(&mut self, rng: &mut R) -> Result<(), rand::Error>
    where
        R: rand::Rng,
    {
        self.on_timeout_with_now(rng, Instant::now())
    }

    fn on_timeout_with_now<R>(&mut self, rng: &mut R, now: Instant) -> Result<(), rand::Error>
    where
        R: rand::Rng,
    {
        debug!("on_timeout_with_now now={:?}", now);
        if self.find_pivot_id_deadline <= now {
            self.find_node_pivot(std::iter::empty(), now);
            self.find_pivot_id_deadline = now + FIND_LOCAL_ID_INTERVAL;
        }

        while let Some(bucket) = self.routing_table.find_refreshable_bucket(&now) {
            bucket.set_refresh_deadline(now + BUCKET_REFRESH_INTERVAL);
            let target_id = bucket.rand_id(rng)?;
            let neighbors = self
                .routing_table
                .find_neighbors(target_id, &now)
                .take(8)
                .map(|a| AddrOptId::new(*a.addr(), Some(a.id())));
            let op = make_find_node_op(target_id, neighbors, self.config.supported_addr);
            self.find_node_ops.push(op);
        }

        Ok(())
    }

    /// Finds and processes a transaction which has timed out.
    ///
    /// Returns information about the transaction which has timed out.
    pub fn find_timed_out_tx(&mut self, now: Instant) -> Option<ReadEvent<Addr>> {
        if let Some(tx) = self.tx_manager.pop_timed_out_tx(&now) {
            if let Some(node_id) = tx.addr_opt_id().id() {
                self.routing_table.on_resp_timeout(
                    AddrId::new(*tx.addr_opt_id().addr(), node_id),
                    tx.tx_id(),
                    now + BUCKET_REFRESH_INTERVAL,
                    &now,
                );
            }

            for op in &mut self.find_node_ops {
                op.handle(&tx, find_node_op::Response::Timeout);
            }

            self.find_node_ops.retain(|op| !op.is_done());
            Some(ReadEvent {
                addr_opt_id: *tx.addr_opt_id(),
                tx_id: Some(*tx.tx_id()),
                msg: MsgEvent::Timeout,
            })
        } else {
            None
        }
    }

    /// Finds a node to ping.
    ///
    /// # Important
    ///
    /// [`Node::on_ping()`] must be called to indicate the node was pinged.
    pub fn find_node_to_ping(
        &mut self,
        now: Instant,
    ) -> Option<&mut cloudburst::dht::routing::Node<Addr, transaction::Id, Instant>> {
        self.routing_table.find_node_to_ping(&now)
    }

    /// Finds the cloesst neighbors for a given `Id`.
    ///
    /// Usually a query is directed towards a target hash value. Nodes with
    /// `Id`s which are "closer" to the target value are more likely to have the
    /// data than other nodes.
    pub fn find_neighbors(&self, id: Id, now: &Instant) -> impl Iterator<Item = AddrId<Addr>>
    where
        Addr: Clone,
    {
        self.routing_table.find_neighbors(id, now)
    }

    fn find_node<I>(&mut self, target_id: Id, bootstrap_addrs: I, now: Instant) -> FindNodeOp<Addr>
    where
        Addr: Clone + Ord,
        I: IntoIterator<Item = Addr>,
    {
        let neighbors = self
            .routing_table
            .find_neighbors(target_id, &now)
            .take(8)
            .map(|a| AddrOptId::new(*a.addr(), Some(a.id())))
            .chain(bootstrap_addrs.into_iter().map(AddrOptId::with_addr));
        make_find_node_op(target_id, neighbors, self.config.supported_addr)
    }

    fn find_node_pivot<I>(&mut self, bootstrap_addrs: I, now: Instant)
    where
        Addr: Clone + Ord,
        I: IntoIterator<Item = Addr>,
    {
        let pivot = self.routing_table.pivot();
        let op = self.find_node(pivot, bootstrap_addrs, now);
        self.find_node_ops.push(op);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

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
    fn test_send_message() {
        let id = node_id();
        let remote_addr = remote_addr();
        let addr_opt_id = AddrOptId::new(remote_addr, Some(id));

        let config = new_config().unwrap();

        let mut node: Node<SocketAddr> = Node::new(config, std::iter::empty(), std::iter::empty());
        let tx_id = node.next_tx_id(&mut rand::thread_rng()).unwrap();
        node.insert_tx(Transaction::new(
            addr_opt_id,
            tx_id,
            Instant::now() + node.config().default_query_timeout,
        ));
    }

    #[test]
    fn test_bootstrap() -> Result<(), Error> {
        let bootstrap_remote_addr = bootstrap_remote_addr();
        let node: Node<SocketAddr> = Node::new(
            new_config().unwrap(),
            std::iter::empty(),
            vec![bootstrap_remote_addr],
        );
        todo!()

        // let mut out: [u8; 65535] = [0; 65535];
        // match node.send_to(&mut out).unwrap() {
        //     Some(send_info) => {
        //         assert_eq!(send_info.addr, bootstrap_remote_addr);

        //         let filled_buf = &out[..send_info.len];
        //         let msg_sent: Value = bt_bencode::from_slice(filled_buf)?;
        //         assert_eq!(msg_sent.ty(), Some(Ty::Query));
        //         assert_eq!(
        //             msg_sent.method_name_str(),
        //             Some(core::str::from_utf8(METHOD_FIND_NODE).unwrap())
        //         );
        //         let find_node_query_args =
        //             cloudburst::dht::krpc::find_node::QueryArgs::try_from(msg_sent.args().unwrap())
        //                 .unwrap();
        //         assert_eq!(
        //             find_node_query_args.target(),
        //             Id::from(node.config.local_id)
        //         );
        //         assert_eq!(find_node_query_args.id(), Id::from(node.config.local_id));

        //         Ok(())
        //     }
        //     None => panic!(),
        // }
    }
}

fn make_find_node_op<A, I>(
    target_id: Id,
    neighbors: I,
    supported_addr: SupportedAddr,
) -> FindNodeOp<A>
where
    A: Clone + Ord,
    I: IntoIterator<Item = AddrOptId<A>>,
{
    FindNodeOp::new(
        target_id,
        supported_addr,
        neighbors
            .into_iter()
            .map(|n| AddrOptId::new(n.addr().clone(), n.id())),
    )
}

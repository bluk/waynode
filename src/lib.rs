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

pub mod find_node_op;

use crate::find_node_op::FindNodeOp;

use bt_bencode::Value;
use cloudburst::dht::{
    krpc::{
        transaction::{self, Transaction, Transactions},
        CompactAddr, Msg as KrpcMsg, QueryMsg, RespMsg, Ty,
    },
    node::{self, AddrId, AddrOptId, Id, LocalId},
    routing::{Bucket, Table},
};
use core::{fmt, time::Duration};
use find_node_op::OpsManager;
use tracing::trace;

use std::{net::SocketAddr, time::Instant};

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

    fn missing_msg_ty(value: Value) -> Self {
        Self {
            kind: ErrorKind::MissingMsgTy(value),
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
            ErrorKind::InvalidInput(value) | ErrorKind::MissingMsgTy(value) => Some(value),
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
    MissingMsgTy(Value),
    InvalidInput(Value),
}

/// Events related to KRPC messages including responses, errors, queries, and timeouts.
#[derive(Clone, Debug, PartialEq)]
pub enum MsgEvent {
    Resp(Value),
    Error(Value),
    Query(Value),
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
    routing_table_next_response_interval: Duration,
    routing_table_next_query_interval: Duration,
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
            routing_table_next_response_interval: Duration::from_secs(15 * 60),
            routing_table_next_query_interval: Duration::from_secs(15 * 60),
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

const FIND_LOCAL_ID_INTERVAL: Duration = Duration::from_secs(3 * 60);
const MAX_BUCKET_SIZE: usize = 8;

use routing::MyTable;

/// The distributed hash table.
#[derive(Debug)]
pub struct Node<Addr> {
    config: Config,
    pub routing_table: Table<routing::Node<Addr, transaction::Id, std::time::Instant>, Instant>,
    find_pivot_deadline: Instant,
    tx_manager: Transactions<Addr, transaction::Id, std::time::Instant>,
    ops_manager: OpsManager,
    bootstrap_addrs: Vec<String>,
}

impl<Addr> Node<Addr>
where
    Addr: Clone + Copy + Ord,
{
    /// Instantiates a new node.
    pub fn new<A, B>(config: Config, addr_ids: A, bootstrap_addrs: B, now: Instant) -> Self
    where
        Addr: Clone + Ord + Into<CompactAddr>,
        A: IntoIterator<Item = AddrId<Addr>>,
        B: IntoIterator<Item = String>,
    {
        let pivot_id = Id::from(config.local_id);
        let routing_table = routing::new_routing_table(
            pivot_id,
            addr_ids,
            now + config.routing_table_next_response_interval,
            now + config.routing_table_next_query_interval,
            now,
        );
        let mut dht = Self {
            config,
            routing_table,
            tx_manager: Transactions::default(),
            find_pivot_deadline: now + FIND_LOCAL_ID_INTERVAL,
            ops_manager: OpsManager::default(),
            bootstrap_addrs: bootstrap_addrs.into_iter().collect(),
        };
        let op = dht.find_node_pivot(now);
        dht.ops_manager.insert_op(op);
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
        if self.tx_manager.len() == usize::from(u16::MAX) {
            // Outbound transactions are full.
            return Err(rand::Error::new("all transaction IDs are used"));
        }

        let mut num: u16 = rng.gen();
        loop {
            let tx_id = transaction::Id::from(num);
            if !self.tx_manager.contains(&tx_id) {
                return Ok(tx_id);
            }
            num = num.wrapping_add(1);
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

    pub fn insert_tx_for_find_node(&mut self, tx_id: transaction::Id, target_id: node::Id) {
        self.ops_manager.insert_tx(tx_id, target_id);
    }

    pub fn queried_node_for_target(
        &mut self,
        addr_opt_id: AddrOptId<CompactAddr>,
        target_id: node::Id,
    ) {
        self.ops_manager
            .queried_node_for_target(addr_opt_id, target_id);
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
        Addr: fmt::Debug + Clone + PartialEq + Into<CompactAddr>,
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
        Addr: fmt::Debug + Clone + PartialEq + Into<CompactAddr>,
    {
        let value: Value = bt_bencode::from_slice(bytes)?;

        if let Some(kind) = value.ty() {
            match kind {
                Ty::Response => {
                    if let Ok(tx) = self.tx_manager.on_recv_resp(
                        &value,
                        self.config.is_response_queried_node_id_strictly_checked,
                        self.config.local_id,
                    ) {
                        trace!(
                            tx_id = ?tx.tx_id(),
                            addr = ?addr,
                            node_id = ?tx.addr_opt_id().id(),
                            client_version_str = ?value.client_version_str(),
                            queried_node_id = ?value.queried_node_id(),
                            "received response"
                        );

                        if let Some(node_id) =
                            tx.addr_opt_id().id().or_else(|| value.queried_node_id())
                        {
                            routing::on_recv(
                                &mut self.routing_table,
                                AddrId::new(addr, node_id),
                                kind,
                                Some(tx.tx_id()),
                                now + routing::BUCKET_REFRESH_INTERVAL,
                                now + self.config.routing_table_next_response_interval,
                                now + self.config.routing_table_next_query_interval,
                                now,
                            );
                        }

                        let addr_opt_id = tx.addr_opt_id();
                        self.ops_manager.on_recv(
                            AddrOptId::new((*addr_opt_id.addr()).into(), addr_opt_id.id()),
                            *tx.tx_id(),
                            &value,
                        );

                        Ok(ReadEvent {
                            addr_opt_id: *tx.addr_opt_id(),
                            tx_id: None,
                            msg: MsgEvent::Resp(value),
                        })
                    } else {
                        trace!(
                            tx_id = ?value.tx_id(),
                            addr = ?addr,
                            client_version_str = ?value.client_version_str(),
                            queried_node_id = ?value.queried_node_id(),
                            "no matching transaction for response"
                        );
                        Err(Error::invalid_input(value))
                    }
                }
                Ty::Error => {
                    if let Ok(tx) = self.tx_manager.on_recv_error(&value) {
                        if let Some(node_id) = tx.addr_opt_id().id() {
                            routing::on_recv(
                                &mut self.routing_table,
                                AddrId::new(addr, node_id),
                                kind,
                                Some(tx.tx_id()),
                                now + routing::BUCKET_REFRESH_INTERVAL,
                                now + self.config.routing_table_next_response_interval,
                                now + self.config.routing_table_next_query_interval,
                                now,
                            );
                        }
                        self.ops_manager.on_error(
                            AddrOptId::new(
                                (*tx.addr_opt_id().addr()).into(),
                                tx.addr_opt_id().id(),
                            ),
                            *tx.tx_id(),
                        );

                        Ok(ReadEvent {
                            addr_opt_id: *tx.addr_opt_id(),
                            tx_id: None,
                            msg: MsgEvent::Error(value),
                        })
                    } else {
                        trace!(
                            tx_id = ?value.tx_id(),
                            addr = ?addr,
                            client_version_str = ?value.client_version_str(),
                            queried_node_id = ?value.queried_node_id(),
                            "no matching transaction for error"
                        );
                        Err(Error::invalid_input(value))
                    }
                }
                Ty::Query | Ty::Unknown(_) => {
                    let querying_node_id = QueryMsg::querying_node_id(&value);
                    let addr_opt_id = AddrOptId::new(addr, querying_node_id);
                    if let Some(node_id) = querying_node_id {
                        routing::on_recv(
                            &mut self.routing_table,
                            AddrId::new(addr, node_id),
                            kind,
                            None,
                            now + routing::BUCKET_REFRESH_INTERVAL,
                            now + self.config.routing_table_next_response_interval,
                            now + self.config.routing_table_next_query_interval,
                            now,
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
            trace!(
                tx_id = ?value.tx_id(),
                addr = ?addr,
                client_version_str = ?value.client_version_str(),
                queried_node_id = ?value.queried_node_id(),
                "no message type"
            );
            Err(Error::missing_msg_ty(value))
        }
    }

    /// Returns the next timeout deadline.
    ///
    /// When the timeout deadline has passed, the following methods should be called:
    ///
    /// * [`Node::on_timeout()`]
    /// * [`Node::pop_timed_out_tx()`]
    /// * [`Node::find_node_to_ping()`]
    /// * [`Node::find_bucket_to_refresh()`]
    ///
    /// The timeout deadline may change if other methods are called on this
    /// instance.
    #[must_use]
    pub fn timeout(&self) -> Option<Instant> {
        [self.tx_manager.timeout(), self.routing_table.timeout()]
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
    pub fn on_timeout<R>(&mut self, rng: &mut R)
    where
        Addr: Into<CompactAddr>,
        R: rand::Rng,
    {
        self.on_timeout_with_now(rng, Instant::now());
    }

    fn on_timeout_with_now<R>(&mut self, rng: &mut R, now: Instant)
    where
        Addr: Into<CompactAddr>,
        R: rand::Rng,
    {
        if self.find_pivot_deadline <= now {
            let op = self.find_node_pivot(now);
            self.ops_manager.insert_op(op);
        }

        self.ops_manager.cleanup();

        while let Some(bucket) = self.find_bucket_to_refresh(now) {
            bucket.set_refresh_deadline(now + Duration::from_secs(15 * 60));
            let target_id = bucket.rand_id(rng);
            let neighbors = self
                .find_neighbors(target_id, now)
                .take(8)
                .map(|a| AddrOptId::new((*a.addr()).into(), Some(a.id())));
            let find_node_op = FindNodeOp::new(target_id, 8, SupportedAddr::Ipv4, neighbors);
            self.ops_manager.insert_op(find_node_op);
        }
    }

    /// Finds a bucket to refresh.
    ///
    /// To refresh a bucket, find a random node with an `Id` in the bucket's range.
    ///
    /// ```no_run
    /// # use sloppy::{Node, SupportedAddr, find_node_op::FindNodeOp};
    /// # use std::{time::{Duration, Instant}, net::SocketAddr};
    /// # use cloudburst::dht::{krpc::CompactAddr, node::AddrOptId};
    /// #
    /// # let node: Node<SocketAddr> = todo!();
    /// # let now = Instant::now();
    /// if let Some(bucket) = node.find_bucket_to_refresh(now) {
    ///     bucket.set_refresh_deadline(now + Duration::from_secs(15 * 60));
    ///     let target_id = bucket.rand_id(&mut rand::thread_rng());
    ///     let neighbors = node.find_neighbors(target_id, now).take(8).map(|a| AddrOptId::new((*a.addr()).into(), Some(a.id())));
    ///     let find_node_op = FindNodeOp::new(
    ///          target_id,
    ///          8,
    ///          SupportedAddr::Ipv4,
    ///          neighbors
    ///    );
    /// }
    /// ```
    pub fn find_bucket_to_refresh(
        &mut self,
        now: Instant,
    ) -> Option<&mut Bucket<routing::Node<Addr, transaction::Id, Instant>, Instant>> {
        self.routing_table.find_bucket_to_refresh(&now)
    }

    /// Finds and processes a transaction which has timed out.
    ///
    /// Returns information about the transaction which has timed out.
    pub fn pop_timed_out_tx(
        &mut self,
        now: Instant,
    ) -> Option<Transaction<Addr, transaction::Id, Instant>>
    where
        Addr: Into<CompactAddr>,
    {
        if let Some(tx) = self.tx_manager.pop_timed_out_tx(&now) {
            if let Some(node_id) = tx.addr_opt_id().id() {
                routing::on_timeout(
                    &mut self.routing_table,
                    &AddrId::new(*tx.addr_opt_id().addr(), node_id),
                    *tx.tx_id(),
                );
            }

            self.ops_manager.on_tx_timeout(
                AddrOptId::new((*tx.addr_opt_id().addr()).into(), tx.addr_opt_id().id()),
                *tx.tx_id(),
            );

            Some(tx)
        } else {
            None
        }
    }

    /// Finds a node to query for a find node target.
    pub fn find_node_to_find_node(&mut self) -> Option<(node::Id, AddrOptId<CompactAddr>)> {
        self.ops_manager.next_addr_to_query()
    }

    /// Finds a node to ping.
    ///
    /// # Important
    ///
    /// [`Node::on_ping()`] must be called to indicate the node was pinged.
    pub fn find_node_to_ping(
        &mut self,
        now: Instant,
    ) -> Option<&mut routing::Node<Addr, transaction::Id, Instant>> {
        self.routing_table.find_node_to_ping(now)
    }

    /// Finds the cloesst neighbors for a given `Id`.
    ///
    /// Usually a query is directed towards a target hash value. Nodes with
    /// `Id`s which are "closer" to the target value are more likely to have the
    /// data than other nodes.
    pub fn find_neighbors(&self, id: Id, now: Instant) -> impl Iterator<Item = AddrId<Addr>>
    where
        Addr: Clone,
    {
        routing::find_neighbors(&self.routing_table, id, now)
    }

    #[must_use]
    fn find_node(&mut self, target_id: Id, now: Instant) -> FindNodeOp
    where
        Addr: Into<CompactAddr>,
    {
        let bootstrap_addrs = self
            .bootstrap_addrs
            .iter()
            .filter_map(|s| {
                use std::net::ToSocketAddrs;
                s.to_socket_addrs().ok()
            })
            .flatten()
            .filter_map(|socket_addr| match socket_addr {
                SocketAddr::V4(socket_addr) => Some(AddrOptId::with_addr(socket_addr.into())),
                SocketAddr::V6(_) => None,
            });
        FindNodeOp::new(
            target_id,
            8,
            self.config.supported_addr,
            routing::find_neighbors(&self.routing_table, target_id, now)
                .take(8)
                .map(|a| AddrOptId::new((*a.addr()).into(), Some(a.id())))
                .chain(bootstrap_addrs),
        )
    }

    #[must_use]
    #[inline]
    fn find_node_pivot(&mut self, now: Instant) -> FindNodeOp
    where
        Addr: Into<CompactAddr>,
    {
        self.find_node(self.routing_table.pivot(), now)
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
            routing_table_next_response_interval: Duration::from_secs(15 * 60),
            routing_table_next_query_interval: Duration::from_secs(15 * 60),
        })
    }

    fn remote_addr() -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 6532))
    }

    fn node_id() -> Id {
        Id::rand(&mut rand::thread_rng()).unwrap()
    }

    #[test]
    fn test_send_message() {
        let id = node_id();
        let remote_addr = remote_addr();
        let addr_opt_id = AddrOptId::new(remote_addr, Some(id));

        let config = new_config().unwrap();

        let mut node: Node<SocketAddr> = Node::new(
            config,
            std::iter::empty(),
            std::iter::empty(),
            Instant::now(),
        );
        let tx_id = node.next_tx_id(&mut rand::thread_rng()).unwrap();
        node.insert_tx(Transaction::new(
            addr_opt_id,
            tx_id,
            Instant::now() + node.config().default_query_timeout,
        ));
    }
}

mod routing {
    use std::time::{Duration, Instant};

    use cloudburst::dht::{
        krpc::{transaction, Ty},
        node::{AddrId, Id},
        routing::{Bucket, Table},
    };

    use crate::MAX_BUCKET_SIZE;

    pub(super) const BUCKET_REFRESH_INTERVAL: Duration = Duration::from_secs(3 * 60);

    pub(super) fn new_routing_table<A, Addr, TxId>(
        pivot_id: Id,
        addr_ids: A,
        next_response_deadline: Instant,
        next_query_deadline: Instant,
        now: Instant,
    ) -> Table<Node<Addr, TxId, Instant>, Instant>
    where
        A: IntoIterator<Item = AddrId<Addr>>,
    {
        let mut routing_table = Table::new(pivot_id, now + BUCKET_REFRESH_INTERVAL);
        let pivot_id = routing_table.pivot();
        for addr_id in addr_ids {
            let mut bucket = routing_table.find_mut(&addr_id.id());

            let mut bucket_len = bucket.len();
            if bucket.range().contains(&pivot_id) {
                while bucket_len == MAX_BUCKET_SIZE {
                    routing_table.split_last();
                    bucket = routing_table.find_mut(&addr_id.id());
                    bucket_len = bucket.len();

                    if !bucket.range().contains(&pivot_id) {
                        break;
                    }
                }
            }

            if bucket_len < MAX_BUCKET_SIZE {
                bucket.insert(Node::new(
                    addr_id,
                    next_response_deadline,
                    next_query_deadline,
                ));
                bucket.set_refresh_deadline(now + BUCKET_REFRESH_INTERVAL);
            }
        }

        routing_table
    }

    pub(super) fn on_recv<Addr>(
        table: &mut Table<Node<Addr, transaction::Id, Instant>, Instant>,
        addr_id: AddrId<Addr>,
        kind: Ty<'_>,
        tx_id: Option<&transaction::Id>,
        refresh_bucket_deadline: Instant,
        next_response_deadline: Instant,
        next_query_deadline: Instant,
        now: Instant,
    ) where
        Addr: PartialEq,
    {
        let pivot_id = table.pivot();
        let mut bucket = table.find_mut(&addr_id.id());
        if let Some(node) = bucket.iter_mut().find(|node| *node.addr_id() == addr_id) {
            node.on_msg_received(&kind, tx_id, next_response_deadline, next_query_deadline);
            return;
        }

        let mut bucket_len = bucket.len();
        if bucket.range().contains(&pivot_id) {
            while bucket_len == MAX_BUCKET_SIZE {
                table.split_last();
                bucket = table.find_mut(&addr_id.id());
                bucket_len = bucket.len();

                if !bucket.range().contains(&pivot_id) {
                    break;
                }
            }
        }

        if bucket_len < MAX_BUCKET_SIZE {
            bucket.insert(Node::new(
                addr_id,
                next_response_deadline,
                next_query_deadline,
            ));
            bucket.set_refresh_deadline(refresh_bucket_deadline);
            return;
        }
        assert_eq!(bucket_len, MAX_BUCKET_SIZE);

        bucket.retain(|node| match node.state_with_now(&now) {
            NodeState::Good | NodeState::Questionable => true,
            NodeState::Bad => false,
        });

        if bucket.len() < MAX_BUCKET_SIZE {
            bucket.insert(Node::new(
                addr_id,
                next_response_deadline,
                next_query_deadline,
            ));
            bucket.set_refresh_deadline(refresh_bucket_deadline);
        }
    }

    pub(super) fn on_timeout<Addr>(
        table: &mut Table<Node<Addr, transaction::Id, Instant>, Instant>,
        addr_id: &AddrId<Addr>,
        tx_id: transaction::Id,
    ) where
        Addr: PartialEq,
    {
        let node_id = addr_id.id();
        let bucket = table.find_mut(&node_id);

        if let Some(node) = bucket.iter_mut().find(|node| *node.addr_id() == *addr_id) {
            node.on_resp_timeout(&tx_id);
        }
    }

    #[derive(Debug, PartialEq)]
    enum NodeState {
        Good,
        Questionable,
        Bad,
    }

    /// Contains the address and [`Id`] for a node with metadata about the last response.
    ///
    /// Used to store a node's information for routing queries to. Contains
    /// "liveliness" information to determine if the `Node` is still likely valid.
    #[derive(Debug, Clone)]
    pub struct Node<Addr, TxId, Instant> {
        addr_id: AddrId<Addr>,
        karma: i8,
        next_response_deadline: Instant,
        next_query_deadline: Instant,
        ping_tx_id: Option<TxId>,
    }

    impl<Addr, TxId, Instant> cloudburst::dht::routing::Node for Node<Addr, TxId, Instant> {
        fn id(&self) -> cloudburst::dht::node::Id {
            self.addr_id.id()
        }
    }

    impl<A, TxId, Instant> Node<A, TxId, Instant>
    where
        Instant: cloudburst::time::Instant,
    {
        /// Instantiates a new Node.
        pub fn new(
            addr_id: AddrId<A>,
            next_response_deadline: Instant,
            next_query_deadline: Instant,
        ) -> Self {
            Self {
                addr_id,
                karma: 0,
                next_response_deadline,
                next_query_deadline,
                ping_tx_id: None,
            }
        }

        /// Returns the address.
        pub fn addr_id(&self) -> &AddrId<A> {
            &self.addr_id
        }

        /// Returns a ping's transaction Id, if the ping is still active.
        pub fn ping_tx_id(&self) -> Option<&TxId> {
            self.ping_tx_id.as_ref()
        }

        /// When pinged, sets the transaction Id to identify the response or time out later.
        pub fn on_ping(&mut self, tx_id: TxId) {
            self.ping_tx_id = Some(tx_id);
        }

        /// Returns the next response deadline.
        #[must_use]
        pub fn next_response_deadline(&self) -> &Instant {
            &self.next_response_deadline
        }

        /// Returns the next query deadline.
        #[must_use]
        pub fn next_query_deadline(&self) -> &Instant {
            &self.next_query_deadline
        }

        /// Returns the timeout deadline when the node should be pinged.
        #[must_use]
        pub fn timeout(&self) -> &Instant {
            core::cmp::max(&self.next_response_deadline, &self.next_query_deadline)
        }

        fn state_with_now(&self, now: &Instant) -> NodeState {
            if *now < self.next_response_deadline {
                return NodeState::Good;
            }

            if *now < self.next_query_deadline {
                return NodeState::Good;
            }

            if self.karma < -2 {
                return NodeState::Bad;
            }

            NodeState::Questionable
        }

        /// Called when a message is received from the node,
        ///
        /// # Important
        ///
        /// In most situations, this method should not be directly called. The
        /// method is called from [`Bucket::on_msg_received()`].
        ///
        /// The callback modifies the internal state based on the message type and
        /// transaction ID. If the message is a response or a query, the node is
        /// considered to still be active. If the message is an error or has an
        /// unknown message type, the node's is considered to be increasingly
        /// questionable.
        pub fn on_msg_received(
            &mut self,
            kind: &Ty<'_>,
            tx_id: Option<&TxId>,
            next_response_deadline: Instant,
            next_query_deadline: Instant,
        ) where
            TxId: PartialEq,
        {
            match kind {
                Ty::Response => {
                    if let Some(tx_id) = tx_id {
                        if let Some(ping_tx_id) = &self.ping_tx_id {
                            if *ping_tx_id == *tx_id {
                                self.ping_tx_id = None;
                            }
                        }
                    }
                    self.next_response_deadline = next_response_deadline;
                    self.karma = self.karma.saturating_add(1);
                    if self.karma > 3 {
                        self.karma = 3;
                    }
                }
                Ty::Query => {
                    self.next_query_deadline = next_query_deadline;
                }
                Ty::Error => {
                    if let Some(tx_id) = tx_id {
                        if let Some(ping_tx_id) = &self.ping_tx_id {
                            if *ping_tx_id == *tx_id {
                                self.ping_tx_id = None;
                            }
                        }
                    }
                    self.next_response_deadline = next_response_deadline;
                    self.karma = self.karma.saturating_sub(1);
                }
                Ty::Unknown(_) => {
                    if let Some(tx_id) = tx_id {
                        if let Some(ping_tx_id) = &self.ping_tx_id {
                            if *ping_tx_id == *tx_id {
                                self.ping_tx_id = None;
                            }
                        }
                    }
                    self.karma = self.karma.saturating_sub(1);
                }
                _ => {
                    unreachable!()
                }
            }
        }

        /// Called when an outbound transaction has timed out.
        ///
        /// # Important
        ///
        /// In most situations, this method should not be directly called. The
        /// method is called from [`Bucket::on_resp_timeout()`].
        ///
        /// The node is considered to be in an increasingly questionable state.
        pub fn on_resp_timeout(&mut self, tx_id: &TxId)
        where
            TxId: PartialEq,
        {
            self.karma = self.karma.saturating_sub(1);

            if let Some(ping_tx_id) = &self.ping_tx_id {
                if *ping_tx_id == *tx_id {
                    self.ping_tx_id = None;
                }
            }
        }
    }

    pub(super) trait MyBucket<T> {
        fn find_node_to_ping(&mut self, now: Instant) -> Option<&mut T>;

        fn timeout(&self) -> &Instant;
    }

    impl<Addr> MyBucket<Node<Addr, transaction::Id, std::time::Instant>>
        for Bucket<Node<Addr, transaction::Id, std::time::Instant>, std::time::Instant>
    {
        /// Finds a node which should be pinged.
        ///
        /// Nodes should have be pinged occasionally in order to determine if they are still active.
        ///
        /// When a node is pinged, the [`Node::on_ping()`] method should be called
        /// on the found `Node` to store the transaction ID of the ping (and to
        /// indicate the node was recently pinged).
        fn find_node_to_ping(
            &mut self,
            now: Instant,
        ) -> Option<&mut Node<Addr, transaction::Id, Instant>> {
            self.iter_mut().find(|n| {
                n.state_with_now(&now) == NodeState::Questionable && n.ping_tx_id.is_none()
            })
        }

        /// Returns the timeout for the bucket.
        ///
        /// Nodes may need to be pinged to ensure they are still active. See [`Bucket::find_node_to_ping()`].
        ///
        /// The bucket may also need to be refreshed.
        ///
        /// A bucket is refreshed by attempting to find a random node `Id` in the
        /// bucket. See [`Bucket::rand_id()`] to find a random ID.
        ///
        /// Set the new refresh deadline by calling [`Bucket::set_refresh_deadline()`] if refreshed.
        fn timeout(&self) -> &Instant {
            if let Some(non_pinged_node_timeout) = self
                .iter()
                .filter(|n| n.ping_tx_id.is_none())
                .map(Node::timeout)
                .min()
            {
                return core::cmp::min(self.refresh_deadline(), non_pinged_node_timeout);
            }

            self.refresh_deadline()
        }
    }

    pub(super) trait MyTable<T> {
        #[must_use]
        fn timeout(&self) -> Option<Instant>;

        fn find_node_to_ping(&mut self, now: Instant) -> Option<&mut T>;
    }

    /// Finds close neighbors for the given `Id` parameter.
    ///
    /// Useful to find nodes which a query should be sent to.
    pub(super) fn find_neighbors<Addr>(
        table: &Table<Node<Addr, transaction::Id, Instant>, Instant>,
        id: Id,
        now: Instant,
    ) -> impl Iterator<Item = AddrId<Addr>>
    where
        Addr: Clone,
    {
        let mut nodes = table
            .iter()
            .flat_map(cloudburst::dht::routing::Bucket::iter)
            .map(|n| n.addr_id().clone())
            // .flat_map(|b| b.prioritized_nodes(now.clone()).cloned())
            .collect::<Vec<_>>();
        nodes.sort_by_key(|a| a.id().distance(id));
        nodes.into_iter()
    }

    impl<Addr> MyTable<Node<Addr, transaction::Id, Instant>>
        for Table<Node<Addr, transaction::Id, Instant>, Instant>
    where
        Addr: PartialEq,
    {
        /// The earliest deadline when at least one of the buckets in the routing
        /// table should be refreshed.
        ///
        /// Once a timeout is reached, call [`Table::find_refreshable_bucket()`] to
        /// find a bucket to refresh.
        #[must_use]
        fn timeout(&self) -> Option<Instant> {
            self.iter().map(Bucket::timeout).min().copied()
        }

        /// Finds a node which should be pinged to determine if the node is still active.
        fn find_node_to_ping(
            &mut self,
            now: Instant,
        ) -> Option<&mut Node<Addr, transaction::Id, Instant>> {
            self.iter_mut().find_map(|b| b.find_node_to_ping(now))
        }
    }
}

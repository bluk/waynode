// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::{
    error::Error,
    find_node_op::FindNodeOp,
    krpc::{ping::PingQueryArgs, Kind},
    msg_buffer,
    node::{Addr, AddrId, AddrOptId, Id},
    transaction,
};
use std::{
    cmp::Ordering,
    net::{SocketAddr, SocketAddrV4, SocketAddrV6},
    ops::RangeInclusive,
    time::{Duration, Instant},
};

#[derive(Debug, PartialEq)]
enum NodeState {
    Good,
    Questionable,
    Bad,
}

#[derive(Debug, Clone, Copy)]
struct Node<A>
where
    A: Addr + Into<SocketAddr>,
{
    addr_id: AddrId<A>,
    missing_responses: u8,
    next_response_deadline: Option<Instant>,
    next_query_deadline: Option<Instant>,
    last_pinged: Option<Instant>,
}

impl<A> Node<A>
where
    A: Addr + Into<SocketAddr>,
{
    const TIMEOUT_INTERVAL: Duration = Duration::from_secs(15 * 60);

    fn with_addr_id(addr_id: AddrId<A>) -> Self {
        Self {
            addr_id,
            missing_responses: 0,
            next_response_deadline: None,
            next_query_deadline: None,
            last_pinged: None,
        }
    }

    fn state_with_now(&self, now: Instant) -> NodeState {
        if let Some(next_response_deadline) = self.next_response_deadline {
            if now < next_response_deadline {
                return NodeState::Good;
            }
        }

        if let Some(next_query_deadline) = self.next_query_deadline {
            if self.next_response_deadline.is_some() && now < next_query_deadline {
                return NodeState::Good;
            }
        }

        if self.missing_responses > 2 {
            return NodeState::Bad;
        }

        NodeState::Questionable
    }

    fn next_msg_deadline(&self) -> Option<Instant> {
        match (self.next_query_deadline, self.next_response_deadline) {
            (Some(query), None) => Some(query),
            (None, Some(resp)) => Some(resp),
            (Some(query), Some(resp)) => {
                if resp < query {
                    Some(query)
                } else {
                    Some(resp)
                }
            }
            (None, None) => None,
        }
    }

    fn on_msg_received(&mut self, kind: &Kind, now: Instant) {
        self.last_pinged = None;
        match kind {
            Kind::Response => {
                self.next_response_deadline = Some(now + Self::TIMEOUT_INTERVAL);
                if self.missing_responses > 0 {
                    self.missing_responses -= 1;
                }
            }
            Kind::Query => {
                self.next_query_deadline = Some(now + Self::TIMEOUT_INTERVAL);
            }
            Kind::Error => {
                self.next_response_deadline = Some(now + Self::TIMEOUT_INTERVAL);
                if self.missing_responses < u8::MAX {
                    self.missing_responses += 1;
                }
            }
            Kind::Unknown(_) => {
                if self.missing_responses < u8::MAX {
                    self.missing_responses += 1;
                }
            }
        }
    }

    fn on_resp_timeout(&mut self) {
        self.last_pinged = None;
        if self.missing_responses < u8::MAX {
            self.missing_responses += 1;
        }
    }

    fn on_ping(&mut self, now: Instant) {
        self.last_pinged = Some(now);
    }
}

const EXPECT_CHANGE_INTERVAL: Duration = Duration::from_secs(15 * 60);
const BUCKET_SIZE: usize = 8;

#[derive(Debug)]
struct Bucket<A>
where
    A: Addr + Into<SocketAddr>,
{
    range: RangeInclusive<Id>,
    nodes: [Option<Node<A>>; BUCKET_SIZE],
    replacement_nodes: [Option<Node<A>>; BUCKET_SIZE],
    expected_change_deadline: Instant,
}

impl<A> Bucket<A>
where
    A: Addr + Into<SocketAddr>,
{
    fn new(range: RangeInclusive<Id>) -> Self {
        Bucket {
            range,
            nodes: [None; BUCKET_SIZE],
            replacement_nodes: [None; BUCKET_SIZE],
            expected_change_deadline: Instant::now() + Duration::from_secs(5 * 60),
        }
    }

    fn is_full(&self) -> bool {
        self.nodes.iter().all(|n| n.is_some())
    }

    #[inline]
    fn update_expected_change_deadline(&mut self) {
        self.expected_change_deadline = Instant::now() + EXPECT_CHANGE_INTERVAL;
    }

    fn try_insert(&mut self, addr_id: AddrId<A>, now: Instant) {
        self.nodes[self.nodes.len() - 1] = Some(Node::with_addr_id(addr_id));
        self.sort_node_ids(now);
        self.update_expected_change_deadline();
    }

    fn ping_least_recently_seen_questionable_node(
        &mut self,
        config: &crate::Config,
        tx_manager: &mut transaction::Manager,
        msg_buffer: &mut msg_buffer::Buffer,
        now: Instant,
    ) -> Result<(), Error> {
        let pinged_nodes_count = self
            .nodes
            .iter()
            .flatten()
            .filter(|n| n.state_with_now(now) == NodeState::Questionable && n.last_pinged.is_some())
            .count();
        let replacement_nodes_count = self
            .replacement_nodes
            .iter()
            .filter(|n| n.is_some())
            .count();
        if pinged_nodes_count < replacement_nodes_count {
            if let Some(node_to_ping) = self.nodes.iter_mut().rev().flatten().find(|n| {
                n.state_with_now(now) == NodeState::Questionable && n.last_pinged.is_none()
            }) {
                msg_buffer.write_query(
                    &PingQueryArgs::new(config.local_id),
                    AddrOptId::new(
                        node_to_ping.addr_id.addr().into(),
                        Some(node_to_ping.addr_id.id()),
                    ),
                    config.default_query_timeout,
                    config.client_version.as_ref(),
                    tx_manager,
                )?;
                node_to_ping.on_ping(now);
            }
        }
        Ok(())
    }

    fn on_msg_received<'a>(
        &mut self,
        addr_id: AddrId<A>,
        kind: &Kind<'a>,
        config: &crate::Config,
        tx_manager: &mut transaction::Manager,
        msg_buffer: &mut msg_buffer::Buffer,
        now: Instant,
    ) -> Result<(), Error> {
        if let Some(node) = self
            .nodes
            .iter_mut()
            .flatten()
            .find(|n| n.addr_id == addr_id)
        {
            node.on_msg_received(kind, now);
            match kind {
                Kind::Response | Kind::Query => {
                    self.sort_node_ids(now);
                    self.ping_least_recently_seen_questionable_node(
                        config, tx_manager, msg_buffer, now,
                    )?;
                    self.update_expected_change_deadline();
                }
                Kind::Error | Kind::Unknown(_) => match node.state_with_now(now) {
                    NodeState::Good => {
                        self.sort_node_ids(now);
                    }
                    NodeState::Questionable => {
                        self.sort_node_ids(now);
                        self.ping_least_recently_seen_questionable_node(
                            config, tx_manager, msg_buffer, now,
                        )?;
                    }
                    NodeState::Bad => {
                        if let Some(replacement_node) =
                            self.replacement_nodes.iter_mut().find(|n| n.is_some())
                        {
                            if let Some(replacement_node) = replacement_node.take() {
                                *node = replacement_node;
                                self.update_expected_change_deadline();
                            }
                        }
                        self.sort_node_ids(now);
                    }
                },
            }
        } else {
            match kind {
                Kind::Response | Kind::Query | Kind::Error => {}
                Kind::Unknown(_) => return Ok(()),
            }

            if let Some(pos) = self.nodes.iter().rev().position(|n| {
                n.map(|n| n.state_with_now(now) == NodeState::Bad)
                    .unwrap_or(false)
            }) {
                let mut node = Node::with_addr_id(addr_id);
                node.on_msg_received(kind, now);
                self.nodes[pos] = Some(node);
                self.sort_node_ids(now);
                self.update_expected_change_deadline();
            } else if let Some(pos) = self
                .replacement_nodes
                .iter_mut()
                .rev()
                .position(|n| n.is_none())
            {
                let mut node = Node::with_addr_id(addr_id);
                node.on_msg_received(kind, now);
                self.replacement_nodes[pos] = Some(node);
                self.sort_node_ids(now);
                self.ping_least_recently_seen_questionable_node(
                    config, tx_manager, msg_buffer, now,
                )?;
            }
        }
        Ok(())
    }

    fn on_resp_timeout(
        &mut self,
        addr_id: AddrId<A>,
        config: &crate::Config,
        tx_manager: &mut transaction::Manager,
        msg_buffer: &mut msg_buffer::Buffer,
        now: Instant,
    ) -> Result<(), Error> {
        if let Some(node) = self
            .nodes
            .iter_mut()
            .flatten()
            .find(|n| n.addr_id == addr_id)
        {
            node.on_resp_timeout();
            match node.state_with_now(now) {
                NodeState::Good => {
                    // The sort order will not change if the state is still good
                }
                NodeState::Questionable => {
                    self.sort_node_ids(now);
                    self.ping_least_recently_seen_questionable_node(
                        config, tx_manager, msg_buffer, now,
                    )?;
                }
                NodeState::Bad => {
                    if let Some(replacement_node) =
                        self.replacement_nodes.iter_mut().find(|n| n.is_some())
                    {
                        if let Some(replacement_node) = replacement_node.take() {
                            *node = replacement_node;
                            self.update_expected_change_deadline();
                        }
                    }
                    self.sort_node_ids(now);
                }
            }
        }
        Ok(())
    }

    fn split_insert(&mut self, node: Node<A>) {
        if let Some(pos) = self.nodes.iter().position(|n| n.is_none()) {
            self.nodes[pos] = Some(node);
        } else {
            unreachable!()
        }
    }

    fn split_replacement_insert(&mut self, node: Node<A>) {
        if let Some(pos) = self.replacement_nodes.iter().position(|n| n.is_none()) {
            self.replacement_nodes[pos] = Some(node);
        } else {
            unreachable!()
        }
    }

    fn split(self) -> (Bucket<A>, Bucket<A>) {
        let middle = self.range.end().middle(*self.range.start());

        let mut lower_bucket = Bucket::new(*self.range.start()..=middle.prev());
        let mut upper_bucket = Bucket::new(middle..=*self.range.end());

        for node in self.nodes.iter().flatten() {
            let node_id = node.addr_id.id();
            if lower_bucket.range.contains(&node_id) {
                lower_bucket.split_insert(*node);
            } else {
                upper_bucket.split_insert(*node);
            }
        }

        for node in self.replacement_nodes.iter().flatten() {
            let node_id = node.addr_id.id();
            if lower_bucket.range.contains(&node_id) {
                lower_bucket.split_replacement_insert(*node);
            } else {
                upper_bucket.split_replacement_insert(*node);
            }
        }

        (lower_bucket, upper_bucket)
    }

    fn prioritized_nodes(&self, now: Instant) -> impl Iterator<Item = &AddrId<A>> {
        self.nodes
            .iter()
            .flatten()
            .filter(move |n| {
                n.state_with_now(now) == NodeState::Questionable
                    || n.state_with_now(now) == NodeState::Good
            })
            .map(|n| &n.addr_id)
    }

    fn sort_node_ids(&mut self, now: Instant) {
        self.nodes.sort_unstable_by(|a, b| match (a, b) {
            (Some(a), Some(b)) => {
                match (a.state_with_now(now), b.state_with_now(now)) {
                    (NodeState::Good, NodeState::Questionable)
                    | (NodeState::Good, NodeState::Bad)
                    | (NodeState::Questionable, NodeState::Bad) => return Ordering::Less,
                    (NodeState::Questionable, NodeState::Good)
                    | (NodeState::Bad, NodeState::Questionable)
                    | (NodeState::Bad, NodeState::Good) => return Ordering::Greater,
                    (NodeState::Good, NodeState::Good)
                    | (NodeState::Questionable, NodeState::Questionable)
                    | (NodeState::Bad, NodeState::Bad) => {}
                }

                match (a.next_msg_deadline(), b.next_msg_deadline()) {
                    (None, None) => Ordering::Equal,
                    (Some(_), None) => Ordering::Less,
                    (None, Some(_)) => Ordering::Greater,
                    (Some(first), Some(second)) => second.cmp(&first),
                }
            }
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        });
    }
}

const FIND_LOCAL_ID_INTERVAL: Duration = Duration::from_secs(15 * 60);

#[derive(Debug)]
pub(crate) struct Table<A>
where
    A: Addr + Into<SocketAddr>,
{
    pivot: Id,
    buckets: Vec<Bucket<A>>,
    find_pivot_id_deadline: Instant,
}

impl<A> Table<A>
where
    A: Addr + Into<SocketAddr>,
{
    pub(crate) fn new(pivot: Id, now: Instant) -> Self {
        Self {
            pivot,
            buckets: vec![Bucket::new(Id::min()..=Id::max())],
            find_pivot_id_deadline: now + FIND_LOCAL_ID_INTERVAL,
        }
    }

    pub(crate) fn find_node<I>(
        &mut self,
        target_id: Id,
        config: &crate::Config,
        tx_manager: &mut transaction::Manager,
        msg_buffer: &mut msg_buffer::Buffer,
        bootstrap_addrs: I,
        now: Instant,
    ) -> Result<FindNodeOp, Error>
    where
        I: IntoIterator<Item = A>,
    {
        let neighbors = self
            .find_neighbors(target_id, now)
            .take(8)
            .map(|a| AddrOptId::new(a.addr(), Some(a.id())))
            .chain(bootstrap_addrs.into_iter().map(AddrOptId::with_addr));
        let mut find_node_op = FindNodeOp::new(config, target_id, neighbors);
        find_node_op.start(&config, tx_manager, msg_buffer)?;
        Ok(find_node_op)
    }

    fn try_insert(&mut self, addr_id: AddrId<A>, now: Instant) {
        let node_id = addr_id.id();
        if node_id == self.pivot {
            return;
        }

        let bucket = self
            .buckets
            .iter_mut()
            .find(|n| n.range.contains(&node_id))
            .expect("bucket should always exist for a node");
        if bucket.range.contains(&self.pivot) && bucket.is_full() {
            let bucket = self.buckets.pop().expect("last bucket should always exist");
            let (mut first_bucket, mut second_bucket) = bucket.split();
            if first_bucket.range.contains(&node_id) {
                first_bucket.try_insert(addr_id, now);
            } else {
                second_bucket.try_insert(addr_id, now);
            }

            if first_bucket.range.contains(&self.pivot) {
                self.buckets.push(second_bucket);
                self.buckets.push(first_bucket);
            } else {
                self.buckets.push(first_bucket);
                self.buckets.push(second_bucket);
            }
        } else {
            bucket.try_insert(addr_id, now);
        }
    }

    pub(crate) fn find_neighbors(&self, id: Id, now: Instant) -> std::vec::IntoIter<AddrId<A>> {
        let mut nodes = self
            .buckets
            .iter()
            .flat_map(|b| b.prioritized_nodes(now).copied())
            .collect::<Vec<_>>();
        nodes.sort_by_key(|a| a.id().distance(id));
        nodes.into_iter()
    }

    pub(crate) fn on_msg_received<'a>(
        &mut self,
        addr_id: AddrId<A>,
        kind: &Kind<'a>,
        config: &crate::Config,
        tx_manager: &mut transaction::Manager,
        msg_buffer: &mut msg_buffer::Buffer,
        now: Instant,
    ) -> Result<(), crate::error::Error> {
        let node_id = addr_id.id();
        if node_id == self.pivot {
            return Ok(());
        }

        let bucket = self
            .buckets
            .iter_mut()
            .find(|n| n.range.contains(&node_id))
            .expect("bucket should always exist for a node");
        if bucket.range.contains(&self.pivot) && bucket.is_full() {
            let bucket = self.buckets.pop().expect("last bucket should always exist");
            let (mut first_bucket, mut second_bucket) = bucket.split();
            if first_bucket.range.contains(&node_id) {
                first_bucket.on_msg_received(addr_id, kind, config, tx_manager, msg_buffer, now)?;
            } else {
                second_bucket
                    .on_msg_received(addr_id, kind, config, tx_manager, msg_buffer, now)?;
            }

            if first_bucket.range.contains(&self.pivot) {
                self.buckets.push(second_bucket);
                self.buckets.push(first_bucket);
            } else {
                self.buckets.push(first_bucket);
                self.buckets.push(second_bucket);
            }
        } else {
            bucket.on_msg_received(addr_id, kind, config, tx_manager, msg_buffer, now)?;
        }
        Ok(())
    }

    pub(crate) fn on_resp_timeout(
        &mut self,
        addr_id: AddrId<A>,
        config: &crate::Config,
        tx_manager: &mut transaction::Manager,
        msg_buffer: &mut msg_buffer::Buffer,
        now: Instant,
    ) -> Result<(), crate::error::Error> {
        let node_id = addr_id.id();
        let bucket = self
            .buckets
            .iter_mut()
            .find(|n| n.range.contains(&node_id))
            .expect("bucket should always exist for a node");
        bucket.on_resp_timeout(addr_id, config, tx_manager, msg_buffer, now)?;
        Ok(())
    }

    pub(crate) fn timeout(&self) -> Option<Instant> {
        self.buckets
            .iter()
            .map(|b| b.expected_change_deadline)
            .chain(std::iter::once(self.find_pivot_id_deadline))
            .min()
    }

    pub(crate) fn on_timeout(
        &mut self,
        config: &crate::Config,
        tx_manager: &mut transaction::Manager,
        msg_buffer: &mut msg_buffer::Buffer,
        find_node_ops: &mut Vec<FindNodeOp>,
        now: Instant,
    ) -> Result<(), Error> {
        if self.find_pivot_id_deadline <= now {
            find_node_ops.push(self.find_node(
                self.pivot,
                config,
                tx_manager,
                msg_buffer,
                std::iter::empty(),
                now,
            )?);
            self.find_pivot_id_deadline = now + FIND_LOCAL_ID_INTERVAL;
        }

        let target_ids = self
            .buckets
            .iter_mut()
            .filter(|b| b.expected_change_deadline <= now)
            .map(|b| {
                b.expected_change_deadline = now + EXPECT_CHANGE_INTERVAL;
                Id::rand_in_inclusive_range(&b.range)
            })
            .collect::<Result<Vec<_>, _>>()?;
        debug!("routing table on_timeout()");
        for b in self.buckets.iter() {
            debug!("bucket: {:?}", b);
        }
        for target_id in target_ids {
            find_node_ops.push(self.find_node(
                target_id,
                config,
                tx_manager,
                msg_buffer,
                std::iter::empty(),
                now,
            )?);
            debug!(
                "starting to find target_id={:?} find_node_ops.len={}",
                target_id,
                find_node_ops.len()
            );
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) enum RoutingTable {
    Ipv4(Table<SocketAddrV4>),
    Ipv6(Table<SocketAddrV6>),
    Ipv4AndIpv6(Table<SocketAddrV4>, Table<SocketAddrV6>),
}

impl RoutingTable {
    pub(crate) fn try_insert_addr_ids<'a, I>(&mut self, addrs: I, now: Instant)
    where
        I: IntoIterator<Item = &'a AddrId<SocketAddr>>,
    {
        for addr_id in addrs.into_iter() {
            match addr_id.addr() {
                SocketAddr::V4(addr) => match self {
                    RoutingTable::Ipv4(routing_table)
                    | RoutingTable::Ipv4AndIpv6(routing_table, _) => {
                        routing_table.try_insert(AddrId::new(addr, addr_id.id()), now)
                    }
                    RoutingTable::Ipv6(_) => {}
                },
                SocketAddr::V6(addr) => match self {
                    RoutingTable::Ipv4(_) => {}
                    RoutingTable::Ipv6(routing_table)
                    | RoutingTable::Ipv4AndIpv6(_, routing_table) => {
                        routing_table.try_insert(AddrId::new(addr, addr_id.id()), now)
                    }
                },
            }
        }
    }

    pub(crate) fn find_node<I>(
        &mut self,
        target_id: Id,
        config: &crate::Config,
        tx_manager: &mut transaction::Manager,
        msg_buffer: &mut msg_buffer::Buffer,
        find_node_ops: &mut Vec<FindNodeOp>,
        bootstrap_addrs: I,
        now: Instant,
    ) -> Result<(), Error>
    where
        I: IntoIterator<Item = SocketAddr>,
    {
        let mut ipv4_socket_addrs = Vec::new();
        let mut ipv6_socket_addrs = Vec::new();
        for socket_addr in bootstrap_addrs.into_iter() {
            match socket_addr {
                SocketAddr::V4(addr) => {
                    ipv4_socket_addrs.push(addr);
                }
                SocketAddr::V6(addr) => {
                    ipv6_socket_addrs.push(addr);
                }
            }
        }
        match self {
            RoutingTable::Ipv4(routing_table) => find_node_ops.push(routing_table.find_node(
                target_id,
                &config,
                tx_manager,
                msg_buffer,
                ipv4_socket_addrs,
                now,
            )?),
            RoutingTable::Ipv6(routing_table) => find_node_ops.push(routing_table.find_node(
                target_id,
                &config,
                tx_manager,
                msg_buffer,
                ipv6_socket_addrs,
                now,
            )?),
            RoutingTable::Ipv4AndIpv6(routing_table_v4, routing_table_v6) => {
                find_node_ops.push(routing_table_v4.find_node(
                    target_id,
                    &config,
                    tx_manager,
                    msg_buffer,
                    ipv4_socket_addrs,
                    now,
                )?);
                find_node_ops.push(routing_table_v6.find_node(
                    target_id,
                    &config,
                    tx_manager,
                    msg_buffer,
                    ipv6_socket_addrs,
                    now,
                )?);
            }
        }
        Ok(())
    }

    pub(crate) fn on_msg_received<'a>(
        &mut self,
        addr_id: AddrId<SocketAddr>,
        kind: &Kind<'a>,
        config: &crate::Config,
        tx_manager: &mut transaction::Manager,
        msg_buffer: &mut msg_buffer::Buffer,
        now: Instant,
    ) -> Result<(), Error> {
        match addr_id.addr() {
            SocketAddr::V4(addr) => match self {
                RoutingTable::Ipv4(routing_table) | RoutingTable::Ipv4AndIpv6(routing_table, _) => {
                    routing_table.on_msg_received(
                        AddrId::new(addr, addr_id.id()),
                        kind,
                        config,
                        tx_manager,
                        msg_buffer,
                        now,
                    )?;
                }
                RoutingTable::Ipv6(_) => {}
            },
            SocketAddr::V6(addr) => match self {
                RoutingTable::Ipv4(_) => {}
                RoutingTable::Ipv6(routing_table) | RoutingTable::Ipv4AndIpv6(_, routing_table) => {
                    routing_table.on_msg_received(
                        AddrId::new(addr, addr_id.id()),
                        kind,
                        config,
                        tx_manager,
                        msg_buffer,
                        now,
                    )?;
                }
            },
        }
        Ok(())
    }

    pub(crate) fn on_resp_timeout(
        &mut self,
        addr_id: AddrId<SocketAddr>,
        config: &crate::Config,
        tx_manager: &mut transaction::Manager,
        msg_buffer: &mut msg_buffer::Buffer,
        now: Instant,
    ) -> Result<(), crate::error::Error> {
        match addr_id.addr() {
            SocketAddr::V4(addr) => match self {
                RoutingTable::Ipv4(routing_table) | RoutingTable::Ipv4AndIpv6(routing_table, _) => {
                    routing_table.on_resp_timeout(
                        AddrId::new(addr, addr_id.id()),
                        config,
                        tx_manager,
                        msg_buffer,
                        now,
                    )?;
                }
                RoutingTable::Ipv6(_) => {}
            },
            SocketAddr::V6(addr) => match self {
                RoutingTable::Ipv4(_) => {}
                RoutingTable::Ipv6(routing_table) | RoutingTable::Ipv4AndIpv6(_, routing_table) => {
                    routing_table.on_resp_timeout(
                        AddrId::new(addr, addr_id.id()),
                        config,
                        tx_manager,
                        msg_buffer,
                        now,
                    )?;
                }
            },
        }
        Ok(())
    }

    pub(crate) fn timeout(&self) -> Option<Instant> {
        match self {
            RoutingTable::Ipv4(routing_table) => routing_table.timeout(),
            RoutingTable::Ipv6(routing_table) => routing_table.timeout(),
            RoutingTable::Ipv4AndIpv6(routing_table_v4, routing_table_v6) => {
                [routing_table_v4.timeout(), routing_table_v6.timeout()]
                    .iter()
                    .filter_map(|&deadline| deadline)
                    .min()
            }
        }
    }

    pub(crate) fn on_timeout(
        &mut self,
        config: &crate::Config,
        tx_manager: &mut transaction::Manager,
        msg_buffer: &mut msg_buffer::Buffer,
        find_node_ops: &mut Vec<FindNodeOp>,
        now: Instant,
    ) -> Result<(), Error> {
        match self {
            RoutingTable::Ipv4(routing_table) => {
                routing_table.on_timeout(config, tx_manager, msg_buffer, find_node_ops, now)
            }
            RoutingTable::Ipv6(routing_table) => {
                routing_table.on_timeout(config, tx_manager, msg_buffer, find_node_ops, now)
            }
            RoutingTable::Ipv4AndIpv6(routing_table_v4, routing_table_v6) => {
                routing_table_v4.on_timeout(config, tx_manager, msg_buffer, find_node_ops, now)?;
                routing_table_v6.on_timeout(config, tx_manager, msg_buffer, find_node_ops, now)?;
                Ok(())
            }
        }
    }

    pub fn find_neighbors_ipv4(&self, id: Id) -> impl Iterator<Item = AddrId<SocketAddrV4>> {
        match self {
            RoutingTable::Ipv4(routing_table) | RoutingTable::Ipv4AndIpv6(routing_table, _) => {
                routing_table.find_neighbors(id, Instant::now())
            }
            RoutingTable::Ipv6(_) => Vec::new().into_iter(),
        }
    }

    pub fn find_neighbors_ipv6(&self, id: Id) -> impl Iterator<Item = AddrId<SocketAddrV6>> {
        match self {
            RoutingTable::Ipv6(routing_table) | RoutingTable::Ipv4AndIpv6(_, routing_table) => {
                routing_table.find_neighbors(id, Instant::now())
            }
            RoutingTable::Ipv4(_) => Vec::new().into_iter(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddrV4;

    #[test]
    fn test_split_bucket() {
        let bucket: Bucket<SocketAddrV4> = Bucket::new(Id::min()..=Id::max());
        let (first_bucket, second_bucket) = bucket.split();
        assert_eq!(
            first_bucket.range,
            Id::min()
                ..=Id::new([
                    0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe
                ])
        );
        assert_eq!(
            second_bucket.range,
            Id::new([
                0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                0xff, 0xff, 0xff, 0xff, 0xff, 0xff
            ])..=Id::max()
        );
    }
}

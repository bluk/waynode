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
    node::{AddrId, Id},
    transaction,
};
use std::cmp::Ordering;
use std::ops::RangeInclusive;
use std::time::{Duration, Instant};

#[derive(Debug, PartialEq)]
enum NodeState {
    Good,
    Questionable,
    Bad,
}

#[derive(Debug)]
struct Node {
    addr_id: AddrId,
    missing_responses: u8,
    next_response_deadline: Option<Instant>,
    next_query_deadline: Option<Instant>,
    last_pinged: Option<Instant>,
}

impl Node {
    const TIMEOUT_INTERVAL: Duration = Duration::from_secs(15 * 60);

    fn with_addr_id(addr_id: AddrId) -> Self {
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

#[derive(Debug)]
struct Bucket {
    range: RangeInclusive<Id>,
    nodes: Vec<Node>,
    replacement_nodes: Vec<Node>,
    expected_change_deadline: Instant,
}

impl Bucket {
    fn new(range: RangeInclusive<Id>, max_nodes_per_bucket: usize) -> Self {
        Bucket {
            range,
            nodes: Vec::with_capacity(max_nodes_per_bucket),
            replacement_nodes: Vec::with_capacity(max_nodes_per_bucket),
            expected_change_deadline: Instant::now() + Duration::from_secs(5 * 60),
        }
    }

    #[inline]
    fn update_expected_change_deadline(&mut self) {
        self.expected_change_deadline = Instant::now() + EXPECT_CHANGE_INTERVAL;
    }

    fn try_insert(&mut self, max_nodes_per_bucket: usize, addr_id: AddrId, now: Instant) {
        if self.nodes.len() < max_nodes_per_bucket {
            let node = Node::with_addr_id(addr_id);
            self.nodes.push(node);
            self.sort_node_ids(now);
            self.update_expected_change_deadline();
        } else if let Some(pos) = self
            .nodes
            .iter()
            .rev()
            .position(|n| n.state_with_now(now) == NodeState::Bad)
        {
            let node = Node::with_addr_id(addr_id);
            self.nodes[pos] = node;
            self.sort_node_ids(now);
            self.update_expected_change_deadline();
        } else {
            self.sort_node_ids(now);
            if let Some(pos) = self
                .nodes
                .iter()
                .rev()
                .position(|n| n.state_with_now(now) == NodeState::Questionable)
            {
                let node = Node::with_addr_id(addr_id);
                self.nodes[pos] = node;
                self.update_expected_change_deadline();
            }
        }
    }

    #[inline]
    fn max_replacement_nodes(&self, now: Instant) -> usize {
        self.nodes
            .iter()
            .filter(|n| n.state_with_now(now) == NodeState::Questionable)
            .count()
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
            .filter(|n| n.state_with_now(now) == NodeState::Questionable && n.last_pinged.is_some())
            .count();
        if pinged_nodes_count < self.replacement_nodes.len() {
            let node_to_ping = self
                .nodes
                .iter_mut()
                .rev()
                .find(|n| {
                    n.state_with_now(now) == NodeState::Questionable && n.last_pinged.is_none()
                })
                .expect("questionable non-pinged node to exist");
            msg_buffer.write_query(
                &PingQueryArgs::with_id(config.local_id),
                node_to_ping.addr_id,
                config.default_query_timeout,
                tx_manager,
            )?;
            node_to_ping.on_ping(now);
        }
        Ok(())
    }

    fn on_msg_received<'a>(
        &mut self,
        max_nodes_per_bucket: usize,
        addr_id: AddrId,
        kind: &Kind<'a>,
        config: &crate::Config,
        tx_manager: &mut transaction::Manager,
        msg_buffer: &mut msg_buffer::Buffer,
        now: Instant,
    ) -> Result<(), Error> {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.addr_id == addr_id) {
            node.on_msg_received(kind, now);
            match kind {
                Kind::Response | Kind::Query => {
                    let max_replacement_nodes = self.max_replacement_nodes(now);
                    if self.replacement_nodes.len() > max_replacement_nodes {
                        self.replacement_nodes.drain(max_replacement_nodes..);
                    }
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
                        if let Some(mut replacement_node) = self.replacement_nodes.pop() {
                            std::mem::swap(node, &mut replacement_node);
                            self.update_expected_change_deadline();
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

            if self.nodes.len() < max_nodes_per_bucket {
                let mut node = Node::with_addr_id(addr_id);
                node.on_msg_received(kind, now);
                self.nodes.push(node);
                self.sort_node_ids(now);
                self.update_expected_change_deadline();
            } else if let Some(pos) = self
                .nodes
                .iter()
                .rev()
                .position(|n| n.state_with_now(now) == NodeState::Bad)
            {
                let mut node = Node::with_addr_id(addr_id);
                node.on_msg_received(kind, now);
                self.nodes[pos] = node;
                self.sort_node_ids(now);
                self.update_expected_change_deadline();
            } else if self.replacement_nodes.len() < self.max_replacement_nodes(now) {
                let mut node = Node::with_addr_id(addr_id);
                node.on_msg_received(kind, now);
                self.replacement_nodes.push(node);
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
        addr_id: AddrId,
        config: &crate::Config,
        tx_manager: &mut transaction::Manager,
        msg_buffer: &mut msg_buffer::Buffer,
        now: Instant,
    ) -> Result<(), Error> {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.addr_id == addr_id) {
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
                    if let Some(mut replacement_node) = self.replacement_nodes.pop() {
                        std::mem::swap(node, &mut replacement_node);
                        self.update_expected_change_deadline();
                    }
                    self.sort_node_ids(now);
                }
            }
        }
        Ok(())
    }

    fn split(self, max_nodes_per_bucket: usize) -> (Bucket, Bucket) {
        let middle = self.range.end().middle(self.range.start());

        let mut lower_bucket =
            Bucket::new(*self.range.start()..=middle.prev(), max_nodes_per_bucket);
        let mut upper_bucket = Bucket::new(middle..=*self.range.end(), max_nodes_per_bucket);

        for node in self.nodes.into_iter() {
            if let Some(node_id) = node.addr_id.id() {
                if lower_bucket.range.contains(&node_id) {
                    lower_bucket.nodes.push(node);
                } else {
                    upper_bucket.nodes.push(node);
                }
            } else {
                panic!("node does not have id");
            }
        }

        for node in self.replacement_nodes.into_iter() {
            if let Some(node_id) = node.addr_id.id() {
                if lower_bucket.range.contains(&node_id) {
                    lower_bucket.replacement_nodes.push(node);
                } else {
                    upper_bucket.replacement_nodes.push(node);
                }
            } else {
                panic!("node does not have id");
            }
        }

        (lower_bucket, upper_bucket)
    }

    fn prioritized_addr_ids(&self, now: Instant) -> impl Iterator<Item = &AddrId> {
        self.nodes
            .iter()
            .filter(move |n| {
                n.state_with_now(now) == NodeState::Questionable
                    || n.state_with_now(now) == NodeState::Good
            })
            .map(|n| &n.addr_id)
    }

    fn sort_node_ids(&mut self, now: Instant) {
        self.nodes.sort_unstable_by(|a, b| {
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
        });
    }
}

#[derive(Debug)]
pub(crate) struct Table {
    pivot: Id,
    buckets: Vec<Bucket>,
    max_nodes_per_bucket: usize,
}

impl Table {
    pub(crate) fn new(
        pivot: Id,
        max_nodes_per_bucket: usize,
        existing_addr_ids: &[AddrId],
    ) -> Self {
        let mut table = Self {
            pivot,
            buckets: vec![Bucket::new(Id::min()..=Id::max(), max_nodes_per_bucket)],
            max_nodes_per_bucket,
        };
        let now = Instant::now();
        for addr_id in existing_addr_ids {
            table.try_insert(*addr_id, now);
        }
        table
    }

    pub(crate) fn find_node(
        &mut self,
        target_id: Id,
        config: &crate::Config,
        tx_manager: &mut transaction::Manager,
        msg_buffer: &mut msg_buffer::Buffer,
        find_node_ops: &mut Vec<FindNodeOp>,
        bootstrap_nodes: &[AddrId],
        now: Instant,
    ) -> Result<(), Error> {
        let mut neighbors = self
            .find_neighbors(target_id, now)
            .take(8)
            .copied()
            .collect::<Vec<AddrId>>();
        neighbors.extend(bootstrap_nodes.iter().copied());
        let mut find_node_op = FindNodeOp::with_target_id_and_neighbors(target_id, neighbors);
        find_node_op.start(&config, tx_manager, msg_buffer)?;
        find_node_ops.push(find_node_op);
        Ok(())
    }

    fn try_insert(&mut self, addr_id: AddrId, now: Instant) {
        if let Some(node_id) = addr_id.id() {
            if node_id == self.pivot {
                return;
            }

            let bucket = self
                .buckets
                .iter_mut()
                .find(|n| n.range.contains(&node_id))
                .expect("bucket should always exist for a node");
            if bucket.range.contains(&self.pivot) && bucket.nodes.len() >= self.max_nodes_per_bucket
            {
                let bucket = self.buckets.pop().expect("last bucket should always exist");
                let (mut first_bucket, mut second_bucket) = bucket.split(self.max_nodes_per_bucket);
                if first_bucket.range.contains(&node_id) {
                    first_bucket.try_insert(self.max_nodes_per_bucket, addr_id, now);
                } else {
                    second_bucket.try_insert(self.max_nodes_per_bucket, addr_id, now);
                }

                if first_bucket.range.contains(&self.pivot) {
                    self.buckets.push(second_bucket);
                    self.buckets.push(first_bucket);
                } else {
                    self.buckets.push(first_bucket);
                    self.buckets.push(second_bucket);
                }
            } else {
                bucket.try_insert(self.max_nodes_per_bucket, addr_id, now);
            }
        }
    }

    pub(crate) fn find_neighbors<'a>(
        &'a self,
        id: Id,
        now: Instant,
    ) -> impl Iterator<Item = &'a AddrId> + 'a {
        let idx = self
            .buckets
            .iter()
            .position(|b| b.range.contains(&id))
            .expect("bucket index should always exist for a node id");
        self.buckets[0..=idx]
            .iter()
            .rev()
            .chain(self.buckets[idx..self.buckets.len()].iter())
            .flat_map(move |b| b.prioritized_addr_ids(now))
    }

    pub(crate) fn on_msg_received<'a>(
        &mut self,
        addr_id: AddrId,
        kind: &Kind<'a>,
        config: &crate::Config,
        tx_manager: &mut crate::transaction::Manager,
        msg_buffer: &mut crate::msg_buffer::Buffer,
        now: Instant,
    ) -> Result<(), crate::error::Error> {
        if let Some(node_id) = addr_id.id() {
            if node_id == self.pivot {
                return Ok(());
            }

            let bucket = self
                .buckets
                .iter_mut()
                .find(|n| n.range.contains(&node_id))
                .expect("bucket should always exist for a node");
            if bucket.range.contains(&self.pivot) && bucket.nodes.len() >= self.max_nodes_per_bucket
            {
                let bucket = self.buckets.pop().expect("last bucket should always exist");
                let (mut first_bucket, mut second_bucket) = bucket.split(self.max_nodes_per_bucket);
                if first_bucket.range.contains(&node_id) {
                    first_bucket.on_msg_received(
                        self.max_nodes_per_bucket,
                        addr_id,
                        kind,
                        config,
                        tx_manager,
                        msg_buffer,
                        now,
                    )?;
                } else {
                    second_bucket.on_msg_received(
                        self.max_nodes_per_bucket,
                        addr_id,
                        kind,
                        config,
                        tx_manager,
                        msg_buffer,
                        now,
                    )?;
                }

                if first_bucket.range.contains(&self.pivot) {
                    self.buckets.push(second_bucket);
                    self.buckets.push(first_bucket);
                } else {
                    self.buckets.push(first_bucket);
                    self.buckets.push(second_bucket);
                }
            } else {
                bucket.on_msg_received(
                    self.max_nodes_per_bucket,
                    addr_id,
                    kind,
                    config,
                    tx_manager,
                    msg_buffer,
                    now,
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn on_resp_timeout(
        &mut self,
        addr_id: AddrId,
        config: &crate::Config,
        tx_manager: &mut crate::transaction::Manager,
        msg_buffer: &mut crate::msg_buffer::Buffer,
        now: Instant,
    ) -> Result<(), crate::error::Error> {
        if let Some(node_id) = addr_id.id() {
            let bucket = self
                .buckets
                .iter_mut()
                .find(|n| n.range.contains(&node_id))
                .expect("bucket should always exist for a node");
            bucket.on_resp_timeout(addr_id, config, tx_manager, msg_buffer, now)?;
        }
        Ok(())
    }

    pub(crate) fn timeout(&self) -> Option<Instant> {
        self.buckets
            .iter()
            .map(|b| b.expected_change_deadline)
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
            self.find_node(
                target_id,
                config,
                tx_manager,
                msg_buffer,
                find_node_ops,
                &[],
                now,
            )?;
            debug!(
                "starting to find target_id={:?} find_node_ops.len={}",
                target_id,
                find_node_ops.len()
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_bucket() {
        let bucket = Bucket::new(Id::min()..=Id::max(), 8);
        let (first_bucket, second_bucket) = bucket.split(8);
        assert_eq!(
            first_bucket.range,
            Id::min()
                ..=Id::with_bytes([
                    0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe
                ])
        );
        assert_eq!(
            second_bucket.range,
            Id::with_bytes([
                0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                0xff, 0xff, 0xff, 0xff, 0xff, 0xff
            ])..=Id::max()
        );
    }
}

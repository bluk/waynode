// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::{
    krpc::{ping::PingQueryArgs, Kind},
    node::{
        remote::{RemoteNode, RemoteState},
        AddrId, Id,
    },
};
use std::cmp::Ordering;
use std::ops::RangeInclusive;
use std::time::Instant;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Bucket {
    range: RangeInclusive<Id>,
    nodes: Vec<RemoteNode>,
    max_nodes: usize,

    replacement_nodes: Vec<RemoteNode>,

    last_find_node: Instant,
}

impl Bucket {
    fn new(range: RangeInclusive<Id>, max_nodes: usize) -> Self {
        Bucket {
            range,
            nodes: Vec::with_capacity(max_nodes),
            max_nodes,
            replacement_nodes: Vec::with_capacity(max_nodes),
            last_find_node: Instant::now(),
        }
    }

    #[inline]
    fn max_replacement_nodes(&self) -> usize {
        self.nodes
            .iter()
            .filter(|n| n.state() == RemoteState::Questionable)
            .count()
    }

    fn ping_least_recently_seen_questionable_node(
        &mut self,
        config: &crate::Config,
        tx_manager: &mut crate::transaction::Manager,
        msg_buffer: &mut crate::msg_buffer::Buffer,
        now: Instant,
    ) -> Result<(), crate::error::Error> {
        let pinged_nodes_count = self
            .nodes
            .iter()
            .filter(|n| n.state() == RemoteState::Questionable && n.last_pinged.is_some())
            .count();
        if pinged_nodes_count < self.replacement_nodes.len() {
            let node_to_ping = self
                .nodes
                .iter_mut()
                .rev()
                .find(|n| n.state() == RemoteState::Questionable && n.last_pinged.is_none())
                .expect("questionable non-pinged node to exist");
            msg_buffer.write_query(
                &PingQueryArgs::with_id(config.local_id),
                &node_to_ping.addr_id,
                config.default_query_timeout,
                tx_manager,
            )?;
            node_to_ping.on_ping(now);
        }
        Ok(())
    }

    fn on_msg_received<'a>(
        &mut self,
        addr_id: &AddrId,
        kind: &Kind<'a>,
        config: &crate::Config,
        tx_manager: &mut crate::transaction::Manager,
        msg_buffer: &mut crate::msg_buffer::Buffer,
        now: Instant,
    ) -> Result<(), crate::error::Error> {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.addr_id == *addr_id) {
            node.on_msg_received(kind, now);
            match kind {
                Kind::Response | Kind::Query => {
                    let max_replacement_nodes = self.max_replacement_nodes();
                    if self.replacement_nodes.len() > max_replacement_nodes {
                        self.replacement_nodes.drain(max_replacement_nodes..);
                    }
                    self.sort_node_ids();
                    self.ping_least_recently_seen_questionable_node(
                        config, tx_manager, msg_buffer, now,
                    )?;
                }
                Kind::Error | Kind::Unknown(_) => match node.state() {
                    RemoteState::Good => {
                        self.sort_node_ids();
                    }
                    RemoteState::Questionable => {
                        self.sort_node_ids();
                        self.ping_least_recently_seen_questionable_node(
                            config, tx_manager, msg_buffer, now,
                        )?;
                    }
                    RemoteState::Bad => {
                        if let Some(mut replacement_node) = self.replacement_nodes.pop() {
                            std::mem::swap(node, &mut replacement_node);
                        }
                        self.sort_node_ids();
                    }
                },
            }
        } else {
            match kind {
                Kind::Response | Kind::Query | Kind::Error => {}
                Kind::Unknown(_) => return Ok(()),
            }

            if self.nodes.len() < self.max_nodes {
                let mut node = RemoteNode::with_addr_id(addr_id.clone());
                node.on_msg_received(kind, now);
                self.nodes.push(node);
                self.sort_node_ids();
            } else if let Some(pos) = self
                .nodes
                .iter()
                .position(|n| n.state() == RemoteState::Bad)
            {
                let mut node = RemoteNode::with_addr_id(addr_id.clone());
                node.on_msg_received(kind, now);
                self.nodes[pos] = node;
                self.sort_node_ids();
            } else if self.replacement_nodes.len() < self.max_replacement_nodes() {
                let mut node = RemoteNode::with_addr_id(addr_id.clone());
                node.on_msg_received(kind, now);
                self.replacement_nodes.push(node);
                self.ping_least_recently_seen_questionable_node(
                    config, tx_manager, msg_buffer, now,
                )?;
            }
        }
        Ok(())
    }

    fn on_resp_timeout(
        &mut self,
        addr_id: &AddrId,
        config: &crate::Config,
        tx_manager: &mut crate::transaction::Manager,
        msg_buffer: &mut crate::msg_buffer::Buffer,
        now: Instant,
    ) -> Result<(), crate::error::Error> {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.addr_id == *addr_id) {
            node.on_resp_timeout();
            match node.state() {
                RemoteState::Good => {
                    // The sort order will not change if the state is still good
                }
                RemoteState::Questionable => {
                    self.sort_node_ids();
                    self.ping_least_recently_seen_questionable_node(
                        config, tx_manager, msg_buffer, now,
                    )?;
                }
                RemoteState::Bad => {
                    if let Some(mut replacement_node) = self.replacement_nodes.pop() {
                        std::mem::swap(node, &mut replacement_node);
                    }
                    self.sort_node_ids();
                }
            }
        }
        Ok(())
    }

    fn split(self) -> (Bucket, Bucket) {
        let middle = self.range.end().middle(self.range.start());

        let mut lower_bucket = Bucket::new(*self.range.start()..=middle, self.max_nodes);
        let mut upper_bucket = Bucket::new(middle.next()..=*self.range.end(), self.max_nodes);

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

    fn is_full(&self) -> bool {
        self.nodes.len() >= self.max_nodes
    }

    fn prioritized_addr_ids(&self) -> impl Iterator<Item = &AddrId> {
        self.nodes
            .iter()
            .filter(|n| n.state() == RemoteState::Questionable || n.state() == RemoteState::Good)
            .map(|n| &n.addr_id)
    }

    fn sort_node_ids(&mut self) {
        self.nodes.sort_unstable_by(|a, b| {
            match (a.state(), b.state()) {
                (RemoteState::Good, RemoteState::Questionable)
                | (RemoteState::Good, RemoteState::Bad)
                | (RemoteState::Questionable, RemoteState::Bad) => return Ordering::Less,
                (RemoteState::Questionable, RemoteState::Good)
                | (RemoteState::Bad, RemoteState::Questionable)
                | (RemoteState::Bad, RemoteState::Good) => return Ordering::Greater,
                (RemoteState::Good, RemoteState::Good)
                | (RemoteState::Questionable, RemoteState::Questionable)
                | (RemoteState::Bad, RemoteState::Bad) => {}
            }

            match (a.last_interaction(), b.last_interaction()) {
                (None, None) => Ordering::Equal,
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some(first), Some(second)) => second.cmp(&first),
            }
        });
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Table {
    pivot: Id,
    buckets: Vec<Bucket>,
}

impl Table {
    pub(crate) fn new(pivot: Id, max_nodes: usize) -> Self {
        Self {
            pivot,
            buckets: vec![Bucket::new(Id::min()..=Id::max(), max_nodes)],
        }
    }

    pub(crate) fn find_neighbors<'a>(&'a self, id: Id) -> impl Iterator<Item = &'a AddrId> + 'a {
        let idx = self
            .buckets
            .iter()
            .position(|b| b.range.contains(&id))
            .expect("bucket index should always exist for a node id");
        self.buckets[0..=idx]
            .iter()
            .rev()
            .flat_map(|b| b.prioritized_addr_ids())
    }

    pub(crate) fn on_msg_received<'a>(
        &mut self,
        addr_id: &AddrId,
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
            if bucket.range.contains(&self.pivot) && bucket.is_full() {
                let bucket = self.buckets.pop().expect("last bucket should always exist");
                let (mut first_bucket, mut second_bucket) = bucket.split();
                if first_bucket.range.contains(&node_id) {
                    first_bucket
                        .on_msg_received(addr_id, kind, config, tx_manager, msg_buffer, now)?;
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
        }
        Ok(())
    }

    pub(crate) fn on_resp_timeout(
        &mut self,
        addr_id: &AddrId,
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

    // TODO: Should initiate a find_node request for each bucket if a deadline is reached without
    // activity
}

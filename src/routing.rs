// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::{
    krpc::Kind,
    node::{
        remote::{RemoteNode, RemoteNodeId, RemoteState},
        Id,
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
            .filter(|n| n.state() == RemoteState::Bad || n.state() == RemoteState::Questionable)
            .count()
    }

    fn on_msg_received<'a>(&mut self, remote_id: &RemoteNodeId, kind: &Kind<'a>, now: Instant) {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.id == *remote_id) {
            node.on_msg_received(kind, now);
            match kind {
                Kind::Response => {
                    let max_replacement_nodes = self.max_replacement_nodes();
                    if self.replacement_nodes.len() > max_replacement_nodes {
                        self.replacement_nodes.drain(max_replacement_nodes..);
                    }
                }
                Kind::Query => {
                    let max_replacement_nodes = self.max_replacement_nodes();
                    if self.replacement_nodes.len() > max_replacement_nodes {
                        self.replacement_nodes.drain(max_replacement_nodes..);
                    }
                }
                Kind::Error => {
                    if node.state() == RemoteState::Bad {
                        if let Some(mut replacement_node) = self.replacement_nodes.pop() {
                            std::mem::swap(node, &mut replacement_node);
                        }
                    }
                }
                Kind::Unknown(_) => return,
            }
            self.sort_node_ids();
        } else {
            match kind {
                Kind::Response | Kind::Query | Kind::Error => {}
                Kind::Unknown(_) => return,
            }

            let is_nodes_maxed = self.nodes.len() >= self.max_nodes;
            let is_replacements_maxed =
                self.replacement_nodes.len() >= self.max_replacement_nodes();
            let bad_node_pos = self
                .nodes
                .iter()
                .position(|n| n.state() == RemoteState::Bad);
            if is_nodes_maxed && is_replacements_maxed && bad_node_pos.is_none() {
                return;
            }

            let mut node = RemoteNode::new_with_id(remote_id.clone());
            node.on_msg_received(kind, now);
            if !is_nodes_maxed {
                self.nodes.push(node);
                self.sort_node_ids();
            } else if let Some(pos) = bad_node_pos {
                self.nodes[pos] = node;
            } else if !is_replacements_maxed {
                self.replacement_nodes.push(node);
            } else {
                unreachable!();
            }
        }
    }

    fn on_resp_timeout(&mut self, id: &RemoteNodeId) {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.id == *id) {
            node.on_resp_timeout();
            if node.state() == RemoteState::Bad {
                if let Some(mut replacement_node) = self.replacement_nodes.pop() {
                    std::mem::swap(node, &mut replacement_node);
                }
            }
            self.sort_node_ids();
        }
    }

    fn split(self) -> (Bucket, Bucket) {
        let middle = self.range.end().middle(self.range.start());

        let mut lower_bucket = Bucket::new(*self.range.start()..=middle, self.max_nodes);
        let mut upper_bucket = Bucket::new(middle.next()..=*self.range.end(), self.max_nodes);

        for node in self.nodes.into_iter() {
            if let Some(node_id) = node.id.node_id {
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
            if let Some(node_id) = node.id.node_id {
                if lower_bucket.range.contains(&node_id) {
                    lower_bucket.replacement_nodes.push(node);
                } else {
                    upper_bucket.replacement_nodes.push(node);
                }
            } else {
                panic!("node does not have id");
            }
        }

        lower_bucket.sort_node_ids();
        upper_bucket.sort_node_ids();

        (lower_bucket, upper_bucket)
    }

    fn is_full(&self) -> bool {
        self.nodes.len() >= self.max_nodes
    }

    fn prioritized_node_ids(&self) -> impl Iterator<Item = &RemoteNodeId> {
        self.nodes
            .iter()
            .rev()
            .filter(|n| n.state() == RemoteState::Questionable || n.state() == RemoteState::Good)
            .map(|n| &n.id)
    }

    fn sort_node_ids(&mut self) {
        self.nodes.sort_by(|a, b| {
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

    pub(crate) fn nodes_to_ping(&mut self, now: Instant) -> impl Iterator<Item = &RemoteNodeId> {
        let pinged_nodes_count = self
            .nodes
            .iter_mut()
            .filter(|n| n.state() == RemoteState::Questionable && n.last_pinged.is_some())
            .count();
        let replacement_nodes_len = self.replacement_nodes.len();
        let nodes_to_ping = if pinged_nodes_count < replacement_nodes_len {
            replacement_nodes_len - pinged_nodes_count
        } else {
            0
        };
        self.nodes
            .iter_mut()
            .filter(|n| n.state() == RemoteState::Questionable && n.last_pinged.is_none())
            .take(nodes_to_ping)
            .map(move |n| {
                n.on_ping(now);
                &n.id
            })
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

    pub(crate) fn find_nearest_neighbor<'a>(
        &'a self,
        id: Id,
        bootstrap_nodes: &'a [RemoteNodeId],
        include_all_bootstrap_nodes: bool,
        want: Option<usize>,
    ) -> Vec<&'a RemoteNodeId> {
        let want = want.unwrap_or(8);
        let mut idx = self
            .buckets
            .iter()
            .position(|b| b.range.contains(&id))
            .expect("bucket index should always exist for a node id");
        let mut remote_ids: Vec<&'a RemoteNodeId> = Vec::with_capacity(want);
        while remote_ids.len() < want {
            remote_ids.extend(self.buckets[idx].prioritized_node_ids());
            if idx == 0 {
                break;
            }
            idx -= 1;
        }

        if include_all_bootstrap_nodes {
            remote_ids.extend(bootstrap_nodes);
        } else {
            let bootstrap_nodes_count = want - remote_ids.len();
            if bootstrap_nodes_count > 0 {
                let bootstrap_iter = bootstrap_nodes.iter().take(bootstrap_nodes_count);
                remote_ids.extend(bootstrap_iter);
            }
        }

        remote_ids
    }

    pub(crate) fn on_msg_received<'a>(
        &mut self,
        remote_id: &RemoteNodeId,
        kind: &Kind<'a>,
        now: Instant,
    ) {
        if let Some(id) = remote_id.node_id {
            if id == self.pivot {
                return;
            }

            let bucket = self
                .buckets
                .iter_mut()
                .find(|n| n.range.contains(&id))
                .expect("bucket should always exist for a node");
            if bucket.range.contains(&self.pivot) && bucket.is_full() {
                let bucket = self.buckets.pop().expect("last bucket should always exist");
                let (mut first_bucket, mut second_bucket) = bucket.split();
                if first_bucket.range.contains(&id) {
                    first_bucket.on_msg_received(remote_id, kind, now);
                } else {
                    second_bucket.on_msg_received(remote_id, kind, now);
                }

                if first_bucket.range.contains(&self.pivot) {
                    self.buckets.push(second_bucket);
                    self.buckets.push(first_bucket);
                } else {
                    self.buckets.push(first_bucket);
                    self.buckets.push(second_bucket);
                }
            } else {
                bucket.on_msg_received(remote_id, kind, now);
            }
        }
    }

    pub(crate) fn on_resp_timeout(&mut self, remote_id: &RemoteNodeId) {
        if let Some(id) = remote_id.node_id {
            let bucket = self
                .buckets
                .iter_mut()
                .find(|n| n.range.contains(&id))
                .expect("bucket should always exist for a node");
            bucket.on_resp_timeout(remote_id);
        }
    }

    pub(crate) fn nodes_to_ping(&mut self, now: Instant) -> impl Iterator<Item = &RemoteNodeId> {
        self.buckets
            .iter_mut()
            .flat_map(move |b| b.nodes_to_ping(now))
    }

    // TODO: Should initiate a find_node request for each bucket if a deadline is reached without
    // activity
}

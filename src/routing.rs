use crate::node::{
    remote::{RemoteNode, RemoteNodeId, RemoteState},
    Id,
};
use std::cmp::Ordering;
use std::ops::RangeInclusive;
use std::time::Instant;

#[derive(Clone, Debug, Eq, PartialEq)]
struct Bucket {
    range: RangeInclusive<Id>,
    nodes: Vec<RemoteNode>,
    max_nodes: usize,
    last_changed: Instant,
}

impl Bucket {
    fn new(range: RangeInclusive<Id>, max_nodes: usize) -> Self {
        Bucket {
            range,
            nodes: Vec::new(),
            max_nodes,
            last_changed: Instant::now(),
        }
    }

    fn on_query_received(&mut self, id: &RemoteNodeId) {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.id == *id) {
            node.on_query();
        }
    }

    fn on_response_received(&mut self, id: &RemoteNodeId) {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.id == *id) {
            node.on_response();
        }
    }

    fn on_error_received(&mut self, id: &RemoteNodeId) {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.id == *id) {
            node.on_error();
        }
    }

    fn on_response_timeout(&mut self, id: &RemoteNodeId) {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.id == *id) {
            node.on_response_timeout();
        }
    }

    fn add(&mut self, id: RemoteNodeId, replaced_id: Option<&RemoteNodeId>) {
        assert!({
            if let Some(node_id) = id.node_id {
                self.range.contains(&node_id)
            } else {
                false
            }
        });

        if let Some(replaced_id) = replaced_id {
            assert!({
                if let Some(node_id) = replaced_id.node_id {
                    self.range.contains(&node_id)
                } else {
                    false
                }
            });

            self.nodes.retain(|n| n.id != *replaced_id);
        }

        if self.nodes.len() >= self.max_nodes {
            return;
        }

        self.nodes.push(RemoteNode::new_with_id(id));
        self.last_changed = Instant::now();
    }

    fn split(self) -> (Bucket, Bucket) {
        let middle = self.range.end().middle(self.range.start());

        let mut lower_bucket = Bucket::new(*self.range.start()..=middle, self.max_nodes);
        let mut upper_bucket = Bucket::new(middle..=*self.range.end(), self.max_nodes);

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

        (lower_bucket, upper_bucket)
    }

    fn contains(&self, id: &RemoteNodeId) -> bool {
        self.nodes.iter().find(|n| n.id == *id).is_some()
    }

    fn is_full(&self) -> bool {
        self.nodes.len() >= self.max_nodes
    }

    fn is_all_good_nodes(&self) -> bool {
        self.nodes.iter().all(|n| n.state() == RemoteState::Good)
    }

    fn bad_nodes_remote_ids(&self) -> impl Iterator<Item = &RemoteNodeId> {
        self.nodes.iter().filter_map(|n| {
            if n.state() == RemoteState::Bad {
                Some(&n.id)
            } else {
                None
            }
        })
    }

    fn least_recently_seen_questionable_node_ids(&self) -> Vec<&RemoteNodeId> {
        let mut questionable_nodes = self
            .nodes
            .iter()
            .filter(|n| n.state() == RemoteState::Questionable)
            .collect::<Vec<&RemoteNode>>();
        questionable_nodes.sort_by(|a, b| match (a.last_interaction(), b.last_interaction()) {
            (None, None) => Ordering::Equal,
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (Some(first), Some(second)) => first.cmp(&second),
        });
        questionable_nodes.iter().map(|n| &n.id).collect()
    }

    fn prioritized_node_ids(&self) -> Vec<&RemoteNodeId> {
        let mut nodes = self
            .nodes
            .iter()
            .filter(|n| n.state() == RemoteState::Questionable || n.state() == RemoteState::Good)
            .collect::<Vec<&RemoteNode>>();
        nodes.sort_by(|a, b| match (a.state(), b.state()) {
            (RemoteState::Good, RemoteState::Questionable) => Ordering::Less,
            (RemoteState::Questionable, RemoteState::Good) => Ordering::Greater,
            (RemoteState::Questionable, RemoteState::Questionable) => Ordering::Equal,
            (RemoteState::Good, RemoteState::Good) => Ordering::Equal,
            _ => unreachable!(),
        });
        nodes.iter().map(|n| &n.id).collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Table {
    pivot: Id,
    buckets: Vec<Bucket>,
}

impl Table {
    pub fn new(pivot: Id, max_nodes: usize) -> Self {
        Self {
            pivot,
            buckets: vec![Bucket::new(Id::min()..=Id::max(), max_nodes)],
        }
    }

    pub fn find_nearest_neighbor<'a>(
        &'a self,
        id: Id,
        bootstrap_nodes: &'a [RemoteNodeId],
        include_all_bootstrap_nodes: bool,
        want: Option<usize>,
    ) -> Vec<&'a RemoteNodeId> {
        let want = want.unwrap_or(8);
        if let Some(mut idx) = self.buckets.iter().position(|b| b.range.contains(&id)) {
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
        } else {
            panic!();
        }
    }

    pub fn contains(&self, remote_id: &RemoteNodeId) -> bool {
        if let Some(id) = remote_id.node_id {
            if let Some(bucket) = self.buckets.iter().find(|n| n.range.contains(&id)) {
                return bucket.contains(remote_id);
            }
        }
        false
    }

    pub fn on_query_received(&mut self, remote_id: &RemoteNodeId) {
        if let Some(id) = remote_id.node_id {
            if let Some(bucket) = self.buckets.iter_mut().find(|n| n.range.contains(&id)) {
                bucket.on_query_received(remote_id)
            }
        }
    }

    pub fn on_response_received(&mut self, remote_id: &RemoteNodeId) {
        if let Some(id) = remote_id.node_id {
            if let Some(bucket) = self.buckets.iter_mut().find(|n| n.range.contains(&id)) {
                bucket.on_response_received(remote_id)
            }
        }
    }

    pub fn on_error_received(&mut self, remote_id: &RemoteNodeId) {
        if let Some(id) = remote_id.node_id {
            if let Some(bucket) = self.buckets.iter_mut().find(|n| n.range.contains(&id)) {
                bucket.on_error_received(remote_id)
            }
        }
    }

    pub fn on_response_timeout(&mut self, remote_id: &RemoteNodeId) {
        if let Some(id) = remote_id.node_id {
            if let Some(bucket) = self.buckets.iter_mut().find(|n| n.range.contains(&id)) {
                bucket.on_response_timeout(remote_id)
            }
        }
    }

    pub fn add(&mut self, remote_id: RemoteNodeId, replaced_id: Option<&RemoteNodeId>) {
        if let Some(id) = remote_id.node_id {
            if id == self.pivot {
                return;
            }

            if let Some(bucket) = self.buckets.iter_mut().find(|n| n.range.contains(&id)) {
                if bucket.contains(&remote_id) {
                    return;
                }

                if bucket.range.contains(&self.pivot) && bucket.is_full() {
                    if let Some(bucket) = self.buckets.pop() {
                        let (mut first_bucket, mut second_bucket) = bucket.split();
                        if first_bucket.range.contains(&id) {
                            first_bucket.add(remote_id, replaced_id);
                        } else {
                            second_bucket.add(remote_id, replaced_id);
                        }

                        if first_bucket.range.contains(&self.pivot) {
                            self.buckets.push(second_bucket);
                            self.buckets.push(first_bucket);
                        } else {
                            self.buckets.push(first_bucket);
                            self.buckets.push(second_bucket);
                        }
                    } else {
                        panic!();
                    }
                } else {
                    bucket.add(remote_id, replaced_id)
                }
            }
        }
    }
}

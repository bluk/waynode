// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use cloudburst::dht::{
    krpc::{find_node::RespValues, transaction, CompactAddr, Msg},
    node::{self, AddrId, AddrOptId},
};
use std::collections::{HashMap, HashSet};
use tracing::{error, trace};

#[derive(Debug)]
pub struct FindNodeOp {
    target_id: node::Id,
    closest_nodes: Vec<AddrId<CompactAddr>>,
    max_found_nodes: usize,
    nodes_to_query: HashSet<AddrOptId<CompactAddr>>,
}

impl FindNodeOp {
    pub fn new<T>(target_id: node::Id, max_found_nodes: usize, nodes_to_query: T) -> Self
    where
        T: IntoIterator<Item = AddrOptId<CompactAddr>>,
    {
        Self {
            target_id,
            closest_nodes: Vec::new(),
            max_found_nodes,
            nodes_to_query: nodes_to_query.into_iter().collect(),
        }
    }

    /// Returns the target ID.
    #[must_use]
    #[inline]
    pub fn target_id(&self) -> node::Id {
        self.target_id
    }

    /// Returns if the space is done.
    #[must_use]
    #[inline]
    pub fn is_done(&self) -> bool {
        self.nodes_to_query.is_empty()
    }

    #[inline]
    pub fn nodes_to_query(&self) -> impl Iterator<Item = &AddrOptId<CompactAddr>> {
        self.nodes_to_query.iter()
    }

    #[inline]
    pub fn insert_node_to_query(&mut self, addr_opt_id: AddrOptId<CompactAddr>) -> bool {
        self.nodes_to_query.insert(addr_opt_id)
    }

    #[inline]
    pub fn remove_node_to_query(&mut self, addr_opt_id: &AddrOptId<CompactAddr>) -> bool {
        self.nodes_to_query.remove(addr_opt_id)
    }

    #[must_use]
    #[inline]
    fn max_distance(&self) -> node::Id {
        if self.closest_nodes.len() < self.max_found_nodes {
            node::Id::max()
        } else {
            self.closest_nodes
                .last()
                .map_or(node::Id::max(), |addr_id| {
                    addr_id.id().distance(self.target_id)
                })
        }
    }

    fn try_replace_closest_nodes(&mut self, addr_id: AddrId<CompactAddr>) {
        let new_distance = addr_id.id().distance(self.target_id);
        let is_max_found_nodes = self.closest_nodes.len() == self.max_found_nodes;
        if is_max_found_nodes {
            let max_distance = self.max_distance();
            if new_distance < max_distance {
                self.closest_nodes.pop();
            } else {
                return;
            }
        }

        self.closest_nodes.push(addr_id);
        let target_id = self.target_id;
        self.closest_nodes
            .sort_by_key(|a| a.id().distance(target_id));

        if is_max_found_nodes {
            let max_distance = self.max_distance();
            self.nodes_to_query.retain(|potential_addr_opt_id| {
                potential_addr_opt_id
                    .id()
                    .map_or(true, |id| id.distance(target_id) < max_distance)
            });
        }
    }
}

#[derive(Debug, Default)]
pub struct OpsManager {
    ops: Vec<(FindNodeOp, usize)>,
    tx_to_op: HashMap<transaction::Id, node::Id>,
}

impl OpsManager {
    pub fn insert_op(&mut self, new_op: FindNodeOp) {
        let target_id = new_op.target_id();
        if self.ops.iter().any(|v| v.0.target_id == target_id) {
            return;
        }
        self.ops.push((new_op, 0));
    }

    pub fn insert_tx(&mut self, tx_id: transaction::Id, target_id: node::Id) {
        if let Some(pos) = self.ops.iter().position(|v| v.0.target_id == target_id) {
            if let Some((_op, count)) = self.ops.get_mut(pos) {
                *count += 1;
                self.tx_to_op.insert(tx_id, target_id);
            } else {
                unreachable!();
            }
        } else {
            debug_assert!(false);
        }
    }

    pub fn next_addr_to_query(&mut self) -> Option<(node::Id, AddrOptId<CompactAddr>)> {
        let txs_count: usize = self.ops.iter().map(|v| v.1).sum();
        if txs_count >= 20 {
            return None;
        }

        self.ops.iter_mut().find_map(|(op, count)| {
            if let Some(addr_opt_id) = op.nodes_to_query().next() {
                trace!(addr = %addr_opt_id.addr, node_id = ?addr_opt_id.id, target_id = %op.target_id, tx_count = *count, "returning address to send find node query to");
                Some((op.target_id, *addr_opt_id))
            } else {
                trace!(target_id = %op.target_id, tx_count = *count, "no more addresses to send find node query to");
                None
            }
        })
    }

    pub fn on_recv(
        &mut self,
        addr_opt_id: AddrOptId<CompactAddr>,
        tx_id: transaction::Id,
        msg: &Msg<'_>,
    ) {
        if let Some(target_id) = self.tx_to_op.remove(&tx_id) {
            if let Some(pos) = self.ops.iter().position(|v| v.0.target_id == target_id) {
                if let Some((op, count)) = self.ops.get_mut(pos) {
                    *count -= 1;
                    if let Some(Ok(resp)) = msg.values::<RespValues<'_>>() {
                        on_resp(op, addr_opt_id, &resp);
                        trace!(?tx_id, ?target_id, "processed find node response");
                    } else {
                        error!(?op, "Could not try_from response message");
                    }

                    if *count == 0 && op.is_done() {
                        self.ops.remove(pos);
                        trace!(?target_id, "Removed op");
                    }
                }
            } else {
                error!(?tx_id, ?target_id, "Could not find op for target_id");
            }
        } else {
            trace!(?tx_id, "Could not find target id for tx");
        }
    }

    pub fn on_error(&mut self, _addr_opt_id: AddrOptId<CompactAddr>, tx_id: transaction::Id) {
        if let Some(target_id) = self.tx_to_op.remove(&tx_id) {
            if let Some(pos) = self.ops.iter().position(|v| v.0.target_id == target_id) {
                if let Some((op, count)) = self.ops.get_mut(pos) {
                    *count -= 1;
                    if *count == 0 && op.is_done() {
                        self.ops.remove(pos);
                        trace!(?target_id, "removed find node op");
                    }
                }
            }
        }
    }

    pub fn on_tx_timeout(&mut self, _addr_opt_id: AddrOptId<CompactAddr>, tx_id: transaction::Id) {
        if let Some(target_id) = self.tx_to_op.remove(&tx_id) {
            if let Some(pos) = self.ops.iter().position(|v| v.0.target_id == target_id) {
                if let Some((op, count)) = self.ops.get_mut(pos) {
                    *count -= 1;
                    if *count == 0 && op.is_done() {
                        self.ops.remove(pos);
                        trace!(?target_id, "removed find node op");
                    }
                }
            }
        }
    }

    pub fn cleanup(&mut self) {
        self.ops
            .retain(|(op, tx_count)| *tx_count != 0 || !op.is_done());
    }
}

pub(crate) fn on_resp(
    op: &mut FindNodeOp,
    addr_opt_id: AddrOptId<CompactAddr>,
    resp: &RespValues<'_>,
) {
    if let Some(node_id) = addr_opt_id.id() {
        op.try_replace_closest_nodes(AddrId::new(*addr_opt_id.addr(), node_id));
    }

    let max_distance = op.max_distance();

    if let Some(Ok(nodes)) = resp.nodes() {
        for node in nodes {
            let node_id = node.id();
            let node_distance = node.id().distance(op.target_id);
            if node_distance >= max_distance {
                trace!(
                    ?node_id,
                    ?node_distance,
                    ?max_distance,
                    "distance is greater than maximum distance"
                );
                continue;
            }

            let addr = CompactAddr::from(*node.addr());
            op.nodes_to_query
                .insert(AddrOptId::new(addr, Some(node.id())));
        }
    }

    // if let Some(nodes) = resp.nodes6() {
    //     for node in nodes {
    //         let node_id = node.id();
    //         let node_distance = node.id().distance(op.target_id);
    //         if node_distance >= max_distance {
    //             trace!(
    //                 ?node_id,
    //                 ?node_distance,
    //                 ?max_distance,
    //                 "distance is greater than maximum distance"
    //             );
    //             continue;
    //         }

    //         let addr = CompactAddr::from(*node.addr());
    //         if op.queried_addrs.contains(&addr) {
    //             trace!(?addr, ?node_id, "already saw address");
    //             continue;
    //         }
    //         op.nodes_to_query
    //             .insert(AddrOptId::new(addr, Some(node.id())));
    //     }
    // }
}

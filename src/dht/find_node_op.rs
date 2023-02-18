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
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use tracing::{error, trace};

#[derive(Debug, PartialEq, Eq)]
enum State {
    NotQueried(u8, Instant),
    Querying(u8, transaction::Id),
    SuccessfulQuery,
    DoNotQuery,
}

#[derive(Debug)]
pub struct FindNodeOp {
    target_id: node::Id,
    closest_nodes: Vec<AddrId<CompactAddr>>,
    max_found_nodes: usize,
    addrs: HashMap<AddrOptId<CompactAddr>, State>,
}

impl FindNodeOp {
    pub fn new<T>(target_id: node::Id, max_found_nodes: usize, addrs: T, now: Instant) -> Self
    where
        T: IntoIterator<Item = AddrOptId<CompactAddr>>,
    {
        Self {
            target_id,
            closest_nodes: Vec::new(),
            max_found_nodes,
            addrs: addrs
                .into_iter()
                .map(|addr| (addr, State::NotQueried(0, now)))
                .collect(),
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
        self.addrs.values().all(|s| match *s {
            State::SuccessfulQuery | State::DoNotQuery => true,
            State::NotQueried(_, _) | State::Querying(_, _) => false,
        })
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
            self.addrs.retain(|potential_addr_opt_id, _| {
                potential_addr_opt_id
                    .id()
                    .map_or(true, |id| id.distance(target_id) < max_distance)
            });
        }
    }
}

#[derive(Debug, Default)]
pub struct OpsManager {
    ops: Vec<FindNodeOp>,
    tx_to_op: HashMap<transaction::Id, node::Id>,
}

impl OpsManager {
    pub fn insert_op(&mut self, new_op: FindNodeOp) {
        let target_id = new_op.target_id();
        if self.ops.iter().any(|op| op.target_id == target_id) {
            return;
        }
        self.ops.push(new_op);
    }

    pub fn insert_tx(
        &mut self,
        tx_id: transaction::Id,
        target_id: node::Id,
        addr_opt_id: AddrOptId<CompactAddr>,
    ) {
        if let Some(pos) = self.ops.iter().position(|op| op.target_id == target_id) {
            if let Some(op) = self.ops.get_mut(pos) {
                if let Some(state) = op.addrs.get_mut(&addr_opt_id) {
                    match state {
                        State::NotQueried(attempts, _) => {
                            *state = State::Querying(*attempts + 1, tx_id);
                        }
                        State::Querying(_, _) | State::SuccessfulQuery | State::DoNotQuery => {
                            panic!("unexpected state")
                        }
                    }
                } else {
                    panic!("started tx for op which does not know about address");
                }
                self.tx_to_op.insert(tx_id, target_id);
            } else {
                unreachable!();
            }
        } else {
            debug_assert!(false);
        }
    }

    pub fn next_addr_to_query(
        &mut self,
        now: Instant,
    ) -> Option<(node::Id, AddrOptId<CompactAddr>)> {
        for op in &mut self.ops {
            for (addr_opt_id, state) in &op.addrs {
                match state {
                    State::NotQueried(_, deadline) => {
                        if *deadline <= now {
                            trace!(addr = %addr_opt_id.addr, node_id = ?addr_opt_id.id, target_id = %op.target_id, "returning address to send find node query to");
                            return Some((op.target_id, *addr_opt_id));
                        }
                    }
                    State::Querying(_, _) | State::SuccessfulQuery | State::DoNotQuery => {}
                }
            }
            trace!(target_id = %op.target_id, "no more addresses to send find node query to");
        }

        None
    }

    pub fn on_recv(
        &mut self,
        addr_opt_id: AddrOptId<CompactAddr>,
        tx_id: transaction::Id,
        msg: &Msg<'_>,
        now: Instant,
    ) {
        if let Some(target_id) = self.tx_to_op.remove(&tx_id) {
            if let Some(pos) = self.ops.iter().position(|op| op.target_id == target_id) {
                if let Some(op) = self.ops.get_mut(pos) {
                    if let Some(Ok(resp)) = msg.values::<RespValues<'_>>() {
                        on_resp(op, addr_opt_id, &resp, now);
                        trace!(?tx_id, ?target_id, "processed find node response");
                    } else {
                        error!(?op, "Could not try_from response message");
                    }

                    if let Some(state) = op.addrs.get_mut(&addr_opt_id) {
                        *state = State::SuccessfulQuery;
                    }

                    if op.is_done() {
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

    pub fn on_error(
        &mut self,
        addr_opt_id: AddrOptId<CompactAddr>,
        tx_id: transaction::Id,
        now: Instant,
    ) {
        if let Some(target_id) = self.tx_to_op.remove(&tx_id) {
            if let Some(pos) = self.ops.iter().position(|op| op.target_id == target_id) {
                if let Some(op) = self.ops.get_mut(pos) {
                    if let Some(state) = op.addrs.get_mut(&addr_opt_id) {
                        match state {
                            State::Querying(attempts, _) => {
                                if *attempts < 3 {
                                    *state =
                                        State::NotQueried(*attempts, now + Duration::from_secs(60));
                                } else {
                                    *state = State::DoNotQuery;
                                }
                            }
                            State::DoNotQuery
                            | State::NotQueried(_, _)
                            | State::SuccessfulQuery => {
                                panic!();
                            }
                        }
                    }

                    if op.is_done() {
                        self.ops.remove(pos);
                        trace!(?target_id, "removed find node op");
                    }
                }
            }
        }
    }

    pub fn on_tx_timeout(
        &mut self,
        addr_opt_id: AddrOptId<CompactAddr>,
        tx_id: transaction::Id,
        now: Instant,
    ) {
        if let Some(target_id) = self.tx_to_op.remove(&tx_id) {
            if let Some(pos) = self.ops.iter().position(|op| op.target_id == target_id) {
                if let Some(op) = self.ops.get_mut(pos) {
                    if let Some(state) = op.addrs.get_mut(&addr_opt_id) {
                        match state {
                            State::Querying(attempts, _) => {
                                if *attempts < 3 {
                                    *state =
                                        State::NotQueried(*attempts, now + Duration::from_secs(60));
                                } else {
                                    *state = State::DoNotQuery;
                                }
                            }
                            State::DoNotQuery
                            | State::NotQueried(_, _)
                            | State::SuccessfulQuery => {
                                panic!();
                            }
                        }
                    }

                    if op.is_done() {
                        self.ops.remove(pos);
                        trace!(?target_id, "removed find node op");
                    }
                }
            }
        }
    }

    pub fn cleanup(&mut self) {
        self.ops.retain(|op| !op.is_done());
    }
}

pub(crate) fn on_resp(
    op: &mut FindNodeOp,
    addr_opt_id: AddrOptId<CompactAddr>,
    resp: &RespValues<'_>,
    now: Instant,
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
            op.addrs
                .entry(AddrOptId::new(addr, Some(node.id())))
                .or_insert(State::NotQueried(0, now));
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

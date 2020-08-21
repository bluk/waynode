// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::{
    addr::Addr,
    error::Error,
    krpc::{
        find_node::{FindNodeQueryArgs, FindNodeRespValues},
        RespMsg,
    },
    msg_buffer,
    node::{self, remote::RemoteNodeId},
    transaction,
};
use bt_bencode::Value;
use std::collections::BTreeSet;
use std::convert::TryFrom;
use std::net::SocketAddr;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PotentialNode {
    distance: Option<node::Id>,
    remote_node_id: RemoteNodeId,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub(crate) enum Response<'a> {
    Resp(&'a Value),
    Error(&'a Value),
    Timeout,
}

const CLOSEST_DISTANCES_LEN: usize = 8;
const MAX_CONCURRENT_REQUESTS: usize = 8;

#[derive(Clone, Debug)]
pub(crate) struct FindNodeOp {
    target_id: node::Id,
    closest_distances: [node::Id; CLOSEST_DISTANCES_LEN],

    queried_addrs: BTreeSet<Addr>,
    tx_local_ids: BTreeSet<transaction::LocalId>,
    potential_nodes: Vec<PotentialNode>,
}

impl FindNodeOp {
    pub(crate) fn with_target_id_and_neighbors(
        target_id: node::Id,
        potential_remote_nodes: Vec<RemoteNodeId>,
    ) -> Self {
        let potential_nodes = potential_remote_nodes
            .into_iter()
            .map(|n| PotentialNode {
                distance: n.node_id.map(|node_id| node_id.distance(&target_id)),
                remote_node_id: n,
            })
            .collect();
        Self {
            target_id,
            closest_distances: [node::Id::max(); CLOSEST_DISTANCES_LEN],
            queried_addrs: BTreeSet::new(),
            tx_local_ids: BTreeSet::new(),
            potential_nodes,
        }
    }

    pub(crate) fn is_done(&self) -> bool {
        let ret = self.tx_local_ids.is_empty()
            && (!self.queried_addrs.is_empty() || self.potential_nodes.is_empty());
        if ret {
            debug!("find_node is done. find_node_op={:?}", self);
        }
        ret
    }

    pub(crate) fn start(
        &mut self,
        config: &crate::Config,
        tx_manager: &mut transaction::Manager,
        msg_buffer: &mut msg_buffer::Buffer,
    ) -> Result<(), Error> {
        for potential_node in self
            .potential_nodes
            .drain(0..std::cmp::min(MAX_CONCURRENT_REQUESTS, self.potential_nodes.len()))
        {
            if self
                .queried_addrs
                .contains(&potential_node.remote_node_id.addr)
            {
                continue;
            }

            let tx_local_id = msg_buffer.write_query(
                &FindNodeQueryArgs::new_with_id_and_target(config.id, self.target_id),
                &potential_node.remote_node_id,
                config.default_query_timeout,
                tx_manager,
            )?;
            self.tx_local_ids.insert(tx_local_id);
            self.queried_addrs
                .insert(potential_node.remote_node_id.addr);
        }
        Ok(())
    }

    #[inline]
    fn max_distance(&self) -> node::Id {
        self.closest_distances[CLOSEST_DISTANCES_LEN - 1]
    }

    #[inline]
    fn replace_closest_queried_nodes(&mut self, new_node_id: node::Id) -> node::Id {
        let new_distance = new_node_id.distance(&self.target_id);
        let mut max_distance = self.max_distance();
        if new_distance < max_distance {
            self.closest_distances[CLOSEST_DISTANCES_LEN - 1] = new_distance;
            self.closest_distances.sort_unstable();
            max_distance = self.max_distance();
            self.potential_nodes.retain(|n| {
                n.distance
                    .map(|n_dist| n_dist < max_distance)
                    .unwrap_or(true)
            });
        }
        max_distance
    }

    pub(crate) fn handle<'a>(
        &mut self,
        tx: &transaction::Transaction,
        resp: Response<'a>,
        config: &crate::Config,
        tx_manager: &mut transaction::Manager,
        msg_buffer: &mut msg_buffer::Buffer,
    ) -> Result<(), Error> {
        if !self.tx_local_ids.contains(&tx.local_id) {
            error!("tried handling wrong tx={:?}", tx);
            return Ok(());
        }
        self.tx_local_ids.remove(&tx.local_id);
        debug!(
            "handle target_id={:?} tx={:?} resp={:?}",
            self.target_id, tx, resp
        );

        let max_distance = match resp {
            Response::Resp(resp) => {
                let max_distance = if let Some(node_id) = tx.remote_id.node_id {
                    self.replace_closest_queried_nodes(node_id)
                } else {
                    self.max_distance()
                };

                if let Some(nodes) = resp
                    .values()
                    .and_then(|values| FindNodeRespValues::try_from(values).ok())
                    .and_then(|find_node_resp| {
                        find_node_resp.nodes().map(|nodes| {
                            nodes
                                .iter()
                                .map(|cn| PotentialNode {
                                    distance: Some(cn.id.distance(&self.target_id)),
                                    remote_node_id: RemoteNodeId {
                                        addr: Addr::SocketAddr(SocketAddr::V4(cn.addr)),
                                        node_id: Some(cn.id),
                                    },
                                })
                                .filter(|pn| {
                                    pn.distance
                                        .map(|n_dist| n_dist < max_distance)
                                        .unwrap_or(true)
                                        && !self.queried_addrs.contains(&pn.remote_node_id.addr)
                                })
                                .collect::<Vec<_>>()
                        })
                    })
                {
                    if !nodes.is_empty() {
                        debug!("new potential nodes={:?}", nodes);
                        self.potential_nodes.extend(nodes);
                    }
                }
                max_distance
            }
            Response::Error(_) | Response::Timeout => self.max_distance(),
        };

        let outstanding_queries = self.tx_local_ids.len();
        if outstanding_queries < MAX_CONCURRENT_REQUESTS {
            let mut queries_to_write = MAX_CONCURRENT_REQUESTS - outstanding_queries;
            while let Some(potential_node) = self.potential_nodes.pop() {
                if self
                    .queried_addrs
                    .contains(&potential_node.remote_node_id.addr)
                {
                    continue;
                }

                if potential_node
                    .distance
                    .map(|node_dist| node_dist >= max_distance)
                    .unwrap_or(false)
                {
                    continue;
                }

                let tx_local_id = msg_buffer.write_query(
                    &FindNodeQueryArgs::new_with_id_and_target(config.id, self.target_id),
                    &potential_node.remote_node_id,
                    config.default_query_timeout,
                    tx_manager,
                )?;
                self.tx_local_ids.insert(tx_local_id);
                self.queried_addrs
                    .insert(potential_node.remote_node_id.addr);

                queries_to_write -= 1;
                if queries_to_write == 0 {
                    break;
                }
            }
        }

        debug!(
            "outstanding tx_local_ids.len={} potential_nodes.len={} queried_addr.len={} closest_distances={:?} ",
            self.tx_local_ids.len(),
            self.potential_nodes.len(),
            self.queried_addrs.len(),
            self.closest_distances
        );

        Ok(())
    }
}

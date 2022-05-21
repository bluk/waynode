// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::SupportedAddr;

use bt_bencode::Value;
use cloudburst::dht::{
    krpc::{
        find_node::RespValues,
        transaction::{self, Transaction},
        RespMsg,
    },
    node::{self, AddrOptId, Id},
};
use std::{collections::BTreeSet, convert::TryFrom, net::SocketAddr};

#[derive(Clone, Copy, Debug)]
pub(crate) enum Response<'a> {
    Resp(&'a Value),
    Error(&'a Value),
    Timeout,
}

const CLOSEST_DISTANCES_LEN: usize = 16;
const MAX_CONCURRENT_REQUESTS: usize = 8;

#[derive(Debug)]
struct AddrInfo {
    closest_distances: [node::Id; CLOSEST_DISTANCES_LEN],
    potential_addr_opt_ids: Vec<AddrOptId<SocketAddr>>,
    queried_addrs: BTreeSet<SocketAddr>,
}

impl AddrInfo {
    fn new<I>(potential_addr_opt_ids: I) -> Self
    where
        I: IntoIterator<Item = AddrOptId<SocketAddr>>,
    {
        Self {
            closest_distances: [node::Id::max(); CLOSEST_DISTANCES_LEN],
            potential_addr_opt_ids: potential_addr_opt_ids.into_iter().collect(),
            queried_addrs: BTreeSet::new(),
        }
    }

    fn max_distance(&self) -> node::Id {
        self.closest_distances[CLOSEST_DISTANCES_LEN - 1]
    }

    fn is_done(&self) -> bool {
        self.potential_addr_opt_ids.is_empty()
    }

    fn replace_closest_queried_nodes(&mut self, target_id: node::Id, new_node_id: node::Id) {
        let new_distance = new_node_id.distance(target_id);
        let max_distance = self.max_distance();
        if new_distance < max_distance {
            self.closest_distances[CLOSEST_DISTANCES_LEN - 1] = new_distance;
            self.closest_distances.sort_unstable();
            self.potential_addr_opt_ids.retain(|potential_addr_opt_id| {
                potential_addr_opt_id
                    .id()
                    .map_or(true, |id| id.distance(target_id) < max_distance)
            });
        }
    }

    fn extend_potential_addrs<I>(&mut self, target_id: Id, potential_addr_opt_ids: I)
    where
        I: IntoIterator<Item = AddrOptId<SocketAddr>>,
    {
        let max_distance = self.max_distance();

        for potential_addr in potential_addr_opt_ids.into_iter().filter(|n| {
            n.id()
                .map_or(true, |id| id.distance(target_id) <= max_distance)
        }) {
            if !self.queried_addrs.contains(potential_addr.addr())
                && !self.potential_addr_opt_ids.contains(&potential_addr)
            {
                self.potential_addr_opt_ids.push(potential_addr);
            }
        }
    }

    fn pop_potential_addr(&mut self) -> Option<AddrOptId<SocketAddr>> {
        while let Some(potential_addr_opt_id) = self.potential_addr_opt_ids.pop() {
            if !self.queried_addrs.insert(*potential_addr_opt_id.addr()) {
                return Some(potential_addr_opt_id);
            }
        }

        None
    }
}

#[derive(Debug)]
pub struct FindNodeOp {
    target_id: node::Id,
    supported_addr: SupportedAddr,
    addr_space: AddrInfo,
    tx_ids: BTreeSet<transaction::Id>,
}

impl FindNodeOp {
    pub fn new<T>(
        target_id: node::Id,
        supported_addr: SupportedAddr,
        potential_addr_opt_ids: T,
    ) -> Self
    where
        T: IntoIterator<Item = AddrOptId<SocketAddr>>,
    {
        let addr_space = AddrInfo::new(potential_addr_opt_ids);

        Self {
            target_id,
            supported_addr,
            addr_space,
            tx_ids: BTreeSet::new(),
        }
    }

    /// Returns the target ID.
    #[must_use]
    pub fn target_id(&self) -> node::Id {
        self.target_id
    }

    /// Returns the supported address space.
    #[must_use]
    pub fn supported_addr(&self) -> SupportedAddr {
        self.supported_addr
    }

    /// Returns if the space is done.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.tx_ids.is_empty() && self.addr_space.is_done()
    }

    pub fn next_addr_id(&mut self) -> Option<AddrOptId<SocketAddr>> {
        self.addr_space.pop_potential_addr()
    }

    // pub(crate) fn start<R>(
    //     &mut self,
    //     config: &crate::Config,
    //     tx_manager: &mut Transactions<SocketAddr, transaction::Id, std::time::Instant>,
    //     msg_buffer: &mut msg_buffer::Buffer<transaction::Id>,
    //     rng: &mut R,
    // ) -> Result<(), Error>
    // where
    //     R: rand::Rng,
    // {
    //     let mut count = 0;
    //     while let Some(potential_addr_opt_id) = self.addr_space.pop_potential_addr() {
    //         let tx_id = crate::krpc::transaction::next_tx_id(tx_manager, rng).unwrap();
    //         msg_buffer.write_query(
    //             tx_id,
    //             &QueryArgs::new(config.local_id, self.target_id),
    //             potential_addr_opt_id.addr_opt_id,
    //             config.default_query_timeout,
    //             config.client_version(),
    //         )?;
    //         self.tx_ids.insert(tx_id);

    //         count += 1;
    //         if count >= MAX_CONCURRENT_REQUESTS {
    //             break;
    //         }
    //     }

    //     Ok(())
    // }

    pub(crate) fn handle<'a>(
        &mut self,
        tx: &Transaction<std::net::SocketAddr, transaction::Id, std::time::Instant>,
        resp: Response<'a>,
    ) {
        if !self.tx_ids.contains(tx.tx_id()) {
            return;
        }
        self.tx_ids.remove(tx.tx_id());

        match resp {
            Response::Resp(resp) => {
                if let Some(node_id) = tx.addr_opt_id().id() {
                    self.addr_space
                        .replace_closest_queried_nodes(self.target_id, node_id);
                }

                if let Some(find_node_resp) = resp
                    .values()
                    .and_then(|values| RespValues::try_from(values).ok())
                {
                    if let Some(nodes) = find_node_resp.nodes() {
                        self.addr_space.extend_potential_addrs(
                            self.target_id,
                            nodes.iter().map(|addr_id| {
                                AddrOptId::new((*addr_id.addr()).into(), Some(addr_id.id()))
                            }),
                        );
                    }

                    if let Some(nodes6) = find_node_resp.nodes6() {
                        self.addr_space.extend_potential_addrs(
                            self.target_id,
                            nodes6.iter().map(|addr_id| {
                                AddrOptId::new((*addr_id.addr()).into(), Some(addr_id.id()))
                            }),
                        );
                    }
                }
            }
            Response::Error(_) | Response::Timeout => {}
        };
    }
}

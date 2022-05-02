// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::{krpc::transaction, msg_buffer, SupportedAddr};

use bt_bencode::Value;
use cloudburst::dht::{
    krpc::{
        find_node::{QueryArgs, RespValues},
        Error, RespMsg,
    },
    node::{self, AddrId, AddrOptId, Id},
};
use std::{
    collections::BTreeSet,
    convert::TryFrom,
    net::{SocketAddr, SocketAddrV4, SocketAddrV6},
};

#[derive(Debug)]
struct PotentialAddrOptId<A>
where
    A: Into<SocketAddr>,
{
    distance: Option<node::Id>,
    addr_opt_id: AddrOptId<A>,
}

impl From<PotentialAddrOptId<SocketAddrV4>> for PotentialAddrOptId<SocketAddr> {
    fn from(
        potential_addr_info: PotentialAddrOptId<SocketAddrV4>,
    ) -> PotentialAddrOptId<SocketAddr> {
        PotentialAddrOptId {
            distance: potential_addr_info.distance,
            addr_opt_id: AddrOptId::new(
                (*potential_addr_info.addr_opt_id.addr()).into(),
                potential_addr_info.addr_opt_id.id(),
            ),
        }
    }
}

impl From<PotentialAddrOptId<SocketAddrV6>> for PotentialAddrOptId<SocketAddr> {
    fn from(
        potential_addr_info: PotentialAddrOptId<SocketAddrV6>,
    ) -> PotentialAddrOptId<SocketAddr> {
        PotentialAddrOptId {
            distance: potential_addr_info.distance,
            addr_opt_id: AddrOptId::new(
                (*potential_addr_info.addr_opt_id.addr()).into(),
                potential_addr_info.addr_opt_id.id(),
            ),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Response<'a> {
    Resp(&'a Value),
    Error(&'a Value),
    Timeout,
}

const CLOSEST_DISTANCES_LEN: usize = 16;
const MAX_CONCURRENT_REQUESTS: usize = 8;

#[derive(Debug)]
struct AddrInfo<A>
where
    A: Into<SocketAddr>,
{
    closest_distances: [node::Id; CLOSEST_DISTANCES_LEN],
    potential_addr_opt_ids: Vec<PotentialAddrOptId<A>>,
    queried_addrs: BTreeSet<A>,
}

impl<A> AddrInfo<A>
where
    A: Into<SocketAddr>,
{
    fn new<I>(potential_addr_opt_ids: I) -> Self
    where
        I: IntoIterator<Item = PotentialAddrOptId<A>>,
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
                    .distance
                    .map_or(true, |potential_dist| potential_dist < max_distance)
            });
        }
    }

    fn extend_potential_addrs<'a, I>(&'a mut self, target_id: Id, potential_addr_opt_ids: I)
    where
        I: IntoIterator<Item = &'a AddrId<A>>,
        A: Ord + Clone,
    {
        let max_distance = self.max_distance();

        let potential_addr_opt_ids = potential_addr_opt_ids
            .into_iter()
            .filter(|n| !self.queried_addrs.contains(n.addr()))
            .map(|n| PotentialAddrOptId {
                distance: Some(n.id().distance(target_id)),
                addr_opt_id: AddrOptId::new(n.addr().clone(), Some(n.id())),
            })
            .filter(|potential_addr| {
                potential_addr
                    .distance
                    .map_or(true, |potential_dist| potential_dist < max_distance)
            })
            .collect::<Vec<_>>();
        self.potential_addr_opt_ids.extend(potential_addr_opt_ids);
    }

    fn pop_potential_addr(&mut self) -> Option<PotentialAddrOptId<A>>
    where
        A: Ord + Clone,
    {
        let max_distance = self.max_distance();
        while let Some(potential_addr_opt_id) = self.potential_addr_opt_ids.pop() {
            if self
                .queried_addrs
                .contains(potential_addr_opt_id.addr_opt_id.addr())
            {
                continue;
            }

            if potential_addr_opt_id
                .distance
                .map_or(true, |node_dist| node_dist < max_distance)
            {
                self.queried_addrs
                    .insert(potential_addr_opt_id.addr_opt_id.addr().clone());

                return Some(potential_addr_opt_id);
            }
        }

        None
    }
}

#[derive(Debug)]
enum AddrSpace {
    V4(Box<AddrInfo<SocketAddrV4>>),
    V6(Box<AddrInfo<SocketAddrV6>>),
    V4AndV6(Box<AddrInfo<SocketAddrV4>>, Box<AddrInfo<SocketAddrV6>>),
}

impl AddrSpace {
    fn is_done(&self) -> bool {
        match self {
            AddrSpace::V4(info) => info.is_done(),
            AddrSpace::V6(info) => info.is_done(),
            AddrSpace::V4AndV6(info_v4, info_v6) => info_v4.is_done() && info_v6.is_done(),
        }
    }

    #[inline]
    fn replace_closest_queried_nodes<A>(
        &mut self,
        addr: A,
        target_id: node::Id,
        new_node_id: node::Id,
    ) where
        A: Into<SocketAddr>,
    {
        match addr.into() {
            SocketAddr::V4(_) => match self {
                AddrSpace::V4(addr_info) | AddrSpace::V4AndV6(addr_info, _) => {
                    addr_info.replace_closest_queried_nodes(target_id, new_node_id);
                }
                AddrSpace::V6(_) => {}
            },
            SocketAddr::V6(_) => match self {
                AddrSpace::V6(addr_info) | AddrSpace::V4AndV6(_, addr_info) => {
                    addr_info.replace_closest_queried_nodes(target_id, new_node_id);
                }
                AddrSpace::V4(_) => {}
            },
        }
    }

    fn pop_potential_addr(&mut self) -> Option<PotentialAddrOptId<SocketAddr>> {
        match self {
            AddrSpace::V4(addr_info) => {
                addr_info.pop_potential_addr().map(std::convert::Into::into)
            }
            AddrSpace::V6(addr_info) => {
                addr_info.pop_potential_addr().map(std::convert::Into::into)
            }
            AddrSpace::V4AndV6(addr_info_v4, addr_info_v6) => addr_info_v4
                .pop_potential_addr()
                .map(std::convert::Into::into)
                .or_else(|| {
                    addr_info_v6
                        .pop_potential_addr()
                        .map(std::convert::Into::into)
                }),
        }
    }
}

#[derive(Debug)]
pub(crate) struct FindNodeOp {
    target_id: node::Id,
    addr_space: AddrSpace,

    tx_ids: BTreeSet<transaction::Id>,
}

impl FindNodeOp {
    pub(crate) fn new<A, T>(
        supported_addr: SupportedAddr,
        target_id: node::Id,
        potential_addr_opt_ids: T,
    ) -> Self
    where
        T: IntoIterator<Item = AddrOptId<A>>,
        A: Into<SocketAddr> + Clone,
    {
        let mut potential_addr_opt_ids_v4 = Vec::new();
        let mut potential_addr_opt_ids_v6 = Vec::new();

        for addr_opt_id in potential_addr_opt_ids {
            match (*addr_opt_id.addr()).clone().into() {
                SocketAddr::V4(addr) => potential_addr_opt_ids_v4.push(PotentialAddrOptId {
                    distance: addr_opt_id.id().map(|node_id| node_id.distance(target_id)),
                    addr_opt_id: AddrOptId::new(addr, addr_opt_id.id()),
                }),
                SocketAddr::V6(addr) => potential_addr_opt_ids_v6.push(PotentialAddrOptId {
                    distance: addr_opt_id.id().map(|node_id| node_id.distance(target_id)),
                    addr_opt_id: AddrOptId::new(addr, addr_opt_id.id()),
                }),
            }
        }

        let addr_space = match supported_addr {
            SupportedAddr::Ipv4 => {
                AddrSpace::V4(Box::new(AddrInfo::new(potential_addr_opt_ids_v4)))
            }
            SupportedAddr::Ipv6 => {
                AddrSpace::V6(Box::new(AddrInfo::new(potential_addr_opt_ids_v6)))
            }
            SupportedAddr::Ipv4AndIpv6 => AddrSpace::V4AndV6(
                Box::new(AddrInfo::new(potential_addr_opt_ids_v4)),
                Box::new(AddrInfo::new(potential_addr_opt_ids_v6)),
            ),
        };

        Self {
            target_id,
            addr_space,
            tx_ids: BTreeSet::new(),
        }
    }

    pub(crate) fn is_done(&self) -> bool {
        let ret = self.tx_ids.is_empty() && self.addr_space.is_done();
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
        let mut count = 0;
        while let Some(potential_addr_opt_id) = self.addr_space.pop_potential_addr() {
            let tx_id = msg_buffer.write_query(
                &QueryArgs::new(config.local_id, self.target_id),
                potential_addr_opt_id.addr_opt_id,
                config.default_query_timeout,
                config.client_version(),
                tx_manager,
            )?;
            self.tx_ids.insert(tx_id);

            count += 1;
            if count >= MAX_CONCURRENT_REQUESTS {
                break;
            }
        }

        Ok(())
    }

    pub(crate) fn handle<'a>(
        &mut self,
        tx: &transaction::Transaction,
        resp: Response<'a>,
        config: &crate::Config,
        tx_manager: &mut transaction::Manager,
        msg_buffer: &mut msg_buffer::Buffer,
    ) -> Result<(), Error> {
        if !self.tx_ids.contains(&tx.tx_id) {
            return Ok(());
        }
        self.tx_ids.remove(&tx.tx_id);
        debug!(
            "handle target_id={:?} tx={:?} resp={:?}",
            self.target_id, tx, resp
        );

        match resp {
            Response::Resp(resp) => {
                if let Some(node_id) = tx.addr_opt_id.id() {
                    self.addr_space.replace_closest_queried_nodes(
                        *tx.addr_opt_id.addr(),
                        self.target_id,
                        node_id,
                    );
                }

                if let Some(find_node_resp) = resp
                    .values()
                    .and_then(|values| RespValues::try_from(values).ok())
                {
                    if let Some(nodes) = find_node_resp.nodes() {
                        match &mut self.addr_space {
                            AddrSpace::V4(addr_info) | AddrSpace::V4AndV6(addr_info, _) => {
                                addr_info.extend_potential_addrs(self.target_id, nodes);
                            }
                            AddrSpace::V6(_) => {}
                        }
                    }

                    if let Some(nodes6) = find_node_resp.nodes6() {
                        match &mut self.addr_space {
                            AddrSpace::V6(addr_info) | AddrSpace::V4AndV6(_, addr_info) => {
                                addr_info.extend_potential_addrs(self.target_id, nodes6);
                            }
                            AddrSpace::V4(_) => {}
                        }
                    }
                }
            }
            Response::Error(_) | Response::Timeout => {}
        };

        let outstanding_queries = self.tx_ids.len();
        if outstanding_queries < MAX_CONCURRENT_REQUESTS {
            let mut queries_to_write = MAX_CONCURRENT_REQUESTS - outstanding_queries;
            while let Some(potential_node) = self.addr_space.pop_potential_addr() {
                let tx_id = msg_buffer.write_query(
                    &QueryArgs::new(config.local_id, self.target_id),
                    potential_node.addr_opt_id,
                    config.default_query_timeout,
                    config.client_version(),
                    tx_manager,
                )?;
                self.tx_ids.insert(tx_id);

                queries_to_write -= 1;
                if queries_to_write == 0 {
                    break;
                }
            }
        }

        // debug!(
        //     "target_id={:?} outstanding tx_ids.len={} potential_addr_opt_ids.len={} queried_addr.len={} closest_distances={:?} ",
        //     self.target_id,
        //     self.tx_ids.len(),
        //     self.potential_addr_opt_ids.len(),
        //     self.queried_addrs.len(),
        //     self.closest_distances
        // );

        Ok(())
    }
}

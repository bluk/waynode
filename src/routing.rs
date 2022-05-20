// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use cloudburst::dht::{
    krpc::Ty,
    node::{AddrId, Id},
    routing::{Node, Table},
};
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};

#[derive(Debug)]
pub enum RoutingTable<TxId, Instant> {
    Ipv4(Table<SocketAddrV4, TxId, Instant>),
    Ipv6(Table<SocketAddrV6, TxId, Instant>),
    Ipv4AndIpv6(
        Table<SocketAddrV4, TxId, Instant>,
        Table<SocketAddrV6, TxId, Instant>,
    ),
}

impl<TxId, Instant> RoutingTable<TxId, Instant>
where
    Instant: cloudburst::time::Instant,
{
    pub(crate) fn try_insert_addr_ids<'a, I>(
        &mut self,
        addrs: I,
        refresh_deadline: &Instant,
        now: &Instant,
    ) where
        I: IntoIterator<Item = &'a AddrId<SocketAddr>>,
        TxId: Clone,
    {
        for addr_id in addrs {
            match addr_id.addr() {
                SocketAddr::V4(addr) => match self {
                    RoutingTable::Ipv4(routing_table)
                    | RoutingTable::Ipv4AndIpv6(routing_table, _) => {
                        routing_table.try_insert(
                            AddrId::new(*addr, addr_id.id()),
                            refresh_deadline.clone(),
                            now,
                        );
                    }
                    RoutingTable::Ipv6(_) => {}
                },
                SocketAddr::V6(addr) => match self {
                    RoutingTable::Ipv4(_) => {}
                    RoutingTable::Ipv6(routing_table)
                    | RoutingTable::Ipv4AndIpv6(_, routing_table) => {
                        routing_table.try_insert(
                            AddrId::new(*addr, addr_id.id()),
                            refresh_deadline.clone(),
                            now,
                        );
                    }
                },
            }
        }
    }

    pub(crate) fn on_msg_received<'a>(
        &mut self,
        addr_id: AddrId<SocketAddr>,
        kind: &Ty<'a>,
        tx_id: Option<&TxId>,
        refresh_deadline: Instant,
        now: &Instant,
    ) where
        TxId: Clone + PartialEq,
    {
        match addr_id.addr() {
            SocketAddr::V4(addr) => match self {
                RoutingTable::Ipv4(routing_table) | RoutingTable::Ipv4AndIpv6(routing_table, _) => {
                    routing_table.on_msg_received(
                        AddrId::new(*addr, addr_id.id()),
                        kind,
                        tx_id,
                        refresh_deadline,
                        now,
                    );
                }
                RoutingTable::Ipv6(_) => {}
            },
            SocketAddr::V6(addr) => match self {
                RoutingTable::Ipv4(_) => {}
                RoutingTable::Ipv6(routing_table) | RoutingTable::Ipv4AndIpv6(_, routing_table) => {
                    routing_table.on_msg_received(
                        AddrId::new(*addr, addr_id.id()),
                        kind,
                        tx_id,
                        refresh_deadline,
                        now,
                    );
                }
            },
        }
    }

    pub(crate) fn on_resp_timeout(
        &mut self,
        addr_id: AddrId<SocketAddr>,
        tx_id: &TxId,
        refresh_deadline: Instant,
        now: &Instant,
    ) where
        TxId: PartialEq,
    {
        match addr_id.addr() {
            SocketAddr::V4(addr) => match self {
                RoutingTable::Ipv4(routing_table) | RoutingTable::Ipv4AndIpv6(routing_table, _) => {
                    routing_table.on_resp_timeout(
                        AddrId::new(*addr, addr_id.id()),
                        tx_id,
                        refresh_deadline,
                        now,
                    );
                }
                RoutingTable::Ipv6(_) => {}
            },
            SocketAddr::V6(addr) => match self {
                RoutingTable::Ipv4(_) => {}
                RoutingTable::Ipv6(routing_table) | RoutingTable::Ipv4AndIpv6(_, routing_table) => {
                    routing_table.on_resp_timeout(
                        AddrId::new(*addr, addr_id.id()),
                        tx_id,
                        refresh_deadline,
                        now,
                    );
                }
            },
        }
    }

    pub(crate) fn timeout(&self) -> Option<Instant> {
        match self {
            RoutingTable::Ipv4(routing_table) => routing_table.timeout(),
            RoutingTable::Ipv6(routing_table) => routing_table.timeout(),
            RoutingTable::Ipv4AndIpv6(routing_table_v4, routing_table_v6) => {
                [routing_table_v4.timeout(), routing_table_v6.timeout()]
                    .as_ref()
                    .iter()
                    .filter_map(std::clone::Clone::clone)
                    .min()
            }
        }
    }

    pub fn find_neighbors_ipv4(
        &self,
        id: Id,
        now: &Instant,
    ) -> impl Iterator<Item = AddrId<SocketAddrV4>> {
        let mut iter = match self {
            RoutingTable::Ipv4(routing_table) | RoutingTable::Ipv4AndIpv6(routing_table, _) => {
                Some(routing_table.find_neighbors(id, now))
            }
            RoutingTable::Ipv6(_) => None,
        };
        core::iter::from_fn(move || iter.as_mut().and_then(std::iter::Iterator::next))
    }

    pub fn find_neighbors_ipv6(
        &self,
        id: Id,
        now: &Instant,
    ) -> impl Iterator<Item = AddrId<SocketAddrV6>> {
        let mut iter = match self {
            RoutingTable::Ipv6(routing_table) | RoutingTable::Ipv4AndIpv6(_, routing_table) => {
                Some(routing_table.find_neighbors(id, now))
            }
            RoutingTable::Ipv4(_) => None,
        };
        core::iter::from_fn(move || iter.as_mut().and_then(std::iter::Iterator::next))
    }

    pub fn find_node_to_ping_ipv4(
        &mut self,
        now: &Instant,
    ) -> Option<&mut Node<std::net::SocketAddrV4, TxId, Instant>> {
        match self {
            RoutingTable::Ipv4(routing_table) | RoutingTable::Ipv4AndIpv6(routing_table, _) => {
                routing_table.find_node_to_ping(now)
            }
            RoutingTable::Ipv6(_) => None,
        }
    }

    pub fn find_node_to_ping_ipv6(
        &mut self,
        now: &Instant,
    ) -> Option<&mut Node<std::net::SocketAddrV6, TxId, Instant>> {
        match self {
            RoutingTable::Ipv6(routing_table) | RoutingTable::Ipv4AndIpv6(_, routing_table) => {
                routing_table.find_node_to_ping(now)
            }
            RoutingTable::Ipv4(_) => None,
        }
    }
}

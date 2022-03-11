// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! A node is a client/server which implements the DHT protocol.

use core::{
    cmp::Ordering,
    cmp::{Ord, PartialOrd},
    convert::TryFrom,
    fmt,
};
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

#[derive(Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
/// A 160-bit value which is used to identify a node's position within the distributed hash table.
pub struct Id(pub(crate) [u8; 20]);

impl Id {
    pub(crate) const fn min() -> Id {
        Id([0; 20])
    }

    pub(crate) const fn max() -> Id {
        Id([0xff; 20])
    }

    /// Determines the distance between this `Id` and the `Id` argument.
    ///
    /// The distance is calculated by XORing the corresponding bytes.
    ///
    /// # Example
    ///
    /// ```
    /// # fn main() -> Result<(), std::io::Error> {
    /// use sloppy::node::Id;
    ///
    /// let id1 = Id::from([
    ///     0xff, 0x00, 0xff, 0x00, 0xff,
    ///     0xff, 0xff, 0xff, 0x00, 0xf0,
    ///     0x0f, 0x00, 0x0f, 0xf0, 0x00,
    ///     0xff, 0x01, 0x10, 0xaa, 0xab
    /// ]);
    ///
    /// assert_eq!(id1.distance(id1), Id::from([0x00; 20]));
    ///
    /// let id2 = Id::from([
    ///     0x01, 0x02, 0x03, 0x04, 0x05,
    ///     0x06, 0x07, 0x08, 0x09, 0x0a,
    ///     0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    ///     0x10, 0x11, 0x12, 0x13, 0x14
    /// ]);
    ///
    /// assert_eq!(id1.distance(id2), Id::from([
    ///     0xfe, 0x02, 0xfc, 0x04, 0xfa,
    ///     0xf9, 0xf8, 0xf7, 0x09, 0xfa,
    ///     0x04, 0x0c, 0x02, 0xfe, 0x0f,
    ///     0xef, 0x10, 0x02, 0xb9, 0xbf
    /// ]));
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn distance(&self, other: Id) -> Id {
        let mut data = [0; 20];
        for (idx, val) in self.0.iter().enumerate() {
            data[idx] = val ^ other.0[idx];
        }
        Id(data)
    }

    /// Instantiates an Id with a random value.
    ///
    /// It may be useful to generate a random `Id` when initializing a DHT node
    /// for the first time.
    pub fn rand<R>(rng: &mut R) -> Result<Id, rand::Error>
    where
        R: rand::Rng,
    {
        let mut arr: [u8; 20] = [0; 20];
        rng.try_fill(&mut arr[..])?;
        Ok(Id(arr))
    }
}

impl From<[u8; 20]> for Id {
    fn from(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }
}

impl From<Id> for Vec<u8> {
    fn from(id: Id) -> Self {
        Vec::from(id.0)
    }
}

impl From<Id> for [u8; 20] {
    fn from(id: Id) -> Self {
        id.0
    }
}

impl From<LocalId> for Id {
    fn from(local_id: LocalId) -> Id {
        local_id.0
    }
}

impl Ord for Id {
    fn cmp(&self, other: &Self) -> Ordering {
        for idx in 0..self.0.len() {
            let ord = self.0[idx].cmp(&other.0[idx]);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    }
}

impl PartialOrd for Id {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TryFrom<&[u8]> for Id {
    type Error = core::array::TryFromSliceError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        <[u8; 20]>::try_from(value).map(Id)
    }
}

fmt_byte_array!(Id);

/// An `Id` that identifies the local node.
///
/// It is a newtype to prevent using the local node ID in KRPC message arguments when a target Id is desired (or vice-versa).
#[derive(Debug, Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct LocalId(pub(crate) Id);

impl LocalId {
    pub fn new(id: Id) -> Self {
        Self(id)
    }
}

impl From<Id> for LocalId {
    fn from(id: Id) -> LocalId {
        LocalId(id)
    }
}

/// A node's network address and Id.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct AddrId<A> {
    addr: A,
    id: Id,
}

impl<A> AddrId<A> {
    /// Instantiate with a network address and an Id.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn main() -> Result<(), std::io::Error> {
    /// use std::net::ToSocketAddrs;
    /// use sloppy::node::{AddrId, Id};
    ///
    /// let socket_addr = "example.com:6881".to_socket_addrs().unwrap().next().unwrap();
    /// let node_id = Id::rand(&mut rand::thread_rng()).unwrap();
    /// let addr_id = AddrId::new(socket_addr, node_id);
    /// assert_eq!(*addr_id.addr(), socket_addr);
    /// assert_eq!(addr_id.id(), node_id);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(addr: A, id: Id) -> Self {
        Self { addr, id }
    }

    /// Returns the network address.
    pub fn addr(&self) -> &A {
        &self.addr
    }

    /// Returns the node Id.
    pub fn id(&self) -> Id {
        self.id
    }
}

impl From<AddrId<SocketAddrV4>> for AddrId<SocketAddr> {
    fn from(addr_id: AddrId<SocketAddrV4>) -> AddrId<SocketAddr> {
        AddrId::new(SocketAddr::V4(*addr_id.addr()), addr_id.id())
    }
}

impl From<AddrId<SocketAddrV6>> for AddrId<SocketAddr> {
    fn from(addr_id: AddrId<SocketAddrV6>) -> AddrId<SocketAddr> {
        AddrId::new(SocketAddr::V6(*addr_id.addr()), addr_id.id())
    }
}

impl From<&AddrId<SocketAddrV4>> for AddrId<SocketAddr> {
    fn from(addr_id: &AddrId<SocketAddrV4>) -> AddrId<SocketAddr> {
        AddrId::new(SocketAddr::V4(*addr_id.addr()), addr_id.id())
    }
}

impl From<&AddrId<SocketAddrV6>> for AddrId<SocketAddr> {
    fn from(addr_id: &AddrId<SocketAddrV6>) -> AddrId<SocketAddr> {
        AddrId::new(SocketAddr::V6(*addr_id.addr()), addr_id.id())
    }
}

impl From<AddrId<SocketAddrV4>> for SocketAddrV4 {
    fn from(addr_id: AddrId<SocketAddrV4>) -> SocketAddrV4 {
        *addr_id.addr()
    }
}

impl From<AddrId<SocketAddrV6>> for SocketAddrV6 {
    fn from(addr_id: AddrId<SocketAddrV6>) -> SocketAddrV6 {
        *addr_id.addr()
    }
}

impl From<AddrId<SocketAddr>> for SocketAddr {
    fn from(addr_id: AddrId<SocketAddr>) -> SocketAddr {
        *addr_id.addr()
    }
}

impl From<AddrId<SocketAddrV4>> for SocketAddr {
    fn from(addr_id: AddrId<SocketAddrV4>) -> SocketAddr {
        SocketAddr::V4(*addr_id.addr())
    }
}

impl From<AddrId<SocketAddrV6>> for SocketAddr {
    fn from(addr_id: AddrId<SocketAddrV6>) -> SocketAddr {
        SocketAddr::V6(*addr_id.addr())
    }
}

/// A node's network address and optional Id.
///
/// In order to send messages to other nodes, a network address is required.
///
/// A node's Id may not be known because the node responded with an invalid or
/// missing `Id` or if the local DHT node is being bootstrapped with only network
/// addresses.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct AddrOptId<A> {
    addr: A,
    id: Option<Id>,
}

impl<A> AddrOptId<A> {
    /// Instantiate with a network address and an optional Id.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn main() -> Result<(), std::io::Error> {
    /// use std::net::ToSocketAddrs;
    /// use sloppy::node::{AddrOptId, Id};
    ///
    /// let socket_addr = "example.com:6881".to_socket_addrs().unwrap().next().unwrap();
    /// let node_id = Id::rand(&mut rand::thread_rng()).unwrap();
    /// let addr_opt_id = AddrOptId::new(socket_addr, Some(node_id));
    /// assert_eq!(*addr_opt_id.addr(), socket_addr);
    /// assert_eq!(addr_opt_id.id(), Some(node_id));
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(addr: A, id: Option<Id>) -> Self {
        Self { addr, id }
    }

    /// Instantiate with only a network address.
    ///
    /// Useful when a new node needs to be bootstrapped and may only have the network address of another node.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn main() -> Result<(), std::io::Error> {
    /// use std::net::ToSocketAddrs;
    /// use sloppy::node::AddrOptId;
    ///
    /// let socket_addr = "example.com:6881".to_socket_addrs().unwrap().next().unwrap();
    /// let addr_opt_id = AddrOptId::with_addr(socket_addr);
    /// assert_eq!(*addr_opt_id.addr(), socket_addr);
    /// assert_eq!(addr_opt_id.id(), None);
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_addr(addr: A) -> Self {
        Self { addr, id: None }
    }

    /// Returns the network address.
    pub fn addr(&self) -> &A {
        &self.addr
    }

    /// Returns the optional node Id.
    pub fn id(&self) -> Option<Id> {
        self.id
    }
}

impl From<SocketAddrV4> for AddrOptId<SocketAddrV4> {
    fn from(addr: SocketAddrV4) -> Self {
        AddrOptId::with_addr(addr)
    }
}

impl From<SocketAddrV6> for AddrOptId<SocketAddrV6> {
    fn from(addr: SocketAddrV6) -> Self {
        AddrOptId::with_addr(addr)
    }
}

impl From<SocketAddr> for AddrOptId<SocketAddr> {
    fn from(addr: SocketAddr) -> Self {
        AddrOptId::with_addr(addr)
    }
}

impl From<AddrOptId<SocketAddrV4>> for AddrOptId<SocketAddr> {
    fn from(addr_opt_id: AddrOptId<SocketAddrV4>) -> Self {
        AddrOptId::new(SocketAddr::V4(*addr_opt_id.addr()), addr_opt_id.id())
    }
}

impl From<AddrOptId<SocketAddrV6>> for AddrOptId<SocketAddr> {
    fn from(addr_opt_id: AddrOptId<SocketAddrV6>) -> Self {
        AddrOptId::new(SocketAddr::V6(*addr_opt_id.addr()), addr_opt_id.id())
    }
}

impl From<&SocketAddrV4> for AddrOptId<SocketAddrV4> {
    fn from(addr: &SocketAddrV4) -> Self {
        AddrOptId::with_addr(*addr)
    }
}

impl From<&SocketAddrV6> for AddrOptId<SocketAddrV6> {
    fn from(addr: &SocketAddrV6) -> Self {
        AddrOptId::with_addr(*addr)
    }
}

impl From<&SocketAddr> for AddrOptId<SocketAddr> {
    fn from(addr: &SocketAddr) -> Self {
        AddrOptId::with_addr(*addr)
    }
}

impl From<&AddrOptId<SocketAddrV4>> for AddrOptId<SocketAddr> {
    fn from(addr_opt_id: &AddrOptId<SocketAddrV4>) -> Self {
        AddrOptId::new(SocketAddr::V4(*addr_opt_id.addr()), addr_opt_id.id())
    }
}

impl From<&AddrOptId<SocketAddrV6>> for AddrOptId<SocketAddr> {
    fn from(addr_opt_id: &AddrOptId<SocketAddrV6>) -> Self {
        AddrOptId::new(SocketAddr::V6(*addr_opt_id.addr()), addr_opt_id.id())
    }
}

impl From<AddrId<SocketAddrV4>> for AddrOptId<SocketAddrV4> {
    fn from(addr_opt_id: AddrId<SocketAddrV4>) -> Self {
        AddrOptId::new(*addr_opt_id.addr(), Some(addr_opt_id.id()))
    }
}

impl From<AddrId<SocketAddrV6>> for AddrOptId<SocketAddrV6> {
    fn from(addr_opt_id: AddrId<SocketAddrV6>) -> Self {
        AddrOptId::new(*addr_opt_id.addr(), Some(addr_opt_id.id()))
    }
}

impl From<AddrId<SocketAddrV4>> for AddrOptId<SocketAddr> {
    fn from(addr_opt_id: AddrId<SocketAddrV4>) -> Self {
        AddrOptId::new(SocketAddr::V4(*addr_opt_id.addr()), Some(addr_opt_id.id()))
    }
}

impl From<AddrId<SocketAddrV6>> for AddrOptId<SocketAddr> {
    fn from(addr_opt_id: AddrId<SocketAddrV6>) -> Self {
        AddrOptId::new(SocketAddr::V6(*addr_opt_id.addr()), Some(addr_opt_id.id()))
    }
}

impl From<AddrId<SocketAddr>> for AddrOptId<SocketAddr> {
    fn from(addr_opt_id: AddrId<SocketAddr>) -> Self {
        AddrOptId::new(*addr_opt_id.addr(), Some(addr_opt_id.id()))
    }
}

impl From<&AddrId<SocketAddrV4>> for AddrOptId<SocketAddrV4> {
    fn from(addr_opt_id: &AddrId<SocketAddrV4>) -> Self {
        AddrOptId::new(*addr_opt_id.addr(), Some(addr_opt_id.id()))
    }
}

impl From<&AddrId<SocketAddrV6>> for AddrOptId<SocketAddrV6> {
    fn from(addr_opt_id: &AddrId<SocketAddrV6>) -> Self {
        AddrOptId::new(*addr_opt_id.addr(), Some(addr_opt_id.id()))
    }
}

impl From<&AddrId<SocketAddrV4>> for AddrOptId<SocketAddr> {
    fn from(addr_opt_id: &AddrId<SocketAddrV4>) -> Self {
        AddrOptId::new(SocketAddr::V4(*addr_opt_id.addr()), Some(addr_opt_id.id()))
    }
}

impl From<&AddrId<SocketAddrV6>> for AddrOptId<SocketAddr> {
    fn from(addr_opt_id: &AddrId<SocketAddrV6>) -> Self {
        AddrOptId::new(SocketAddr::V6(*addr_opt_id.addr()), Some(addr_opt_id.id()))
    }
}

impl From<&AddrId<SocketAddr>> for AddrOptId<SocketAddr> {
    fn from(addr_opt_id: &AddrId<SocketAddr>) -> Self {
        AddrOptId::new(*addr_opt_id.addr(), Some(addr_opt_id.id()))
    }
}

impl From<AddrOptId<SocketAddrV4>> for SocketAddrV4 {
    fn from(addr_opt_id: AddrOptId<SocketAddrV4>) -> SocketAddrV4 {
        *addr_opt_id.addr()
    }
}

impl From<AddrOptId<SocketAddrV6>> for SocketAddrV6 {
    fn from(addr_opt_id: AddrOptId<SocketAddrV6>) -> SocketAddrV6 {
        *addr_opt_id.addr()
    }
}

impl From<AddrOptId<SocketAddr>> for SocketAddr {
    fn from(addr_opt_id: AddrOptId<SocketAddr>) -> SocketAddr {
        *addr_opt_id.addr()
    }
}

impl From<AddrOptId<SocketAddrV4>> for SocketAddr {
    fn from(addr_opt_id: AddrOptId<SocketAddrV4>) -> SocketAddr {
        SocketAddr::V4(*addr_opt_id.addr())
    }
}

impl From<AddrOptId<SocketAddrV6>> for SocketAddr {
    fn from(addr_opt_id: AddrOptId<SocketAddrV6>) -> SocketAddr {
        SocketAddr::V6(*addr_opt_id.addr())
    }
}

// impl AddrOptId {
// use crate::{addr::NodeIdGenerator, node::Id};
// use serde::{Deserialize, Serialize};
// use std::net::{SocketAddr, ToSocketAddrs};
// pub(crate) fn is_valid_node_id(&self) -> bool {
//     if let Some(id) = self.node_id.as_ref() {
//         match self.addr {
//             Addr::HostPort(ref host) => {
//                 let addrs = host.to_socket_addrs();
//                 match addrs {
//                     Ok(mut addrs) => match addrs.next() {
//                         Some(addr) => match addr {
//                             SocketAddr::V4(addr) => return addr.ip().is_valid_node_id(id),
//                             SocketAddr::V6(addr) => return addr.ip().is_valid_node_id(id),
//                         },
//                         None => return false,
//                     },
//                     Err(_) => return false,
//                 }
//             }
//             Addr::SocketAddr(addr) => match addr {
//                 SocketAddr::V4(addr) => return addr.ip().is_valid_node_id(id),
//                 SocketAddr::V6(addr) => return addr.ip().is_valid_node_id(id),
//             },
//         }
//     }
//     false
// }
// }

trait Crc32cMaker {
    fn make_crc32c(&self, rand: u8) -> u32;
}

impl Crc32cMaker for Ipv4Addr {
    fn make_crc32c(&self, rand: u8) -> u32 {
        const MASK: [u8; 4] = [0x03, 0x0F, 0x3F, 0xFF];
        let r = rand & 0x7;

        let octets = self.octets();

        let mut masked_bytes: [u8; 4] = [0; 4];
        masked_bytes[0] = octets[0] & MASK[0];
        masked_bytes[1] = octets[1] & MASK[1];
        masked_bytes[2] = octets[2] & MASK[2];
        masked_bytes[3] = octets[3] & MASK[3];

        masked_bytes[0] |= r << 5;

        crc32c::crc32c(&masked_bytes)
    }
}

impl Crc32cMaker for Ipv6Addr {
    fn make_crc32c(&self, rand: u8) -> u32 {
        const MASK: [u8; 8] = [0x01, 0x03, 0x07, 0x0F, 0x01F, 0x3F, 0x7F, 0xFF];
        let r = rand & 0x7;

        let octets = self.octets();

        let mut masked_bytes: [u8; 8] = [0; 8];
        masked_bytes[0] = octets[0] & MASK[0];
        masked_bytes[1] = octets[1] & MASK[1];
        masked_bytes[2] = octets[2] & MASK[2];
        masked_bytes[3] = octets[3] & MASK[3];
        masked_bytes[4] = octets[4] & MASK[4];
        masked_bytes[5] = octets[5] & MASK[5];
        masked_bytes[6] = octets[6] & MASK[6];
        masked_bytes[7] = octets[7] & MASK[7];

        masked_bytes[0] |= r << 5;

        crc32c::crc32c(&masked_bytes)
    }
}

pub(crate) trait NodeIdGenerator {
    fn make_node_id<R>(&self, rand: Option<u8>, rng: &mut R) -> Result<Id, rand::Error>
    where
        R: rand::Rng;

    fn is_valid_node_id(&self, id: Id) -> bool;
}

impl NodeIdGenerator for IpAddr {
    fn make_node_id<R>(&self, rand: Option<u8>, rng: &mut R) -> Result<Id, rand::Error>
    where
        R: rand::Rng,
    {
        match self {
            IpAddr::V4(addr) => addr.make_node_id(rand, rng),
            IpAddr::V6(addr) => addr.make_node_id(rand, rng),
        }
    }

    fn is_valid_node_id(&self, id: Id) -> bool {
        match self {
            IpAddr::V4(addr) => addr.is_valid_node_id(id),
            IpAddr::V6(addr) => addr.is_valid_node_id(id),
        }
    }
}

impl NodeIdGenerator for Ipv4Addr {
    fn make_node_id<R>(&self, rand: Option<u8>, rng: &mut R) -> Result<Id, rand::Error>
    where
        R: rand::Rng,
    {
        let rand = rand.unwrap_or_else(|| rng.gen_range(0..8));
        let crc32_val = self.make_crc32c(rand).to_be_bytes();
        let mut id = Id::rand(rng)?;
        id.0[0] = crc32_val[0];
        id.0[1] = crc32_val[1];
        id.0[2] = crc32_val[2] & 0xF8 | rng.gen_range(0..8);
        id.0[19] = rand;

        Ok(id)
    }

    fn is_valid_node_id(&self, id: Id) -> bool {
        let octets = self.octets();
        // loopback
        if octets[0] == 127 {
            return true;
        }

        // self-assigned
        if octets[0] == 169 && octets[1] == 254 {
            return true;
        }

        // local network
        if octets[0] == 10
            || (octets[0] == 172 && octets[1] >> 4 == 1)
            || (octets[0] == 192 && octets[1] == 168)
        {
            return true;
        }

        let rand = id.0[19];
        let crc32c_val = self.make_crc32c(rand).to_be_bytes();

        if id.0[0] != crc32c_val[0] {
            return false;
        }

        if id.0[1] != crc32c_val[1] {
            return false;
        }

        if (id.0[2] & 0xF8) != (crc32c_val[2] & 0xF8) {
            return false;
        }

        true
    }
}

impl NodeIdGenerator for Ipv6Addr {
    fn make_node_id<R>(&self, rand: Option<u8>, rng: &mut R) -> Result<Id, rand::Error>
    where
        R: rand::Rng,
    {
        let rand = rand.unwrap_or_else(|| rng.gen_range(0..8));
        let crc32_val = self.make_crc32c(rand).to_be_bytes();
        let mut id = Id::rand(rng)?;
        id.0[0] = crc32_val[0];
        id.0[1] = crc32_val[1];
        id.0[2] = crc32_val[2] & 0xF8 | rng.gen_range(0..8);
        id.0[19] = rand;
        Ok(id)
    }

    fn is_valid_node_id(&self, id: Id) -> bool {
        let rand = id.0[19];
        let crc32c_val = self.make_crc32c(rand).to_be_bytes();

        if id.0[0] != crc32c_val[0] {
            return false;
        }

        if id.0[1] != crc32c_val[1] {
            return false;
        }

        if (id.0[2] & 0xF8) != (crc32c_val[2] & 0xF8) {
            return false;
        }

        true
    }
}

mod private {
    use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};

    pub trait Sealed {}

    impl Sealed for SocketAddr {}
    impl Sealed for SocketAddrV4 {}
    impl Sealed for SocketAddrV6 {}
}

#[cfg(test)]
impl quickcheck::Arbitrary for Id {
    fn arbitrary(g: &mut quickcheck::Gen) -> Id {
        Id::from([
            u8::arbitrary(g),
            u8::arbitrary(g),
            u8::arbitrary(g),
            u8::arbitrary(g),
            u8::arbitrary(g),
            u8::arbitrary(g),
            u8::arbitrary(g),
            u8::arbitrary(g),
            u8::arbitrary(g),
            u8::arbitrary(g),
            u8::arbitrary(g),
            u8::arbitrary(g),
            u8::arbitrary(g),
            u8::arbitrary(g),
            u8::arbitrary(g),
            u8::arbitrary(g),
            u8::arbitrary(g),
            u8::arbitrary(g),
            u8::arbitrary(g),
            u8::arbitrary(g),
        ])
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::error::Error;
    use quickcheck_macros::quickcheck;

    #[test]
    fn test_debug() -> Result<(), Error> {
        let node_id = Id::max();
        let debug_str = format!("{:?}", node_id);
        assert_eq!(debug_str, "Id(FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF)");
        Ok(())
    }

    #[quickcheck]
    fn id_distance_commutative(id1: Id, id2: Id) -> bool {
        id1.distance(id2) == id2.distance(id1)
    }

    #[quickcheck]
    fn make_only_valid_node_ids_for_ipv4(ip: Ipv4Addr, rand: Option<u8>) -> bool {
        ip.is_valid_node_id(ip.make_node_id(rand, &mut rand::thread_rng()).unwrap())
    }

    #[quickcheck]
    fn make_only_valid_node_ids_for_ipv6(ip: Ipv6Addr, rand: Option<u8>) -> bool {
        ip.is_valid_node_id(ip.make_node_id(rand, &mut rand::thread_rng()).unwrap())
    }

    #[quickcheck]
    fn id_try_from_slice(values: Vec<u8>) -> bool {
        if values.len() == 20 {
            Id::try_from(values.as_slice()).is_ok()
        } else {
            Id::try_from(values.as_slice()).is_err()
        }
    }

    #[quickcheck]
    fn id_cmp(id1: Id, id2: Id) -> bool {
        match id1.cmp(&id2) {
            Ordering::Equal => id2.cmp(&id1) == Ordering::Equal,
            Ordering::Less => id2.cmp(&id1) == Ordering::Greater,
            Ordering::Greater => id2.cmp(&id1) == Ordering::Less,
        }
    }

    #[test]
    fn test_ipv4_make_node_id_1() {
        let ip = "124.31.75.21".parse::<Ipv4Addr>().unwrap();
        let id = ip.make_node_id(None, &mut rand::thread_rng()).unwrap();
        assert!(ip.is_valid_node_id(id));
    }

    #[test]
    fn test_ipv4_valid_node_id_1() {
        let ip = "124.31.75.21".parse::<Ipv4Addr>().unwrap();
        assert!(ip.is_valid_node_id(Id::from([
            0x5f, 0xbf, 0xbf, 0xf1, 0x0c, 0x5d, 0x6a, 0x4e, 0xc8, 0xa8, 0x8e, 0x4c, 0x6a, 0xb4,
            0xc2, 0x8b, 0x95, 0xee, 0xe4, 0x01
        ])));
    }

    #[test]
    fn test_ipv4_make_node_id_2() {
        let ip = "21.75.31.124".parse::<Ipv4Addr>().unwrap();
        let id = ip.make_node_id(None, &mut rand::thread_rng()).unwrap();
        assert!(ip.is_valid_node_id(id));
    }

    #[test]
    fn test_ipv4_valid_node_id_2() {
        let ip = "21.75.31.124".parse::<Ipv4Addr>().unwrap();
        assert!(ip.is_valid_node_id(Id::from([
            0x5a, 0x3c, 0xe9, 0xc1, 0x4e, 0x7a, 0x08, 0x64, 0x56, 0x77, 0xbb, 0xd1, 0xcf, 0xe7,
            0xd8, 0xf9, 0x56, 0xd5, 0x32, 0x56
        ])));
    }

    #[test]
    fn test_ipv4_make_node_id_3() {
        let ip = "65.23.51.170".parse::<Ipv4Addr>().unwrap();
        let id = ip.make_node_id(None, &mut rand::thread_rng()).unwrap();
        assert!(ip.is_valid_node_id(id));
    }

    #[test]
    fn test_ipv4_valid_node_id_3() {
        let ip = "65.23.51.170".parse::<Ipv4Addr>().unwrap();
        assert!(ip.is_valid_node_id(Id::from([
            0xa5, 0xd4, 0x32, 0x20, 0xbc, 0x8f, 0x11, 0x2a, 0x3d, 0x42, 0x6c, 0x84, 0x76, 0x4f,
            0x8c, 0x2a, 0x11, 0x50, 0xe6, 0x16
        ])));
    }

    #[test]
    fn test_ipv4_make_node_id_4() {
        let ip = "84.124.73.14".parse::<Ipv4Addr>().unwrap();
        let id = ip.make_node_id(None, &mut rand::thread_rng()).unwrap();
        assert!(ip.is_valid_node_id(id));
    }

    #[test]
    fn test_ipv4_valid_node_id_4() {
        let ip = "84.124.73.14".parse::<Ipv4Addr>().unwrap();
        assert!(ip.is_valid_node_id(Id::from([
            0x1b, 0x03, 0x21, 0xdd, 0x1b, 0xb1, 0xfe, 0x51, 0x81, 0x01, 0xce, 0xef, 0x99, 0x46,
            0x2b, 0x94, 0x7a, 0x01, 0xff, 0x41
        ])));
    }

    #[test]
    fn test_ipv4_make_node_id_5() {
        let ip = "43.213.53.83".parse::<Ipv4Addr>().unwrap();
        let id = ip.make_node_id(None, &mut rand::thread_rng()).unwrap();
        assert!(ip.is_valid_node_id(id));
    }

    #[test]
    fn test_ipv4_valid_node_id_5() {
        let ip = "43.213.53.83".parse::<Ipv4Addr>().unwrap();
        assert!(ip.is_valid_node_id(Id::from([
            0xe5, 0x6f, 0x6c, 0xbf, 0x5b, 0x7c, 0x4b, 0xe0, 0x23, 0x79, 0x86, 0xd5, 0x24, 0x3b,
            0x87, 0xaa, 0x6d, 0x51, 0x30, 0x5a
        ])));
    }
}

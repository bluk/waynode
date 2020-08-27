// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! A node is a client/server which implements the DHT protocol.

use crate::error::Error;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    cmp::{Ord, PartialOrd},
    convert::TryFrom,
    fmt,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
};

#[derive(Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
/// A 160-bit value which is used to identify a node.
pub struct Id(pub(crate) [u8; 20]);

impl Id {
    /// Instantiates an Id with bytes representing the 160-bit value.
    pub fn with_bytes(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }

    pub(crate) const fn min() -> Id {
        Id([0; 20])
    }

    pub(crate) const fn max() -> Id {
        Id([0xff; 20])
    }

    /// Instantiates an Id with a random value.
    pub fn rand() -> Result<Id, Error> {
        use rand::thread_rng;
        let mut arr: [u8; 20] = [0; 20];
        thread_rng()
            .try_fill(&mut arr[..])
            .map_err(|_| Error::RngError)?;
        Ok(Id(arr))
    }

    // pub(crate) fn rand_in_range(rng: &std::ops::Range<Id>) -> Result<Id, Error> {
    //     let data_bit_diff = rng.end.difference(&rng.start);
    //     let mut rand_bits: [u8; 20] = <[u8; 20]>::randomize_up_to(data_bit_diff.0, false)?;
    //     let _ = rand_bits.overflowing_add(&rng.start.0);
    //     Ok(Id(rand_bits))
    // }

    pub(crate) fn rand_in_inclusive_range(rng: &std::ops::RangeInclusive<Id>) -> Result<Id, Error> {
        let data_bit_diff = rng.end().difference(*rng.start());
        let mut rand_bits: [u8; 20] = <[u8; 20]>::randomize_up_to(data_bit_diff.0, true)?;
        let _ = rand_bits.overflowing_add(&rng.start().0);
        Ok(Id(rand_bits))
    }

    #[inline]
    #[must_use]
    fn difference(&self, other: Id) -> Id {
        let mut bigger: [u8; 20];
        let mut smaller: [u8; 20];
        if self < &other {
            bigger = other.0;
            smaller = self.0;
        } else {
            bigger = self.0;
            smaller = other.0;
        }
        smaller.twos_complement();
        let _ = bigger.overflowing_add(&smaller);
        Id(bigger)
    }

    /// Determines the distance between this node ID and the node ID argument.
    #[must_use]
    pub(crate) fn distance(&self, other: Id) -> Id {
        let mut data = [0; 20];
        for (idx, val) in self.0.iter().enumerate() {
            data[idx] = val ^ other.0[idx];
        }
        Id(data)
    }

    /// Finds the middle id between this node ID and the node ID argument.
    #[inline]
    #[must_use]
    pub(crate) fn middle(&self, other: Id) -> Id {
        let mut data = self.0;
        let overflow = data.overflowing_add(&other.0);
        data.shift_right();
        if overflow {
            data[0] |= 0x80;
        }
        Id(data)
    }

    #[inline]
    #[must_use]
    pub(crate) fn prev(&self) -> Id {
        let mut data: [u8; 20] = [0; 20];
        let offset_from_end = self.0.iter().rposition(|v| *v != 0).unwrap_or(0);
        for (val, self_val) in data.iter_mut().zip(self.0.iter()).take(offset_from_end) {
            *val = *self_val;
        }

        data[offset_from_end] = if self.0[offset_from_end] == 0 {
            0xff
        } else {
            self.0[offset_from_end] - 1
        };

        for val in data.iter_mut().take(self.0.len()).skip(offset_from_end + 1) {
            *val = 0xff;
        }

        Id(data)
    }
}

impl From<[u8; 20]> for Id {
    fn from(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }
}

impl From<&[u8; 20]> for Id {
    fn from(bytes: &[u8; 20]) -> Self {
        Self(*bytes)
    }
}

impl From<&Id> for Vec<u8> {
    fn from(id: &Id) -> Self {
        Vec::from(id.0)
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

impl Ord for Id {
    fn cmp(&self, other: &Self) -> Ordering {
        for idx in 0..self.0.len() {
            if self.0[idx] == other.0[idx] {
                continue;
            }
            return self.0[idx].cmp(&other.0[idx]);
        }
        Ordering::Equal
    }
}

impl PartialOrd for Id {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in self.0.iter() {
            write!(f, "{:02x}", b)?;
        }
        Ok(())
    }
}

impl TryFrom<&[u8]> for Id {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != 20 {
            return Err(Error::InvalidNodeId);
        }

        let mut data: [u8; 20] = [0; 20];
        data.copy_from_slice(value);
        Ok(Id(data))
    }
}

trait IdBytes {
    #[must_use]
    fn overflowing_add(&mut self, other: &Self) -> bool;

    fn twos_complement(&mut self);

    /// Shifts the bits right by 1.
    fn shift_right(&mut self);

    fn randomize_up_to(end: Self, is_closed_range: bool) -> Result<Self, Error>
    where
        Self: Sized;
}

impl IdBytes for [u8; 20] {
    #[must_use]
    fn overflowing_add(&mut self, other: &Self) -> bool {
        let mut carry_over: u8 = 0;

        for idx in (0..self.len()).rev() {
            let (partial_val, overflow) = self[idx].overflowing_add(other[idx]);
            let (final_val, carry_over_overflow) = partial_val.overflowing_add(carry_over);
            self[idx] = final_val;
            carry_over = if carry_over_overflow || overflow {
                1
            } else {
                0
            };
        }

        carry_over == 1
    }

    fn twos_complement(&mut self) {
        for val in self.iter_mut() {
            *val = !(*val);
        }
        let one_bit = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let _ = self.overflowing_add(&one_bit);
    }

    fn shift_right(&mut self) {
        let mut add_high_bit = false;
        for val in self.iter_mut() {
            let is_lower_bit_set = (*val & 0x01) == 1;
            *val >>= 1;
            if add_high_bit {
                *val |= 0x80;
            }
            add_high_bit = is_lower_bit_set;
        }
    }

    fn randomize_up_to(end: Self, is_closed_range: bool) -> Result<Self, Error> {
        use rand::thread_rng;

        let mut data: Self = [0; 20];
        let mut lower_than_max = false;
        let mut rng = thread_rng();

        if !is_closed_range && end == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] {
            return Err(Error::CannotGenerateNodeId);
        }

        loop {
            for idx in 0..data.len() {
                data[idx] = if lower_than_max {
                    u8::try_from(rng.gen_range(0, u16::from(u8::MAX) + 1))
                        .map_err(|_| Error::RngError)?
                } else {
                    let idx_val = end[idx];
                    let val = u8::try_from(rng.gen_range(0, u16::from(idx_val) + 1))
                        .map_err(|_| Error::RngError)?;
                    if val < idx_val {
                        lower_than_max = true;
                    }
                    val
                };
            }

            if lower_than_max || is_closed_range {
                break;
            }
        }

        Ok(data)
    }
}

/// A node's network address.
///
/// External code should not implement this trait.
pub trait Addr:
    fmt::Debug + Clone + Copy + Eq + std::hash::Hash + Ord + PartialEq + PartialOrd
{
}

impl Addr for SocketAddr {}

impl Addr for SocketAddrV4 {}

impl Addr for SocketAddrV6 {}

/// A network address and optional Id.
pub trait AddrId:
    Clone + Copy + fmt::Debug + Eq + std::hash::Hash + Ord + PartialEq + PartialOrd
// TODO: Add serialize/deserialize
{
    type Addr: Addr;

    /// Returns the socket address.
    fn addr(&self) -> Self::Addr;

    /// Returns the optional node Id.
    fn id(&self) -> Option<Id>;
}

/// A node's network address and optional Id.
///
/// In order to send messages to other nodes, a network address is required.
///
/// The node's Id may not be known, so it is optional.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct NodeAddrId<A>
where
    A: Addr,
{
    addr: A,
    id: Option<Id>,
}

// TODO: Add serialize/deserialize for NodeAddrId

impl<A> NodeAddrId<A>
where
    A: Addr,
{
    /// Instantiate with only a socket address.
    pub fn with_addr(addr: A) -> Self {
        Self { addr, id: None }
    }

    /// Instantiate with a socket address and an optional Id.
    pub fn with_addr_and_id(addr: A, id: Option<Id>) -> Self {
        Self { addr, id }
    }
}

impl AddrId for NodeAddrId<SocketAddrV4> {
    type Addr = SocketAddrV4;

    fn addr(&self) -> SocketAddrV4 {
        self.addr
    }

    fn id(&self) -> Option<Id> {
        self.id
    }
}

impl From<SocketAddrV4> for NodeAddrId<SocketAddrV4> {
    fn from(addr: SocketAddrV4) -> Self {
        NodeAddrId::with_addr(addr)
    }
}

impl AddrId for NodeAddrId<SocketAddrV6> {
    type Addr = SocketAddrV6;

    fn addr(&self) -> SocketAddrV6 {
        self.addr
    }

    fn id(&self) -> Option<Id> {
        self.id
    }
}

impl From<SocketAddrV6> for NodeAddrId<SocketAddrV6> {
    fn from(addr: SocketAddrV6) -> Self {
        NodeAddrId::with_addr(addr)
    }
}

impl AddrId for NodeAddrId<SocketAddr> {
    type Addr = SocketAddr;

    fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn id(&self) -> Option<Id> {
        self.id
    }
}

impl From<SocketAddr> for NodeAddrId<SocketAddr> {
    fn from(addr: SocketAddr) -> Self {
        NodeAddrId::with_addr(addr)
    }
}

impl From<NodeAddrId<SocketAddrV4>> for NodeAddrId<SocketAddr> {
    fn from(addr_id: NodeAddrId<SocketAddrV4>) -> Self {
        NodeAddrId::with_addr_and_id(SocketAddr::V4(addr_id.addr()), addr_id.id())
    }
}

impl From<NodeAddrId<SocketAddrV6>> for NodeAddrId<SocketAddr> {
    fn from(addr_id: NodeAddrId<SocketAddrV6>) -> Self {
        NodeAddrId::with_addr_and_id(SocketAddr::V6(addr_id.addr()), addr_id.id())
    }
}

// impl NodeAddrId {
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
    fn make_node_id(&self, rand: Option<u8>) -> Result<Id, Error>;

    fn is_valid_node_id(&self, id: Id) -> bool;
}

impl NodeIdGenerator for Ipv4Addr {
    fn make_node_id(&self, rand: Option<u8>) -> Result<Id, Error> {
        let rand = rand.unwrap_or_else(|| rand::thread_rng().gen_range(0, 8));
        let crc32_val = self.make_crc32c(rand);
        let mut id = Id::rand()?;
        id.0[0] = u8::try_from(crc32_val >> 24 & 0xFF).unwrap();
        id.0[1] = u8::try_from(crc32_val >> 16 & 0xFF).unwrap();
        id.0[2] = u8::try_from(crc32_val >> 8 & 0xF8 | rand::thread_rng().gen_range(0, 8)).unwrap();
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
        let crc32c_val = self.make_crc32c(rand);

        if id.0[0] != u8::try_from((crc32c_val >> 24) & 0xFF).unwrap() {
            return false;
        }

        if id.0[1] != u8::try_from((crc32c_val >> 16) & 0xFF).unwrap() {
            return false;
        }

        if (id.0[2] & 0xF8) != u8::try_from((crc32c_val >> 8) & 0xF8).unwrap() {
            return false;
        }

        true
    }
}

impl NodeIdGenerator for Ipv6Addr {
    fn make_node_id(&self, rand: Option<u8>) -> Result<Id, Error> {
        let rand = rand.unwrap_or_else(|| rand::thread_rng().gen_range(0, 8));
        let crc32_val = self.make_crc32c(rand);
        let mut id = Id::rand()?;
        id.0[0] = u8::try_from(crc32_val >> 24 & 0xFF).unwrap();
        id.0[1] = u8::try_from(crc32_val >> 16 & 0xFF).unwrap();
        id.0[2] = u8::try_from(crc32_val >> 8 & 0xF8 | rand::thread_rng().gen_range(0, 8)).unwrap();
        id.0[19] = rand;
        Ok(id)
    }

    fn is_valid_node_id(&self, id: Id) -> bool {
        let rand = id.0[19];
        let crc32c_val = self.make_crc32c(rand);

        if id.0[0] != u8::try_from((crc32c_val >> 24) & 0xFF).unwrap() {
            return false;
        }

        if id.0[1] != u8::try_from((crc32c_val >> 16) & 0xFF).unwrap() {
            return false;
        }

        if (id.0[2] & 0xF8) != u8::try_from((crc32c_val >> 8) & 0xF8).unwrap() {
            return false;
        }

        true
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_debug() -> Result<(), Error> {
        let node_id = Id::max();
        let debug_str = format!("{:?}", node_id);
        assert_eq!(debug_str, "ffffffffffffffffffffffffffffffffffffffff");
        Ok(())
    }

    #[test]
    fn test_overflowing_add() {
        let mut bytes: [u8; 20] = [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        ];
        let overflow = bytes.overflowing_add(&[
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ]);
        assert!(!overflow);
        assert_eq!(
            bytes,
            [1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33, 35, 37, 39,]
        );
    }

    #[test]
    fn test_twos_complement() {
        let mut bytes: [u8; 20] = [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        ];
        bytes.twos_complement();
        assert_eq!(
            bytes,
            [
                255, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245, 244, 243, 242, 241, 240,
                239, 238, 237, 237
            ]
        );
    }

    #[test]
    fn test_shift_right() {
        let mut bytes: [u8; 20] = [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        ];
        bytes.shift_right();
        assert_eq!(
            bytes,
            [0, 0, 129, 1, 130, 2, 131, 3, 132, 4, 133, 5, 134, 6, 135, 7, 136, 8, 137, 9]
        );
    }

    #[test]
    fn test_id_ord() {
        let mut node_ids = vec![
            Id([0xff; 20]),
            Id([0x00; 20]),
            Id([0xff; 20]).middle(Id([0x00; 20])),
        ];
        node_ids.sort();
        assert_eq!(
            node_ids,
            vec![
                Id([0x00; 20]),
                Id([0xff; 20]).middle(Id([0x00; 20])),
                Id([0xff; 20]),
            ]
        );
    }

    #[test]
    fn test_id_distance_ord() {
        let mut node_ids = vec![
            Id([0x00; 20]),
            Id([0xff; 20]).middle(Id([0x00; 20])),
            Id([0xff; 20]),
        ];
        let pivot_id = Id([0xef; 20]).middle(Id([0x00; 20]));
        node_ids.sort_by(|a, b| a.distance(pivot_id).cmp(&b.distance(pivot_id)));
        assert_eq!(
            node_ids,
            vec![
                Id([0xff; 20]).middle(Id([0x00; 20])),
                Id([0x00; 20]),
                Id([0xff; 20]),
            ]
        );
    }

    #[test]
    fn test_ipv4_make_node_id_1() {
        let ip = "124.31.75.21".parse::<Ipv4Addr>().unwrap();
        let id = ip.make_node_id(None).unwrap();
        assert!(ip.is_valid_node_id(id));
    }

    #[test]
    fn test_ipv4_valid_node_id_1() {
        let ip = "124.31.75.21".parse::<Ipv4Addr>().unwrap();
        assert!(ip.is_valid_node_id(Id::with_bytes([
            0x5f, 0xbf, 0xbf, 0xf1, 0x0c, 0x5d, 0x6a, 0x4e, 0xc8, 0xa8, 0x8e, 0x4c, 0x6a, 0xb4,
            0xc2, 0x8b, 0x95, 0xee, 0xe4, 0x01
        ])));
    }

    #[test]
    fn test_ipv4_make_node_id_2() {
        let ip = "21.75.31.124".parse::<Ipv4Addr>().unwrap();
        let id = ip.make_node_id(None).unwrap();
        assert!(ip.is_valid_node_id(id));
    }

    #[test]
    fn test_ipv4_valid_node_id_2() {
        let ip = "21.75.31.124".parse::<Ipv4Addr>().unwrap();
        assert!(ip.is_valid_node_id(Id::with_bytes([
            0x5a, 0x3c, 0xe9, 0xc1, 0x4e, 0x7a, 0x08, 0x64, 0x56, 0x77, 0xbb, 0xd1, 0xcf, 0xe7,
            0xd8, 0xf9, 0x56, 0xd5, 0x32, 0x56
        ])));
    }

    #[test]
    fn test_ipv4_make_node_id_3() {
        let ip = "65.23.51.170".parse::<Ipv4Addr>().unwrap();
        let id = ip.make_node_id(None).unwrap();
        assert!(ip.is_valid_node_id(id));
    }

    #[test]
    fn test_ipv4_valid_node_id_3() {
        let ip = "65.23.51.170".parse::<Ipv4Addr>().unwrap();
        assert!(ip.is_valid_node_id(Id::with_bytes([
            0xa5, 0xd4, 0x32, 0x20, 0xbc, 0x8f, 0x11, 0x2a, 0x3d, 0x42, 0x6c, 0x84, 0x76, 0x4f,
            0x8c, 0x2a, 0x11, 0x50, 0xe6, 0x16
        ])));
    }

    #[test]
    fn test_ipv4_make_node_id_4() {
        let ip = "84.124.73.14".parse::<Ipv4Addr>().unwrap();
        let id = ip.make_node_id(None).unwrap();
        assert!(ip.is_valid_node_id(id));
    }

    #[test]
    fn test_ipv4_valid_node_id_4() {
        let ip = "84.124.73.14".parse::<Ipv4Addr>().unwrap();
        assert!(ip.is_valid_node_id(Id::with_bytes([
            0x1b, 0x03, 0x21, 0xdd, 0x1b, 0xb1, 0xfe, 0x51, 0x81, 0x01, 0xce, 0xef, 0x99, 0x46,
            0x2b, 0x94, 0x7a, 0x01, 0xff, 0x41
        ])));
    }

    #[test]
    fn test_ipv4_make_node_id_5() {
        let ip = "43.213.53.83".parse::<Ipv4Addr>().unwrap();
        let id = ip.make_node_id(None).unwrap();
        assert!(ip.is_valid_node_id(id));
    }

    #[test]
    fn test_ipv4_valid_node_id_5() {
        let ip = "43.213.53.83".parse::<Ipv4Addr>().unwrap();
        assert!(ip.is_valid_node_id(Id::with_bytes([
            0xe5, 0x6f, 0x6c, 0xbf, 0x5b, 0x7c, 0x4b, 0xe0, 0x23, 0x79, 0x86, 0xd5, 0x24, 0x3b,
            0x87, 0xaa, 0x6d, 0x51, 0x30, 0x5a
        ])));
    }
}

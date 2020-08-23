// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! A node is a client/server which implements the DHT protocol.

use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt;
use std::net::SocketAddr;

#[derive(Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
/// Every node is addressed via a 160-bit identifier.
pub struct Id(pub(crate) [u8; 20]);

impl Id {
    /// Instantiates an Id with bytes representing the 160-bit identifier.
    pub fn with_bytes(bytes: [u8; 20]) -> Self {
        Id(bytes)
    }

    pub(crate) const fn min() -> Id {
        Id([0; 20])
    }

    pub(crate) const fn max() -> Id {
        Id([0xff; 20])
    }

    /// Instantiates an Id with a random value.
    pub fn rand() -> Result<Id, Error> {
        use rand::{thread_rng, Rng};
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
        let data_bit_diff = rng.end().difference(rng.start());
        let mut rand_bits: [u8; 20] = <[u8; 20]>::randomize_up_to(data_bit_diff.0, true)?;
        let _ = rand_bits.overflowing_add(&rng.start().0);
        Ok(Id(rand_bits))
    }

    #[inline]
    #[must_use]
    fn difference(&self, other: &Id) -> Id {
        let mut bigger: [u8; 20];
        let mut smaller: [u8; 20];
        if self < other {
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
    pub(crate) fn distance(&self, other: &Id) -> Id {
        let mut data = [0; 20];
        for (idx, val) in self.0.iter().enumerate() {
            data[idx] = val ^ other.0[idx];
        }
        Id(data)
    }

    /// Finds the middle id between this node ID and the node ID argument.
    #[inline]
    #[must_use]
    pub(crate) fn middle(&self, other: &Id) -> Id {
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
        let mut data_idx = 0;
        let offset_from_end = self.0.iter().rposition(|v| *v != 0).unwrap_or(0);
        for idx in 0..offset_from_end {
            data[data_idx] = self.0[idx];
            data_idx += 1;
        }

        data[data_idx] = if self.0[offset_from_end] == 0 {
            0xff
        } else {
            self.0[offset_from_end] - 1
        };

        for idx in (offset_from_end + 1)..self.0.len() {
            data[idx] = 0xff;
            data_idx += 1;
        }

        Id(data)
    }
}

impl Into<Vec<u8>> for Id {
    fn into(self) -> Vec<u8> {
        Vec::from(self.0)
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
        use rand::{thread_rng, Rng};

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

/// A node's socket address and optional Id.
///
/// In order to send messages to other nodes, a socket address is required.
///
/// The node's Id may not be known, so it is optional.
///
/// Internally, a node must have both a socket address and an Id to be added to
/// the DHT routing table. Queried nodes which reply with an Id which does not
/// match the expected Id may have their responses dropped.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AddrId {
    addr: SocketAddr,
    id: Option<Id>,
}

impl AddrId {
    /// Instantiate with only a socket address.
    pub fn with_addr(addr: SocketAddr) -> Self {
        Self { addr, id: None }
    }

    /// Instantiate with a socket address and an Id.
    pub fn with_addr_and_id(addr: SocketAddr, id: Id) -> Self {
        Self { addr, id: Some(id) }
    }

    /// Returns the optional node Id.
    pub fn id(&self) -> Option<Id> {
        self.id
    }

    /// Returns the socket address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl AddrId {
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
            Id([0xff; 20]).middle(&Id([0x00; 20])),
        ];
        node_ids.sort();
        assert_eq!(
            node_ids,
            vec![
                Id([0x00; 20]),
                Id([0xff; 20]).middle(&Id([0x00; 20])),
                Id([0xff; 20]),
            ]
        );
    }

    #[test]
    fn test_id_distance_ord() {
        let mut node_ids = vec![
            Id([0x00; 20]),
            Id([0xff; 20]).middle(&Id([0x00; 20])),
            Id([0xff; 20]),
        ];
        let pivot_id = Id([0xef; 20]).middle(&Id([0x00; 20]));
        node_ids.sort_by(|a, b| a.distance(&pivot_id).cmp(&b.distance(&pivot_id)));
        assert_eq!(
            node_ids,
            vec![
                Id([0xff; 20]).middle(&Id([0x00; 20])),
                Id([0x00; 20]),
                Id([0xff; 20]),
            ]
        );
    }
}

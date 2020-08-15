// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt;

#[derive(Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
/// Every node is addressed via a 160-bit identifier.
pub struct Id(pub(crate) [u8; 20]);

impl Id {
    pub const fn min() -> Id {
        Id([0; 20])
    }

    pub const fn max() -> Id {
        Id([0xff; 20])
    }

    pub fn new_with_bytes(bytes: [u8; 20]) -> Self {
        Id(bytes)
    }

    pub fn rand() -> Result<Id, Error> {
        use rand::{thread_rng, Rng};
        let mut arr: [u8; 20] = [0; 20];
        thread_rng()
            .try_fill(&mut arr[..])
            .map_err(|_| Error::RngError)?;
        Ok(Id(arr))
    }

    pub(crate) fn rand_in_range(rng: std::ops::Range<Id>) -> Result<Id, Error> {
        let data_bit_diff = rng.end.difference(&rng.start);
        let mut rand_bits: [u8; 20] = <[u8; 20]>::randomize_up_to(data_bit_diff.0, false)?;
        let _ = rand_bits.overflowing_add(&rng.start.0);
        Ok(Id(rand_bits))
    }

    pub(crate) fn rand_in_inclusive_range(rng: std::ops::RangeInclusive<Id>) -> Result<Id, Error> {
        let data_bit_diff = rng.end().difference(rng.start());
        let mut rand_bits: [u8; 20] = <[u8; 20]>::randomize_up_to(data_bit_diff.0, true)?;
        let _ = rand_bits.overflowing_add(&rng.start().0);
        Ok(Id(rand_bits))
    }

    #[must_use]
    fn difference(&self, other: &Id) -> Id {
        let mut bigger: [u8; 20];
        let mut smaller: [u8; 20];
        if self < other {
            bigger = other.0.clone();
            smaller = self.0.clone();
        } else {
            bigger = self.0.clone();
            smaller = other.0.clone();
        }
        smaller.twos_complement();
        let _ = bigger.overflowing_add(&smaller);
        Id(bigger)
    }

    /// Determines the distance between this node ID and the node ID argument.
    #[must_use]
    pub(crate) fn distance(&self, other: &Id) -> Id {
        let mut data = [0; 20];
        for idx in 0..self.0.len() {
            data[idx] = self.0[idx] ^ other.0[idx];
        }
        Id(data)
    }

    /// Finds the middle id between this node ID and the node ID argument.
    #[must_use]
    pub(crate) fn middle(&self, other: &Id) -> Id {
        let mut data = self.0.clone();
        let overflow = data.overflowing_add(&other.0);
        data.shift_right();
        if overflow {
            data[0] |= 0x80;
        }
        Id(data)
    }

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
        return Ordering::Equal;
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
            write!(f, "{:x}", b)?;
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

    #[must_use]
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
        for idx in 0..self.len() {
            self[idx] = !self[idx];
        }
        let one_bit = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let _ = self.overflowing_add(&one_bit);
    }

    #[must_use]
    fn shift_right(&mut self) {
        let mut add_high_bit = false;
        for idx in 0..self.len() {
            let is_lower_bit_set = (self[idx] & 0x01) == 1;
            self[idx] = self[idx] >> 1;
            if add_high_bit {
                self[idx] |= 0x80;
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

        while !lower_than_max && !is_closed_range {
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
        }

        Ok(data)
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
}

pub mod remote;

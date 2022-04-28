// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! A torrent is a peer to peer network exchanging data.

use core::{convert::TryFrom, fmt};
use serde::{Deserialize, Serialize};

/// A 160-bit value which is used to identify a torrent.
#[derive(Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct InfoHash(pub(crate) [u8; 20]);

impl InfoHash {
    /// Instantiates an [`InfoHash`] with bytes representing the 160-bit value.
    #[must_use]
    pub fn new(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }
}

impl From<[u8; 20]> for InfoHash {
    fn from(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }
}

impl From<InfoHash> for Vec<u8> {
    fn from(info_hash: InfoHash) -> Self {
        Vec::from(info_hash.0)
    }
}

impl From<InfoHash> for [u8; 20] {
    fn from(info_hash: InfoHash) -> Self {
        info_hash.0
    }
}

impl TryFrom<&[u8]> for InfoHash {
    type Error = core::array::TryFromSliceError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        <[u8; 20]>::try_from(value).map(InfoHash)
    }
}

fmt_byte_array!(InfoHash);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_formats() {
        let info_hash = InfoHash(hex_literal::hex!(
            "8EFA979AD7627693BA91D48E941F025BAE78CB77"
        ));
        assert_eq!(
            format!("{:X}", info_hash),
            "8EFA979AD7627693BA91D48E941F025BAE78CB77"
        );
        assert_eq!(
            format!("{:#X}", info_hash),
            "0x8EFA979AD7627693BA91D48E941F025BAE78CB77"
        );
        assert_eq!(
            format!("{:x}", info_hash),
            "8efa979ad7627693ba91d48e941f025bae78cb77"
        );
        assert_eq!(
            format!("{:#x}", info_hash),
            "0x8efa979ad7627693ba91d48e941f025bae78cb77"
        );
        assert_eq!(
            format!("{:b}", info_hash),
            "10001110111110101001011110011010110101111100010111011010010011101110101001000111010100100011101001010011111101011011101011101111000110010111110111"
        );
        assert_eq!(
            format!("{:#b}", info_hash),
            "0b10001110111110101001011110011010110101111100010111011010010011101110101001000111010100100011101001010011111101011011101011101111000110010111110111"
        );
    }
}

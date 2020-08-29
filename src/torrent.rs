// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! A torrent is a peer to peer network exchanging data.

use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::{convert::TryFrom, fmt};

/// A 160-bit value which is used to identify a torrent.
#[derive(Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct InfoHash(pub(crate) [u8; 20]);

impl InfoHash {
    /// Instantiates an InfoHash with bytes representing the 160-bit value.
    pub fn new(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }
}

impl From<[u8; 20]> for InfoHash {
    fn from(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }
}

impl From<&[u8; 20]> for InfoHash {
    fn from(bytes: &[u8; 20]) -> Self {
        Self(*bytes)
    }
}

impl From<&InfoHash> for Vec<u8> {
    fn from(info_hash: &InfoHash) -> Self {
        Vec::from(info_hash.0)
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

impl fmt::Debug for InfoHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in self.0.iter() {
            write!(f, "{:02x}", b)?;
        }
        Ok(())
    }
}

impl TryFrom<&[u8]> for InfoHash {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != 20 {
            return Err(Error::InvalidInfoHash);
        }

        let mut data: [u8; 20] = [0; 20];
        data.copy_from_slice(value);
        Ok(InfoHash(data))
    }
}

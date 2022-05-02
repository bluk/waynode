// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! KRPC messages are the protocol messages exchanged.

use crate::error::Error;
use cloudburst::dht::{
    krpc::{CompactAddrV4Info, CompactAddrV6Info},
    node::{AddrId, Id},
};
use std::net::{SocketAddrV4, SocketAddrV6};

fn decode_addr_ipv4_list<B>(nodes: B) -> Result<Vec<AddrId<SocketAddrV4>>, Error>
where
    B: AsRef<[u8]>,
{
    let nodes = nodes.as_ref();

    if nodes.len() % 26 != 0 {
        return Err(Error::CannotDeserializeKrpcMessage);
    }

    let addr_len = nodes.len() / 26;
    Ok((0..addr_len)
        .map(|i| {
            let offset = i * 26;

            let mut id: [u8; 20] = [0; 20];
            id.copy_from_slice(&nodes[offset..offset + 20]);
            let id = Id::from(id);

            let mut compact_addr: [u8; 6] = [0; 6];
            compact_addr.copy_from_slice(&nodes[offset + 20..offset + 26]);
            AddrId::new(SocketAddrV4::from_compact_addr(compact_addr), id)
        })
        .collect::<Vec<_>>())
}

fn decode_addr_ipv6_list<B>(nodes6: B) -> Result<Vec<AddrId<SocketAddrV6>>, Error>
where
    B: AsRef<[u8]>,
{
    let nodes6 = nodes6.as_ref();

    if nodes6.len() % 38 != 0 {
        return Err(Error::CannotDeserializeKrpcMessage);
    }

    let addr_len = nodes6.len() / 38;
    Ok((0..addr_len)
        .map(|i| {
            let offset = i * 38;

            let mut id: [u8; 20] = [0; 20];
            id.copy_from_slice(&nodes6[offset..offset + 20]);
            let id = Id::from(id);

            let mut compact_addr: [u8; 18] = [0; 18];
            compact_addr.copy_from_slice(&nodes6[offset + 20..offset + 38]);
            let addr = SocketAddrV6::from_compact_addr(compact_addr);

            AddrId::new(addr, id)
        })
        .collect::<Vec<_>>())
}

pub mod announce_peer;
pub mod get_peers;
pub mod transaction;

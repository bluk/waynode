// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! KRPC messages are the protocol messages exchanged.

use crate::{
    error::Error,
    node::{AddrId, Id},
};
use bt_bencode::Value;
use serde_bytes::{ByteBuf, Bytes};
use std::{
    collections::BTreeMap,
    convert::TryFrom,
    net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6},
};

/// Type of KRPC message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Kind<'a> {
    Query,
    Response,
    Error,
    Unknown(&'a str),
}

impl<'a> Kind<'a> {
    pub fn val(&self) -> &'a str {
        match self {
            Kind::Query => "q",
            Kind::Response => "r",
            Kind::Error => "e",
            Kind::Unknown(v) => v,
        }
    }
}

/// A KRPC message.
pub trait Msg {
    /// The transaction id for the message.
    fn tx_id(&self) -> Option<&[u8]>;

    /// The type of message.
    fn kind(&self) -> Option<Kind>;

    /// The client version as a byte buffer.
    fn client_version(&self) -> Option<&[u8]>;

    /// The client version as a string.
    fn client_version_str(&self) -> Option<&str> {
        self.client_version()
            .and_then(|v| std::str::from_utf8(v).ok())
    }
}

impl Msg for Value {
    fn tx_id(&self) -> Option<&[u8]> {
        self.get("t")
            .and_then(|t| t.as_byte_str())
            .map(|t| t.as_slice())
    }

    fn kind(&self) -> Option<Kind> {
        self.get("y").and_then(|y| y.as_str()).map(|y| match y {
            "q" => Kind::Query,
            "r" => Kind::Response,
            "e" => Kind::Error,
            y => Kind::Unknown(y),
        })
    }

    fn client_version(&self) -> Option<&[u8]> {
        self.get("v")
            .and_then(|v| v.as_byte_str())
            .map(|v| v.as_slice())
    }
}

/// A KPRC query message.
pub trait QueryMsg: Msg {
    /// The method name of the query.
    fn method_name(&self) -> Option<&[u8]>;

    /// The method name of the query as a string.
    fn method_name_str(&self) -> Option<&str> {
        self.method_name()
            .and_then(|v| core::str::from_utf8(v).ok())
    }

    /// The arguments for the query.
    fn args(&self) -> Option<&BTreeMap<ByteBuf, Value>>;

    /// The querying node ID.
    fn querying_node_id(&self) -> Option<Id> {
        self.args()
            .and_then(|a| a.get(Bytes::new(b"id")))
            .and_then(|id| id.as_byte_str())
            .and_then(|id| Id::try_from(id.as_slice()).ok())
    }
}

impl QueryMsg for Value {
    fn method_name(&self) -> Option<&[u8]> {
        self.get("q")
            .and_then(|q| q.as_byte_str())
            .map(|v| v.as_slice())
    }

    fn args(&self) -> Option<&BTreeMap<ByteBuf, Value>> {
        self.as_dict()
            .and_then(|dict| dict.get(Bytes::new(b"a")))
            .and_then(|a| a.as_dict())
    }
}

/// KRPC query arguments.
pub trait QueryArgs {
    /// The query method name.
    fn method_name() -> &'static [u8];

    /// The querying node ID.
    fn id(&self) -> Id;

    /// Represents the arguments as a Bencoded Value.
    fn to_value(&self) -> Value;
}

/// A KPRC response message.
pub trait RespMsg: Msg {
    /// The response values.
    fn values(&self) -> Option<&BTreeMap<ByteBuf, Value>>;

    /// The queried node id.
    fn queried_node_id(&self) -> Option<Id>;
}

impl RespMsg for Value {
    fn values(&self) -> Option<&BTreeMap<ByteBuf, Value>> {
        self.as_dict()
            .and_then(|dict| dict.get(Bytes::new(b"r")))
            .and_then(|a| a.as_dict())
    }

    fn queried_node_id(&self) -> Option<Id> {
        self.as_dict()
            .and_then(|dict| dict.get(Bytes::new(b"r")))
            .and_then(|a| a.as_dict())
            .and_then(|a| a.get(Bytes::new(b"id")))
            .and_then(|id| id.as_byte_str())
            .and_then(|id| Id::try_from(id.as_slice()).ok())
    }
}

/// KRPC response values.
pub trait RespVal {
    /// The queried node ID.
    fn id(&self) -> Id;

    /// Represents the values as a Bencoded value.
    fn to_value(&self) -> Value;
}

/// A KRPC error message.
pub trait ErrorMsg: Msg {
    /// The error value.
    fn error(&self) -> Option<&[Value]>;
}

impl ErrorMsg for Value {
    fn error(&self) -> Option<&[Value]> {
        self.get("e").and_then(|e| e.as_array()).map(|v| v.as_ref())
    }
}

/// Standard error codes in KRPC error messages.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ErrorCode {
    GenericError,
    ServerError,
    ProtocolError,
    MethodUnknown,
    Other(i32),
}

impl ErrorCode {
    fn code(&self) -> i32 {
        match self {
            ErrorCode::GenericError => 201,
            ErrorCode::ServerError => 202,
            ErrorCode::ProtocolError => 203,
            ErrorCode::MethodUnknown => 204,
            ErrorCode::Other(n) => *n,
        }
    }
}

/// The error value.
pub trait ErrorVal {
    /// The error code.
    fn code(&self) -> ErrorCode;

    /// The error description.
    fn description(&self) -> &str;

    /// Represents the arguments as a Bencoded Value.
    fn to_value(&self) -> Value;
}

/// An IPv4 socket address representable by a compact format.
///
/// The trait is intended to help convert an IPv4 socket address to the compact form used in KRPC messages.
///
/// This trait is sealed and cannot be implemented for types outside this crate.
pub trait CompactAddrV4Info: private::Sealed {
    /// Returns the address encoded as a compact address.
    fn to_compact_address(&self) -> [u8; 6];

    /// Converts from the compact address to the self type.
    fn from_compact_address(bytes: [u8; 6]) -> Self;
}

impl CompactAddrV4Info for SocketAddrV4 {
    fn to_compact_address(&self) -> [u8; 6] {
        let mut a: [u8; 6] = [0; 6];
        a[0..4].copy_from_slice(&self.ip().octets());
        a[4..6].copy_from_slice(&self.port().to_be_bytes());
        a
    }

    fn from_compact_address(bytes: [u8; 6]) -> Self {
        let mut ip: [u8; 4] = [0; 4];
        ip[0..4].copy_from_slice(&bytes[0..4]);
        let ip = Ipv4Addr::from(ip);

        let mut port: [u8; 2] = [0; 2];
        port[0..2].copy_from_slice(&bytes[4..6]);
        let port = u16::from_be_bytes(port);

        SocketAddrV4::new(ip, port)
    }
}

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
            let id = Id::new(id);

            let mut compact_addr: [u8; 6] = [0; 6];
            compact_addr.copy_from_slice(&nodes[offset + 20..offset + 26]);
            AddrId::new(SocketAddrV4::from_compact_address(compact_addr), id)
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
            let id = Id::new(id);

            let mut compact_addr: [u8; 18] = [0; 18];
            compact_addr.copy_from_slice(&nodes6[offset + 20..offset + 38]);
            let addr = SocketAddrV6::from_compact_address(compact_addr);

            AddrId::new(addr, id)
        })
        .collect::<Vec<_>>())
}

/// An IPv6 socket address representable by a compact format.
///
/// The trait is intended to help convert an IPv6 socket address to the compact form used in KRPC messages.
///
/// This trait is sealed and cannot be implemented for types outside this crate.
pub trait CompactAddrV6Info: private::Sealed {
    /// Returns the address encoded as a compact address.
    fn to_compact_address(&self) -> [u8; 18];

    /// Converts from the compact address to the self type.
    fn from_compact_address(bytes: [u8; 18]) -> Self;
}

impl CompactAddrV6Info for SocketAddrV6 {
    fn to_compact_address(&self) -> [u8; 18] {
        let mut a: [u8; 18] = [0; 18];
        a[0..16].copy_from_slice(&self.ip().octets());
        a[16..18].copy_from_slice(&self.port().to_be_bytes());
        a
    }

    fn from_compact_address(bytes: [u8; 18]) -> Self {
        let mut ip: [u8; 16] = [0; 16];
        ip[0..16].copy_from_slice(&bytes[0..16]);
        let ip = Ipv6Addr::from(ip);

        let mut port: [u8; 2] = [0; 2];
        port[0..2].copy_from_slice(&bytes[16..18]);
        let port = u16::from_be_bytes(port);

        SocketAddrV6::new(ip, port, 0, 0)
    }
}

mod private {
    use std::net::{SocketAddrV4, SocketAddrV6};

    pub trait Sealed {}

    impl Sealed for SocketAddrV6 {}
    impl Sealed for SocketAddrV4 {}
}

pub mod announce_peer;
pub mod error;
pub mod find_node;
pub mod get_peers;
pub mod ping;
pub(crate) mod ser;
pub mod transaction;

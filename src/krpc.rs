// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! KRPC messages are the protocol messages exchanged.

use crate::node::Id;
use bt_bencode::Value;
use serde_bytes::ByteBuf;
use std::collections::BTreeMap;
use std::convert::TryFrom;

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

// pub const METHOD_ANNOUNCE_PEER: &str = "announce_peer";

/// A KRPC message.
pub trait Msg {
    /// The transaction id for the message.
    fn tx_id(&self) -> Option<&ByteBuf>;

    /// The type of message.
    fn kind(&self) -> Option<Kind>;

    /// The client version as a byte buffer.
    fn client_version(&self) -> Option<&ByteBuf>;

    /// The client version as a string.
    fn client_version_str(&self) -> Option<&str>;
}

impl Msg for Value {
    fn tx_id(&self) -> Option<&ByteBuf> {
        self.as_dict()
            .and_then(|dict| dict.get(&ByteBuf::from(String::from("t"))))
            .and_then(|t| t.as_byte_str())
    }

    fn kind(&self) -> Option<Kind> {
        self.as_dict()
            .and_then(|dict| dict.get(&ByteBuf::from(String::from("y"))))
            .and_then(|y| y.as_byte_str())
            .and_then(|y| match y.as_slice() {
                b"q" => Some(Kind::Query),
                b"r" => Some(Kind::Response),
                b"e" => Some(Kind::Error),
                _ => None,
            })
    }

    fn client_version(&self) -> Option<&ByteBuf> {
        self.as_dict()
            .and_then(|dict| dict.get(&ByteBuf::from(String::from("v"))))
            .and_then(|v| v.as_byte_str())
    }

    fn client_version_str(&self) -> Option<&str> {
        self.client_version()
            .and_then(|v| std::str::from_utf8(v).ok())
    }
}

/// A KPRC query message.
pub trait QueryMsg: Msg {
    /// The method name of the query.
    fn method_name(&self) -> Option<&ByteBuf>;

    /// The method name of the query as a string.
    fn method_name_str(&self) -> Option<&str>;

    /// The arguments for the query.
    fn args(&self) -> Option<&BTreeMap<ByteBuf, Value>>;

    /// The querying node ID.
    fn querying_node_id(&self) -> Option<Id>;
}

impl QueryMsg for Value {
    fn method_name(&self) -> Option<&ByteBuf> {
        self.as_dict()
            .and_then(|v| v.get(&ByteBuf::from(String::from("q"))))
            .and_then(|q| q.as_byte_str())
    }

    fn method_name_str(&self) -> Option<&str> {
        self.method_name().and_then(|v| std::str::from_utf8(v).ok())
    }

    fn args(&self) -> Option<&BTreeMap<ByteBuf, Value>> {
        self.as_dict()
            .and_then(|dict| dict.get(&ByteBuf::from(String::from("a"))))
            .and_then(|a| a.as_dict())
    }

    fn querying_node_id(&self) -> Option<Id> {
        self.args()
            .and_then(|a| a.get(&ByteBuf::from(String::from("id"))))
            .and_then(|id| id.as_byte_str())
            .and_then(|id| Id::try_from(id.as_slice()).ok())
    }
}

/// KRPC query arguments.
pub trait QueryArgs {
    /// The query method name.
    fn method_name() -> &'static [u8];

    /// The querying node ID.
    fn id(&self) -> Id;

    /// Sets the querying node ID in the arguments.
    fn set_id(&mut self, id: Id);

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
            .and_then(|dict| dict.get(&ByteBuf::from(String::from("r"))))
            .and_then(|a| a.as_dict())
    }

    fn queried_node_id(&self) -> Option<Id> {
        self.as_dict()
            .and_then(|dict| dict.get(&ByteBuf::from(String::from("r"))))
            .and_then(|a| a.as_dict())
            .and_then(|a| a.get(&ByteBuf::from(String::from("id"))))
            .and_then(|id| id.as_byte_str())
            .and_then(|id| Id::try_from(id.as_slice()).ok())
    }
}
/// A KRPC error message.
pub trait ErrorMsg: Msg {
    /// The error value.
    fn error(&self) -> Option<&Vec<Value>>;
}

impl ErrorMsg for Value {
    fn error(&self) -> Option<&Vec<Value>> {
        self.as_dict()
            .and_then(|dict| dict.get(&ByteBuf::from(String::from("e"))))
            .and_then(|e| e.as_array())
    }
}

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

    /// Sets the error code.
    fn set_code(&mut self, code: ErrorCode);

    /// The error description.
    fn description(&self) -> &String;

    /// Sets the error description
    fn set_description(&mut self, description: String);

    /// Represents the arguments as a Bencoded Value.
    fn to_value(&self) -> Value;
}

pub mod announce_peer;
pub mod error;
pub mod find_node;
pub mod get_peers;
pub mod ping;
pub(crate) mod ser;

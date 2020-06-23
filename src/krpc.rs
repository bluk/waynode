// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! KRPC messages are the protocol messages exchanged.

use crate::{error::Error, node::Id};
use bt_bencode::{value::Number, Value};
use serde::{ser::SerializeMap, Deserialize, Serialize, Serializer};
use serde_bytes::ByteBuf;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::str::FromStr;

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

impl<'a> FromStr for Kind<'a> {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "q" => Ok(Kind::Query),
            "r" => Ok(Kind::Response),
            "e" => Ok(Kind::Error),
            _ => Err(Error::UnknownMessageKind),
        }
    }
}

pub const METHOD_PING: &str = "ping";
pub const METHOD_FIND_NODE: &str = "find_node";
pub const METHOD_GET_PEERS: &str = "get_peers";
pub const METHOD_ANNOUNCE_PEER: &str = "announce_peer";

pub trait KrpcMsg {
    fn transaction_id(&self) -> Option<&ByteBuf>;

    /// The type of message.
    fn kind(&self) -> Option<Kind>;

    fn client_version(&self) -> Option<&ByteBuf>;
}

impl KrpcMsg for Value {
    fn transaction_id(&self) -> Option<&ByteBuf> {
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
}

pub trait KrpcQueryMsg: KrpcMsg {
    fn querying_node_id(&self) -> Option<Id>;
}

impl KrpcQueryMsg for Value {
    fn querying_node_id(&self) -> Option<Id> {
        self.as_dict()
            .and_then(|dict| dict.get(&ByteBuf::from(String::from("a"))))
            .and_then(|a| a.as_dict())
            .and_then(|a| a.get(&ByteBuf::from(String::from("id"))))
            .and_then(|id| id.as_byte_str())
            .and_then(|id| Id::try_from(id.as_slice()).ok())
    }
}

pub trait KrpcRespMsg: KrpcMsg {
    fn queried_node_id(&self) -> Option<Id>;
}

impl KrpcRespMsg for Value {
    fn queried_node_id(&self) -> Option<Id> {
        self.as_dict()
            .and_then(|dict| dict.get(&ByteBuf::from(String::from("r"))))
            .and_then(|a| a.as_dict())
            .and_then(|a| a.get(&ByteBuf::from(String::from("id"))))
            .and_then(|id| id.as_byte_str())
            .and_then(|id| Id::try_from(id.as_slice()).ok())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Msg {
    Query(QueryMsg),
    Response(RespMsg),
    Error(ErrMsg),
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct QueryMsg {
    /// query arguments
    pub a: Option<Value>,
    /// method name of query
    pub q: ByteBuf,
    /// transaction id
    pub t: ByteBuf,
    /// client version
    pub v: Option<ByteBuf>,
}

impl QueryMsg {
    pub fn method_name(&self) -> Option<String> {
        String::from_utf8(self.q.to_vec()).ok()
    }
}

impl Serialize for QueryMsg {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        if self.a.is_some() {
            map.serialize_entry("a", &self.a)?;
        }
        map.serialize_entry("q", &self.q)?;
        map.serialize_entry("t", &self.t)?;
        if self.v.is_some() {
            map.serialize_entry("v", &self.v)?;
        }
        map.serialize_entry("y", "q")?;
        map.end()
    }
}

trait PingQueryMsg {
    fn id(&self) -> Option<Id>;

    fn set_id(&mut self, id: Id);
}

impl PingQueryMsg for QueryMsg {
    fn id(&self) -> Option<Id> {
        self.a
            .as_ref()
            .and_then(|a| a.as_dict())
            .and_then(|a| a.get(&ByteBuf::from(String::from("id"))))
            .and_then(|id| id.as_byte_str())
            .and_then(|id| Id::try_from(id.as_slice()).ok())
    }

    fn set_id(&mut self, id: Id) {
        let mut args: BTreeMap<ByteBuf, Value> = BTreeMap::new();
        args.insert(
            ByteBuf::from(String::from("id")),
            Value::ByteStr(ByteBuf::from(id.0)),
        );
        self.a = Some(Value::Dict(args));
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct RespMsg {
    /// return values
    pub r: Option<Value>,
    /// transaction id
    pub t: ByteBuf,
    /// client version
    pub v: Option<ByteBuf>,
}

impl Serialize for RespMsg {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        if self.r.is_some() {
            map.serialize_entry("r", &self.r)?;
        }
        map.serialize_entry("t", &self.t)?;
        if self.v.is_some() {
            map.serialize_entry("v", &self.v)?;
        }
        map.serialize_entry("y", "r")?;
        map.end()
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct ErrMsg {
    /// error details
    pub e: Option<Value>,
    /// transaction id
    pub t: ByteBuf,
    /// client version
    pub v: Option<ByteBuf>,
}

impl ErrMsg {
    pub fn code(&self) -> Option<Number> {
        self.e
            .as_ref()
            .and_then(|e| e.as_array())
            .and_then(|l| l.get(0))
            .and_then(|n| match n {
                Value::Int(num) => Some(*num),
                _ => None,
            })
    }

    pub fn msg(&self) -> Option<String> {
        self.e
            .as_ref()
            .and_then(|e| e.as_array())
            .and_then(|l| l.get(1))
            .and_then(|m| m.as_byte_str())
            .and_then(|s| String::from_utf8(s.to_vec()).ok())
    }
}

impl Serialize for ErrMsg {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        if self.e.is_some() {
            map.serialize_entry("e", &self.e)?;
        }
        map.serialize_entry("t", &self.t)?;
        if self.v.is_some() {
            map.serialize_entry("v", &self.v)?;
        }
        map.serialize_entry("y", "e")?;
        map.end()
    }
}

pub fn from_slice(bytes: &[u8]) -> Result<Msg, Error> {
    let value: Value =
        bt_bencode::from_slice(bytes).map_err(|_| Error::CannotDeserializeKrpcMessage)?;
    match &value {
        Value::Dict(dict) => {
            if let Some(kind) = dict.get(&ByteBuf::from(String::from("y"))) {
                match kind {
                    Value::ByteStr(kind) => match kind.as_slice() {
                        b"q" => {
                            let query_msg: QueryMsg = bt_bencode::from_value(value)
                                .map_err(|_| Error::CannotDeserializeKrpcMessage)?;
                            return Ok(Msg::Query(query_msg));
                        }
                        b"r" => {
                            let resp_msg: RespMsg = bt_bencode::from_value(value)
                                .map_err(|_| Error::CannotDeserializeKrpcMessage)?;
                            return Ok(Msg::Response(resp_msg));
                        }
                        b"e" => {
                            let err_msg: ErrMsg = bt_bencode::from_value(value)
                                .map_err(|_| Error::CannotDeserializeKrpcMessage)?;
                            return Ok(Msg::Error(err_msg));
                        }
                        _ => {}
                    },
                    Value::List(_) | Value::Dict(_) | Value::Int(_) => {}
                }
            }
        }
        Value::List(_) | Value::ByteStr(_) | Value::Int(_) => {}
    }

    Err(Error::CannotDeserializeKrpcMessage)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_ping() -> Result<(), Error> {
        let ping_query = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe";

        let de_msg: Msg = from_slice(ping_query)?;
        match de_msg {
            Msg::Query(q) => {
                assert_eq!(q.q, ByteBuf::from(METHOD_PING.as_bytes()));
                assert_eq!(q.method_name(), Some(String::from(METHOD_PING)));
                assert_eq!(
                    PingQueryMsg::id(&q),
                    Some(Id::try_from("abcdefghij0123456789".as_bytes())?)
                );

                let ser_msg =
                    bt_bencode::to_vec(&q).map_err(|_| Error::CannotDeserializeKrpcMessage)?;
                assert_eq!(ser_msg, ping_query.to_vec());
                Ok(())
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_serialize_ping() -> Result<(), Error> {
        let mut ping_query: QueryMsg = QueryMsg {
            a: None,
            q: ByteBuf::from(METHOD_PING.as_bytes()),
            t: ByteBuf::from("aa".as_bytes()),
            v: None,
        };
        PingQueryMsg::set_id(
            &mut ping_query,
            Id::try_from("abcdefghij0123456789".as_bytes())?,
        );
        let ser_msg =
            bt_bencode::to_vec(&ping_query).map_err(|_| Error::CannotDeserializeKrpcMessage)?;

        let expected = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe";
        assert_eq!(ser_msg, expected.to_vec());

        Ok(())
    }
}

// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Pings a node.
//!
//! The query and response are described in [BEP 5][bep_0005].
//!
//! [bep_0005]: http://bittorrent.org/beps/bep_0005.html

use crate::node::{Id, LocalId};
use bt_bencode::Value;
use serde_bytes::{ByteBuf, Bytes};
use std::collections::BTreeMap;
use std::convert::TryFrom;

/// The "ping" query method name.
pub const METHOD_PING: &[u8] = b"ping";

/// The arguments for the ping query message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PingQueryArgs {
    id: Id,
}

impl PingQueryArgs {
    /// Instantiates a new query message.
    pub fn new(id: LocalId) -> Self {
        Self { id: Id::from(id) }
    }

    /// Sets the querying node ID in the arguments.
    pub fn set_id<I>(&mut self, id: I)
    where
        I: Into<Id>,
    {
        self.id = id.into();
    }
}

impl super::QueryArgs for PingQueryArgs {
    fn method_name() -> &'static [u8] {
        METHOD_PING
    }

    fn id(&self) -> Id {
        self.id
    }

    fn to_value(&self) -> Value {
        Value::from(self)
    }
}

impl TryFrom<Value> for PingQueryArgs {
    type Error = crate::error::Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Self::try_from(&value)
    }
}

impl TryFrom<&Value> for PingQueryArgs {
    type Error = crate::error::Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        Self::try_from(
            value
                .as_dict()
                .ok_or(crate::error::Error::CannotDeserializeKrpcMessage)?,
        )
    }
}

impl TryFrom<&BTreeMap<ByteBuf, Value>> for PingQueryArgs {
    type Error = crate::error::Error;

    fn try_from(args: &BTreeMap<ByteBuf, Value>) -> Result<Self, Self::Error> {
        args.get(Bytes::new(b"id"))
            .and_then(|id| id.as_byte_str())
            .and_then(|id| Id::try_from(id.as_slice()).ok())
            .map(|id| PingQueryArgs { id })
            .ok_or(crate::error::Error::CannotDeserializeKrpcMessage)
    }
}

impl From<PingQueryArgs> for Value {
    fn from(args: PingQueryArgs) -> Self {
        let mut d: BTreeMap<ByteBuf, Value> = BTreeMap::new();
        d.insert(
            ByteBuf::from(String::from("id")),
            Value::ByteStr(ByteBuf::from(args.id)),
        );
        Value::Dict(d)
    }
}

impl From<&PingQueryArgs> for Value {
    fn from(args: &PingQueryArgs) -> Self {
        Value::from(*args)
    }
}

/// The value for the ping response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PingRespValues {
    id: Id,
}

impl PingRespValues {
    /// Instantiates a new instance.
    pub fn new(id: LocalId) -> Self {
        Self { id: Id::from(id) }
    }

    /// Sets the queried node Id.
    pub fn set_id<I>(&mut self, id: I)
    where
        I: Into<Id>,
    {
        self.id = id.into();
    }
}

impl super::RespVal for PingRespValues {
    fn id(&self) -> Id {
        self.id
    }

    fn to_value(&self) -> Value {
        Value::from(*self)
    }
}

impl TryFrom<&BTreeMap<ByteBuf, Value>> for PingRespValues {
    type Error = crate::error::Error;

    fn try_from(values: &BTreeMap<ByteBuf, Value>) -> Result<Self, Self::Error> {
        values
            .get(Bytes::new(b"id"))
            .and_then(|id| id.as_byte_str())
            .and_then(|id| Id::try_from(id.as_slice()).ok())
            .map(|id| PingRespValues { id })
            .ok_or(crate::error::Error::CannotDeserializeKrpcMessage)
    }
}

impl From<PingRespValues> for Value {
    fn from(values: PingRespValues) -> Self {
        Value::from(&values)
    }
}

impl From<&PingRespValues> for Value {
    fn from(values: &PingRespValues) -> Self {
        let mut args: BTreeMap<ByteBuf, Value> = BTreeMap::new();
        args.insert(
            ByteBuf::from(String::from("id")),
            Value::ByteStr(ByteBuf::from(values.id)),
        );
        Value::Dict(args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::error::Error;
    use crate::krpc::{Kind, Msg, QueryArgs, QueryMsg, RespMsg, RespVal};

    #[test]
    fn test_serde_ping_query() -> Result<(), Error> {
        let ping_query = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe";

        let msg_value: Value = bt_bencode::from_reader(&ping_query[..])?;
        assert_eq!(msg_value.kind(), Some(Kind::Query));
        assert_eq!(msg_value.method_name(), Some(METHOD_PING));
        assert_eq!(
            msg_value.method_name_str(),
            Some(core::str::from_utf8(METHOD_PING).unwrap())
        );
        assert_eq!(msg_value.tx_id(), Some(b"aa".as_ref()));
        if let Some(args) = msg_value
            .args()
            .and_then(|a| PingQueryArgs::try_from(a).ok())
        {
            assert_eq!(args.id(), Id::new(*b"abcdefghij0123456789"));

            let args_value = args.into();
            let ser_query_msg = crate::krpc::ser::QueryMsg {
                a: Some(&args_value),
                q: Bytes::new(METHOD_PING),
                t: Bytes::new(b"aa"),
                v: None,
            };
            let ser_msg = bt_bencode::to_vec(&ser_query_msg)
                .map_err(|_| Error::CannotDeserializeKrpcMessage)?;
            assert_eq!(ser_msg, ping_query);
            Ok(())
        } else {
            panic!()
        }
    }

    #[test]
    fn test_serde_ping_response_values() -> Result<(), Error> {
        let ping_resp = b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re";

        let msg_value: Value = bt_bencode::from_reader(&ping_resp[..])?;
        assert_eq!(msg_value.kind(), Some(Kind::Response));
        assert_eq!(msg_value.method_name(), None);
        assert_eq!(msg_value.method_name_str(), None);
        assert_eq!(msg_value.tx_id(), Some(b"aa".as_ref()));

        if let Some(values) = msg_value
            .values()
            .and_then(|a| PingRespValues::try_from(a).ok())
        {
            assert_eq!(values.id(), Id::new(*b"mnopqrstuvwxyz123456"));

            let resp_values = values.into();
            let ser_resp_msg = crate::krpc::ser::RespMsg {
                r: Some(&resp_values),
                t: Bytes::new(b"aa"),
                v: None,
            };
            let ser_msg = bt_bencode::to_vec(&ser_resp_msg)
                .map_err(|_| Error::CannotDeserializeKrpcMessage)?;
            assert_eq!(ser_msg, ping_resp);
            Ok(())
        } else {
            panic!()
        }
    }
}

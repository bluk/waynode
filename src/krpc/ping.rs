// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::node::Id;
use bt_bencode::Value;
use serde_bytes::ByteBuf;
use std::collections::BTreeMap;
use std::convert::TryFrom;

pub const METHOD_PING: &str = "ping";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PingQueryArgs {
    id: Id,
}

impl PingQueryArgs {
    pub fn with_id(id: Id) -> Self {
        Self { id }
    }
}

impl super::QueryArgs for PingQueryArgs {
    fn method_name() -> &'static [u8] {
        METHOD_PING.as_bytes()
    }

    fn id(&self) -> &Id {
        &self.id
    }

    fn set_id(&mut self, id: Id) {
        self.id = id;
    }

    fn to_value(&self) -> Value {
        Value::from(self)
    }
}

impl TryFrom<Value> for PingQueryArgs {
    type Error = crate::error::Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
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
        args.get(&ByteBuf::from(String::from("id")))
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PingRespValues {
    id: Id,
}

impl PingRespValues {
    pub fn with_id(id: Id) -> Self {
        Self { id }
    }

    pub fn id(&self) -> &Id {
        &self.id
    }

    pub fn set_id(&mut self, id: Id) {
        self.id = id;
    }
}

impl TryFrom<&BTreeMap<ByteBuf, Value>> for PingRespValues {
    type Error = crate::error::Error;

    fn try_from(values: &BTreeMap<ByteBuf, Value>) -> Result<Self, Self::Error> {
        values
            .get(&ByteBuf::from(String::from("id")))
            .and_then(|id| id.as_byte_str())
            .and_then(|id| Id::try_from(id.as_slice()).ok())
            .map(|id| PingRespValues { id })
            .ok_or(crate::error::Error::CannotDeserializeKrpcMessage)
    }
}

impl From<PingRespValues> for Value {
    fn from(ping_values: PingRespValues) -> Self {
        let mut args: BTreeMap<ByteBuf, Value> = BTreeMap::new();
        args.insert(
            ByteBuf::from(String::from("id")),
            Value::ByteStr(ByteBuf::from(ping_values.id)),
        );
        Value::Dict(args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::error::Error;
    use crate::krpc::{Kind, Msg, QueryArgs, QueryMsg, RespMsg};

    #[test]
    fn test_serde_ping_query() -> Result<(), Error> {
        let ping_query = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe";

        let msg_value: Value = bt_bencode::from_reader(&ping_query[..])?;
        assert_eq!(msg_value.kind(), Some(Kind::Query));
        assert_eq!(
            msg_value.method_name(),
            Some(&ByteBuf::from(METHOD_PING.as_bytes()))
        );
        assert_eq!(msg_value.method_name_str(), Some(METHOD_PING));
        assert_eq!(msg_value.tx_id(), Some(&ByteBuf::from("aa")));
        if let Some(args) = msg_value
            .args()
            .and_then(|a| PingQueryArgs::try_from(a).ok())
        {
            assert_eq!(args.id(), &Id::try_from("abcdefghij0123456789".as_bytes())?);

            let args_value = args.into();
            let ser_query_msg = crate::krpc::ser::QueryMsg {
                a: Some(&args_value),
                q: &ByteBuf::from(METHOD_PING),
                t: &ByteBuf::from("aa"),
                v: None,
            };
            let ser_msg = bt_bencode::to_vec(&ser_query_msg)
                .map_err(|_| Error::CannotDeserializeKrpcMessage)?;
            assert_eq!(ser_msg, ping_query.to_vec());
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
        assert_eq!(msg_value.tx_id(), Some(&ByteBuf::from("aa")));

        if let Some(values) = msg_value
            .values()
            .and_then(|a| PingRespValues::try_from(a).ok())
        {
            assert_eq!(
                values.id(),
                &Id::try_from("mnopqrstuvwxyz123456".as_bytes())?
            );

            let resp_values = values.into();
            let ser_resp_msg = crate::krpc::ser::RespMsg {
                r: Some(&resp_values),
                t: &ByteBuf::from("aa"),
                v: None,
            };
            let ser_msg = bt_bencode::to_vec(&ser_resp_msg)
                .map_err(|_| Error::CannotDeserializeKrpcMessage)?;
            assert_eq!(ser_msg, ping_resp.to_vec());
            Ok(())
        } else {
            panic!()
        }
    }
}

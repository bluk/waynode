// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Announces a peer for a torrent.
//!
//! The query and response are described in [BEP 5][bep_0005].
//!
//! [bep_0005]: http://bittorrent.org/beps/bep_0005.html

use crate::{
    error::Error,
    node::{Id, LocalId},
    torrent::InfoHash,
};
use bt_bencode::{value::Number, Value};
use serde_bytes::ByteBuf;
use std::{collections::BTreeMap, convert::TryFrom};

/// The "announce_peer" query method name.
pub const METHOD_ANNOUNCE_PEER: &str = "announce_peer";

/// The arguments for the announce peer query message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnnouncePeerQueryArgs {
    id: Id,
    info_hash: InfoHash,
    token: ByteBuf,
    port: Option<u16>,
    implied_port: Option<bool>,
}

impl AnnouncePeerQueryArgs {
    /// Instantiates a new query message.
    pub fn new(
        id: LocalId,
        info_hash: InfoHash,
        token: ByteBuf,
        port: Option<u16>,
        implied_port: Option<bool>,
    ) -> Self {
        Self {
            id: Id::from(id),
            info_hash,
            token,
            port,
            implied_port,
        }
    }

    /// Sets the querying node ID in the arguments.
    pub fn set_id<I>(&mut self, id: I)
    where
        I: Into<Id>,
    {
        self.id = id.into();
    }

    /// Returns the `InfoHash` for the relevant torrent.
    pub fn info_hash(&self) -> InfoHash {
        self.info_hash
    }

    /// Sets the `InfoHash` for the relevant torrent.
    pub fn set_info_hash(&mut self, info_hash: InfoHash) {
        self.info_hash = info_hash;
    }

    /// Returns the token which is used by the queried node for verification.
    pub fn token(&self) -> &ByteBuf {
        &self.token
    }

    /// Sets the token which is used by the queried node for verification.
    pub fn set_token(&mut self, token: ByteBuf) {
        self.token = token;
    }

    /// Returns the port which peers in the torrent should connect to.
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    /// Sets the port which peers in the torrent should connect to.
    pub fn set_port(&mut self, port: Option<u16>) {
        self.port = port;
    }

    /// Returns if the port should be implied from the querying node's DHT sending port.
    pub fn implied_port(&self) -> Option<bool> {
        self.implied_port
    }

    /// Sets if the port should be implied from the querying node's DHT sending port.
    pub fn set_implied_port(&mut self, implied_port: Option<bool>) {
        self.implied_port = implied_port;
    }
}

impl super::QueryArgs for AnnouncePeerQueryArgs {
    fn method_name() -> &'static [u8] {
        METHOD_ANNOUNCE_PEER.as_bytes()
    }

    fn id(&self) -> Id {
        self.id
    }

    fn to_value(&self) -> Value {
        Value::from(self)
    }
}

impl TryFrom<Value> for AnnouncePeerQueryArgs {
    type Error = crate::error::Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Self::try_from(&value)
    }
}

impl TryFrom<&Value> for AnnouncePeerQueryArgs {
    type Error = crate::error::Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        Self::try_from(
            value
                .as_dict()
                .ok_or(crate::error::Error::CannotDeserializeKrpcMessage)?,
        )
    }
}

impl TryFrom<&BTreeMap<ByteBuf, Value>> for AnnouncePeerQueryArgs {
    type Error = crate::error::Error;

    fn try_from(args: &BTreeMap<ByteBuf, Value>) -> Result<Self, Self::Error> {
        match (
            args.get(&ByteBuf::from(String::from("id")))
                .and_then(|id| id.as_byte_str())
                .and_then(|id| Id::try_from(id.as_slice()).ok()),
            args.get(&ByteBuf::from(String::from("info_hash")))
                .and_then(|info_hash| info_hash.as_byte_str())
                .and_then(|info_hash| InfoHash::try_from(info_hash.as_slice()).ok()),
            args.get(&ByteBuf::from(String::from("token")))
                .and_then(|token| token.as_byte_str()),
            args.get(&ByteBuf::from(String::from("port")))
                .and_then(|port| port.as_u64())
                .and_then(|port| u16::try_from(port).ok()),
            args.get(&ByteBuf::from(String::from("implied_port")))
                .and_then(|implied_port| implied_port.as_u64())
                .map(|implied_port| implied_port != 0),
        ) {
            (Some(id), Some(info_hash), Some(token), port, implied_port) => {
                Ok(AnnouncePeerQueryArgs {
                    id,
                    info_hash,
                    token: token.clone(),
                    port,
                    implied_port,
                })
            }
            _ => Err(crate::error::Error::CannotDeserializeKrpcMessage),
        }
    }
}

impl From<AnnouncePeerQueryArgs> for Value {
    fn from(args: AnnouncePeerQueryArgs) -> Self {
        Value::from(&args)
    }
}

impl From<&AnnouncePeerQueryArgs> for Value {
    fn from(args: &AnnouncePeerQueryArgs) -> Self {
        let mut d: BTreeMap<ByteBuf, Value> = BTreeMap::new();
        d.insert(
            ByteBuf::from(String::from("id")),
            Value::ByteStr(ByteBuf::from(args.id)),
        );
        if let Some(implied_port) = args.implied_port {
            d.insert(
                ByteBuf::from(String::from("implied_port")),
                Value::Int(if implied_port {
                    Number::Unsigned(1)
                } else {
                    Number::Unsigned(0)
                }),
            );
        }
        d.insert(
            ByteBuf::from(String::from("info_hash")),
            Value::ByteStr(ByteBuf::from(args.info_hash)),
        );
        d.insert(
            ByteBuf::from(String::from("port")),
            Value::Int(
                args.port
                    .map(|port| Number::Unsigned(u64::from(port)))
                    .unwrap_or(Number::Unsigned(0)),
            ),
        );
        d.insert(
            ByteBuf::from(String::from("token")),
            Value::ByteStr(args.token.clone()),
        );
        Value::Dict(d)
    }
}

/// The value for the announce peer response.
pub struct AnnouncePeerRespValues {
    id: Id,
}

impl AnnouncePeerRespValues {
    /// Instantiates a new instance.
    pub fn new(id: LocalId) -> Self {
        Self { id: id.into() }
    }

    /// Sets the queried node Id.
    pub fn set_id<I>(&mut self, id: I)
    where
        I: Into<Id>,
    {
        self.id = id.into();
    }
}

impl super::RespVal for AnnouncePeerRespValues {
    fn id(&self) -> Id {
        self.id
    }

    fn to_value(&self) -> Value {
        Value::from(self)
    }
}

impl TryFrom<&BTreeMap<ByteBuf, Value>> for AnnouncePeerRespValues {
    type Error = Error;

    fn try_from(values: &BTreeMap<ByteBuf, Value>) -> Result<Self, Self::Error> {
        match values
            .get(&ByteBuf::from(String::from("id")))
            .and_then(|id| id.as_byte_str())
            .and_then(|id| Id::try_from(id.as_slice()).ok())
        {
            Some(id) => Ok(AnnouncePeerRespValues { id }),
            _ => Err(crate::error::Error::CannotDeserializeKrpcMessage),
        }
    }
}

impl From<AnnouncePeerRespValues> for Value {
    fn from(values: AnnouncePeerRespValues) -> Self {
        Value::from(&values)
    }
}

impl From<&AnnouncePeerRespValues> for Value {
    fn from(values: &AnnouncePeerRespValues) -> Self {
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
    use serde_bytes::Bytes;

    use super::*;

    use crate::error::Error;
    use crate::krpc::{Kind, Msg, QueryArgs, QueryMsg, RespMsg, RespVal};

    #[test]
    fn test_serde_announce_peer_query() -> Result<(), Error> {
        let announce_peeer_query = b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti6331e5:token8:abcd1234e1:q13:announce_peer1:t2:aa1:y1:qe";

        let msg_value: Value = bt_bencode::from_reader(&announce_peeer_query[..])?;
        assert_eq!(msg_value.kind(), Some(Kind::Query));
        assert_eq!(
            msg_value.method_name(),
            Some(METHOD_ANNOUNCE_PEER.as_bytes())
        );
        assert_eq!(msg_value.method_name_str(), Some(METHOD_ANNOUNCE_PEER));
        assert_eq!(msg_value.tx_id(), Some("aa".as_bytes()));
        if let Some(args) = msg_value
            .args()
            .and_then(|a| AnnouncePeerQueryArgs::try_from(a).ok())
        {
            assert_eq!(args.id(), Id::try_from("abcdefghij0123456789".as_bytes())?);
            assert_eq!(
                args.info_hash(),
                InfoHash::try_from("mnopqrstuvwxyz123456".as_bytes())?
            );
            assert_eq!(args.token(), &ByteBuf::from("abcd1234"));
            assert_eq!(args.port(), Some(6331));
            assert!(args.implied_port().is_none());

            let args_value = args.into();
            let ser_query_msg = crate::krpc::ser::QueryMsg {
                a: Some(&args_value),
                q: Bytes::new(METHOD_ANNOUNCE_PEER.as_bytes()),
                t: Bytes::new(b"aa"),
                v: None,
            };
            let ser_msg = bt_bencode::to_vec(&ser_query_msg)
                .map_err(|_| Error::CannotDeserializeKrpcMessage)?;
            assert_eq!(ser_msg, announce_peeer_query.to_vec());
            Ok(())
        } else {
            panic!()
        }
    }

    #[test]
    fn test_serde_announce_peer_response_values() -> Result<(), Error> {
        let announce_peer_resp = b"d1:rd2:id20:0123456789abcdefghije1:t2:aa1:y1:re";

        let msg_value: Value = bt_bencode::from_reader(&announce_peer_resp[..])?;
        assert_eq!(msg_value.kind(), Some(Kind::Response));
        assert_eq!(msg_value.method_name(), None);
        assert_eq!(msg_value.method_name_str(), None);
        assert_eq!(msg_value.tx_id(), Some("aa".as_bytes()));

        if let Some(values) = msg_value
            .values()
            .and_then(|a| AnnouncePeerRespValues::try_from(a).ok())
        {
            assert_eq!(
                values.id(),
                Id::try_from("0123456789abcdefghij".as_bytes())?
            );

            let resp_values = values.into();
            let ser_resp_msg = crate::krpc::ser::RespMsg {
                r: Some(&resp_values),
                t: Bytes::new("aa".as_bytes()),
                v: None,
            };
            let ser_msg = bt_bencode::to_vec(&ser_resp_msg)
                .map_err(|_| Error::CannotDeserializeKrpcMessage)?;
            assert_eq!(ser_msg, announce_peer_resp.to_vec());
            Ok(())
        } else {
            panic!()
        }
    }
}

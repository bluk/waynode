// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Finds nodes in the distributed hash table.
//!
//! The query and response are described in [BEP 5][bep_0005].
//!
//! [bep_0005]: http://bittorrent.org/beps/bep_0005.html

use crate::{
    krpc::{CompactAddrV4Info, CompactAddrV6Info},
    node::{AddrId, Id, LocalId},
};
use bt_bencode::Value;
use serde_bytes::{ByteBuf, Bytes};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::net::{SocketAddrV4, SocketAddrV6};

/// The "find_node" query method name.
pub const METHOD_FIND_NODE: &[u8] = b"find_node";

/// The arguments for the find node query message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FindNodeQueryArgs {
    id: Id,
    target: Id,
}

impl FindNodeQueryArgs {
    /// Instantiates a new query message with the local querying node Id and the target Id.
    pub fn new(id: LocalId, target: Id) -> Self {
        Self {
            id: id.into(),
            target,
        }
    }

    /// Sets the querying node ID in the arguments.
    pub fn set_id<I>(&mut self, id: I)
    where
        I: Into<Id>,
    {
        self.id = id.into();
    }

    /// Returns the target Id.
    pub fn target(&self) -> Id {
        self.target
    }

    /// Sets the target Id.
    pub fn set_target(&mut self, target: Id) {
        self.target = target;
    }
}

impl super::QueryArgs for FindNodeQueryArgs {
    fn method_name() -> &'static [u8] {
        METHOD_FIND_NODE
    }

    fn id(&self) -> Id {
        self.id
    }

    fn to_value(&self) -> Value {
        Value::from(self)
    }
}

impl TryFrom<Value> for FindNodeQueryArgs {
    type Error = crate::error::Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Self::try_from(&value)
    }
}

impl TryFrom<&Value> for FindNodeQueryArgs {
    type Error = crate::error::Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        Self::try_from(
            value
                .as_dict()
                .ok_or(crate::error::Error::CannotDeserializeKrpcMessage)?,
        )
    }
}

impl TryFrom<&BTreeMap<ByteBuf, Value>> for FindNodeQueryArgs {
    type Error = crate::error::Error;

    fn try_from(args: &BTreeMap<ByteBuf, Value>) -> Result<Self, Self::Error> {
        match (
            args.get(Bytes::new(b"id"))
                .and_then(|id| id.as_byte_str())
                .and_then(|id| Id::try_from(id.as_slice()).ok()),
            args.get(Bytes::new(b"target"))
                .and_then(|t| t.as_byte_str())
                .and_then(|t| Id::try_from(t.as_slice()).ok()),
        ) {
            (Some(id), Some(target)) => Ok(FindNodeQueryArgs { id, target }),
            _ => Err(crate::error::Error::CannotDeserializeKrpcMessage),
        }
    }
}

impl From<FindNodeQueryArgs> for Value {
    fn from(args: FindNodeQueryArgs) -> Self {
        Value::from(&args)
    }
}

impl From<&FindNodeQueryArgs> for Value {
    fn from(args: &FindNodeQueryArgs) -> Self {
        let mut d: BTreeMap<ByteBuf, Value> = BTreeMap::new();
        d.insert(
            ByteBuf::from(String::from("id")),
            Value::ByteStr(ByteBuf::from(args.id)),
        );
        d.insert(
            ByteBuf::from(String::from("target")),
            Value::ByteStr(ByteBuf::from(args.target)),
        );
        Value::Dict(d)
    }
}

/// The value for the find node response.
pub struct FindNodeRespValues {
    id: Id,
    nodes: Option<Vec<AddrId<SocketAddrV4>>>,
    nodes6: Option<Vec<AddrId<SocketAddrV6>>>,
}

impl FindNodeRespValues {
    /// Instantiates a new instance.
    pub fn with_id_and_nodes_and_nodes6(
        id: LocalId,
        nodes: Option<Vec<AddrId<SocketAddrV4>>>,
        nodes6: Option<Vec<AddrId<SocketAddrV6>>>,
    ) -> Self {
        Self {
            id: Id::from(id),
            nodes,
            nodes6,
        }
    }

    /// Sets the queried node Id.
    pub fn set_id<I>(&mut self, id: I)
    where
        I: Into<Id>,
    {
        self.id = id.into();
    }

    /// Returns the IPv4 nodes.
    pub fn nodes(&self) -> Option<&Vec<AddrId<SocketAddrV4>>> {
        self.nodes.as_ref()
    }

    /// Sets the IPv4 nodes.
    pub fn set_nodes(&mut self, nodes: Option<Vec<AddrId<SocketAddrV4>>>) {
        self.nodes = nodes;
    }

    /// Returns the IPv6 nodes.
    pub fn nodes6(&self) -> Option<&Vec<AddrId<SocketAddrV6>>> {
        self.nodes6.as_ref()
    }

    /// Sets the IPv6 nodes.
    pub fn set_nodes6(&mut self, nodes6: Option<Vec<AddrId<SocketAddrV6>>>) {
        self.nodes6 = nodes6;
    }
}

impl super::RespVal for FindNodeRespValues {
    fn id(&self) -> Id {
        self.id
    }

    fn to_value(&self) -> Value {
        Value::from(self)
    }
}

impl TryFrom<&BTreeMap<ByteBuf, Value>> for FindNodeRespValues {
    type Error = crate::error::Error;

    fn try_from(values: &BTreeMap<ByteBuf, Value>) -> Result<Self, Self::Error> {
        match (
            values
                .get(Bytes::new(b"id"))
                .and_then(|id| id.as_byte_str())
                .and_then(|id| Id::try_from(id.as_slice()).ok()),
            values
                .get(Bytes::new(b"nodes"))
                .and_then(|nodes| nodes.as_byte_str())
                .map(super::decode_addr_ipv4_list),
            values
                .get(Bytes::new(b"nodes6"))
                .and_then(|nodes6| nodes6.as_byte_str())
                .map(super::decode_addr_ipv6_list),
        ) {
            (Some(id), nodes, nodes6) => {
                let nodes = nodes.transpose()?;
                let nodes6 = nodes6.transpose()?;
                Ok(FindNodeRespValues { id, nodes, nodes6 })
            }
            _ => Err(crate::error::Error::CannotDeserializeKrpcMessage),
        }
    }
}

impl From<FindNodeRespValues> for Value {
    fn from(values: FindNodeRespValues) -> Self {
        Value::from(&values)
    }
}

impl From<&FindNodeRespValues> for Value {
    fn from(values: &FindNodeRespValues) -> Self {
        let mut args: BTreeMap<ByteBuf, Value> = BTreeMap::new();
        args.insert(
            ByteBuf::from(String::from("id")),
            Value::ByteStr(ByteBuf::from(values.id)),
        );

        if let Some(nodes) = &values.nodes {
            let mut byte_str: Vec<u8> = vec![];
            for n in nodes {
                byte_str.extend_from_slice(&n.id().0);
                byte_str.extend_from_slice(&n.addr().to_compact_address());
            }
            args.insert(
                ByteBuf::from(String::from("nodes")),
                Value::ByteStr(ByteBuf::from(byte_str)),
            );
        }

        if let Some(nodes6) = &values.nodes6 {
            let mut byte_str: Vec<u8> = vec![];
            for n in nodes6 {
                byte_str.extend_from_slice(&n.id().0);
                byte_str.extend_from_slice(&n.addr().to_compact_address());
            }
            args.insert(
                ByteBuf::from(String::from("nodes6")),
                Value::ByteStr(ByteBuf::from(byte_str)),
            );
        }

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
    fn test_serde_find_node_query() -> Result<(), Error> {
        let find_node_query = b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe";

        let msg_value: Value = bt_bencode::from_reader(&find_node_query[..])?;
        assert_eq!(msg_value.kind(), Some(Kind::Query));
        assert_eq!(msg_value.method_name(), Some(METHOD_FIND_NODE));
        assert_eq!(
            msg_value.method_name_str(),
            Some(core::str::from_utf8(METHOD_FIND_NODE).unwrap())
        );
        assert_eq!(msg_value.tx_id(), Some(b"aa".as_ref()));
        if let Some(args) = msg_value
            .args()
            .and_then(|a| FindNodeQueryArgs::try_from(a).ok())
        {
            assert_eq!(args.id(), Id::new(*b"abcdefghij0123456789"));
            assert_eq!(args.target(), Id::new(*b"mnopqrstuvwxyz123456"));

            let args_value = args.into();
            let ser_query_msg = crate::krpc::ser::QueryMsg {
                a: Some(&args_value),
                q: Bytes::new(METHOD_FIND_NODE),
                t: Bytes::new(b"aa"),
                v: None,
            };
            let ser_msg = bt_bencode::to_vec(&ser_query_msg)
                .map_err(|_| Error::CannotDeserializeKrpcMessage)?;
            assert_eq!(ser_msg, find_node_query);
            Ok(())
        } else {
            panic!()
        }
    }

    #[test]
    fn test_serde_find_node_response_values_one_node() -> Result<(), Error> {
        use crate::node::NodeIdGenerator;
        use std::net::Ipv4Addr;

        let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1234);
        let compact_addr = addr.to_compact_address();
        let node_id = addr.ip().make_node_id(None)?;
        let mut find_node_resp = vec![];
        find_node_resp.extend_from_slice(b"d1:rd2:id20:0123456789abcdefghij5:nodes26:");
        find_node_resp.extend_from_slice(&node_id.0[..]);
        find_node_resp.extend_from_slice(&compact_addr[..]);
        find_node_resp.extend_from_slice(b"e1:t2:aa1:y1:re");

        let msg_value: Value = bt_bencode::from_reader(&find_node_resp[..])?;
        assert_eq!(msg_value.kind(), Some(Kind::Response));
        assert_eq!(msg_value.method_name(), None);
        assert_eq!(msg_value.method_name_str(), None);
        assert_eq!(msg_value.tx_id(), Some(b"aa".as_ref()));

        if let Some(values) = msg_value
            .values()
            .and_then(|a| FindNodeRespValues::try_from(a).ok())
        {
            assert_eq!(values.id(), Id::new(*b"0123456789abcdefghij"));

            let resp_values = values.into();
            let ser_resp_msg = crate::krpc::ser::RespMsg {
                r: Some(&resp_values),
                t: Bytes::new(b"aa"),
                v: None,
            };
            let ser_msg = bt_bencode::to_vec(&ser_resp_msg)
                .map_err(|_| Error::CannotDeserializeKrpcMessage)?;
            assert_eq!(ser_msg, find_node_resp);
            Ok(())
        } else {
            panic!()
        }
    }
}

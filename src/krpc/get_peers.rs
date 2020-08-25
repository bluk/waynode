// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::{
    addr::{CompactAddressV4, CompactAddressV6, CompactNodeInfo},
    error::Error,
    node::Id,
};
use bt_bencode::Value;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use std::collections::BTreeMap;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::{convert::TryFrom, fmt};

pub const METHOD_GET_PEERS: &str = "get_peers";

#[derive(Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct InfoHash(pub(crate) [u8; 20]);

impl InfoHash {
    /// Instantiates an Id with bytes representing the 160-bit identifier.
    pub fn with_bytes(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }
}

impl Into<Vec<u8>> for InfoHash {
    fn into(self) -> Vec<u8> {
        Vec::from(self.0)
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GetPeersQueryArgs {
    id: Id,
    info_hash: InfoHash,
}

impl GetPeersQueryArgs {
    pub fn with_local_and_info_hash(id: Id, info_hash: InfoHash) -> Self {
        Self { id, info_hash }
    }

    pub fn info_hash(&self) -> InfoHash {
        self.info_hash
    }

    pub fn set_info_hash(&mut self, info_hash: InfoHash) {
        self.info_hash = info_hash;
    }
}

impl super::QueryArgs for GetPeersQueryArgs {
    fn method_name() -> &'static [u8] {
        METHOD_GET_PEERS.as_bytes()
    }

    fn id(&self) -> Id {
        self.id
    }

    fn set_id(&mut self, id: Id) {
        self.id = id;
    }

    fn to_value(&self) -> Value {
        Value::from(self)
    }
}

impl TryFrom<Value> for GetPeersQueryArgs {
    type Error = crate::error::Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Self::try_from(
            value
                .as_dict()
                .ok_or(crate::error::Error::CannotDeserializeKrpcMessage)?,
        )
    }
}

impl TryFrom<&BTreeMap<ByteBuf, Value>> for GetPeersQueryArgs {
    type Error = crate::error::Error;

    fn try_from(args: &BTreeMap<ByteBuf, Value>) -> Result<Self, Self::Error> {
        match (
            args.get(&ByteBuf::from(String::from("id")))
                .and_then(|id| id.as_byte_str())
                .and_then(|id| Id::try_from(id.as_slice()).ok()),
            args.get(&ByteBuf::from(String::from("info_hash")))
                .and_then(|t| t.as_byte_str())
                .and_then(|t| InfoHash::try_from(t.as_slice()).ok()),
        ) {
            (Some(id), Some(info_hash)) => Ok(GetPeersQueryArgs { id, info_hash }),
            _ => Err(crate::error::Error::CannotDeserializeKrpcMessage),
        }
    }
}

impl From<GetPeersQueryArgs> for Value {
    fn from(args: GetPeersQueryArgs) -> Self {
        let mut d: BTreeMap<ByteBuf, Value> = BTreeMap::new();
        d.insert(
            ByteBuf::from(String::from("id")),
            Value::ByteStr(ByteBuf::from(args.id)),
        );
        d.insert(
            ByteBuf::from(String::from("info_hash")),
            Value::ByteStr(ByteBuf::from(args.info_hash)),
        );
        Value::Dict(d)
    }
}

impl From<&GetPeersQueryArgs> for Value {
    fn from(args: &GetPeersQueryArgs) -> Self {
        Value::from(*args)
    }
}

pub struct GetPeersRespValues {
    id: Id,
    token: ByteBuf,
    values: Option<Vec<SocketAddr>>,
    nodes: Option<Vec<CompactNodeInfo<SocketAddrV4>>>,
    nodes6: Option<Vec<CompactNodeInfo<SocketAddrV6>>>,
}

impl GetPeersRespValues {
    pub fn with_id_and_token_and_values_and_nodes_and_nodes6(
        id: Id,
        token: ByteBuf,
        values: Option<Vec<SocketAddr>>,
        nodes: Option<Vec<CompactNodeInfo<SocketAddrV4>>>,
        nodes6: Option<Vec<CompactNodeInfo<SocketAddrV6>>>,
    ) -> Self {
        Self {
            id,
            token,
            values,
            nodes,
            nodes6,
        }
    }

    pub fn id(&self) -> Id {
        self.id
    }

    pub fn set_id(&mut self, id: Id) {
        self.id = id;
    }

    pub fn token(&self) -> &ByteBuf {
        &self.token
    }

    pub fn set_token(&mut self, token: ByteBuf) {
        self.token = token
    }

    pub fn values(&self) -> Option<&Vec<SocketAddr>> {
        self.values.as_ref()
    }

    pub fn set_values(&mut self, values: Option<Vec<SocketAddr>>) {
        self.values = values;
    }

    pub fn nodes(&self) -> Option<&Vec<CompactNodeInfo<SocketAddrV4>>> {
        self.nodes.as_ref()
    }

    pub fn set_nodes(&mut self, nodes: Option<Vec<CompactNodeInfo<SocketAddrV4>>>) {
        self.nodes = nodes;
    }

    pub fn nodes6(&self) -> Option<&Vec<CompactNodeInfo<SocketAddrV6>>> {
        self.nodes6.as_ref()
    }

    pub fn set_nodes6(&mut self, nodes6: Option<Vec<CompactNodeInfo<SocketAddrV6>>>) {
        self.nodes6 = nodes6;
    }
}

impl TryFrom<&BTreeMap<ByteBuf, Value>> for GetPeersRespValues {
    type Error = crate::error::Error;

    fn try_from(values: &BTreeMap<ByteBuf, Value>) -> Result<Self, Self::Error> {
        match (
            values
                .get(&ByteBuf::from(String::from("id")))
                .and_then(|id| id.as_byte_str())
                .and_then(|id| Id::try_from(id.as_slice()).ok()),
            values
                .get(&ByteBuf::from(String::from("token")))
                .and_then(|id| id.as_byte_str())
                .and_then(|id| Some(id.clone())),
            values
                .get(&ByteBuf::from(String::from("values")))
                .and_then(|values| values.as_array())
                .map(|values| {
                    values
                        .iter()
                        .map(|v| {
                            if let Some(v) = v.as_byte_str() {
                                match v.len() {
                                    6 => {
                                        let mut compact_addr: [u8; 6] = [0; 6];
                                        compact_addr.copy_from_slice(&v.as_slice()[0..6]);
                                        Ok(SocketAddr::V4(SocketAddrV4::from_compact_address(
                                            compact_addr,
                                        )))
                                    }
                                    18 => {
                                        let mut compact_addr: [u8; 18] = [0; 18];
                                        compact_addr.copy_from_slice(&v.as_slice()[0..18]);
                                        Ok(SocketAddr::V6(SocketAddrV6::from_compact_address(
                                            compact_addr,
                                        )))
                                    }
                                    _ => Err(Error::InvalidCompactAddr),
                                }
                            } else {
                                Err(Error::InvalidCompactAddr)
                            }
                        })
                        .collect::<Result<Vec<SocketAddr>, Error>>()
                })
                .transpose(),
            values
                .get(&ByteBuf::from(String::from("nodes")))
                .and_then(|nodes| nodes.as_byte_str())
                .map(|nodes| {
                    let mut c = 0;
                    let mut node_info: Vec<CompactNodeInfo<SocketAddrV4>> = vec![];
                    // TODO: For all of these, need to verify that nodes.len() is a correct multiple and all of the data is consumed. If not, return an error.
                    while c * 26 < nodes.len() {
                        let offset = c * 26;

                        let mut id: [u8; 20] = [0; 20];
                        id.copy_from_slice(&nodes[offset..offset + 20]);
                        let id = Id::with_bytes(id);

                        let mut compact_addr: [u8; 6] = [0; 6];
                        compact_addr.copy_from_slice(&nodes[offset + 20..offset + 26]);
                        let addr = SocketAddrV4::from_compact_address(compact_addr);

                        node_info.push(CompactNodeInfo { id, addr });

                        c += 1;
                    }
                    node_info
                }),
            values
                .get(&ByteBuf::from(String::from("nodes6")))
                .and_then(|nodes6| nodes6.as_byte_str())
                .map(|nodes6| {
                    // TODO: For all of these, need to verify that nodes.len() is a correct multiple and all of the data is consumed. If not, return an error.
                    let mut c = 0;
                    let mut node_info: Vec<CompactNodeInfo<SocketAddrV6>> = vec![];
                    while c * 38 < nodes6.len() {
                        let offset = c * 38;

                        let mut id: [u8; 20] = [0; 20];
                        id.copy_from_slice(&nodes6[offset..offset + 20]);
                        let id = Id::with_bytes(id);

                        let mut compact_addr: [u8; 18] = [0; 18];
                        compact_addr.copy_from_slice(&nodes6[offset + 20..offset + 38]);
                        let addr = SocketAddrV6::from_compact_address(compact_addr);

                        node_info.push(CompactNodeInfo { id, addr });

                        c += 1;
                    }
                    node_info
                }),
        ) {
            (Some(id), Some(token), values, nodes, nodes6) => match values {
                Ok(values) => Ok(GetPeersRespValues {
                    id,
                    token,
                    values,
                    nodes,
                    nodes6,
                }),
                Err(e) => Err(e),
            },
            _ => Err(crate::error::Error::CannotDeserializeKrpcMessage),
        }
    }
}

impl From<GetPeersRespValues> for Value {
    fn from(values: GetPeersRespValues) -> Self {
        let mut args: BTreeMap<ByteBuf, Value> = BTreeMap::new();
        args.insert(
            ByteBuf::from(String::from("id")),
            Value::ByteStr(ByteBuf::from(values.id)),
        );

        if let Some(nodes) = values.nodes {
            let mut byte_str: Vec<u8> = vec![];
            for n in nodes {
                byte_str.extend_from_slice(&n.id.0);
                byte_str.extend_from_slice(&n.addr.to_compact_address());
            }
            args.insert(
                ByteBuf::from(String::from("nodes")),
                Value::ByteStr(ByteBuf::from(byte_str)),
            );
        }

        if let Some(nodes6) = values.nodes6 {
            let mut byte_str: Vec<u8> = vec![];
            for n in nodes6 {
                byte_str.extend_from_slice(&n.id.0);
                byte_str.extend_from_slice(&n.addr.to_compact_address());
            }
            args.insert(
                ByteBuf::from(String::from("nodes6")),
                Value::ByteStr(ByteBuf::from(byte_str)),
            );
        }

        args.insert(
            ByteBuf::from(String::from("token")),
            Value::ByteStr(values.token),
        );

        if let Some(values) = values.values {
            args.insert(
                ByteBuf::from(String::from("values")),
                Value::List(
                    values
                        .iter()
                        .map(|addr| {
                            Value::ByteStr(match addr {
                                SocketAddr::V4(addr) => ByteBuf::from(addr.to_compact_address()),
                                SocketAddr::V6(addr) => ByteBuf::from(addr.to_compact_address()),
                            })
                        })
                        .collect::<Vec<Value>>(),
                ),
            );
        }

        Value::Dict(args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::error::Error;
    use crate::krpc::{Kind, Msg, QueryArgs, QueryMsg, RespMsg};

    #[test]
    fn test_serde_get_peers_query() -> Result<(), Error> {
        let get_peers_query = b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe";

        let msg_value: Value = bt_bencode::from_reader(&get_peers_query[..])?;
        assert_eq!(msg_value.kind(), Some(Kind::Query));
        assert_eq!(
            msg_value.method_name(),
            Some(&ByteBuf::from(METHOD_GET_PEERS.as_bytes()))
        );
        assert_eq!(msg_value.method_name_str(), Some(METHOD_GET_PEERS));
        assert_eq!(msg_value.tx_id(), Some(&ByteBuf::from("aa")));
        if let Some(args) = msg_value
            .args()
            .and_then(|a| GetPeersQueryArgs::try_from(a).ok())
        {
            assert_eq!(args.id(), Id::try_from("abcdefghij0123456789".as_bytes())?);
            assert_eq!(
                args.info_hash(),
                InfoHash::try_from("mnopqrstuvwxyz123456".as_bytes())?
            );

            let args_value = args.into();
            let ser_query_msg = crate::krpc::ser::QueryMsg {
                a: Some(&args_value),
                q: &ByteBuf::from(METHOD_GET_PEERS),
                t: &ByteBuf::from("aa"),
                v: None,
            };
            let ser_msg = bt_bencode::to_vec(&ser_query_msg)
                .map_err(|_| Error::CannotDeserializeKrpcMessage)?;
            assert_eq!(ser_msg, get_peers_query.to_vec());
            Ok(())
        } else {
            panic!()
        }
    }

    #[test]
    fn test_serde_get_peers_response_values_one_node() -> Result<(), Error> {
        use crate::addr::{CompactAddressV4, NodeIdGenerator};
        use std::net::{Ipv4Addr, SocketAddrV4};

        let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1234);
        let compact_addr = addr.to_compact_address();
        let node_id = addr.ip().make_node_id(None)?;
        let mut get_peers_resp = vec![];
        get_peers_resp.extend_from_slice(b"d1:rd2:id20:0123456789abcdefghij5:nodes26:");
        get_peers_resp.extend_from_slice(&node_id.0[..]);
        get_peers_resp.extend_from_slice(&compact_addr[..]);
        get_peers_resp.extend_from_slice(b"5:token8:12345678e1:t2:aa1:y1:re");

        let msg_value: Value = bt_bencode::from_reader(&get_peers_resp[..])?;
        assert_eq!(msg_value.kind(), Some(Kind::Response));
        assert_eq!(msg_value.method_name(), None);
        assert_eq!(msg_value.method_name_str(), None);
        assert_eq!(msg_value.tx_id(), Some(&ByteBuf::from("aa")));

        if let Some(values) = msg_value
            .values()
            .and_then(|a| GetPeersRespValues::try_from(a).ok())
        {
            assert_eq!(
                values.id(),
                Id::try_from("0123456789abcdefghij".as_bytes())?
            );

            let resp_values = values.into();
            let ser_resp_msg = crate::krpc::ser::RespMsg {
                r: Some(&resp_values),
                t: &ByteBuf::from("aa"),
                v: None,
            };
            let ser_msg = bt_bencode::to_vec(&ser_resp_msg)
                .map_err(|_| Error::CannotDeserializeKrpcMessage)?;
            assert_eq!(ser_msg, get_peers_resp.to_vec());
            Ok(())
        } else {
            dbg!(msg_value);
            panic!()
        }
    }
}
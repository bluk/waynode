use crate::{
    addr::{CompactAddressV4, CompactAddressV6, CompactNodeInfo},
    node::Id,
};
use bt_bencode::Value;
use serde_bytes::ByteBuf;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::net::{SocketAddrV4, SocketAddrV6};

pub const METHOD_FIND_NODE: &str = "find_node";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FindNodeQueryArgs {
    id: Id,
    target: Id,
}

impl FindNodeQueryArgs {
    pub fn new_with_id_and_target(id: Id, target: Id) -> Self {
        Self { id, target }
    }

    pub fn id(&self) -> &Id {
        &self.id
    }

    pub fn set_id(&mut self, id: Id) {
        self.id = id;
    }

    pub fn target(&self) -> &Id {
        &self.target
    }

    pub fn set_target(&mut self, target: Id) {
        self.target = target;
    }
}

impl TryFrom<Value> for FindNodeQueryArgs {
    type Error = crate::error::Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
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
            args.get(&ByteBuf::from(String::from("id")))
                .and_then(|id| id.as_byte_str())
                .and_then(|id| Id::try_from(id.as_slice()).ok()),
            args.get(&ByteBuf::from(String::from("target")))
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

pub struct FindNodeRespValues {
    id: Id,
    nodes: Option<Vec<CompactNodeInfo<SocketAddrV4>>>,
    nodes6: Option<Vec<CompactNodeInfo<SocketAddrV6>>>,
}

impl FindNodeRespValues {
    pub fn new_with_id(
        id: Id,
        nodes: Option<Vec<CompactNodeInfo<SocketAddrV4>>>,
        nodes6: Option<Vec<CompactNodeInfo<SocketAddrV6>>>,
    ) -> Self {
        Self { id, nodes, nodes6 }
    }

    pub fn id(&self) -> &Id {
        &self.id
    }

    pub fn set_id(&mut self, id: Id) {
        self.id = id;
    }

    pub fn nodes(&self) -> &Option<Vec<CompactNodeInfo<SocketAddrV4>>> {
        &self.nodes
    }

    pub fn set_nodes(&mut self, nodes: Option<Vec<CompactNodeInfo<SocketAddrV4>>>) {
        self.nodes = nodes;
    }

    pub fn nodes6(&self) -> &Option<Vec<CompactNodeInfo<SocketAddrV6>>> {
        &self.nodes6
    }

    pub fn set_nodes6(&mut self, nodes6: Option<Vec<CompactNodeInfo<SocketAddrV6>>>) {
        self.nodes6 = nodes6;
    }
}

impl TryFrom<&BTreeMap<ByteBuf, Value>> for FindNodeRespValues {
    type Error = crate::error::Error;

    fn try_from(values: &BTreeMap<ByteBuf, Value>) -> Result<Self, Self::Error> {
        match (
            values
                .get(&ByteBuf::from(String::from("id")))
                .and_then(|id| id.as_byte_str())
                .and_then(|id| Id::try_from(id.as_slice()).ok()),
            values
                .get(&ByteBuf::from(String::from("nodes")))
                .and_then(|nodes| nodes.as_byte_str())
                .map(|nodes| {
                    let mut c = 0;
                    let mut node_info: Vec<CompactNodeInfo<SocketAddrV4>> = vec![];
                    while c * 26 < nodes.len() {
                        let offset = c * 26;

                        let mut id: [u8; 20] = [0; 20];
                        id.copy_from_slice(&nodes[offset..offset + 20]);
                        let id = Id::new_with_bytes(id);

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
                    let mut c = 0;
                    let mut node_info: Vec<CompactNodeInfo<SocketAddrV6>> = vec![];
                    while c * 38 < nodes6.len() {
                        let offset = c * 38;

                        let mut id: [u8; 20] = [0; 20];
                        id.copy_from_slice(&nodes6[offset..offset + 20]);
                        let id = Id::new_with_bytes(id);

                        let mut compact_addr: [u8; 18] = [0; 18];
                        compact_addr.copy_from_slice(&nodes6[offset + 20..offset + 38]);
                        let addr = SocketAddrV6::from_compact_address(compact_addr);

                        node_info.push(CompactNodeInfo { id, addr });

                        c += 1;
                    }
                    node_info
                }),
        ) {
            (Some(id), nodes, nodes6) => Ok(FindNodeRespValues { id, nodes, nodes6 }),
            _ => Err(crate::error::Error::CannotDeserializeKrpcMessage),
        }
    }
}

impl From<FindNodeRespValues> for Value {
    fn from(values: FindNodeRespValues) -> Self {
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

        Value::Dict(args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::error::Error;
    use crate::krpc::{Kind, Msg, QueryMsg, RespMsg};

    #[test]
    fn test_serde_find_node_query() -> Result<(), Error> {
        let find_node_query = b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe";

        let msg_value: Value = bt_bencode::from_reader(&find_node_query[..])?;
        assert_eq!(msg_value.kind(), Some(Kind::Query));
        assert_eq!(
            msg_value.method_name(),
            Some(&ByteBuf::from(METHOD_FIND_NODE.as_bytes()))
        );
        assert_eq!(msg_value.method_name_str(), Some(METHOD_FIND_NODE));
        assert_eq!(msg_value.transaction_id(), Some(&ByteBuf::from("aa")));
        if let Some(args) = msg_value
            .args()
            .and_then(|a| FindNodeQueryArgs::try_from(a).ok())
        {
            assert_eq!(args.id(), &Id::try_from("abcdefghij0123456789".as_bytes())?);
            assert_eq!(
                args.target(),
                &Id::try_from("mnopqrstuvwxyz123456".as_bytes())?
            );

            let args_value = args.into();
            let ser_query_msg = crate::krpc::ser::QueryMsg {
                a: Some(&args_value),
                q: &ByteBuf::from(METHOD_FIND_NODE),
                t: &ByteBuf::from("aa"),
                v: None,
            };
            let ser_msg = bt_bencode::to_vec(&ser_query_msg)
                .map_err(|_| Error::CannotDeserializeKrpcMessage)?;
            assert_eq!(ser_msg, find_node_query.to_vec());
            Ok(())
        } else {
            panic!()
        }
    }

    #[test]
    fn test_serde_find_node_response_values_one_node() -> Result<(), Error> {
        use crate::addr::{CompactAddressV4, NodeIdGenerator};
        use std::net::{Ipv4Addr, SocketAddrV4};

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
        assert_eq!(msg_value.transaction_id(), Some(&ByteBuf::from("aa")));

        if let Some(values) = msg_value
            .values()
            .and_then(|a| FindNodeRespValues::try_from(a).ok())
        {
            assert_eq!(
                values.id(),
                &Id::try_from("0123456789abcdefghij".as_bytes())?
            );

            let resp_values = values.into();
            let ser_resp_msg = crate::krpc::ser::RespMsg {
                r: Some(&resp_values),
                t: &ByteBuf::from("aa"),
                v: None,
            };
            let ser_msg = bt_bencode::to_vec(&ser_resp_msg)
                .map_err(|_| Error::CannotDeserializeKrpcMessage)?;
            assert_eq!(ser_msg, find_node_resp.to_vec());
            Ok(())
        } else {
            panic!()
        }
    }
}

use crate::{
    addr::Addr,
    krpc::{
        find_node::{FindNodeQueryArgs, FindNodeRespValues},
        Kind, Msg, RespMsg,
    },
    msg_buffer,
    node::{self, remote::RemoteNodeId},
    transaction,
};
use bt_bencode::Value;
use std::collections::BTreeSet;
use std::convert::TryFrom;
use std::net::SocketAddr;

#[derive(Clone, Debug)]
pub(crate) struct FindNodeOp {
    id_to_find: node::Id,
    local_id: node::Id,
    pub(crate) queried_remote_nodes: BTreeSet<RemoteNodeId>,
    potential_remote_nodes: Vec<RemoteNodeId>,
    pub(crate) tx_local_ids: BTreeSet<transaction::LocalId>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum Status {
    Keep,
    Done,
}

pub(crate) enum Response<'a> {
    Msg(&'a Value),
    Timeout,
}

impl FindNodeOp {
    pub(crate) fn with_local_id_and_id_to_find(
        local_id: node::Id,
        id_to_find: node::Id,
        potential_remote_nodes: Vec<RemoteNodeId>,
    ) -> Self {
        Self {
            local_id,
            id_to_find,
            queried_remote_nodes: BTreeSet::new(),
            potential_remote_nodes,
            tx_local_ids: BTreeSet::new(),
        }
    }

    pub(crate) fn status(&self) -> Status {
        if self.tx_local_ids.is_empty()
            && (!self.queried_remote_nodes.is_empty() || self.potential_remote_nodes.is_empty())
        {
            Status::Done
        } else {
            Status::Keep
        }
    }

    fn filter_new_nodes(&self, resp: &FindNodeRespValues) -> Option<Vec<RemoteNodeId>> {
        resp.nodes().as_ref().map(|nodes| {
            nodes
                .iter()
                .filter(|n| {
                    !self.queried_remote_nodes.iter().any(|queried_remote_id| {
                        queried_remote_id
                            .node_id
                            .map(|e_nid| e_nid == n.id)
                            .unwrap_or(false)
                            || queried_remote_id.addr == Addr::SocketAddr(SocketAddr::V4(n.addr))
                    })
                })
                .map(|cn| RemoteNodeId {
                    addr: Addr::SocketAddr(SocketAddr::V4(cn.addr)),
                    node_id: Some(cn.id),
                })
                .collect::<Vec<_>>()
        })
    }

    pub(crate) fn start(
        &mut self,
        tx_manager: &mut transaction::Manager,
        msg_buffer: &mut msg_buffer::Buffer,
    ) {
        // TODO:
        for id in self.potential_remote_nodes.iter() {
            if let Ok(tx_local_id) = msg_buffer.write_query(
                &FindNodeQueryArgs::new_with_id_and_target(self.local_id, self.id_to_find),
                &id,
                tx_manager,
            ) {
                self.queried_remote_nodes.insert(id.clone());
                self.tx_local_ids.insert(tx_local_id);
            }
        }
    }

    pub(crate) fn handle<'a>(
        &mut self,
        tx: &transaction::Transaction,
        resp: Response<'a>,
        tx_manager: &mut transaction::Manager,
        msg_buffer: &mut msg_buffer::Buffer,
    ) {
        if !self.tx_local_ids.contains(&tx.local_id) {
            return;
        }
        self.tx_local_ids.remove(&tx.local_id);

        if let Response::Msg(msg) = resp {
            if let Some(kind) = msg.kind() {
                match kind {
                    Kind::Response => {
                        if let Some(new_node_ids) = msg
                            .values()
                            .and_then(|values| FindNodeRespValues::try_from(values).ok())
                            .and_then(|resp| self.filter_new_nodes(&resp))
                        {
                            for id in new_node_ids {
                                if let Ok(tx_local_id) = msg_buffer.write_query(
                                    &FindNodeQueryArgs::new_with_id_and_target(
                                        self.local_id,
                                        self.id_to_find,
                                    ),
                                    &id,
                                    tx_manager,
                                ) {
                                    self.queried_remote_nodes.insert(id.clone());
                                    self.tx_local_ids.insert(tx_local_id);
                                }
                            }
                        }
                    }
                    Kind::Error => {}
                    Kind::Query | Kind::Unknown(_) => unreachable!(),
                }
            }
        }
    }
}

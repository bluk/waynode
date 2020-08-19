use crate::{
    addr::Addr,
    krpc::{
        find_node::{FindNodeQueryArgs, FindNodeRespValues},
        Kind, Msg, RespMsg,
    },
    node::{self, remote::RemoteNodeId},
    transaction,
};
use bt_bencode::Value;
use std::convert::TryFrom;
use std::net::SocketAddr;

#[derive(Clone, Debug)]
pub(crate) struct FindNodeOp {
    id_to_find: node::Id,
    pub(crate) queried_remote_nodes: Vec<RemoteNodeId>,
    pub(crate) tx_local_ids: Vec<transaction::LocalId>,
}

impl FindNodeOp {
    pub(crate) fn new_with_id(id_to_find: node::Id) -> Self {
        Self {
            id_to_find,
            queried_remote_nodes: Vec::new(),
            tx_local_ids: Vec::new(),
        }
    }

    pub(crate) fn id_to_find(&self) -> node::Id {
        self.id_to_find
    }

    pub(crate) fn add_queried_node_id(&mut self, remote_id: RemoteNodeId) {
        self.queried_remote_nodes.push(remote_id);
    }

    pub(crate) fn add_tx_local_id(&mut self, tx_local_id: transaction::LocalId) {
        self.tx_local_ids.push(tx_local_id);
    }

    pub(crate) fn contains_tx_local_id(&self, tx_local_id: transaction::LocalId) -> bool {
        self.tx_local_ids.contains(&tx_local_id)
    }

    pub(crate) fn remove_tx_local_id(&mut self, tx_local_id: transaction::LocalId) {
        self.tx_local_ids.retain(|&id| id != tx_local_id);
    }

    pub(crate) fn filter_new_nodes(&self, resp: &FindNodeRespValues) -> Option<Vec<RemoteNodeId>> {
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

    pub(crate) fn process_msg(
        &mut self,
        dht: &mut crate::Dht,
        tx: transaction::Transaction,
        msg: Value,
    ) {
        self.remove_tx_local_id(tx.local_id);

        if let Some(kind) = msg.kind() {
            match kind {
                Kind::Response => {
                    if let Some(new_node_ids) = msg
                        .values()
                        .and_then(|values| FindNodeRespValues::try_from(values).ok())
                        .and_then(|resp| self.filter_new_nodes(&resp))
                    {
                        for id in new_node_ids {
                            if let Ok(tx_local_id) = dht.write_query(
                                &FindNodeQueryArgs::new_with_id_and_target(
                                    dht.config.id,
                                    self.id_to_find(),
                                ),
                                &id,
                            ) {
                                self.add_queried_node_id(id.clone());
                                self.add_tx_local_id(tx_local_id);
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

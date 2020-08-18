use crate::{
    addr::Addr,
    krpc::find_node::FindNodeRespValues,
    node::{self, remote::RemoteNodeId},
    transaction,
};
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
}

use crate::node::remote::RemoteNodeId;
use serde_bytes::ByteBuf;
use std::net::SocketAddr;
use std::time::Instant;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct Id(pub u16);

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Transaction {
    pub id: ByteBuf,
    pub remote_id: RemoteNodeId,
    pub resolved_addr: SocketAddr,
    pub sent: Instant,
}

impl Transaction {
    pub(crate) fn new_with_id_and_outbound_msg(id: ByteBuf, msg: crate::OutboundMsg) -> Self {
        Self {
            id,
            remote_id: msg.remote_id,
            resolved_addr: msg.resolved_addr,
            sent: Instant::now(),
        }
    }
}

impl std::hash::Hash for Transaction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.remote_id.hash(state)
    }
}

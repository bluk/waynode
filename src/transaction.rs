use crate::{error::Error, node::remote::RemoteNodeId};
use serde_bytes::ByteBuf;
use std::convert::{TryFrom, TryInto};
use std::net::SocketAddr;
use std::time::Instant;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Id(pub(crate) u16);

impl Id {
    pub(crate) fn next(&self) -> Self {
        let (next_id, _) = self.0.overflowing_add(1);
        Id(next_id)
    }

    pub(crate) fn to_bytebuf(&self) -> ByteBuf {
        ByteBuf::from(self.0.to_be_bytes())
    }
}

impl TryFrom<&ByteBuf> for Id {
    type Error = Error;

    fn try_from(other: &ByteBuf) -> Result<Self, Self::Error> {
        if other.len() != std::mem::size_of::<u16>() {
            return Err(Error::InvalidLocalTransactionId);
        }
        let int_bytes = other
            .as_slice()
            .try_into()
            .map_err(|_| Error::InvalidLocalTransactionId)?;
        Ok(Id(u16::from_be_bytes(int_bytes)))
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct LocalId {
    pub(crate) id: Id,
    pub(crate) addr: SocketAddr,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Transaction {
    pub local_id: LocalId,
    pub remote_id: RemoteNodeId,
    pub sent: Instant,
}

impl std::hash::Hash for Transaction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.local_id.hash(state);
        self.remote_id.hash(state)
    }
}

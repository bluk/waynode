// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::{error::Error, node::AddrId};
use serde_bytes::ByteBuf;
use std::convert::{TryFrom, TryInto};
use std::net::SocketAddr;
use std::time::Instant;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub(crate) struct Id(u16);

impl Id {
    fn next(&self) -> Self {
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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct LocalId {
    id: Id,
    addr: SocketAddr,
}

impl LocalId {
    #[cfg(test)]
    pub(crate) fn id(&self) -> Id {
        self.id
    }
}

impl LocalId {
    pub(crate) fn with_id_and_addr(id: Id, addr: SocketAddr) -> Self {
        Self { id, addr }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Transaction {
    pub(crate) local_id: LocalId,
    pub(crate) addr_id: AddrId,
    pub(crate) deadline: Instant,
}

impl std::hash::Hash for Transaction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.local_id.hash(state);
        self.addr_id.hash(state)
    }
}

impl Transaction {
    pub(crate) fn is_node_id_match(&self, other_node_id: Option<crate::node::Id>) -> bool {
        self.addr_id
            .id()
            .map(|tx_node_id| {
                other_node_id
                    .map(|other_node_id| other_node_id == tx_node_id)
                    .unwrap_or(false)
            })
            .unwrap_or(true)
    }
}

#[derive(Debug)]
pub(crate) struct Manager {
    transactions: Vec<Transaction>,
    next_transaction_id: Id,
}

impl Manager {
    pub(crate) fn new() -> Self {
        Self {
            transactions: Vec::new(),
            next_transaction_id: Id(0),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.transactions.len()
    }

    #[inline]
    pub(crate) fn next_transaction_id(&mut self) -> Id {
        let transaction_id = self.next_transaction_id;
        self.next_transaction_id = self.next_transaction_id.next();
        transaction_id
    }

    pub(crate) fn min_deadline(&self) -> Option<Instant> {
        self.transactions.iter().map(|t| t.deadline).min()
    }

    pub(crate) fn find(&mut self, tx_id: &ByteBuf, addr: SocketAddr) -> Option<Transaction> {
        Id::try_from(tx_id)
            .map(|tx_id| LocalId { id: tx_id, addr })
            .ok()
            .and_then(|tx_local_id| {
                self.transactions
                    .iter()
                    .position(|t| t.local_id == tx_local_id)
            })
            .map(|idx| self.transactions.remove(idx))
    }

    pub(crate) fn push(&mut self, tx: Transaction) {
        self.transactions.push(tx);
        self.transactions
            .sort_unstable_by(|a, b| a.deadline.cmp(&b.deadline));
    }

    pub(crate) fn timed_out_txs(&mut self, now: Instant) -> Option<Vec<Transaction>> {
        if let Some(idx) = self
            .transactions
            .iter()
            .rev()
            .position(|tx| tx.deadline <= now)
        {
            Some(self.transactions.drain(0..=idx).collect())
        } else {
            None
        }
    }
}

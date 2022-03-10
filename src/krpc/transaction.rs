// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Transactions correlate a KRPC query with its response.

use crate::{error::Error, node::AddrOptId};
use serde_bytes::ByteBuf;
use std::{
    convert::{TryFrom, TryInto},
    net::SocketAddr,
    time::Instant,
};

/// An opaque identifer which correlates a KRPC query with a response or error.
///
/// An `Id` is returned when a query is written to the `Dht`. The caller should
/// hold onto the `Id`. When a message is read from the `Dht`, then the caller
/// should determine if the read message's `Id` is equal to the previously held
/// `Id`. If they are the same, then the read message is in response to the
/// original query.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct Id(u16);

impl Id {
    fn next(&self) -> Self {
        let (next_id, _) = self.0.overflowing_add(1);
        Id(next_id)
    }
}

impl TryFrom<&[u8]> for Id {
    type Error = Error;

    fn try_from(other: &[u8]) -> Result<Self, Self::Error> {
        if other.len() != std::mem::size_of::<u16>() {
            return Err(Error::InvalidLocalTransactionId);
        }
        let int_bytes = other
            .try_into()
            .map_err(|_| Error::InvalidLocalTransactionId)?;
        Ok(Id(u16::from_be_bytes(int_bytes)))
    }
}

impl From<Id> for ByteBuf {
    fn from(id: Id) -> ByteBuf {
        ByteBuf::from(id.0.to_be_bytes())
    }
}

impl From<Id> for Vec<u8> {
    fn from(id: Id) -> Vec<u8> {
        Vec::from(id.0.to_be_bytes())
    }
}

#[derive(Debug)]
pub(crate) struct Transaction {
    pub(crate) tx_id: Id,
    pub(crate) addr_opt_id: AddrOptId<SocketAddr>,
    pub(crate) deadline: Instant,
}

impl std::hash::Hash for Transaction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.tx_id.hash(state);
        self.addr_opt_id.hash(state)
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

    pub(crate) fn timeout(&self) -> Option<Instant> {
        self.transactions.iter().map(|t| t.deadline).min()
    }

    pub(crate) fn remove(&mut self, tx_id: &[u8], addr: SocketAddr) -> Option<Transaction> {
        Id::try_from(tx_id)
            .ok()
            .and_then(|tx_local_id| {
                self.transactions
                    .iter()
                    .position(|t| t.tx_id == tx_local_id && t.addr_opt_id.addr() == addr)
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

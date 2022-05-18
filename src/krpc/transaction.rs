// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Transactions correlate a KRPC query with its response.

use cloudburst::dht::{krpc::transaction::Id, node::AddrOptId};
use std::{net::SocketAddr, time::Instant};

#[derive(Debug)]
pub(crate) struct Transaction<TxId> {
    pub(crate) tx_id: TxId,
    pub(crate) addr_opt_id: AddrOptId<SocketAddr>,
    pub(crate) deadline: Instant,
}

impl<TxId> std::hash::Hash for Transaction<TxId>
where
    TxId: std::hash::Hash,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.tx_id.hash(state);
        self.addr_opt_id.hash(state);
    }
}

#[derive(Debug)]
pub(crate) struct Manager<TxId> {
    transactions: Vec<Transaction<TxId>>,
}

impl<TxId> Default for Manager<TxId> {
    fn default() -> Self {
        Self {
            transactions: Vec::default(),
        }
    }
}

impl<TxId> Manager<TxId> {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn len(&self) -> usize {
        self.transactions.len()
    }

    pub(crate) fn timeout(&self) -> Option<Instant> {
        self.transactions.iter().map(|t| t.deadline).min()
    }

    pub(crate) fn remove(&mut self, tx_id: &TxId, addr: SocketAddr) -> Option<Transaction<TxId>>
    where
        TxId: PartialEq,
    {
        if let Some(idx) = self
            .transactions
            .iter()
            .position(|t| t.tx_id == *tx_id && *t.addr_opt_id.addr() == addr)
        {
            Some(self.transactions.remove(idx))
        } else {
            None
        }
    }

    pub(crate) fn push(&mut self, tx: Transaction<TxId>) {
        self.transactions.push(tx);
        self.transactions
            .sort_unstable_by(|a, b| a.deadline.cmp(&b.deadline));
    }

    pub(crate) fn timed_out_txs(&mut self, now: Instant) -> Option<Vec<Transaction<TxId>>> {
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

impl Manager<Id> {
    #[inline]
    pub(crate) fn next_transaction_id<R>(&mut self, rng: &mut R) -> Result<Id, rand::Error>
    where
        R: rand::Rng,
    {
        loop {
            let tx_id = Id::rand(rng)?;
            if self.transactions.iter().all(|tx| tx_id != tx.tx_id) {
                return Ok(tx_id);
            }
        }
    }
}

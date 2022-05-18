// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Transactions correlate a KRPC query with its response.

use cloudburst::dht::krpc::transaction::{Id, Transactions};

#[inline]
pub(crate) fn next_tx_id<R, A, I>(
    active_txs: &Transactions<Id, A, I>,
    rng: &mut R,
) -> Result<Id, rand::Error>
where
    R: rand::Rng,
{
    loop {
        let tx_id = Id::rand(rng)?;
        if !active_txs.contains_tx_id(&tx_id) {
            return Ok(tx_id);
        }
    }
}

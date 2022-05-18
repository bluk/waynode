// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::{find_node_op::FindNodeOp, msg_buffer};
use cloudburst::dht::{
    krpc::{
        ping::QueryArgs,
        transaction::{self, Transactions},
        Error, Ty,
    },
    node::{AddrId, AddrOptId, Id},
};
use core::{cmp::Ordering, ops::RangeInclusive, time::Duration};
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};

#[derive(Debug, PartialEq)]
enum NodeState {
    Good,
    Questionable,
    Bad,
}

#[derive(Debug, Clone, Copy)]
struct Node<Addr, TxId, Instant> {
    addr_id: AddrId<Addr>,
    karma: i8,
    next_response_deadline: Instant,
    next_query_deadline: Instant,
    tx_id: Option<TxId>,
    last_pinged: Option<Instant>,
}

impl<A, TxId, Instant> Node<A, TxId, Instant>
where
    Instant: cloudburst::time::Instant,
{
    const TIMEOUT_INTERVAL: Duration = Duration::from_secs(15 * 60);

    fn with_addr_id(addr_id: AddrId<A>, now: Instant) -> Self {
        Self {
            addr_id,
            karma: 0,
            next_response_deadline: now.clone() + Self::TIMEOUT_INTERVAL,
            next_query_deadline: now + Self::TIMEOUT_INTERVAL,
            tx_id: None,
            last_pinged: None,
        }
    }

    fn state_with_now(&self, now: &Instant) -> NodeState {
        if *now < self.next_response_deadline {
            return NodeState::Good;
        }

        if *now < self.next_query_deadline {
            return NodeState::Good;
        }

        if self.karma < -2 {
            return NodeState::Bad;
        }

        NodeState::Questionable
    }

    fn next_msg_deadline(&self) -> Instant {
        core::cmp::max(&self.next_response_deadline, &self.next_query_deadline).clone()
    }

    fn on_msg_received(&mut self, kind: &Ty, now: Instant) {
        self.last_pinged = None;
        match kind {
            Ty::Response => {
                self.next_response_deadline = now + Self::TIMEOUT_INTERVAL;
                self.karma = self.karma.saturating_add(1);
                if self.karma > 3 {
                    self.karma = 3;
                }
            }
            Ty::Query => {
                self.next_query_deadline = now + Self::TIMEOUT_INTERVAL;
            }
            Ty::Error => {
                self.next_response_deadline = now + Self::TIMEOUT_INTERVAL;
                self.karma = self.karma.saturating_sub(1);
            }
            Ty::Unknown(_) => {
                self.karma = self.karma.saturating_sub(1);
            }
            _ => {
                todo!()
            }
        }
    }

    fn on_ping_timeout(&mut self) {
        self.last_pinged = None;
        self.karma = self.karma.saturating_sub(1);
    }

    fn on_ping(&mut self, now: Instant) {
        self.last_pinged = Some(now);
    }
}

const EXPECT_CHANGE_INTERVAL: Duration = Duration::from_secs(15 * 60);
const BUCKET_SIZE: usize = 8;

#[derive(Debug)]
struct Bucket<A, TxId, Instant> {
    range: RangeInclusive<Id>,
    nodes: [Option<Node<A, TxId, Instant>>; BUCKET_SIZE],
    replacement_nodes: [Option<Node<A, TxId, Instant>>; BUCKET_SIZE],
    expected_change_deadline: Instant,
}

impl<A, TxId, Instant> Bucket<A, TxId, Instant>
where
    A: Into<SocketAddr>,
    Instant: cloudburst::time::Instant,
{
    const NODES_NONE: Option<Node<A, TxId, Instant>> = None;

    fn new(range: RangeInclusive<Id>, now: Instant) -> Self {
        Bucket {
            range,
            nodes: [Self::NODES_NONE; BUCKET_SIZE],
            replacement_nodes: [Self::NODES_NONE; BUCKET_SIZE],
            expected_change_deadline: now + Duration::from_secs(5 * 60),
        }
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.nodes.iter().all(std::option::Option::is_some)
    }

    #[inline]
    fn update_expected_change_deadline(&mut self, now: Instant) {
        self.expected_change_deadline = now + EXPECT_CHANGE_INTERVAL;
    }

    fn try_insert(&mut self, addr_id: AddrId<A>, now: Instant) {
        self.nodes[self.nodes.len() - 1] = Some(Node::with_addr_id(addr_id, now.clone()));
        self.sort_node_ids(&now);
        self.update_expected_change_deadline(now);
    }

    fn ping_least_recently_seen_questionable_node<R>(
        &mut self,
        config: &crate::Config,
        tx_manager: &mut Transactions<transaction::Id, std::net::SocketAddr, std::time::Instant>,
        msg_buffer: &mut msg_buffer::Buffer<transaction::Id>,
        rng: &mut R,
        now: Instant,
    ) -> Result<(), Error>
    where
        A: Clone,
        R: rand::Rng,
    {
        let pinged_nodes_count = self
            .nodes
            .iter()
            .flatten()
            .filter(|n| {
                n.state_with_now(&now) == NodeState::Questionable && n.last_pinged.is_some()
            })
            .count();
        let replacement_nodes_count = self
            .replacement_nodes
            .iter()
            .filter(|n| n.is_some())
            .count();
        if pinged_nodes_count < replacement_nodes_count {
            if let Some(node_to_ping) = self.nodes.iter_mut().rev().flatten().find(|n| {
                n.state_with_now(&now) == NodeState::Questionable && n.last_pinged.is_none()
            }) {
                let tx_id = crate::krpc::transaction::next_tx_id(tx_manager, rng).unwrap();
                msg_buffer.write_query(
                    tx_id,
                    &QueryArgs::new(config.local_id),
                    AddrOptId::new(
                        ((*node_to_ping.addr_id.addr()).clone()).into(),
                        Some(node_to_ping.addr_id.id()),
                    ),
                    config.default_query_timeout,
                    config.client_version(),
                )?;
                node_to_ping.on_ping(now);
            }
        }
        Ok(())
    }

    fn on_msg_received<'a, R>(
        &mut self,
        addr_id: AddrId<A>,
        kind: &Ty<'a>,
        config: &crate::Config,
        tx_manager: &mut Transactions<transaction::Id, std::net::SocketAddr, std::time::Instant>,
        msg_buffer: &mut msg_buffer::Buffer<transaction::Id>,
        rng: &mut R,
        now: Instant,
    ) -> Result<(), Error>
    where
        A: PartialEq + Clone,
        R: rand::Rng,
    {
        if let Some(node) = self
            .nodes
            .iter_mut()
            .flatten()
            .find(|n| n.addr_id == addr_id)
        {
            node.on_msg_received(kind, now.clone());
            match kind {
                Ty::Response | Ty::Query => {
                    self.sort_node_ids(&now);
                    self.ping_least_recently_seen_questionable_node(
                        config,
                        tx_manager,
                        msg_buffer,
                        rng,
                        now.clone(),
                    )?;
                    self.update_expected_change_deadline(now);
                }
                Ty::Error | Ty::Unknown(_) => match node.state_with_now(&now) {
                    NodeState::Good => {
                        self.sort_node_ids(&now);
                    }
                    NodeState::Questionable => {
                        self.sort_node_ids(&now);
                        self.ping_least_recently_seen_questionable_node(
                            config, tx_manager, msg_buffer, rng, now,
                        )?;
                    }
                    NodeState::Bad => {
                        if let Some(replacement_node) =
                            self.replacement_nodes.iter_mut().find(|n| n.is_some())
                        {
                            if let Some(replacement_node) = replacement_node.take() {
                                *node = replacement_node;
                                self.update_expected_change_deadline(now.clone());
                            }
                        }
                        self.sort_node_ids(&now);
                    }
                },
                _ => {
                    todo!()
                }
            }
        } else {
            match kind {
                Ty::Response | Ty::Query | Ty::Error => {}
                Ty::Unknown(_) => return Ok(()),
                _ => {
                    todo!()
                }
            }

            if let Some(pos) = self.nodes.iter().rev().position(|n| {
                n.as_ref()
                    .map_or(false, |n| n.state_with_now(&now) == NodeState::Bad)
            }) {
                let mut node = Node::with_addr_id(addr_id, now.clone());
                node.on_msg_received(kind, now.clone());
                self.nodes[pos] = Some(node);
                self.sort_node_ids(&now);
                self.update_expected_change_deadline(now);
            } else if let Some(pos) = self
                .replacement_nodes
                .iter_mut()
                .rev()
                .position(|n| n.is_none())
            {
                let mut node = Node::with_addr_id(addr_id, now.clone());
                node.on_msg_received(kind, now.clone());
                self.replacement_nodes[pos] = Some(node);
                self.sort_node_ids(&now);
                self.ping_least_recently_seen_questionable_node(
                    config, tx_manager, msg_buffer, rng, now,
                )?;
            }
        }
        Ok(())
    }

    fn on_resp_timeout<R>(
        &mut self,
        addr_id: &AddrId<A>,
        config: &crate::Config,
        tx_manager: &mut Transactions<transaction::Id, std::net::SocketAddr, std::time::Instant>,
        msg_buffer: &mut msg_buffer::Buffer<transaction::Id>,
        rng: &mut R,
        now: Instant,
    ) -> Result<(), Error>
    where
        A: PartialEq + Clone,
        R: rand::Rng,
    {
        if let Some(node) = self
            .nodes
            .iter_mut()
            .flatten()
            .find(|n| n.addr_id == *addr_id)
        {
            node.on_ping_timeout();
            match node.state_with_now(&now) {
                NodeState::Good => {
                    // The sort order will not change if the state is still good
                }
                NodeState::Questionable => {
                    self.sort_node_ids(&now);
                    self.ping_least_recently_seen_questionable_node(
                        config, tx_manager, msg_buffer, rng, now,
                    )?;
                }
                NodeState::Bad => {
                    if let Some(replacement_node) =
                        self.replacement_nodes.iter_mut().find(|n| n.is_some())
                    {
                        if let Some(replacement_node) = replacement_node.take() {
                            *node = replacement_node;
                            self.update_expected_change_deadline(now.clone());
                        }
                    }
                    self.sort_node_ids(&now);
                }
            }
        }
        Ok(())
    }

    fn split_insert(&mut self, node: Node<A, TxId, Instant>) {
        if let Some(pos) = self.nodes.iter().position(std::option::Option::is_none) {
            self.nodes[pos] = Some(node);
        } else {
            unreachable!()
        }
    }

    fn split_replacement_insert(&mut self, node: Node<A, TxId, Instant>) {
        if let Some(pos) = self
            .replacement_nodes
            .iter()
            .position(std::option::Option::is_none)
        {
            self.replacement_nodes[pos] = Some(node);
        } else {
            unreachable!()
        }
    }

    fn split(self, now: Instant) -> (Bucket<A, TxId, Instant>, Bucket<A, TxId, Instant>)
    where
        A: Clone,
        TxId: Clone,
    {
        let middle = r::middle(*self.range.end(), *self.range.start());

        let mut lower_bucket = Bucket::new(*self.range.start()..=r::prev(middle), now.clone());
        let mut upper_bucket = Bucket::new(middle..=*self.range.end(), now);

        for node in self.nodes.iter().flatten() {
            let node_id = node.addr_id.id();
            if lower_bucket.range.contains(&node_id) {
                lower_bucket.split_insert(node.clone());
            } else {
                upper_bucket.split_insert(node.clone());
            }
        }

        for node in self.replacement_nodes.iter().flatten() {
            let node_id = node.addr_id.id();
            if lower_bucket.range.contains(&node_id) {
                lower_bucket.split_replacement_insert(node.clone());
            } else {
                upper_bucket.split_replacement_insert(node.clone());
            }
        }

        (lower_bucket, upper_bucket)
    }

    fn prioritized_nodes(&self, now: Instant) -> impl Iterator<Item = &AddrId<A>> {
        self.nodes
            .iter()
            .flatten()
            .filter(move |n| {
                n.state_with_now(&now) == NodeState::Questionable
                    || n.state_with_now(&now) == NodeState::Good
            })
            .map(|n| &n.addr_id)
    }

    fn sort_node_ids(&mut self, now: &Instant) {
        self.nodes.sort_unstable_by(|a, b| match (a, b) {
            (Some(a), Some(b)) => {
                match (a.state_with_now(now), b.state_with_now(now)) {
                    (NodeState::Good, NodeState::Questionable | NodeState::Bad)
                    | (NodeState::Questionable, NodeState::Bad) => return Ordering::Less,
                    (NodeState::Questionable | NodeState::Bad, NodeState::Good)
                    | (NodeState::Bad, NodeState::Questionable) => return Ordering::Greater,
                    (NodeState::Good, NodeState::Good)
                    | (NodeState::Questionable, NodeState::Questionable)
                    | (NodeState::Bad, NodeState::Bad) => {}
                }

                b.next_msg_deadline().cmp(&a.next_msg_deadline())
            }
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        });
    }
}

mod r {
    use cloudburst::dht::{krpc::Error, node::Id};

    pub(super) fn rand_in_inclusive_range<R>(
        range: &std::ops::RangeInclusive<Id>,
        rng: &mut R,
    ) -> Result<Id, Error>
    where
        R: rand::Rng,
    {
        #[inline]
        #[must_use]
        fn difference(first: Id, second: Id) -> Id {
            let mut bigger: [u8; 20];
            let mut smaller: [u8; 20];
            if first < second {
                bigger = <[u8; 20]>::from(second);
                smaller = <[u8; 20]>::from(first);
            } else {
                bigger = <[u8; 20]>::from(first);
                smaller = <[u8; 20]>::from(second);
            }
            smaller.twos_complement();
            let _ = bigger.overflowing_add(&smaller);
            Id::from(bigger)
        }

        let data_bit_diff = difference(*range.end(), *range.start());

        let mut rand_bits: [u8; 20] =
            <[u8; 20]>::randomize_up_to(<[u8; 20]>::from(data_bit_diff), rng)?;
        let _ = rand_bits.overflowing_add(&<[u8; 20]>::from(*range.start()));
        Ok(Id::from(rand_bits))
    }

    /// Finds the middle id between this node ID and the node ID argument.
    #[inline]
    #[must_use]
    pub(super) fn middle(first: Id, second: Id) -> Id {
        let mut data = <[u8; 20]>::from(first);
        let overflow = data.overflowing_add(&<[u8; 20]>::from(second));
        data.shift_right();
        if overflow {
            data[0] |= 0x80;
        }
        Id::from(data)
    }

    #[inline]
    #[must_use]
    pub(super) fn prev(id: Id) -> Id {
        let id_bytes = <[u8; 20]>::from(id);
        let mut data: [u8; 20] = [0; 20];
        let offset_from_end = id_bytes.iter().rposition(|v| *v != 0).unwrap_or(0);
        for (val, self_val) in data.iter_mut().zip(id_bytes.iter()).take(offset_from_end) {
            *val = *self_val;
        }

        data[offset_from_end] = if id_bytes[offset_from_end] == 0 {
            0xff
        } else {
            id_bytes[offset_from_end] - 1
        };

        for val in data
            .iter_mut()
            .take(id_bytes.len())
            .skip(offset_from_end + 1)
        {
            *val = 0xff;
        }

        Id::from(data)
    }

    trait IdBytes {
        #[must_use]
        fn overflowing_add(&mut self, other: &Self) -> bool;

        fn twos_complement(&mut self);

        /// Shifts the bits right by 1.
        fn shift_right(&mut self);

        fn randomize_up_to<R>(end: Self, rng: &mut R) -> Result<Self, Error>
        where
            R: rand::Rng,
            Self: Sized;
    }

    impl IdBytes for [u8; 20] {
        #[must_use]
        fn overflowing_add(&mut self, other: &Self) -> bool {
            let mut carry_over: u8 = 0;

            for idx in (0..self.len()).rev() {
                let (partial_val, overflow) = self[idx].overflowing_add(other[idx]);
                let (final_val, carry_over_overflow) = partial_val.overflowing_add(carry_over);
                self[idx] = final_val;
                carry_over = if carry_over_overflow || overflow {
                    1
                } else {
                    0
                };
            }

            carry_over == 1
        }

        fn twos_complement(&mut self) {
            for val in self.iter_mut() {
                *val = !(*val);
            }
            let one_bit = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
            let _ = self.overflowing_add(&one_bit);
        }

        fn shift_right(&mut self) {
            let mut add_high_bit = false;
            for val in self.iter_mut() {
                let is_lower_bit_set = (*val & 0x01) == 1;
                *val >>= 1;
                if add_high_bit {
                    *val |= 0x80;
                }
                add_high_bit = is_lower_bit_set;
            }
        }

        /// An inclusive randomize up to.
        fn randomize_up_to<R>(end: Self, rng: &mut R) -> Result<Self, Error>
        where
            R: rand::Rng,
        {
            let mut data: Self = [0; 20];
            let mut lower_than_max = false;

            for idx in 0..data.len() {
                data[idx] = if lower_than_max {
                    rng.gen()
                } else {
                    let idx_val = end[idx];
                    let val = rng.gen_range(0..=idx_val);
                    if val < idx_val {
                        lower_than_max = true;
                    }
                    val
                };
            }

            Ok(data)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_debug() {
            let node_id = Id::max();
            let debug_str = format!("{:?}", node_id);
            assert_eq!(debug_str, "Id(FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF)");
        }

        #[test]
        fn test_overflowing_add() {
            let mut bytes: [u8; 20] = [
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
            ];
            let overflow = bytes.overflowing_add(&[
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            ]);
            assert!(!overflow);
            assert_eq!(
                bytes,
                [1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33, 35, 37, 39,]
            );
        }

        #[test]
        fn test_twos_complement() {
            let mut bytes: [u8; 20] = [
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
            ];
            bytes.twos_complement();
            assert_eq!(
                bytes,
                [
                    255, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245, 244, 243, 242, 241, 240,
                    239, 238, 237, 237
                ]
            );
        }

        #[test]
        fn test_shift_right() {
            let mut bytes: [u8; 20] = [
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
            ];
            bytes.shift_right();
            assert_eq!(
                bytes,
                [0, 0, 129, 1, 130, 2, 131, 3, 132, 4, 133, 5, 134, 6, 135, 7, 136, 8, 137, 9]
            );
        }

        #[test]
        fn test_id_ord() {
            let mut node_ids = vec![
                Id::from([0xff; 20]),
                Id::from([0x00; 20]),
                super::middle(Id::from([0xff; 20]), Id::from([0x00; 20])),
            ];
            node_ids.sort();
            assert_eq!(
                node_ids,
                vec![
                    Id::from([0x00; 20]),
                    super::middle(Id::from([0xff; 20]), Id::from([0x00; 20])),
                    Id::from([0xff; 20]),
                ]
            );
        }

        #[test]
        fn test_id_distance_ord() {
            let mut node_ids = vec![
                Id::from([0x00; 20]),
                super::middle(Id::from([0xff; 20]), Id::from([0x00; 20])),
                Id::from([0xff; 20]),
            ];
            let pivot_id = super::middle(Id::from([0xef; 20]), Id::from([0x00; 20]));
            node_ids.sort_by_key(|a| a.distance(pivot_id));
            assert_eq!(
                node_ids,
                vec![
                    super::middle(Id::from([0xff; 20]), Id::from([0x00; 20])),
                    Id::from([0x00; 20]),
                    Id::from([0xff; 20]),
                ]
            );
        }
    }
}

const FIND_LOCAL_ID_INTERVAL: Duration = Duration::from_secs(15 * 60);

#[derive(Debug)]
pub struct Table<A, TxId, Instant> {
    pivot: Id,
    buckets: Vec<Bucket<A, TxId, Instant>>,
    find_pivot_id_deadline: Instant,
}

impl<A, TxId, Instant> Table<A, TxId, Instant>
where
    A: Into<SocketAddr>,
    Instant: cloudburst::time::Instant,
{
    pub fn new(pivot: Id, now: Instant) -> Self {
        Self {
            pivot,
            buckets: vec![Bucket::new(Id::min()..=Id::max(), now.clone())],
            find_pivot_id_deadline: now,
        }
    }

    pub(crate) fn find_node<I, R>(
        &mut self,
        target_id: Id,
        config: &crate::Config,
        tx_manager: &mut Transactions<transaction::Id, std::net::SocketAddr, std::time::Instant>,
        msg_buffer: &mut msg_buffer::Buffer<transaction::Id>,
        bootstrap_addrs: I,
        rng: &mut R,
        now: Instant,
    ) -> Result<FindNodeOp, Error>
    where
        I: IntoIterator<Item = A>,
        A: Clone,
        R: rand::Rng,
    {
        let neighbors = self
            .find_neighbors(target_id, &now)
            .take(8)
            .map(|a| AddrOptId::new((*a.addr()).clone(), Some(a.id())))
            .chain(bootstrap_addrs.into_iter().map(AddrOptId::with_addr));
        let mut find_node_op = FindNodeOp::new(config.supported_addr, target_id, neighbors);
        find_node_op.start(config, tx_manager, msg_buffer, rng)?;
        Ok(find_node_op)
    }

    fn try_insert(&mut self, addr_id: AddrId<A>, now: Instant)
    where
        A: Clone,
        TxId: Clone,
    {
        let node_id = addr_id.id();
        if node_id == self.pivot {
            return;
        }

        let bucket = self
            .buckets
            .iter_mut()
            .find(|n| n.range.contains(&node_id))
            .expect("bucket should always exist for a node");
        if bucket.range.contains(&self.pivot) && bucket.is_full() {
            let bucket = self.buckets.pop().expect("last bucket should always exist");
            let (mut first_bucket, mut second_bucket) = bucket.split(now.clone());
            if first_bucket.range.contains(&node_id) {
                first_bucket.try_insert(addr_id, now);
            } else {
                second_bucket.try_insert(addr_id, now);
            }

            if first_bucket.range.contains(&self.pivot) {
                self.buckets.push(second_bucket);
                self.buckets.push(first_bucket);
            } else {
                self.buckets.push(first_bucket);
                self.buckets.push(second_bucket);
            }
        } else {
            bucket.try_insert(addr_id, now);
        }
    }

    pub(crate) fn find_neighbors(&self, id: Id, now: &Instant) -> std::vec::IntoIter<AddrId<A>>
    where
        A: Clone,
    {
        let mut nodes = self
            .buckets
            .iter()
            .flat_map(|b| b.prioritized_nodes(now.clone()).cloned())
            .collect::<Vec<_>>();
        nodes.sort_by_key(|a| a.id().distance(id));
        nodes.into_iter()
    }

    pub(crate) fn on_msg_received<'a, R>(
        &mut self,
        addr_id: AddrId<A>,
        kind: &Ty<'a>,
        config: &crate::Config,
        tx_manager: &mut Transactions<transaction::Id, std::net::SocketAddr, std::time::Instant>,
        msg_buffer: &mut msg_buffer::Buffer<transaction::Id>,
        rng: &mut R,
        now: Instant,
    ) -> Result<(), Error>
    where
        A: Clone + PartialEq,
        TxId: Clone,
        R: rand::Rng,
    {
        let node_id = addr_id.id();
        if node_id == self.pivot {
            return Ok(());
        }

        let bucket = self
            .buckets
            .iter_mut()
            .find(|n| n.range.contains(&node_id))
            .expect("bucket should always exist for a node");
        if bucket.range.contains(&self.pivot) && bucket.is_full() {
            let bucket = self.buckets.pop().expect("last bucket should always exist");
            let (mut first_bucket, mut second_bucket) = bucket.split(now.clone());
            if first_bucket.range.contains(&node_id) {
                first_bucket
                    .on_msg_received(addr_id, kind, config, tx_manager, msg_buffer, rng, now)?;
            } else {
                second_bucket
                    .on_msg_received(addr_id, kind, config, tx_manager, msg_buffer, rng, now)?;
            }

            if first_bucket.range.contains(&self.pivot) {
                self.buckets.push(second_bucket);
                self.buckets.push(first_bucket);
            } else {
                self.buckets.push(first_bucket);
                self.buckets.push(second_bucket);
            }
        } else {
            bucket.on_msg_received(addr_id, kind, config, tx_manager, msg_buffer, rng, now)?;
        }
        Ok(())
    }

    pub(crate) fn on_resp_timeout<R>(
        &mut self,
        addr_id: AddrId<A>,
        config: &crate::Config,
        tx_manager: &mut Transactions<transaction::Id, std::net::SocketAddr, std::time::Instant>,
        msg_buffer: &mut msg_buffer::Buffer<transaction::Id>,
        rng: &mut R,
        now: Instant,
    ) -> Result<(), Error>
    where
        A: PartialEq + Copy,
        R: rand::Rng,
    {
        let node_id = addr_id.id();
        let bucket = self
            .buckets
            .iter_mut()
            .find(|n| n.range.contains(&node_id))
            .expect("bucket should always exist for a node");
        bucket.on_resp_timeout(&addr_id, config, tx_manager, msg_buffer, rng, now)?;
        Ok(())
    }

    pub(crate) fn timeout(&self) -> Option<Instant> {
        self.buckets
            .iter()
            .map(|b| b.expected_change_deadline.clone())
            .chain(std::iter::once(self.find_pivot_id_deadline.clone()))
            .min()
    }

    pub(crate) fn on_timeout<R>(
        &mut self,
        config: &crate::Config,
        tx_manager: &mut Transactions<transaction::Id, std::net::SocketAddr, std::time::Instant>,
        msg_buffer: &mut msg_buffer::Buffer<transaction::Id>,
        find_node_ops: &mut Vec<FindNodeOp>,
        rng: &mut R,
        now: Instant,
    ) -> Result<(), Error>
    where
        R: rand::Rng,
        A: Clone,
    {
        if self.find_pivot_id_deadline <= now {
            find_node_ops.push(self.find_node(
                self.pivot,
                config,
                tx_manager,
                msg_buffer,
                std::iter::empty(),
                rng,
                now.clone(),
            )?);
            self.find_pivot_id_deadline = now.clone() + FIND_LOCAL_ID_INTERVAL;
        }

        let target_ids = self
            .buckets
            .iter_mut()
            .filter(|b| b.expected_change_deadline <= now.clone())
            .map(|b| {
                b.expected_change_deadline = now.clone() + EXPECT_CHANGE_INTERVAL;
                r::rand_in_inclusive_range(&b.range, rng)
            })
            .collect::<Result<Vec<_>, _>>()?;
        debug!("routing table on_timeout()");
        for target_id in target_ids {
            find_node_ops.push(self.find_node(
                target_id,
                config,
                tx_manager,
                msg_buffer,
                std::iter::empty(),
                rng,
                now.clone(),
            )?);
            debug!(
                "starting to find target_id={:?} find_node_ops.len={}",
                target_id,
                find_node_ops.len()
            );
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) enum RoutingTable<TxId, Instant> {
    Ipv4(Table<SocketAddrV4, TxId, Instant>),
    Ipv6(Table<SocketAddrV6, TxId, Instant>),
    Ipv4AndIpv6(
        Table<SocketAddrV4, TxId, Instant>,
        Table<SocketAddrV6, TxId, Instant>,
    ),
}

impl<TxId, Instant> RoutingTable<TxId, Instant>
where
    Instant: cloudburst::time::Instant,
{
    pub(crate) fn try_insert_addr_ids<'a, I>(&mut self, addrs: I, now: Instant)
    where
        I: IntoIterator<Item = &'a AddrId<SocketAddr>>,
        TxId: Clone,
    {
        for addr_id in addrs {
            match addr_id.addr() {
                SocketAddr::V4(addr) => match self {
                    RoutingTable::Ipv4(routing_table)
                    | RoutingTable::Ipv4AndIpv6(routing_table, _) => {
                        routing_table.try_insert(AddrId::new(*addr, addr_id.id()), now.clone());
                    }
                    RoutingTable::Ipv6(_) => {}
                },
                SocketAddr::V6(addr) => match self {
                    RoutingTable::Ipv4(_) => {}
                    RoutingTable::Ipv6(routing_table)
                    | RoutingTable::Ipv4AndIpv6(_, routing_table) => {
                        routing_table.try_insert(AddrId::new(*addr, addr_id.id()), now.clone());
                    }
                },
            }
        }
    }

    pub(crate) fn find_node<I, R>(
        &mut self,
        target_id: Id,
        config: &crate::Config,
        tx_manager: &mut Transactions<transaction::Id, std::net::SocketAddr, std::time::Instant>,
        msg_buffer: &mut msg_buffer::Buffer<transaction::Id>,
        find_node_ops: &mut Vec<FindNodeOp>,
        bootstrap_addrs: I,
        rng: &mut R,
        now: Instant,
    ) -> Result<(), Error>
    where
        I: IntoIterator<Item = SocketAddr>,
        R: rand::Rng,
    {
        let mut ipv4_socket_addrs = Vec::new();
        let mut ipv6_socket_addrs = Vec::new();
        for socket_addr in bootstrap_addrs {
            match socket_addr {
                SocketAddr::V4(addr) => {
                    ipv4_socket_addrs.push(addr);
                }
                SocketAddr::V6(addr) => {
                    ipv6_socket_addrs.push(addr);
                }
            }
        }
        match self {
            RoutingTable::Ipv4(routing_table) => find_node_ops.push(routing_table.find_node(
                target_id,
                config,
                tx_manager,
                msg_buffer,
                ipv4_socket_addrs,
                rng,
                now,
            )?),
            RoutingTable::Ipv6(routing_table) => find_node_ops.push(routing_table.find_node(
                target_id,
                config,
                tx_manager,
                msg_buffer,
                ipv6_socket_addrs,
                rng,
                now,
            )?),
            RoutingTable::Ipv4AndIpv6(routing_table_v4, routing_table_v6) => {
                find_node_ops.push(routing_table_v4.find_node(
                    target_id,
                    config,
                    tx_manager,
                    msg_buffer,
                    ipv4_socket_addrs,
                    rng,
                    now.clone(),
                )?);
                find_node_ops.push(routing_table_v6.find_node(
                    target_id,
                    config,
                    tx_manager,
                    msg_buffer,
                    ipv6_socket_addrs,
                    rng,
                    now,
                )?);
            }
        }
        Ok(())
    }

    pub(crate) fn on_msg_received<'a, R>(
        &mut self,
        addr_id: AddrId<SocketAddr>,
        kind: &Ty<'a>,
        config: &crate::Config,
        tx_manager: &mut Transactions<transaction::Id, std::net::SocketAddr, std::time::Instant>,
        msg_buffer: &mut msg_buffer::Buffer<transaction::Id>,
        rng: &mut R,
        now: Instant,
    ) -> Result<(), Error>
    where
        TxId: Clone,
        R: rand::Rng,
    {
        match addr_id.addr() {
            SocketAddr::V4(addr) => match self {
                RoutingTable::Ipv4(routing_table) | RoutingTable::Ipv4AndIpv6(routing_table, _) => {
                    routing_table.on_msg_received(
                        AddrId::new(*addr, addr_id.id()),
                        kind,
                        config,
                        tx_manager,
                        msg_buffer,
                        rng,
                        now,
                    )?;
                }
                RoutingTable::Ipv6(_) => {}
            },
            SocketAddr::V6(addr) => match self {
                RoutingTable::Ipv4(_) => {}
                RoutingTable::Ipv6(routing_table) | RoutingTable::Ipv4AndIpv6(_, routing_table) => {
                    routing_table.on_msg_received(
                        AddrId::new(*addr, addr_id.id()),
                        kind,
                        config,
                        tx_manager,
                        msg_buffer,
                        rng,
                        now,
                    )?;
                }
            },
        }
        Ok(())
    }

    pub(crate) fn on_resp_timeout<R>(
        &mut self,
        addr_id: AddrId<SocketAddr>,
        config: &crate::Config,
        tx_manager: &mut Transactions<transaction::Id, std::net::SocketAddr, std::time::Instant>,
        msg_buffer: &mut msg_buffer::Buffer<transaction::Id>,
        rng: &mut R,
        now: Instant,
    ) -> Result<(), Error>
    where
        R: rand::Rng,
    {
        match addr_id.addr() {
            SocketAddr::V4(addr) => match self {
                RoutingTable::Ipv4(routing_table) | RoutingTable::Ipv4AndIpv6(routing_table, _) => {
                    routing_table.on_resp_timeout(
                        AddrId::new(*addr, addr_id.id()),
                        config,
                        tx_manager,
                        msg_buffer,
                        rng,
                        now,
                    )?;
                }
                RoutingTable::Ipv6(_) => {}
            },
            SocketAddr::V6(addr) => match self {
                RoutingTable::Ipv4(_) => {}
                RoutingTable::Ipv6(routing_table) | RoutingTable::Ipv4AndIpv6(_, routing_table) => {
                    routing_table.on_resp_timeout(
                        AddrId::new(*addr, addr_id.id()),
                        config,
                        tx_manager,
                        msg_buffer,
                        rng,
                        now,
                    )?;
                }
            },
        }
        Ok(())
    }

    pub(crate) fn timeout(&self) -> Option<Instant> {
        match self {
            RoutingTable::Ipv4(routing_table) => routing_table.timeout(),
            RoutingTable::Ipv6(routing_table) => routing_table.timeout(),
            RoutingTable::Ipv4AndIpv6(routing_table_v4, routing_table_v6) => {
                [routing_table_v4.timeout(), routing_table_v6.timeout()]
                    .as_ref()
                    .iter()
                    .filter_map(std::clone::Clone::clone)
                    .min()
            }
        }
    }

    pub(crate) fn on_timeout<R>(
        &mut self,
        config: &crate::Config,
        tx_manager: &mut Transactions<transaction::Id, std::net::SocketAddr, std::time::Instant>,
        msg_buffer: &mut msg_buffer::Buffer<transaction::Id>,
        find_node_ops: &mut Vec<FindNodeOp>,
        rng: &mut R,
        now: Instant,
    ) -> Result<(), Error>
    where
        R: rand::Rng,
    {
        match self {
            RoutingTable::Ipv4(routing_table) => {
                routing_table.on_timeout(config, tx_manager, msg_buffer, find_node_ops, rng, now)
            }
            RoutingTable::Ipv6(routing_table) => {
                routing_table.on_timeout(config, tx_manager, msg_buffer, find_node_ops, rng, now)
            }
            RoutingTable::Ipv4AndIpv6(routing_table_v4, routing_table_v6) => {
                routing_table_v4.on_timeout(
                    config,
                    tx_manager,
                    msg_buffer,
                    find_node_ops,
                    rng,
                    now.clone(),
                )?;
                routing_table_v6.on_timeout(
                    config,
                    tx_manager,
                    msg_buffer,
                    find_node_ops,
                    rng,
                    now,
                )?;
                Ok(())
            }
        }
    }

    pub fn find_neighbors_ipv4(&self, id: Id) -> impl Iterator<Item = AddrId<SocketAddrV4>> {
        match self {
            RoutingTable::Ipv4(routing_table) | RoutingTable::Ipv4AndIpv6(routing_table, _) => {
                routing_table.find_neighbors(id, &Instant::now())
            }
            RoutingTable::Ipv6(_) => Vec::new().into_iter(),
        }
    }

    pub fn find_neighbors_ipv6(&self, id: Id) -> impl Iterator<Item = AddrId<SocketAddrV6>> {
        match self {
            RoutingTable::Ipv6(routing_table) | RoutingTable::Ipv4AndIpv6(_, routing_table) => {
                routing_table.find_neighbors(id, &Instant::now())
            }
            RoutingTable::Ipv4(_) => Vec::new().into_iter(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cloudburst::dht::krpc::transaction;
    use std::net::SocketAddrV4;

    #[test]
    fn test_split_bucket() {
        let now = std::time::Instant::now();
        let bucket: Bucket<SocketAddrV4, transaction::Id, _> =
            Bucket::new(Id::min()..=Id::max(), now);
        let (first_bucket, second_bucket) = bucket.split(now);
        assert_eq!(
            first_bucket.range,
            Id::min()
                ..=Id::from([
                    0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe
                ])
        );
        assert_eq!(
            second_bucket.range,
            Id::from([
                0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                0xff, 0xff, 0xff, 0xff, 0xff, 0xff
            ])..=Id::max()
        );
    }
}

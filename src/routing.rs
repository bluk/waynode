// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use cloudburst::dht::{
    krpc::Ty,
    node::{AddrId, Id},
};
use core::{cmp::Ordering, ops::RangeInclusive, time::Duration};
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};

#[derive(Debug, PartialEq)]
enum NodeState {
    Good,
    Questionable,
    Bad,
}

/// Contains the address and [`Id`] for a node with metadata about the last response.
///
/// Used to store a node's information for routing queries to. Contains
/// "liveliness" information to determine if the `Node` is still likely valid.
#[derive(Debug, Clone, Copy)]
pub struct Node<Addr, TxId, Instant> {
    addr_id: AddrId<Addr>,
    karma: i8,
    next_response_deadline: Instant,
    next_query_deadline: Instant,
    ping_tx_id: Option<TxId>,
}

impl<A, TxId, Instant> Node<A, TxId, Instant>
where
    Instant: cloudburst::time::Instant,
{
    const TIMEOUT_INTERVAL: Duration = Duration::from_secs(15 * 60);

    fn new(addr_id: AddrId<A>, now: Instant) -> Self {
        Self {
            addr_id,
            karma: 0,
            next_response_deadline: now.clone() + Self::TIMEOUT_INTERVAL,
            next_query_deadline: now + Self::TIMEOUT_INTERVAL,
            ping_tx_id: None,
        }
    }

    /// Returns the address.
    pub fn addr_id(&self) -> &AddrId<A> {
        &self.addr_id
    }

    /// Returns a ping's transaction Id, if the ping is still active.
    pub fn ping_tx_id(&self) -> Option<&TxId> {
        self.ping_tx_id.as_ref()
    }

    /// When pinged, sets the transaction Id to identify the response or time out later.
    pub fn on_ping(&mut self, tx_id: TxId) {
        self.ping_tx_id = Some(tx_id);
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

    fn max_msg_recv_deadline(&self) -> &Instant {
        core::cmp::max(&self.next_response_deadline, &self.next_query_deadline)
    }

    fn on_msg_received(&mut self, kind: &Ty, tx_id: Option<&TxId>, now: Instant)
    where
        TxId: PartialEq,
    {
        match kind {
            Ty::Response => {
                if let Some(tx_id) = tx_id {
                    if let Some(ping_tx_id) = &self.ping_tx_id {
                        if *ping_tx_id == *tx_id {
                            self.ping_tx_id = None;
                        }
                    }
                }
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
                if let Some(tx_id) = tx_id {
                    if let Some(ping_tx_id) = &self.ping_tx_id {
                        if *ping_tx_id == *tx_id {
                            self.ping_tx_id = None;
                        }
                    }
                }
                self.next_response_deadline = now + Self::TIMEOUT_INTERVAL;
                self.karma = self.karma.saturating_sub(1);
            }
            Ty::Unknown(_) => {
                if let Some(tx_id) = tx_id {
                    if let Some(ping_tx_id) = &self.ping_tx_id {
                        if *ping_tx_id == *tx_id {
                            self.ping_tx_id = None;
                        }
                    }
                }
                self.karma = self.karma.saturating_sub(1);
            }
            _ => {
                todo!()
            }
        }
    }

    fn on_resp_timeout(&mut self, tx_id: &TxId)
    where
        TxId: PartialEq,
    {
        self.karma = self.karma.saturating_sub(1);

        if let Some(ping_tx_id) = &self.ping_tx_id {
            if *ping_tx_id == *tx_id {
                self.ping_tx_id = None;
            }
        }
    }
}

/// A bucket contains information about [`Node`]s which have an [`Id`] within a specific range.
///
/// Individual nodes may need to be pinged to ensure the node is still active.
///
/// Buckets may occassionally need to be refreshed if there is no activity for
/// the nodes within the bucket.
#[derive(Debug)]
pub struct Bucket<A, TxId, Instant, const SIZE: usize = 8, const REPLACEMENT_SIZE: usize = 8> {
    range: RangeInclusive<Id>,
    nodes: [Option<Node<A, TxId, Instant>>; SIZE],
    replacement_nodes: [Option<Node<A, TxId, Instant>>; REPLACEMENT_SIZE],
    refresh_deadline: Instant,
}

impl<A, TxId, Instant, const SIZE: usize, const REPLACEMENT_SIZE: usize>
    Bucket<A, TxId, Instant, SIZE, REPLACEMENT_SIZE>
where
    A: Into<SocketAddr>,
    Instant: cloudburst::time::Instant,
{
    const NODES_NONE: Option<Node<A, TxId, Instant>> = None;

    /// Creates a new `Bucket` for nodes which are within the inclusive `Id` range.
    pub fn new(range: RangeInclusive<Id>, refresh_deadline: Instant) -> Self {
        Bucket {
            range,
            nodes: [Self::NODES_NONE; SIZE],
            replacement_nodes: [Self::NODES_NONE; REPLACEMENT_SIZE],
            refresh_deadline,
        }
    }

    /// Returns the bucket's `Id` range.
    #[must_use]
    #[inline]
    pub fn range(&self) -> &RangeInclusive<Id> {
        &self.range
    }

    /// Returns a random `Id` which would be in the bucket's range.
    ///
    /// # Errors
    ///
    /// Returns a [`rand::Error`] if the random number generator cannot generate a random `Id`.
    #[inline]
    pub fn rand_id<R>(&self, rng: &mut R) -> Result<Id, rand::Error>
    where
        R: rand::RngCore,
    {
        internal::rand_in_inclusive_range(&self.range, rng)
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.nodes.iter().all(std::option::Option::is_some)
    }

    /// Updates the refresh deadline.
    #[inline]
    pub fn set_refresh_deadline(&mut self, refresh_deadline: Instant) {
        self.refresh_deadline = refresh_deadline;
    }

    /// Returns the deadline which a `Node` from within the bucket's range should be pinged or found.
    #[inline]
    pub fn refresh_deadline(&self) -> &Instant {
        &self.refresh_deadline
    }

    fn insert(&mut self, addr_id: AddrId<A>, refresh_deadline: Instant, now: &Instant) {
        self.nodes[self.nodes.len() - 1] = Some(Node::new(addr_id, now.clone()));
        self.sort_node_ids(now);
        self.set_refresh_deadline(refresh_deadline);
    }

    pub fn find_node_to_ping(&mut self, now: &Instant) -> Option<&mut Node<A, TxId, Instant>> {
        let pinged_nodes_count = self
            .nodes
            .iter()
            .flatten()
            .filter(|n| n.state_with_now(now) == NodeState::Questionable && n.ping_tx_id.is_some())
            .count();
        let replacement_nodes_count = self
            .replacement_nodes
            .iter()
            .filter(|n| n.is_some())
            .count();
        if pinged_nodes_count < replacement_nodes_count {
            self.nodes.iter_mut().rev().flatten().find(|n| {
                n.state_with_now(now) == NodeState::Questionable && n.ping_tx_id.is_none()
            })
        } else {
            None
        }
    }

    fn on_msg_received<'a>(
        &mut self,
        addr_id: AddrId<A>,
        kind: &Ty<'a>,
        tx_id: Option<&TxId>,
        refresh_deadline: Instant,
        now: &Instant,
    ) where
        A: PartialEq + Clone,
        TxId: PartialEq,
    {
        if let Some(node) = self
            .nodes
            .iter_mut()
            .flatten()
            .find(|n| n.addr_id == addr_id)
        {
            node.on_msg_received(kind, tx_id, now.clone());
            match kind {
                Ty::Response | Ty::Query => {
                    self.sort_node_ids(now);

                    self.set_refresh_deadline(refresh_deadline);
                }
                Ty::Error | Ty::Unknown(_) => match node.state_with_now(now) {
                    NodeState::Good | NodeState::Questionable => {
                        self.sort_node_ids(now);
                    }
                    NodeState::Bad => {
                        if let Some(replacement_node) =
                            self.replacement_nodes.iter_mut().find(|n| n.is_some())
                        {
                            if let Some(replacement_node) = replacement_node.take() {
                                *node = replacement_node;
                                self.set_refresh_deadline(refresh_deadline);
                            }
                        }
                        self.sort_node_ids(now);
                    }
                },
                _ => {
                    todo!()
                }
            }
        } else {
            match kind {
                Ty::Response | Ty::Query | Ty::Error => {}
                Ty::Unknown(_) => return,
                _ => {
                    todo!()
                }
            }

            if let Some(pos) = self.nodes.iter().rev().position(|n| {
                n.as_ref()
                    .map_or(false, |n| n.state_with_now(now) == NodeState::Bad)
            }) {
                let mut node = Node::new(addr_id, now.clone());
                node.on_msg_received(kind, tx_id, now.clone());
                self.nodes[pos] = Some(node);
                self.sort_node_ids(now);
                self.set_refresh_deadline(refresh_deadline);
            } else if let Some(pos) = self
                .replacement_nodes
                .iter_mut()
                .rev()
                .position(|n| n.is_none())
            {
                let mut node = Node::new(addr_id, now.clone());
                node.on_msg_received(kind, tx_id, now.clone());
                self.replacement_nodes[pos] = Some(node);
                self.sort_node_ids(now);
            }
        }
    }

    fn on_resp_timeout(
        &mut self,
        addr_id: &AddrId<A>,
        tx_id: &TxId,
        refresh_deadline: Instant,
        now: &Instant,
    ) where
        A: PartialEq + Clone,
        TxId: PartialEq,
    {
        if let Some(node) = self
            .nodes
            .iter_mut()
            .flatten()
            .find(|n| n.addr_id == *addr_id)
        {
            node.on_resp_timeout(tx_id);
            match node.state_with_now(now) {
                NodeState::Good => {
                    // The sort order will not change if the state is still good
                }
                NodeState::Questionable => {
                    self.sort_node_ids(now);
                }
                NodeState::Bad => {
                    if let Some(replacement_node) =
                        self.replacement_nodes.iter_mut().find(|n| n.is_some())
                    {
                        if let Some(replacement_node) = replacement_node.take() {
                            *node = replacement_node;
                            self.set_refresh_deadline(refresh_deadline);
                        }
                    }
                    self.sort_node_ids(now);
                }
            }
        }
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

    fn split(
        self,
        refresh_deadline: Instant,
    ) -> (Bucket<A, TxId, Instant>, Bucket<A, TxId, Instant>)
    where
        A: Clone,
        TxId: Clone,
    {
        let middle = internal::middle(*self.range.end(), *self.range.start());

        let mut lower_bucket = Bucket::new(
            *self.range.start()..=internal::prev(middle),
            refresh_deadline.clone(),
        );
        let mut upper_bucket = Bucket::new(middle..=*self.range.end(), refresh_deadline);

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

                b.max_msg_recv_deadline().cmp(a.max_msg_recv_deadline())
            }
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        });
    }
}

mod internal {
    use cloudburst::dht::node::Id;

    pub(super) fn rand_in_inclusive_range<R>(
        range: &std::ops::RangeInclusive<Id>,
        rng: &mut R,
    ) -> Result<Id, rand::Error>
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

        fn randomize_up_to<R>(end: Self, rng: &mut R) -> Result<Self, rand::Error>
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
        fn randomize_up_to<R>(end: Self, rng: &mut R) -> Result<Self, rand::Error>
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
                middle(Id::from([0xff; 20]), Id::from([0x00; 20])),
            ];
            node_ids.sort();
            assert_eq!(
                node_ids,
                vec![
                    Id::from([0x00; 20]),
                    middle(Id::from([0xff; 20]), Id::from([0x00; 20])),
                    Id::from([0xff; 20]),
                ]
            );
        }

        #[test]
        fn test_id_distance_ord() {
            let mut node_ids = vec![
                Id::from([0x00; 20]),
                middle(Id::from([0xff; 20]), Id::from([0x00; 20])),
                Id::from([0xff; 20]),
            ];
            let pivot_id = super::middle(Id::from([0xef; 20]), Id::from([0x00; 20]));
            node_ids.sort_by_key(|a| a.distance(pivot_id));
            assert_eq!(
                node_ids,
                vec![
                    middle(Id::from([0xff; 20]), Id::from([0x00; 20])),
                    Id::from([0x00; 20]),
                    Id::from([0xff; 20]),
                ]
            );
        }
    }
}

#[derive(Debug)]
pub struct Table<A, TxId, Instant> {
    pivot: Id,
    buckets: Vec<Bucket<A, TxId, Instant>>,
}

impl<A, TxId, Instant> Table<A, TxId, Instant>
where
    A: Into<SocketAddr>,
    Instant: cloudburst::time::Instant,
{
    /// Creates a new table based on the given pivot.
    ///
    /// The `refresh_deadline` is the `Instant` which the initial bucket(s) should be refreshed at.
    pub fn new(pivot: Id, refresh_deadline: Instant) -> Self {
        Self {
            pivot,
            buckets: vec![Bucket::new(Id::min()..=Id::max(), refresh_deadline)],
        }
    }

    /// Returns the local ID which serves as the pivot around which the table is
    /// based on.
    ///
    /// More nodes which have an `Id` closer to the pivot `Id` are kept in the
    /// routing table compared to nodes which have an `Id` further away.
    #[must_use]
    pub fn pivot(&self) -> Id {
        self.pivot
    }

    fn try_insert(&mut self, addr_id: AddrId<A>, refresh_deadline: Instant, now: &Instant)
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
            let (mut first_bucket, mut second_bucket) = bucket.split(refresh_deadline.clone());
            if first_bucket.range.contains(&node_id) {
                first_bucket.insert(addr_id, refresh_deadline, now);
            } else {
                second_bucket.insert(addr_id, refresh_deadline, now);
            }

            if first_bucket.range.contains(&self.pivot) {
                self.buckets.push(second_bucket);
                self.buckets.push(first_bucket);
            } else {
                self.buckets.push(first_bucket);
                self.buckets.push(second_bucket);
            }
        } else {
            bucket.insert(addr_id, refresh_deadline, now);
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

    pub(crate) fn on_msg_received<'a>(
        &mut self,
        addr_id: AddrId<A>,
        kind: &Ty<'a>,
        tx_id: Option<&TxId>,
        refresh_deadline: Instant,
        now: &Instant,
    ) where
        A: Clone + PartialEq,
        TxId: Clone + PartialEq,
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
            let (mut first_bucket, mut second_bucket) = bucket.split(refresh_deadline.clone());
            if first_bucket.range.contains(&node_id) {
                first_bucket.on_msg_received(addr_id, kind, tx_id, refresh_deadline, now);
            } else {
                second_bucket.on_msg_received(addr_id, kind, tx_id, refresh_deadline, now);
            }

            if first_bucket.range.contains(&self.pivot) {
                self.buckets.push(second_bucket);
                self.buckets.push(first_bucket);
            } else {
                self.buckets.push(first_bucket);
                self.buckets.push(second_bucket);
            }
        } else {
            bucket.on_msg_received(addr_id, kind, tx_id, refresh_deadline, now);
        }
    }

    pub(crate) fn on_resp_timeout(
        &mut self,
        addr_id: AddrId<A>,
        tx_id: &TxId,
        refresh_deadline: Instant,
        now: &Instant,
    ) where
        A: PartialEq + Copy,
        TxId: PartialEq,
    {
        let node_id = addr_id.id();
        let bucket = self
            .buckets
            .iter_mut()
            .find(|n| n.range.contains(&node_id))
            .expect("bucket should always exist for a node");
        bucket.on_resp_timeout(&addr_id, tx_id, refresh_deadline, now);
    }

    /// The earliest deadline when at least one of the buckets in the routing table should be refreshed.
    #[must_use]
    pub fn timeout(&self) -> Option<Instant> {
        self.buckets
            .iter()
            .map(Bucket::refresh_deadline)
            .min()
            .cloned()
    }

    /// Finds a bucket which needs to be refreshed.
    pub fn find_refreshable_bucket(
        &mut self,
        now: &Instant,
    ) -> Option<&mut Bucket<A, TxId, Instant>> {
        self.buckets
            .iter_mut()
            .find(|b| *b.refresh_deadline() <= *now)
    }

    pub(crate) fn find_node_to_ping(
        &mut self,
        now: &Instant,
    ) -> Option<&mut Node<A, TxId, Instant>> {
        self.buckets
            .iter_mut()
            .find_map(|b| b.find_node_to_ping(now))
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
    pub(crate) fn try_insert_addr_ids<'a, I>(
        &mut self,
        addrs: I,
        refresh_deadline: &Instant,
        now: &Instant,
    ) where
        I: IntoIterator<Item = &'a AddrId<SocketAddr>>,
        TxId: Clone,
    {
        for addr_id in addrs {
            match addr_id.addr() {
                SocketAddr::V4(addr) => match self {
                    RoutingTable::Ipv4(routing_table)
                    | RoutingTable::Ipv4AndIpv6(routing_table, _) => {
                        routing_table.try_insert(
                            AddrId::new(*addr, addr_id.id()),
                            refresh_deadline.clone(),
                            now,
                        );
                    }
                    RoutingTable::Ipv6(_) => {}
                },
                SocketAddr::V6(addr) => match self {
                    RoutingTable::Ipv4(_) => {}
                    RoutingTable::Ipv6(routing_table)
                    | RoutingTable::Ipv4AndIpv6(_, routing_table) => {
                        routing_table.try_insert(
                            AddrId::new(*addr, addr_id.id()),
                            refresh_deadline.clone(),
                            now,
                        );
                    }
                },
            }
        }
    }

    pub(crate) fn on_msg_received<'a>(
        &mut self,
        addr_id: AddrId<SocketAddr>,
        kind: &Ty<'a>,
        tx_id: Option<&TxId>,
        refresh_deadline: Instant,
        now: &Instant,
    ) where
        TxId: Clone + PartialEq,
    {
        match addr_id.addr() {
            SocketAddr::V4(addr) => match self {
                RoutingTable::Ipv4(routing_table) | RoutingTable::Ipv4AndIpv6(routing_table, _) => {
                    routing_table.on_msg_received(
                        AddrId::new(*addr, addr_id.id()),
                        kind,
                        tx_id,
                        refresh_deadline,
                        now,
                    );
                }
                RoutingTable::Ipv6(_) => {}
            },
            SocketAddr::V6(addr) => match self {
                RoutingTable::Ipv4(_) => {}
                RoutingTable::Ipv6(routing_table) | RoutingTable::Ipv4AndIpv6(_, routing_table) => {
                    routing_table.on_msg_received(
                        AddrId::new(*addr, addr_id.id()),
                        kind,
                        tx_id,
                        refresh_deadline,
                        now,
                    );
                }
            },
        }
    }

    pub(crate) fn on_resp_timeout(
        &mut self,
        addr_id: AddrId<SocketAddr>,
        tx_id: &TxId,
        refresh_deadline: Instant,
        now: &Instant,
    ) where
        TxId: PartialEq,
    {
        match addr_id.addr() {
            SocketAddr::V4(addr) => match self {
                RoutingTable::Ipv4(routing_table) | RoutingTable::Ipv4AndIpv6(routing_table, _) => {
                    routing_table.on_resp_timeout(
                        AddrId::new(*addr, addr_id.id()),
                        tx_id,
                        refresh_deadline,
                        now,
                    );
                }
                RoutingTable::Ipv6(_) => {}
            },
            SocketAddr::V6(addr) => match self {
                RoutingTable::Ipv4(_) => {}
                RoutingTable::Ipv6(routing_table) | RoutingTable::Ipv4AndIpv6(_, routing_table) => {
                    routing_table.on_resp_timeout(
                        AddrId::new(*addr, addr_id.id()),
                        tx_id,
                        refresh_deadline,
                        now,
                    );
                }
            },
        }
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

    pub fn find_neighbors_ipv4(
        &self,
        id: Id,
        now: &Instant,
    ) -> impl Iterator<Item = AddrId<SocketAddrV4>> {
        match self {
            RoutingTable::Ipv4(routing_table) | RoutingTable::Ipv4AndIpv6(routing_table, _) => {
                routing_table.find_neighbors(id, now)
            }
            RoutingTable::Ipv6(_) => Vec::new().into_iter(),
        }
    }

    pub fn find_neighbors_ipv6(
        &self,
        id: Id,
        now: &Instant,
    ) -> impl Iterator<Item = AddrId<SocketAddrV6>> {
        match self {
            RoutingTable::Ipv6(routing_table) | RoutingTable::Ipv4AndIpv6(_, routing_table) => {
                routing_table.find_neighbors(id, now)
            }
            RoutingTable::Ipv4(_) => Vec::new().into_iter(),
        }
    }

    pub fn find_node_to_ping_ipv4(
        &mut self,
        now: &Instant,
    ) -> Option<&mut Node<std::net::SocketAddrV4, TxId, Instant>> {
        match self {
            RoutingTable::Ipv4(routing_table) | RoutingTable::Ipv4AndIpv6(routing_table, _) => {
                routing_table.find_node_to_ping(now)
            }
            RoutingTable::Ipv6(_) => None,
        }
    }

    pub fn find_node_to_ping_ipv6(
        &mut self,
        now: &Instant,
    ) -> Option<&mut Node<std::net::SocketAddrV6, TxId, Instant>> {
        match self {
            RoutingTable::Ipv6(routing_table) | RoutingTable::Ipv4AndIpv6(_, routing_table) => {
                routing_table.find_node_to_ping(now)
            }
            RoutingTable::Ipv4(_) => None,
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

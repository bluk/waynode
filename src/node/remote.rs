// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::{krpc::Kind, node::AddrId};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub(crate) enum RemoteState {
    Good,
    Questionable,
    Bad,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RemoteNode {
    pub(crate) addr_id: AddrId,
    pub(crate) missing_responses: u8,
    pub(crate) next_response_deadline: Option<Instant>,
    pub(crate) next_query_deadline: Option<Instant>,
    pub(crate) last_pinged: Option<Instant>,
}

impl RemoteNode {
    const TIMEOUT_INTERVAL: Duration = Duration::from_secs(15 * 60);

    pub(crate) fn with_addr_id(addr_id: AddrId) -> Self {
        Self {
            addr_id,
            missing_responses: 0,
            next_response_deadline: None,
            next_query_deadline: None,
            last_pinged: None,
        }
    }

    pub(crate) fn state_with_now(&self, now: Instant) -> RemoteState {
        if let Some(next_response_deadline) = self.next_response_deadline {
            if next_response_deadline < now {
                return RemoteState::Good;
            }
        }

        if let Some(next_query_deadline) = self.next_query_deadline {
            if self.next_response_deadline.is_some() && next_query_deadline < now {
                return RemoteState::Good;
            }
        }

        if self.missing_responses > 2 {
            return RemoteState::Bad;
        }

        RemoteState::Questionable
    }

    pub(crate) fn next_msg_deadline(&self) -> Option<Instant> {
        match (self.next_query_deadline, self.next_response_deadline) {
            (Some(query), None) => Some(query),
            (None, Some(resp)) => Some(resp),
            (Some(query), Some(resp)) => {
                if resp < query {
                    Some(query)
                } else {
                    Some(resp)
                }
            }
            (None, None) => None,
        }
    }

    pub(crate) fn on_msg_received(&mut self, kind: &Kind, now: Instant) {
        self.last_pinged = None;
        match kind {
            Kind::Response => {
                self.next_response_deadline = Some(now + Self::TIMEOUT_INTERVAL);
                if self.missing_responses > 0 {
                    self.missing_responses -= 1;
                }
            }
            Kind::Query => {
                self.next_query_deadline = Some(now + Self::TIMEOUT_INTERVAL);
            }
            Kind::Error => {
                self.next_response_deadline = Some(now + Self::TIMEOUT_INTERVAL);
                if self.missing_responses < u8::MAX {
                    self.missing_responses += 1;
                }
            }
            Kind::Unknown(_) => {
                if self.missing_responses < u8::MAX {
                    self.missing_responses += 1;
                }
            }
        }
    }

    pub(crate) fn on_resp_timeout(&mut self) {
        self.last_pinged = None;
        if self.missing_responses < u8::MAX {
            self.missing_responses += 1;
        }
    }

    pub(crate) fn on_ping(&mut self, now: Instant) {
        self.last_pinged = Some(now);
    }
}

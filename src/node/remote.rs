// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::{addr::Addr, error::Error, krpc::Kind, node::Id};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RemoteNodeId {
    pub node_id: Option<Id>,
    pub addr: Addr,
}

impl RemoteNodeId {
    pub(crate) fn resolve_addr(&self) -> Result<SocketAddr, Error> {
        Ok(match &self.addr {
            Addr::SocketAddr(s) => *s,
            Addr::HostPort(s) => {
                use std::net::ToSocketAddrs;
                s.to_socket_addrs()
                    .map_err(|_| Error::CannotResolveSocketAddr)?
                    .next()
                    .ok_or(Error::CannotResolveSocketAddr)?
            }
        })
    }

    // use crate::{addr::NodeIdGenerator, node::Id};
    // use serde::{Deserialize, Serialize};
    // use std::net::{SocketAddr, ToSocketAddrs};
    // pub(crate) fn is_valid_node_id(&self) -> bool {
    //     if let Some(id) = self.node_id.as_ref() {
    //         match self.addr {
    //             Addr::HostPort(ref host) => {
    //                 let addrs = host.to_socket_addrs();
    //                 match addrs {
    //                     Ok(mut addrs) => match addrs.next() {
    //                         Some(addr) => match addr {
    //                             SocketAddr::V4(addr) => return addr.ip().is_valid_node_id(id),
    //                             SocketAddr::V6(addr) => return addr.ip().is_valid_node_id(id),
    //                         },
    //                         None => return false,
    //                     },
    //                     Err(_) => return false,
    //                 }
    //             }
    //             Addr::SocketAddr(addr) => match addr {
    //                 SocketAddr::V4(addr) => return addr.ip().is_valid_node_id(id),
    //                 SocketAddr::V6(addr) => return addr.ip().is_valid_node_id(id),
    //             },
    //         }
    //     }
    //     false
    // }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub(crate) enum RemoteState {
    Good,
    Questionable,
    Bad,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RemoteNode {
    pub(crate) id: RemoteNodeId,
    pub(crate) missing_responses: u8,
    pub(crate) last_response: Option<Instant>,
    pub(crate) last_query: Option<Instant>,
    pub(crate) last_pinged: Option<Instant>,
}

impl RemoteNode {
    const TIMEOUT_INTERVAL: Duration = Duration::from_secs(15 * 60);

    pub(crate) fn new_with_id(id: RemoteNodeId) -> Self {
        Self {
            id,
            missing_responses: 0,
            last_response: None,
            last_query: None,
            last_pinged: None,
        }
    }

    pub(crate) fn state(&self) -> RemoteState {
        let now = Instant::now();

        if let Some(last_response) = self.last_response {
            if last_response + Self::TIMEOUT_INTERVAL > now {
                return RemoteState::Good;
            }
        }

        if let Some(last_query) = self.last_query {
            if self.last_response.is_some() && last_query + Self::TIMEOUT_INTERVAL > now {
                return RemoteState::Good;
            }
        }

        if self.missing_responses > 2 {
            return RemoteState::Bad;
        }

        RemoteState::Questionable
    }

    pub(crate) fn last_interaction(&self) -> Option<Instant> {
        match (self.last_query, self.last_response) {
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
                self.last_response = Some(now);
                if self.missing_responses > 0 {
                    self.missing_responses -= 1;
                }
            }
            Kind::Query => {
                self.last_query = Some(now);
            }
            Kind::Error => {
                self.last_response = Some(now);
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

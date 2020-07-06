use crate::{addr::NodeIdGenerator, node::Id};
use serde::{Deserialize, Serialize};
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum RemoteAddr {
    HostPort(String),
    SocketAddr(SocketAddr),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct RemoteNodeId {
    pub addr: RemoteAddr,
    pub node_id: Option<Id>,
}

impl RemoteNodeId {
    pub(crate) fn is_valid_node_id(&self) -> bool {
        if let Some(id) = self.node_id.as_ref() {
            match self.addr {
                RemoteAddr::HostPort(ref host) => {
                    let addrs = host.to_socket_addrs();
                    match addrs {
                        Ok(mut addrs) => match addrs.next() {
                            Some(addr) => match addr {
                                SocketAddr::V4(addr) => return addr.ip().is_valid_node_id(id),
                                SocketAddr::V6(addr) => return addr.ip().is_valid_node_id(id),
                            },
                            None => return false,
                        },
                        Err(_) => return false,
                    }
                }
                RemoteAddr::SocketAddr(addr) => match addr {
                    SocketAddr::V4(addr) => return addr.ip().is_valid_node_id(id),
                    SocketAddr::V6(addr) => return addr.ip().is_valid_node_id(id),
                },
            }
        }
        false
    }
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
    pub(crate) last_response: Option<Instant>,
    pub(crate) last_query: Option<Instant>,
    pub(crate) missing_responses: usize,
}

impl RemoteNode {
    const TIMEOUT_INTERVAL: Duration = Duration::from_secs(15 * 60);

    pub(crate) fn new_with_id(id: RemoteNodeId) -> Self {
        Self {
            id,
            last_response: None,
            last_query: None,
            missing_responses: 0,
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
                    return Some(query);
                } else {
                    return Some(resp);
                }
            }
            (None, None) => None,
        }
    }

    pub(crate) fn on_response_timeout(&mut self) {
        self.missing_responses += 1;
    }

    pub(crate) fn on_error(&mut self) {
        self.last_response = Some(Instant::now());
        self.missing_responses += 1;
    }

    pub(crate) fn on_response(&mut self) {
        self.last_response = Some(Instant::now());
        if self.missing_responses > 0 {
            self.missing_responses -= 1;
        }
    }

    pub(crate) fn on_query(&mut self) {
        self.last_query = Some(Instant::now());
    }
}
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn test_1() {
//         let v = Vec::<RemoteNodeId>::new();
//     }
// }

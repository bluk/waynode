use crate::node::Id;
use serde_bytes::ByteBuf;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    /// Local IPv4 node id
    pub ipv4_id: Id,
    /// Local IPv6 node id
    pub ipv6_id: Id,
    /// Client version identifier
    pub client_version: ByteBuf,
    /// The amount of time before a query without a response is considered timed out
    pub query_timeout: Duration,
    /// If the node is read only
    pub is_read_only_node: bool,
    /// The max amount of nodes in a routing table bucket
    pub max_node_count_per_bucket: usize,
}

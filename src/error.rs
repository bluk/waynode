//! Possible crate errors.

#[derive(Debug)]
pub enum Error {
    /// An error with the random number generator
    RngError,
    /// If the node ID is invalid or malformed
    InvalidNodeId,
    /// Random node ID cannot be generated
    CannotGenerateNodeId,
    CannotDeserializeKrpcMessage,
    CannotSerializeKrpcMessage,
    UnknownMessageKind,
    CannotResolveSocketAddr,
}

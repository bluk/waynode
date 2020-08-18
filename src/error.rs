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
    InvalidLocalTransactionId,
}

impl From<bt_bencode::Error> for Error {
    fn from(e: bt_bencode::Error) -> Self {
        match e {
            bt_bencode::Error::Serialize(_) => Error::CannotSerializeKrpcMessage,
            bt_bencode::Error::Deserialize(_) => Error::CannotDeserializeKrpcMessage,
            _ => Error::CannotDeserializeKrpcMessage,
        }
    }
}

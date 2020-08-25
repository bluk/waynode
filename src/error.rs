// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

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
    InvalidInfoHash,
    InvalidCompactAddr,
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

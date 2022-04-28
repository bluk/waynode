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
    CannotDeserializeKrpcMessage,
    CannotSerializeKrpcMessage,
    InvalidCompactAddr,
}

impl From<bt_bencode::Error> for Error {
    fn from(e: bt_bencode::Error) -> Self {
        match e {
            bt_bencode::Error::Serialize(_) => Error::CannotSerializeKrpcMessage,
            _ => Error::CannotDeserializeKrpcMessage,
        }
    }
}

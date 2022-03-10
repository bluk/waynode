// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Error message values.
//!
//! Error messages are described in [BEP 5][bep_0005].
//!
//! [bep_0005]: http://bittorrent.org/beps/bep_0005.html

use super::ErrorCode;
use bt_bencode::{value::Number, Value};
use serde_bytes::ByteBuf;
use std::convert::TryFrom;

/// The error message values.
#[derive(Clone, Debug, PartialEq)]
pub struct ErrorValues {
    code: ErrorCode,
    description: String,
}

impl ErrorValues {
    /// Instantiates a standard error value with a code and description.
    pub fn new(code: ErrorCode, description: String) -> Self {
        Self { code, description }
    }
}

impl ErrorValues {
    /// Sets the code.
    pub fn set_code(&mut self, code: ErrorCode) {
        self.code = code;
    }

    /// Sets the description.
    pub fn set_description(&mut self, description: String) {
        self.description = description
    }
}

impl super::ErrorVal for ErrorValues {
    fn code(&self) -> ErrorCode {
        self.code
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn to_value(&self) -> Value {
        Value::from(self)
    }
}

impl TryFrom<Value> for ErrorValues {
    type Error = crate::error::Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Self::try_from(
            value
                .as_array()
                .ok_or(crate::error::Error::CannotDeserializeKrpcMessage)?,
        )
    }
}

impl TryFrom<&Vec<Value>> for ErrorValues {
    type Error = crate::error::Error;

    fn try_from(value: &Vec<Value>) -> Result<Self, Self::Error> {
        match (
            value.get(0).and_then(|code| {
                code.as_i64()
                    .map(Number::Signed)
                    .or_else(|| code.as_u64().map(Number::Unsigned))
            }),
            value
                .get(1)
                .and_then(|value| value.as_byte_str())
                .and_then(|bs| std::str::from_utf8(bs).ok())
                .map(String::from),
        ) {
            (Some(code), Some(description)) => {
                let code = match code {
                    Number::Signed(code) => match code {
                        201 => ErrorCode::GenericError,
                        202 => ErrorCode::ServerError,
                        203 => ErrorCode::ProtocolError,
                        204 => ErrorCode::MethodUnknown,
                        _ => ErrorCode::Other(
                            i32::try_from(code)
                                .map_err(|_| crate::error::Error::CannotDeserializeKrpcMessage)?,
                        ),
                    },
                    Number::Unsigned(code) => match code {
                        201 => ErrorCode::GenericError,
                        202 => ErrorCode::ServerError,
                        203 => ErrorCode::ProtocolError,
                        204 => ErrorCode::MethodUnknown,
                        _ => ErrorCode::Other(
                            i32::try_from(code)
                                .map_err(|_| crate::error::Error::CannotDeserializeKrpcMessage)?,
                        ),
                    },
                };
                Ok(ErrorValues { code, description })
            }
            _ => Err(crate::error::Error::CannotDeserializeKrpcMessage),
        }
    }
}

impl From<ErrorValues> for Value {
    fn from(value: ErrorValues) -> Self {
        Value::from(&value)
    }
}

impl From<&ErrorValues> for Value {
    fn from(value: &ErrorValues) -> Self {
        Value::List(vec![
            Value::Int(Number::Signed(i64::from(value.code.code()))),
            Value::ByteStr(ByteBuf::from(value.description.clone())),
        ])
    }
}

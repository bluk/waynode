// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::ErrorCode;
use bt_bencode::{value::Number, Value};
use serde_bytes::ByteBuf;
use std::convert::TryFrom;

#[derive(Clone, Debug, PartialEq)]
pub struct ErrorValue {
    code: ErrorCode,
    description: String,
}

impl ErrorValue {
    pub fn with_code_and_desc(code: ErrorCode, description: String) -> Self {
        Self { code, description }
    }
}

impl super::ErrorVal for ErrorValue {
    fn code(&self) -> ErrorCode {
        self.code
    }

    fn set_code(&mut self, code: ErrorCode) {
        self.code = code;
    }

    fn description(&self) -> &String {
        &self.description
    }

    fn set_description(&mut self, description: String) {
        self.description = description
    }

    fn to_value(&self) -> Value {
        Value::from(self)
    }
}

impl TryFrom<Value> for ErrorValue {
    type Error = crate::error::Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Self::try_from(
            value
                .as_array()
                .ok_or(crate::error::Error::CannotDeserializeKrpcMessage)?,
        )
    }
}

impl TryFrom<&Vec<Value>> for ErrorValue {
    type Error = crate::error::Error;

    fn try_from(value: &Vec<Value>) -> Result<Self, Self::Error> {
        match (
            value.get(0).and_then(|code| {
                code.as_i64()
                    .map(|n| Number::Signed(n))
                    .or_else(|| code.as_u64().map(|n| Number::Unsigned(n)))
            }),
            value
                .get(1)
                .and_then(|value| value.as_byte_str())
                .and_then(|bs| std::str::from_utf8(bs).ok())
                .map(|s| String::from(s)),
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
                Ok(ErrorValue { code, description })
            }
            _ => Err(crate::error::Error::CannotDeserializeKrpcMessage),
        }
    }
}

impl From<ErrorValue> for Value {
    fn from(value: ErrorValue) -> Self {
        Value::from(&value)
    }
}

impl From<&ErrorValue> for Value {
    fn from(value: &ErrorValue) -> Self {
        Value::List(vec![
            Value::Int(Number::Signed(i64::from(value.code.code()))),
            Value::ByteStr(ByteBuf::from(value.description.clone())),
        ])
    }
}
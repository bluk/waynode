// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use bt_bencode::Value;
use serde::{ser::SerializeMap, Serialize, Serializer};
use serde_bytes::ByteBuf;

#[derive(Debug)]
pub(crate) struct QueryMsg<'a> {
    /// query arguments
    pub a: Option<&'a Value>,
    /// method name of query
    pub q: &'a ByteBuf,
    /// transaction id
    pub t: &'a ByteBuf,
    /// client version
    pub v: Option<&'a ByteBuf>,
}

impl<'a> Serialize for QueryMsg<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        if self.a.is_some() {
            map.serialize_entry("a", &self.a)?;
        }
        map.serialize_entry("q", &self.q)?;
        map.serialize_entry("t", &self.t)?;
        if self.v.is_some() {
            map.serialize_entry("v", &self.v)?;
        }
        map.serialize_entry("y", "q")?;
        map.end()
    }
}

#[derive(Debug)]
pub(crate) struct RespMsg<'a> {
    /// return values
    pub r: Option<&'a Value>,
    /// transaction id
    pub t: &'a ByteBuf,
    /// client version
    pub v: Option<&'a ByteBuf>,
}

impl<'a> Serialize for RespMsg<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        if self.r.is_some() {
            map.serialize_entry("r", &self.r)?;
        }
        map.serialize_entry("t", &self.t)?;
        if self.v.is_some() {
            map.serialize_entry("v", &self.v)?;
        }
        map.serialize_entry("y", "r")?;
        map.end()
    }
}

#[derive(Debug)]
pub(crate) struct ErrMsg<'a> {
    /// error details
    pub e: Option<&'a Value>,
    /// transaction id
    pub t: &'a ByteBuf,
    /// client version
    pub v: Option<&'a ByteBuf>,
}

// impl<'a> ErrMsg<'a> {
//     pub fn code(&self) -> Option<Number> {
//         self.e
//             .as_ref()
//             .and_then(|e| e.as_array())
//             .and_then(|l| l.get(0))
//             .and_then(|n| match n {
//                 Value::Int(num) => Some(*num),
//                 _ => None,
//             })
//     }
//
//     pub fn msg(&self) -> Option<String> {
//         self.e
//             .as_ref()
//             .and_then(|e| e.as_array())
//             .and_then(|l| l.get(1))
//             .and_then(|m| m.as_byte_str())
//             .and_then(|s| String::from_utf8(s.to_vec()).ok())
//     }
// }

impl<'a> Serialize for ErrMsg<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        if self.e.is_some() {
            map.serialize_entry("e", &self.e)?;
        }
        map.serialize_entry("t", &self.t)?;
        if self.v.is_some() {
            map.serialize_entry("v", &self.v)?;
        }
        map.serialize_entry("y", "e")?;
        map.end()
    }
}

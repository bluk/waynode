// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use bt_bencode::Value;
use serde_bytes::ByteBuf;
use std::{
    collections::VecDeque,
    net::SocketAddr,
    time::{Duration, Instant},
};

use crate::{
    error::Error,
    krpc::{self, QueryArgs},
    node::remote::RemoteNodeId,
    transaction,
};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct OutboundMsg {
    tx_id: Option<transaction::Id>,
    pub(crate) addr: SocketAddr,
    timeout: Duration,
    remote_id: RemoteNodeId,
    pub(crate) msg_data: Vec<u8>,
}

impl OutboundMsg {
    pub(crate) fn into_transaction(self) -> Option<transaction::Transaction> {
        let remote_id = self.remote_id;
        let resolved_addr = self.addr;
        let timeout = self.timeout;
        self.tx_id.map(|tx_id| transaction::Transaction {
            local_id: transaction::LocalId::with_id_and_addr(tx_id, resolved_addr),
            remote_id,
            deadline: Instant::now() + timeout,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Msg {
    Resp(Value),
    Error(Value),
    Query(Value),
    Timeout,
}

#[derive(Clone, Debug)]
pub struct InboundMsg {
    pub(crate) remote_id: RemoteNodeId,
    pub(crate) tx_local_id: Option<transaction::LocalId>,
    pub(crate) msg: Msg,
}

impl InboundMsg {
    pub fn remote_id(&self) -> &RemoteNodeId {
        &self.remote_id
    }

    pub fn tx_local_id(&self) -> Option<transaction::LocalId> {
        self.tx_local_id
    }

    pub fn msg(&self) -> &Msg {
        &self.msg
    }
}

#[derive(Debug)]
pub(crate) struct Buffer {
    client_version: Option<ByteBuf>,
    inbound: VecDeque<InboundMsg>,
    outbound: VecDeque<OutboundMsg>,
}

impl Buffer {
    pub(crate) fn with_client_version(client_version: Option<ByteBuf>) -> Self {
        Self {
            client_version,
            inbound: VecDeque::new(),
            outbound: VecDeque::new(),
        }
    }

    pub(crate) fn push_inbound(&mut self, msg: InboundMsg) {
        self.inbound.push_back(msg);
    }

    pub(crate) fn pop_inbound(&mut self) -> Option<InboundMsg> {
        self.inbound.pop_front()
    }

    pub(crate) fn write_query<T>(
        &mut self,
        args: &T,
        remote_id: &RemoteNodeId,
        timeout: Duration,
        tx_manager: &mut transaction::Manager,
    ) -> Result<transaction::LocalId, Error>
    where
        T: QueryArgs + std::fmt::Debug,
    {
        let addr = remote_id.resolve_addr()?;
        let transaction_id = tx_manager.next_transaction_id();

        debug!(
            "write_query tx_id={:?} method_name={:?} remote_id={:?} args={:?}",
            transaction_id,
            String::from_utf8(Vec::from(T::method_name().clone())),
            &remote_id,
            &args
        );

        self.outbound.push_back(OutboundMsg {
            tx_id: Some(transaction_id),
            remote_id: remote_id.clone(),
            addr,
            msg_data: bt_bencode::to_vec(&krpc::ser::QueryMsg {
                a: Some(&args.to_value()),
                q: &ByteBuf::from(T::method_name()),
                t: &transaction_id.to_bytebuf(),
                v: self.client_version.as_ref(),
            })
            .map_err(|_| Error::CannotSerializeKrpcMessage)?,
            timeout,
        });
        Ok(transaction::LocalId::with_id_and_addr(transaction_id, addr))
    }

    pub(crate) fn write_resp(
        &mut self,
        transaction_id: &ByteBuf,
        resp: Option<Value>,
        remote_id: &RemoteNodeId,
    ) -> Result<(), Error> {
        let addr = remote_id.resolve_addr()?;
        self.outbound.push_back(OutboundMsg {
            tx_id: None,
            remote_id: remote_id.clone(),
            addr,
            msg_data: bt_bencode::to_vec(&krpc::ser::RespMsg {
                r: resp.as_ref(),
                t: &transaction_id,
                v: self.client_version.as_ref(),
            })
            .map_err(|_| Error::CannotSerializeKrpcMessage)?,
            timeout: Duration::new(0, 0),
        });
        Ok(())
    }

    pub fn write_err(
        &mut self,
        transaction_id: &ByteBuf,
        details: Option<Value>,
        remote_id: &RemoteNodeId,
    ) -> Result<(), Error> {
        let addr = remote_id.resolve_addr()?;
        self.outbound.push_back(OutboundMsg {
            tx_id: None,
            remote_id: remote_id.clone(),
            addr,
            msg_data: bt_bencode::to_vec(&krpc::ser::ErrMsg {
                e: details.as_ref(),
                t: &transaction_id,
                v: self.client_version.as_ref(),
            })
            .map_err(|_| Error::CannotSerializeKrpcMessage)?,
            timeout: Duration::new(0, 0),
        });
        Ok(())
    }

    pub(crate) fn pop_outbound(&mut self) -> Option<OutboundMsg> {
        self.outbound.pop_front()
    }
}

// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Demonstrates code to run a DHT node.
//!
//! Run the example.
//! $ cargo run --example node

use bt_bencode::Value;
use clap::Arg;
use clap::Command;
use cloudburst::dht::{
    krpc::{
        self, find_node, ping, transaction::Transaction, CompactAddr, ErrorCode, ErrorVal, Msg,
        QueryArgs, QueryMsg, RespVal,
    },
    node::{AddrOptId, Id, LocalId},
};
use mio::{net::UdpSocket, Events, Interest, Poll, Token};
use serde_bytes::Bytes;
use sloppy::{Config, Node};
use std::{
    io,
    net::{SocketAddr, SocketAddrV4},
    time::Instant,
};
use tracing::{debug, error, info, trace};

struct Args {
    bind_socket: SocketAddr,
}

fn get_args() -> Args {
    let matches = Command::new("Example node program")
        .version("1.0")
        .about("Demonstrates running a DHT node.")
        .arg(
            Arg::new("ip")
                .long("ip-address")
                .short('a')
                .value_name("IP")
                .help("The IP address to bind to")
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::new("port")
                .long("port")
                .short('p')
                .value_name("PORT")
                .help("The port to bind to")
                .required(false)
                .takes_value(true),
        )
        .get_matches();
    let ip = matches.value_of("ip").unwrap_or("0.0.0.0");
    let port = matches.value_of("port").unwrap_or("6881");
    let bind_socket = format!("{}:{}", ip, port).parse().unwrap();
    Args { bind_socket }
}

fn get_config(local_id: LocalId) -> Config {
    let mut config = Config::new(local_id);
    config.set_client_version(Some("ab12".into()));
    config.set_is_read_only_node(true);
    config.set_supported_addr(sloppy::SupportedAddr::Ipv4);
    config
}

fn main() -> io::Result<()> {
    tracing_subscriber::fmt::init();

    let args = get_args();
    let mut socket = mio::net::UdpSocket::bind(args.bind_socket)?;
    info!(bind_socket = %args.bind_socket, "listening...");

    let mut rng = rand::thread_rng();

    let local_id = Id::rand(&mut rng).unwrap();
    info!(local_id = %local_id);
    let config = get_config(LocalId::from(local_id));

    let mut node: Node<SocketAddrV4> = Node::new(
        config,
        std::iter::empty(),
        vec![
            String::from("router.magnets.im:6881"),
            String::from("router.bittorent.com:6881"),
            String::from("router.utorrent.com:6881"),
            String::from("dht.transmissionbt.com:6881"),
        ],
        Instant::now(),
    );
    let dht_token = Token(0);

    let mut poll = Poll::new()?;
    poll.registry().register(
        &mut socket,
        dht_token,
        Interest::READABLE | Interest::WRITABLE,
    )?;

    let mut events = Events::with_capacity(1024);

    let mut buf = [0; 4096];

    'event: loop {
        let now = Instant::now();
        let timeout_deadline: Option<Instant> = node.timeout();
        trace!(?now, ?timeout_deadline, "polling");

        poll.poll(
            &mut events,
            timeout_deadline.map(|deadline| deadline.saturating_duration_since(now)),
        )?;

        if events.is_empty() {
            let now = Instant::now();
            trace!(?now, "timed out");
            node.on_timeout(&mut rng);

            while let Some(tx) = node.pop_timed_out_tx(now) {
                let Transaction {
                    addr_opt_id,
                    tx_id,
                    timeout_deadline,
                } = tx;
                trace!(tx_id = ?tx_id, addr = %addr_opt_id.addr(), node_id = ?addr_opt_id.id(), ?timeout_deadline, "tx timed out");
                // normally, look at any locally initiated transactions and
                // considered them timed out if they match the read event
            }

            let break_loop = send_pings_to_nodes(&mut node, &mut socket, now)?;
            if break_loop {
                trace!("breaking loop after sending pings");
                continue 'event;
            }
        }

        'recv: loop {
            let (bytes_read, src_addr) = match socket.recv_from(&mut buf) {
                Ok(v) => v,
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        break 'recv;
                    }

                    error!(%e, "recv_from io error");
                    return Err(e);
                }
            };

            debug!(%src_addr, %bytes_read, "received");

            let filled_buf = &buf[..bytes_read];

            match src_addr {
                SocketAddr::V6(_) => {}
                SocketAddr::V4(src_addr) => match node.on_recv(filled_buf, src_addr) {
                    Ok(inbound_msg) => match inbound_msg.msg() {
                        sloppy::MsgEvent::Query(msg) => {
                            let break_loop = reply_to_query(
                                &mut node,
                                &mut socket,
                                *inbound_msg.addr_opt_id(),
                                msg,
                            )?;
                            if break_loop {
                                trace!("breaking loop after replying to query");
                                continue 'event;
                            }
                        }
                        sloppy::MsgEvent::Resp(_) | sloppy::MsgEvent::Error(_) => {}
                    },
                    Err(e) => {
                        error!(?e, "on_recv error");
                    }
                },
            }
        }

        let break_loop = send_find_node_queries(&mut node, &mut socket)?;
        if break_loop {
            trace!("breaking loop after sending find node queries");
            continue 'event;
        }
    }
}

fn reply_to_query(
    node: &mut Node<SocketAddrV4>,
    socket: &mut UdpSocket,
    addr_opt_id: AddrOptId<SocketAddrV4>,
    msg: &Value,
) -> io::Result<bool> {
    let method_name = msg.method_name();
    if let Some(s) = method_name.and_then(|v| core::str::from_utf8(v).ok()) {
        debug!(s, "received query");
    }
    match msg.method_name() {
        Some(ping::METHOD_PING) => {
            if let Some(tx_id) = msg.tx_id() {
                let ping_resp = ping::RespValues::new(node.config().local_id());
                let out = bt_bencode::to_vec(&krpc::ser::RespMsg {
                    r: Some(&ping_resp.to_value()),
                    t: Bytes::new(tx_id),
                    v: node.config().client_version().map(Bytes::new),
                })?;

                match socket.send_to(&out, (*addr_opt_id.addr()).into()) {
                    Ok(v) => v,
                    Err(e) => {
                        if e.kind() == io::ErrorKind::WouldBlock {
                            return Ok(true);
                        }

                        error!(%e, "send_to io error");
                        return Err(e);
                    }
                };
            }
        }
        Some(method_name) => {
            if let Some(tx_id) = msg.tx_id() {
                let error = cloudburst::dht::krpc::error::Values::new(
                    ErrorCode::MethodUnknown,
                    core::str::from_utf8(method_name).unwrap_or("").to_string(),
                );
                let out = bt_bencode::to_vec(&krpc::ser::RespMsg {
                    r: Some(&error.to_value()),
                    t: Bytes::new(tx_id),
                    v: node.config().client_version().map(Bytes::new),
                })?;

                match socket.send_to(&out, (*addr_opt_id.addr()).into()) {
                    Ok(v) => v,
                    Err(e) => {
                        if e.kind() == io::ErrorKind::WouldBlock {
                            return Ok(true);
                        }

                        error!(%e, "send_to io error");
                        return Err(e);
                    }
                };
            }
        }
        None => {
            if let Some(tx_id) = msg.tx_id() {
                let error = cloudburst::dht::krpc::error::Values::new(
                    ErrorCode::ProtocolError,
                    String::from("method name not listed"),
                );
                let out = bt_bencode::to_vec(&krpc::ser::RespMsg {
                    r: Some(&error.to_value()),
                    t: Bytes::new(tx_id),
                    v: node.config().client_version().map(Bytes::new),
                })?;

                match socket.send_to(&out, (*addr_opt_id.addr()).into()) {
                    Ok(v) => v,
                    Err(e) => {
                        if e.kind() == io::ErrorKind::WouldBlock {
                            return Ok(true);
                        }

                        error!(%e, "send_to io error");
                        return Err(e);
                    }
                };
            }
        }
    }
    Ok(false)
}

fn send_pings_to_nodes(
    node: &mut Node<SocketAddrV4>,
    socket: &mut UdpSocket,
    now: Instant,
) -> io::Result<bool> {
    let local_id = node.config().local_id();
    let client_version = node.config().client_version().map(<[u8]>::to_vec);
    let query_args = Some(ping::QueryArgs::new(local_id).to_value());
    let ping_method = Bytes::new(ping::METHOD_PING);
    loop {
        let tx_id = node.next_tx_id(&mut rand::thread_rng())?;
        if let Some(node_to_ping) = node.find_node_to_ping(now) {
            let addr_id = *node_to_ping.addr_id();
            let addr = (*addr_id.addr()).into();

            debug!(%addr, ?tx_id, "sending ping query");

            let out = bt_bencode::to_vec(&krpc::ser::QueryMsg {
                a: query_args.as_ref(),
                q: ping_method,
                t: Bytes::new(tx_id.as_ref()),
                v: client_version.as_deref().map(Bytes::new),
            })?;

            match socket.send_to(&out, addr) {
                Ok(v) => v,
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        return Ok(true);
                    }

                    error!(%e, "send_to io error");
                    return Err(e);
                }
            };

            node_to_ping.on_ping(tx_id);

            node.insert_tx(Transaction::new(
                addr_id.into(),
                tx_id,
                Instant::now() + node.config().default_query_timeout(),
            ));
        } else {
            break;
        }
    }

    Ok(false)
}

fn send_find_node_queries(
    node: &mut Node<SocketAddrV4>,
    socket: &mut UdpSocket,
) -> io::Result<bool> {
    while let Some((target_id, addr_opt_id)) = node.find_node_to_find_node() {
        let addr: SocketAddrV4 = match addr_opt_id.addr() {
            CompactAddr::V4(addr) => (*addr).into(),
            CompactAddr::V6(_) => continue,
        };

        let tx_id = node.next_tx_id(&mut rand::thread_rng())?;
        debug!(%addr, ?tx_id, %target_id, "sending find node query");

        let out = bt_bencode::to_vec(&krpc::ser::QueryMsg {
            a: Some(&find_node::QueryArgs::new(node.config().local_id(), target_id).to_value()),
            q: Bytes::new(find_node::METHOD_FIND_NODE),
            t: Bytes::new(tx_id.as_ref()),
            v: node.config().client_version().map(Bytes::new),
        })?;

        match socket.send_to(&out, addr.into()) {
            Ok(v) => v,
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    return Ok(true);
                }

                error!(%e, "send_to io error");
                return Err(e);
            }
        };

        node.insert_tx(Transaction::new(
            AddrOptId::new(addr, addr_opt_id.id()),
            tx_id,
            Instant::now() + node.config().default_query_timeout(),
        ));
        node.insert_tx_for_find_node(tx_id, target_id);
        node.queried_node_for_target(AddrOptId::new(addr.into(), addr_opt_id.id()), target_id);
    }

    Ok(false)
}

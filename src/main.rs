// Copyright 2022 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#![warn(
    rust_2018_idioms,
    missing_docs,
    missing_debug_implementations,
    unused_lifetimes,
    unused_qualifications
)]

use bt_bencode::Value;
use clap::{Arg, Command};
use cloudburst::dht::{
    krpc::{
        self, find_node, ping, transaction::Transaction, CompactAddr, CompactAddrV4, ErrorCode,
        ErrorVal, Msg, QueryArgs, QueryMsg, RespVal,
    },
    node::{AddrId, AddrOptId, Id, LocalId},
};
use core::{convert::TryFrom, time::Duration};
use serde_bytes::Bytes;
use std::{
    io,
    net::{SocketAddr, SocketAddrV4},
    time::Instant,
};
use tokio::{net::UdpSocket, time};
use tracing::{debug, error, info, trace};

mod dht;

use dht::Node;

struct Args {
    dht_bind_socket: SocketAddr,
    http_bind_socket: SocketAddr,
}

fn get_args() -> Args {
    let matches = Command::new("Example node program")
        .version("1.0")
        .about("Demonstrates running a DHT node.")
        .arg(
            Arg::new("dht-ip")
                .long("dht-ip-address")
                .value_name("DHT_IP")
                .help("The IP address to bind to for the DHT service")
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::new("dht-port")
                .long("dht-port")
                .value_name("DHT_PORT")
                .help("The port to bind to for the DHT service")
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::new("http-ip")
                .long("http-ip-address")
                .value_name("HTTP_IP")
                .help("The IP address to bind to for the HTTP service")
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::new("http-port")
                .long("http-port")
                .value_name("HTTP_PORT")
                .help("The port to bind to for the HTTP service")
                .required(false)
                .takes_value(true),
        )
        .get_matches();

    let dht_ip = matches.value_of("dht-ip").unwrap_or("0.0.0.0");
    let dht_port = matches.value_of("dht-port").unwrap_or("6881");
    let dht_bind_socket = format!("{}:{}", dht_ip, dht_port).parse().unwrap();

    let http_ip = matches.value_of("http-ip").unwrap_or("0.0.0.0");
    let http_port = matches.value_of("http-port").unwrap_or("8080");
    let http_bind_socket = format!("{}:{}", http_ip, http_port).parse().unwrap();

    Args {
        dht_bind_socket,
        http_bind_socket,
    }
}

fn get_config(local_id: LocalId) -> dht::Config {
    let mut config = dht::Config::new(local_id);
    config.set_client_version(Some("ab12".into()));
    config.set_is_read_only_node(true);
    config.set_supported_addr(dht::SupportedAddr::Ipv4);
    config
}

#[tokio::main]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt::init();

    let args = get_args();
    let mut socket = UdpSocket::bind(args.dht_bind_socket).await?;

    let mut rng = rand::thread_rng();

    let local_id = Id::rand(&mut rng).unwrap();

    info!(dht_bind_socket = %args.dht_bind_socket, %local_id, "listening...");

    let config = get_config(LocalId::from(local_id));

    let mut node: Node<SocketAddrV4> = dht::Node::new(
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

    let mut buf = [0; 4096];

    loop {
        send_find_node_queries(&mut node, &mut socket).await?;

        let now = Instant::now();
        let timeout_deadline = node.timeout().map_or(
            tokio::time::Instant::from(now) + Duration::from_secs(60),
            tokio::time::Instant::from,
        );
        trace!(?now, ?timeout_deadline, "polling");

        let sleep = time::sleep_until(timeout_deadline);
        tokio::pin!(sleep);

        tokio::select! {
            res = socket.recv_from(&mut buf) => {
                on_recv(res, &mut buf, &mut socket, &mut node, Instant::now()).await?;
            }
            _ = sleep => {
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

                send_pings_to_nodes(&mut node, &mut socket, now).await?;
            }
        };
    }
}

async fn on_recv(
    recv_from_result: io::Result<(usize, SocketAddr)>,
    buf: &mut [u8],
    socket: &mut UdpSocket,
    node: &mut Node<SocketAddrV4>,
    now: Instant,
) -> io::Result<()> {
    let (bytes_read, src_addr) = match recv_from_result {
        Ok(v) => v,
        Err(e) => {
            if e.kind() == io::ErrorKind::WouldBlock {
                return Ok(());
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
                dht::MsgEvent::Query(msg) => {
                    reply_to_query(node, socket, *inbound_msg.addr_opt_id(), msg, now).await?;
                }
                dht::MsgEvent::Resp(_) | dht::MsgEvent::Error(_) => {}
            },
            Err(e) => {
                error!(?e, "on_recv error");
            }
        },
    }
    Ok(())
}

async fn reply_to_query(
    node: &mut Node<SocketAddrV4>,
    socket: &mut UdpSocket,
    addr_opt_id: AddrOptId<SocketAddrV4>,
    msg: &Value,
    now: Instant,
) -> io::Result<()> {
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

                let AddrOptId { addr, id: _ } = addr_opt_id;

                debug!(%addr, ?tx_id, "sending ping response reply");

                match socket.send_to(&out, addr).await {
                    Ok(v) => v,
                    Err(e) => {
                        if e.kind() == io::ErrorKind::WouldBlock {
                            return Ok(());
                        }

                        error!(%e, "send_to io error");
                        return Err(e);
                    }
                };
            }
        }
        Some(find_node::METHOD_FIND_NODE) => {
            if let Some(tx_id) = msg.tx_id() {
                if let Ok(find_node_query) = find_node::QueryArgs::try_from(msg) {
                    let mut nodes = Vec::with_capacity(8);
                    for neighbor in node.find_neighbors(find_node_query.target(), now) {
                        let AddrId { addr, id } = neighbor;
                        nodes.push(AddrId {
                            addr: CompactAddrV4::from(addr),
                            id,
                        });
                        if nodes.len() == 8 {
                            break;
                        }
                    }

                    if !nodes.is_empty() {
                        while nodes.len() < 8 {
                            let copy = nodes[0];
                            nodes.push(copy);
                        }
                    }

                    let find_node_resp = find_node::RespValues::with_id_and_nodes_and_nodes6(
                        node.config().local_id(),
                        Some(nodes),
                        None,
                    );
                    let out = bt_bencode::to_vec(&krpc::ser::RespMsg {
                        r: Some(&find_node_resp.to_value()),
                        t: Bytes::new(tx_id),
                        v: node.config().client_version().map(Bytes::new),
                    })?;

                    let AddrOptId { addr, id: _ } = addr_opt_id;

                    debug!(%addr, ?tx_id, "sending find node response reply");

                    match socket.send_to(&out, addr).await {
                        Ok(v) => v,
                        Err(e) => {
                            if e.kind() == io::ErrorKind::WouldBlock {
                                return Ok(());
                            }

                            error!(%e, "send_to io error");
                            return Err(e);
                        }
                    };
                }
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

                let AddrOptId { addr, id: _ } = addr_opt_id;

                let method_name = core::str::from_utf8(method_name).unwrap_or("unknown");
                debug!(%addr, %method_name, ?tx_id, "sending find node response reply");

                match socket.send_to(&out, addr).await {
                    Ok(v) => v,
                    Err(e) => {
                        if e.kind() == io::ErrorKind::WouldBlock {
                            return Ok(());
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

                match socket.send_to(&out, *addr_opt_id.addr()).await {
                    Ok(v) => v,
                    Err(e) => {
                        if e.kind() == io::ErrorKind::WouldBlock {
                            return Ok(());
                        }

                        error!(%e, "send_to io error");
                        return Err(e);
                    }
                };
            }
        }
    }
    Ok(())
}

async fn send_pings_to_nodes(
    node: &mut Node<SocketAddrV4>,
    socket: &mut UdpSocket,
    now: Instant,
) -> io::Result<()> {
    let local_id = node.config().local_id();
    let client_version = node.config().client_version().map(<[u8]>::to_vec);
    let query_args = Some(ping::QueryArgs::new(local_id).to_value());
    let ping_method = Bytes::new(ping::METHOD_PING);
    loop {
        let tx_id = node.next_tx_id(&mut rand::thread_rng())?;
        if let Some(node_to_ping) = node.find_node_to_ping(now) {
            let addr_id = *node_to_ping.addr_id();
            let addr = *addr_id.addr();

            debug!(%addr, ?tx_id, "sending ping query");

            let out = bt_bencode::to_vec(&krpc::ser::QueryMsg {
                a: query_args.as_ref(),
                q: ping_method,
                t: Bytes::new(tx_id.as_ref()),
                v: client_version.as_deref().map(Bytes::new),
            })?;

            match socket.send_to(&out, addr).await {
                Ok(v) => v,
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        return Ok(());
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

    Ok(())
}

async fn send_find_node_queries(
    node: &mut Node<SocketAddrV4>,
    socket: &mut UdpSocket,
) -> io::Result<()> {
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

        match socket.send_to(&out, addr).await {
            Ok(v) => v,
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    return Ok(());
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

    Ok(())
}

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

#[macro_use]
extern crate log;

use std::io;
use std::net;

use std::time::Instant;

use clap::Arg;
use clap::Command;

use cloudburst::dht::krpc;
use cloudburst::dht::{
    krpc::{ping, ErrorCode, ErrorVal, Msg, QueryArgs, QueryMsg, RespVal},
    node::{Id, LocalId},
};
use mio::{Events, Interest, Poll, Token};

use serde_bytes::Bytes;
use sloppy::Node;

fn main() -> io::Result<()> {
    env_logger::init();

    let mut rng = rand::thread_rng();

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
    eprintln!("Listening on: {}:{}", ip, port);

    let local_addr: net::SocketAddr = format!("{}:{}", ip, port).parse().unwrap();
    let mut socket = mio::net::UdpSocket::bind(local_addr)?;

    let bootstrap_addrs = [
        "router.magnets.im:6881",
        "router.bittorent.com:6881",
        "dht.transmissionbt.com:6881",
    ]
    .iter()
    .map(|&s| {
        use std::net::ToSocketAddrs;
        s.to_socket_addrs()
    })
    .collect::<Result<Vec<_>, std::io::Error>>()
    .map(|v| v.into_iter().flatten().collect::<Vec<_>>())
    .expect("addresses to resolve");

    let mut config = sloppy::Config::new(LocalId::new(Id::rand(&mut rng).unwrap()));
    config.set_client_version(Some("ab12".into()));
    config.set_is_read_only_node(true);
    config.set_supported_addr(sloppy::SupportedAddr::Ipv4AndIpv6);

    let mut node: Node = Node::new(config, &[], bootstrap_addrs);
    let dht_token = Token(0);

    let mut poll = Poll::new()?;
    poll.registry().register(
        &mut socket,
        dht_token,
        Interest::READABLE | Interest::WRITABLE,
    )?;

    let mut events = Events::with_capacity(1024);

    let mut buf = [0; 65535];

    loop {
        let timeout_deadline: Option<Instant> = node.timeout();

        poll.poll(
            &mut events,
            timeout_deadline.map(|deadline| {
                let now = Instant::now();
                deadline.saturating_duration_since(now)
            }),
        )?;

        'recv: loop {
            if events.is_empty() {
                debug!("Timed out");
                match node.on_timeout(&mut rng) {
                    Ok(()) => {}
                    Err(e) => error!("on_timeout error: {:?}", e),
                };

                let now = Instant::now();

                while let Some(_read_evt) = node.find_timed_out_tx(now) {
                    // normally, look at any locally initiated transactions and
                    // considered them timed out if they match the read event
                }

                loop {
                    let tx_id = node.next_tx_id(&mut rand::thread_rng())?;
                    if let Some(node_to_ping) = node.find_node_to_ping_ipv4(now) {
                        let addr = (*node_to_ping.addr_id().addr()).into();
                        node_to_ping.on_ping(tx_id);

                        let out = bt_bencode::to_vec(&krpc::ser::QueryMsg {
                            a: Some(&ping::QueryArgs::new(node.config().local_id()).to_value()),
                            q: Bytes::new(ping::METHOD_PING),
                            t: Bytes::new(tx_id.as_ref()),
                            v: node.config().client_version().map(Bytes::new),
                        })?;

                        match socket.send_to(&out, addr) {
                            Ok(v) => v,
                            Err(e) => {
                                if e.kind() == io::ErrorKind::WouldBlock {
                                    todo!();
                                }

                                error!("send_to io error: {}", e);
                                continue;
                            }
                        };
                    } else {
                        break;
                    }
                }

                loop {
                    let tx_id = node.next_tx_id(&mut rand::thread_rng())?;
                    if let Some(node_to_ping) = node.find_node_to_ping_ipv6(now) {
                        let addr = (*node_to_ping.addr_id().addr()).into();
                        node_to_ping.on_ping(tx_id);

                        let out = bt_bencode::to_vec(&krpc::ser::QueryMsg {
                            a: Some(&ping::QueryArgs::new(node.config().local_id()).to_value()),
                            q: Bytes::new(ping::METHOD_PING),
                            t: Bytes::new(tx_id.as_ref()),
                            v: node.config().client_version().map(Bytes::new),
                        })?;

                        match socket.send_to(&out, addr) {
                            Ok(v) => v,
                            Err(e) => {
                                if e.kind() == io::ErrorKind::WouldBlock {
                                    todo!();
                                }

                                error!("send_to io error: {}", e);
                                continue;
                            }
                        };
                    } else {
                        break;
                    }
                }

                break 'recv;
            }

            let (bytes_read, src_addr) = match socket.recv_from(&mut buf) {
                Ok(v) => v,
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        break 'recv;
                    }

                    error!("recv_from io error: {}", e);
                    continue;
                }
            };

            let filled_buf = &buf[..bytes_read];

            match node.on_recv(filled_buf, src_addr) {
                Ok(inbound_msg) => {
                    // debug!("Read message: {:?}", inbound_msg);
                    match inbound_msg.msg() {
                        sloppy::MsgEvent::Query(msg) => match msg.method_name() {
                            Some(ping::METHOD_PING) => {
                                if let Some(tx_id) = msg.tx_id() {
                                    let ping_resp = ping::RespValues::new(node.config().local_id());
                                    let out = bt_bencode::to_vec(&krpc::ser::RespMsg {
                                        r: Some(&ping_resp.to_value()),
                                        t: Bytes::new(tx_id),
                                        v: node.config().client_version().map(Bytes::new),
                                    })?;

                                    match socket.send_to(&out, *inbound_msg.addr_opt_id().addr()) {
                                        Ok(v) => v,
                                        Err(e) => {
                                            if e.kind() == io::ErrorKind::WouldBlock {
                                                // return Ok(false);
                                                todo!();
                                            }

                                            error!("send_to io error: {}", e);
                                            continue;
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

                                    match socket.send_to(&out, *inbound_msg.addr_opt_id().addr()) {
                                        Ok(v) => v,
                                        Err(e) => {
                                            if e.kind() == io::ErrorKind::WouldBlock {
                                                // return Ok(false);
                                                todo!();
                                            }

                                            error!("send_to io error: {}", e);
                                            continue;
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

                                    match socket.send_to(&out, *inbound_msg.addr_opt_id().addr()) {
                                        Ok(v) => v,
                                        Err(e) => {
                                            if e.kind() == io::ErrorKind::WouldBlock {
                                                // return Ok(false);
                                                todo!();
                                            }

                                            error!("send_to io error: {}", e);
                                            continue;
                                        }
                                    };
                                }
                            }
                        },
                        sloppy::MsgEvent::Resp(_)
                        | sloppy::MsgEvent::Error(_)
                        | sloppy::MsgEvent::Timeout => {}
                    }
                }
                Err(e) => {
                    error!("on_recv error: {:?}", e);
                }
            }

            debug!("Sending after read");

            // match send_packets(&mut node, &socket, &mut out) {
            //     Ok(break_event) => {
            //         if break_event {
            //             break 'event;
            //         }
            //     }
            //     Err(e) => {
            //         error!("send_packets error: {:?}", e);
            //         return Err(e);
            //     }
            // }
        }

        debug!("Sending after recv");

        // match send_packets(&mut node, &socket, &mut out) {
        //     Ok(break_event) => {
        //         if break_event {
        //             break 'event;
        //         }
        //     }
        //     Err(e) => {
        //         error!("send_packets error: {:?}", e);
        //         return Err(e);
        //     }
        // }
    }

    Ok(())
}

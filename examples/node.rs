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
use std::time::Duration;

use clap::{App, Arg};

use mio::{Events, Interest, Poll, Token};

use sloppy::{
    krpc::{ping, ErrorCode, Msg, QueryMsg},
    Dht,
};

fn main() -> io::Result<()> {
    env_logger::init();

    let matches = App::new("Example node program")
        .version("1.0")
        .about("Demonstrates running a DHT node.")
        .arg(
            Arg::with_name("ip")
                .long("ip-address")
                .short("a")
                .value_name("IP")
                .help("The IP address to bind to")
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("port")
                .long("port")
                .short("p")
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
    .expect("addresses to resolve")
    .into_iter()
    .flatten()
    .map(|a| sloppy::node::AddrId::with_addr(a).into())
    .collect::<Vec<_>>();

    let mut dht: Dht = Dht::with_config(
        sloppy::Config {
            local_id: sloppy::NodeId::rand().unwrap(),
            client_version: Some(serde_bytes::ByteBuf::from("ab12")),
            default_query_timeout: Duration::from_secs(30),
            is_read_only_node: true,
            max_node_count_per_bucket: 10,
        },
        &bootstrap_addrs[..],
    )
    .expect("dht to bootstrap successfully");
    let dht_token = Token(0);

    let mut poll = Poll::new()?;
    poll.registry().register(
        &mut socket,
        dht_token,
        Interest::READABLE | Interest::WRITABLE,
    )?;

    let mut events = Events::with_capacity(1024);

    let mut buf = [0; 65535];
    let mut out = [0; 65535];

    // let stdout = io::stdout();
    // let mut buf_writer = BufWriter::new(stdout.lock());

    'event: loop {
        let timeout: Option<Duration> = dht.timeout();

        poll.poll(&mut events, timeout)?;

        'recv: loop {
            if events.is_empty() {
                debug!("Timed out");
                match dht.on_timeout() {
                    Ok(()) => {}
                    Err(e) => error!("on_timeout error: {:?}", e),
                };
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

            match dht.on_recv(filled_buf, src_addr) {
                Ok(()) => {}
                Err(e) => {
                    error!("on_recv error: {:?}", e);
                }
            }

            'read: loop {
                if let Some(inbound_msg) = dht.read() {
                    // debug!("Read message: {:?}", inbound_msg);
                    match inbound_msg.msg() {
                        sloppy::msg_buffer::Msg::Query(msg) => match msg.method_name_str() {
                            Some(ping::METHOD_PING) => {
                                let ping_resp =
                                    ping::PingRespValues::with_id(dht.config().local_id);
                                if let Some(tx_id) = msg.tx_id() {
                                    match dht.write_resp(
                                        tx_id,
                                        Some(ping_resp),
                                        inbound_msg.addr_id(),
                                    ) {
                                        Ok(()) => {}
                                        Err(e) => error!("ping write_resp error: {:?}", e),
                                    };
                                }
                            }
                            Some(method_name @ _) => {
                                if let Some(tx_id) = msg.tx_id() {
                                    let error =
                                        sloppy::krpc::error::ErrorValues::with_code_and_desc(
                                            ErrorCode::MethodUnknown,
                                            method_name.to_string(),
                                        );
                                    match dht.write_err(tx_id, error, inbound_msg.addr_id()) {
                                        Ok(()) => {}
                                        Err(e) => error!("write_err error: {:?}", e),
                                    };
                                }
                            }
                            None => {
                                if let Some(tx_id) = msg.tx_id() {
                                    let error =
                                        sloppy::krpc::error::ErrorValues::with_code_and_desc(
                                            ErrorCode::ProtocolError,
                                            String::from("method name not listed"),
                                        );
                                    match dht.write_err(tx_id, error, inbound_msg.addr_id()) {
                                        Ok(()) => {}
                                        Err(e) => error!("write_err error: {:?}", e),
                                    };
                                }
                            }
                        },
                        sloppy::msg_buffer::Msg::Resp(_)
                        | sloppy::msg_buffer::Msg::Error(_)
                        | sloppy::msg_buffer::Msg::Timeout => {}
                    }
                } else {
                    break 'read;
                }
            }

            debug!("Sending after read");

            match send_packets(&mut dht, &socket, &mut out) {
                Ok(break_event) => {
                    if break_event {
                        break 'event;
                    }
                }
                Err(e) => {
                    error!("send_packets error: {:?}", e);
                    return Err(e);
                }
            }
        }

        debug!("Sending after recv");

        match send_packets(&mut dht, &socket, &mut out) {
            Ok(break_event) => {
                if break_event {
                    break 'event;
                }
            }
            Err(e) => {
                error!("send_packets error: {:?}", e);
                return Err(e);
            }
        }
    }

    Ok(())
}

fn send_packets(dht: &mut Dht, socket: &mio::net::UdpSocket, out: &mut [u8]) -> io::Result<bool> {
    loop {
        match dht.send_to(out) {
            Ok(v) => {
                if let Some(send_info) = v {
                    if send_info.len == 0 {
                        return Ok(false);
                    }

                    match socket.send_to(&out[..send_info.len], send_info.addr) {
                        Ok(v) => v,
                        Err(e) => {
                            if e.kind() == io::ErrorKind::WouldBlock {
                                return Ok(false);
                            }

                            error!("send_to io error: {}", e);
                            continue;
                        }
                    };
                } else {
                    return Ok(false);
                }
            }
            Err(e) => {
                error!("send_to error: {:?}", e);
                return Ok(true);
            }
        };
    }
}

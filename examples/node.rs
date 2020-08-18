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

use std::io::{self, BufWriter, Write};
use std::net;
use std::time::Duration;

use clap::{App, Arg};

use mio::{Events, Interest, Poll, Token};

use bt_dht::{
    krpc::{ping, Kind, Msg, QueryMsg},
    Dht,
};

fn main() -> io::Result<()> {
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

    let ip = matches.value_of("ip").unwrap_or("127.0.0.1");
    let port = matches.value_of("port").unwrap_or("6532");
    eprintln!("Listening on: {}:{}", ip, port);

    let local_addr: net::SocketAddr = format!("{}:{}", ip, port).parse().unwrap();
    let mut socket = mio::net::UdpSocket::bind(local_addr)?;

    let mut dht: Dht = Dht::new_with_config(bt_dht::Config {
        id: bt_dht::node::Id::rand().unwrap(),
        client_version: Some(serde_bytes::ByteBuf::from("ab12")),
        query_timeout: Duration::from_secs(60),
        is_read_only_node: true,
        max_node_count_per_bucket: 10,
    });
    dht.bootstrap(&[
        bt_dht::node::remote::RemoteNodeId {
            addr: bt_dht::addr::Addr::HostPort(String::from("router.magnets.im:6881")),
            node_id: None,
        },
        bt_dht::node::remote::RemoteNodeId {
            addr: bt_dht::addr::Addr::HostPort(String::from("router.bittorrent.com:6881")),
            node_id: None,
        },
        bt_dht::node::remote::RemoteNodeId {
            addr: bt_dht::addr::Addr::HostPort(String::from("dht.transmissionbt.com:6881")),
            node_id: None,
        },
    ]);
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

    let stdout = io::stdout();
    // let mut buf_writer = BufWriter::new(stdout.lock());

    'event: loop {
        let timeout: Option<Duration> = dht.timeout();

        poll.poll(&mut events, timeout)?;

        'recv: loop {
            if events.is_empty() {
                dht.on_timeout();
                break 'recv;
            }

            let (bytes_read, src_addr) = match socket.recv_from(&mut buf) {
                Ok(v) => v,
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        break 'recv;
                    }

                    panic!("io error: {}", e);
                }
            };

            let filled_buf = &buf[..bytes_read];

            // dbg!("Received:");
            //
            // dbg!(filled_buf);

            match dht.on_recv(filled_buf, src_addr) {
                Ok(()) => {}
                Err(e) => {}
            }

            'read: loop {
                if let Some(inbound_msg) = dht.read() {
                    if let Some(msg) = &inbound_msg.msg {
                        match msg.kind() {
                            Some(Kind::Query) => {
                                match msg.method_name_str() {
                                    Some(ping::METHOD_PING) => {
                                        // write back
                                        let ping_resp =
                                            ping::PingRespValues::new_with_id(dht.config().id);
                                        if let Some(tx_id) = msg.transaction_id() {
                                            match dht.write_resp(
                                                tx_id,
                                                Some(ping_resp.into()),
                                                inbound_msg.return_remote_id(),
                                            ) {
                                                Ok((socket_addr)) => {}
                                                Err(_) => panic!(),
                                            };
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            _ => {}
                        }
                        // dbg!("Read:");
                        // dbg!(inbound_msg);
                    }
                }

                if bytes_read == 0 {
                    break 'read;
                }
            }

            // dbg!("Sending after recv:");

            match send_packets(&mut dht, &socket, &mut out) {
                Ok(break_event) => {
                    if break_event {
                        break 'event;
                    }
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        // dht.on_recv_complete();

        // dbg!("Sending data:");

        match send_packets(&mut dht, &socket, &mut out) {
            Ok(break_event) => {
                if break_event {
                    break 'event;
                }
            }
            Err(e) => {
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

                            panic!("io error: {}", e);
                        }
                    };
                } else {
                    return Ok(true);
                }
            }
            Err(_) => return Ok(true),
        };
    }
}

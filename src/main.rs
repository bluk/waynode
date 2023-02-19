// Copyright 2022 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Sloppy is a distributed hash table node.

#![warn(
    rust_2018_idioms,
    missing_docs,
    missing_debug_implementations,
    unused_lifetimes,
    unused_qualifications
)]

use clap::{Arg, Command};
use cloudburst::dht::node::{Id, LocalId};
use std::{
    io,
    net::{SocketAddr, SocketAddrV4},
    time::Instant,
};
use tokio::{
    net::UdpSocket,
    signal,
    sync::{mpsc, oneshot},
};
use tracing::info;

mod dht;
mod http;

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
                .required(false)
                .num_args(1)
                .help("The IP address to bind to for the DHT service"),
        )
        .arg(
            Arg::new("dht-port")
                .long("dht-port")
                .value_name("DHT_PORT")
                .value_parser(clap::value_parser!(u16))
                .required(false)
                .num_args(1)
                .help("The port to bind to for the DHT service"),
        )
        .arg(
            Arg::new("http-ip")
                .long("http-ip-address")
                .value_name("HTTP_IP")
                .required(false)
                .num_args(1)
                .help("The IP address to bind to for the HTTP service"),
        )
        .arg(
            Arg::new("http-port")
                .long("http-port")
                .value_name("HTTP_PORT")
                .value_parser(clap::value_parser!(u16))
                .required(false)
                .num_args(1)
                .help("The port to bind to for the HTTP service"),
        )
        .get_matches();

    let dht_ip: String = matches
        .get_one("dht-ip")
        .unwrap_or(&"0.0.0.0".to_string())
        .to_string();
    let dht_port: u16 = *matches.get_one("dht-port").unwrap_or(&6881);
    let dht_bind_socket = format!("{dht_ip}:{dht_port}").parse().unwrap();

    let http_ip: String = matches
        .get_one("http-ip")
        .unwrap_or(&"127.0.0.1".to_string())
        .to_string();
    let http_port: u16 = *matches.get_one("http-port").unwrap_or(&8080);
    let http_bind_socket = format!("{http_ip}:{http_port}").parse().unwrap();

    Args {
        dht_bind_socket,
        http_bind_socket,
    }
}

fn get_config(local_id: LocalId) -> dht::Config {
    let mut config = dht::Config::new(local_id);
    config.set_client_version(Some("ab12".into()));
    config.set_is_read_only_node(true);
    config
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("signal received, starting graceful shutdown");
}

#[tokio::main]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt::init();

    let args = get_args();

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    let socket = UdpSocket::bind(args.dht_bind_socket).await?;
    let local_id = Id::rand(&mut rand::thread_rng()).unwrap();
    info!(dht_bind_socket = %args.dht_bind_socket, %local_id, "listening...");

    let config = get_config(LocalId::from(local_id));
    let node: Node<SocketAddrV4> = dht::Node::new(
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

    let (dht_cmd_tx, dht_cmd_rx) = mpsc::channel(32);
    let (dht_completion_tx, dht_completion_rx) = oneshot::channel();
    let dht_handle = tokio::spawn(dht::dht_task(socket, node, dht_cmd_rx, dht_completion_tx));

    let (http_completion_tx, http_completion_rx) = oneshot::channel();
    let (http_shutdown_tx, http_shutdown_rx) = oneshot::channel();
    let http_handle = tokio::spawn(http::http_task(
        args.http_bind_socket,
        dht_cmd_tx.clone(),
        http_shutdown_rx,
        http_completion_tx,
    ));

    tokio::select! {
        _ = dht_completion_rx => {
        }
        _ = http_completion_rx => {
        }
        _ = &mut shutdown => {
        }
    }

    drop(dht_cmd_tx);
    http_shutdown_tx.send(()).unwrap();

    dht_handle
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;
    http_handle
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
}

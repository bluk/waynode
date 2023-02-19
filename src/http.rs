use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_derive::Serialize;
use std::{fmt, io, net::SocketAddr, time::Duration};

use crate::dht::{self, Cmd};

#[derive(Debug, Serialize)]
struct Config {
    local_id: String,
    client_version: Option<String>,
    default_query_timeout: Duration,
    is_read_only_node: bool,
    is_response_queried_node_id_strictly_checked: bool,
    routing_table_next_response_interval: Duration,
    routing_table_next_query_interval: Duration,
}

impl From<dht::Config> for Config {
    fn from(value: dht::Config) -> Self {
        struct ClientVersion<'a> {
            version: &'a [u8],
        }

        impl<'a> fmt::Display for ClientVersion<'a> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                for b in self.version {
                    write!(f, "{b:02X}")?;
                }
                Ok(())
            }
        }

        Self {
            local_id: format!("{}", value.local_id().0),
            client_version: value.client_version.map(|version| {
                let v = ClientVersion { version: &version };
                v.to_string()
            }),
            default_query_timeout: value.default_query_timeout,
            is_read_only_node: value.is_read_only_node,
            is_response_queried_node_id_strictly_checked: value
                .is_response_queried_node_id_strictly_checked,
            routing_table_next_response_interval: value.routing_table_next_response_interval,
            routing_table_next_query_interval: value.routing_table_next_query_interval,
        }
    }
}

async fn get_config(cmd_tx: tokio::sync::mpsc::Sender<Cmd>) -> Response {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let _ = cmd_tx.send(Cmd::GetConfig(tx)).await;

    match rx.await {
        Ok(config) => {
            let config = Config::from(config);
            Json(config).into_response()
        }
        Err(_e) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

pub(super) async fn http_task(
    socket_addr: SocketAddr,
    cmd_tx: tokio::sync::mpsc::Sender<Cmd>,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    completion_tx: tokio::sync::oneshot::Sender<()>,
) -> io::Result<()> {
    use axum::{routing::get, Router};

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route(
            "/config",
            get(|| async move { get_config(cmd_tx.clone()).await }),
        );

    let result = axum::Server::bind(&socket_addr)
        .serve(app.into_make_service())
        .with_graceful_shutdown(async {
            shutdown_rx.await.unwrap();
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e));

    let _ = completion_tx.send(());

    result
}

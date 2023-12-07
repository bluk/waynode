use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use hyper::{body::Incoming, Request};
use hyper_util::rt::TokioIo;
use serde_derive::Serialize;
use std::{fmt, io, net::SocketAddr, time::Duration};
use tokio::{net::TcpListener, sync::watch};
use tower::Service;
use tower_http::{timeout::TimeoutLayer, trace::TraceLayer};

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
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    completion_tx: tokio::sync::oneshot::Sender<()>,
) -> io::Result<()> {
    use axum::{routing::get, Router};

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route(
            "/config",
            get(|| async move { get_config(cmd_tx.clone()).await }),
        )
        .layer((
            TraceLayer::new_for_http(),
            TimeoutLayer::new(Duration::from_secs(10)),
        ));

    let listener = TcpListener::bind(socket_addr).await.unwrap();

    let (close_tx, close_rx) = watch::channel(());

    loop {
        let (socket, _) = tokio::select! {
            result = listener.accept() => {
                result.unwrap()
            }
            _ = &mut shutdown_rx => {
                break;
            }
        };

        let service = app.clone();

        let mut close_rx = close_rx.clone();

        tokio::spawn(async move {
            let socket = TokioIo::new(socket);

            let hyper_service = hyper::service::service_fn(move |request: Request<Incoming>| {
                service.clone().call(request)
            });

            let conn = hyper::server::conn::http1::Builder::new()
                .serve_connection(socket, hyper_service)
                .with_upgrades();

            let mut conn = std::pin::pin!(conn);

            loop {
                tokio::select! {
                    result = conn.as_mut() => {
                        if let Err(err) = result {
                            tracing::debug!("http connection failed: {err}");
                        }
                        break;
                    }
                    _ = close_rx.changed() => {
                        conn.as_mut().graceful_shutdown();
                    }
                }
            }

            drop(close_rx);
        });
    }

    close_tx.send(()).unwrap();

    drop(close_rx);
    drop(listener);

    close_tx.closed().await;

    let _ = completion_tx.send(());

    Ok(())
}

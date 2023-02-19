use std::{io, net::SocketAddr};

use axum::{http::StatusCode, response::IntoResponse, Json};

use crate::dht::Cmd;

async fn get_config(cmd_tx: tokio::sync::mpsc::Sender<Cmd>) -> impl IntoResponse {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let _ = cmd_tx.send(Cmd::GetConfig(tx)).await;
    let result = rx.await;

    match result {
        Ok(config) => Json(config).into_response(),
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

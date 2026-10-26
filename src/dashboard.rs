//! A local live view of the collector: `/` serves a single-page dashboard and
//! `/status` the engine's latest [`Status`] as JSON. Plain HTTP/1.1, one
//! request per connection; meant for localhost, not the internet.

use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::engine::Status;

const PAGE: &str = include_str!("dashboard.html");

pub async fn serve(
    addr: SocketAddr,
    status: watch::Receiver<Status>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("dashboard on http://{}", listener.local_addr()?);
    loop {
        let (stream, _) = tokio::select! {
            accepted = listener.accept() => accepted?,
            _ = cancel.cancelled() => return Ok(()),
        };
        let status = status.clone();
        tokio::spawn(async move {
            if let Err(e) = respond(stream, &status).await {
                tracing::debug!(error = %e, "dashboard request failed");
            }
        });
    }
}

async fn respond(mut stream: TcpStream, status: &watch::Receiver<Status>) -> anyhow::Result<()> {
    let mut head = Vec::new();
    let mut buf = [0u8; 1024];
    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = stream.read(&mut buf).await?;
        anyhow::ensure!(n > 0 && head.len() < 8192, "bad request");
        head.extend_from_slice(&buf[..n]);
    }
    let path = std::str::from_utf8(&head)?
        .split_whitespace()
        .nth(1)
        .unwrap_or("/");
    let (code, kind, body) = match path {
        "/" => (
            "200 OK",
            "text/html; charset=utf-8",
            PAGE.as_bytes().to_vec(),
        ),
        "/status" => (
            "200 OK",
            "application/json",
            serde_json::to_vec(&*status.borrow())?,
        ),
        _ => ("404 Not Found", "text/plain", b"not found".to_vec()),
    };
    let header = format!(
        "HTTP/1.1 {code}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\n\
         Cache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(&body).await?;
    Ok(())
}

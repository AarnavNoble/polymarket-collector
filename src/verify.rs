//! Fetches reference books from the exchange's REST API on the engine's
//! behalf, so slow HTTP never runs inside the engine loop.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::exchange::{Event, Exchange};

pub async fn run<E: Exchange>(
    exchange: Arc<E>,
    mut requests: mpsc::Receiver<Vec<String>>,
    results: mpsc::Sender<Vec<Event>>,
) {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("http client");
    while let Some(symbols) = requests.recv().await {
        match exchange.fetch_books(&http, &symbols).await {
            Ok(books) => {
                if results.send(books).await.is_err() {
                    return;
                }
            }
            Err(e) => tracing::warn!(error = %e, "reference book fetch failed"),
        }
    }
}

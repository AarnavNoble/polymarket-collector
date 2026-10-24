//! Runs the real manager, legs and engine against a local websocket server
//! that kills one leg mid-stream, and checks the output stays consistent.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use engineering_challenge::Side;
use engineering_challenge::book::Level;
use engineering_challenge::connection::{LegConfig, Pacer};
use engineering_challenge::engine::Engine;
use engineering_challenge::exchange::{Event, Exchange, Listing, Scope};
use engineering_challenge::manager::{self, ManagerConfig};
use engineering_challenge::writer::{Record, Sink};

/// A toy wire format: `S <symbol> <qty>` is a snapshot with one bid at price
/// 1, `L <symbol> <qty>` sets that bid.
struct Mock {
    url: String,
}

impl Exchange for Mock {
    fn ws_url(&self) -> &str {
        &self.url
    }
    fn symbols_per_connection(&self, _activity: f64) -> usize {
        10
    }
    fn max_batch(&self) -> usize {
        10
    }
    fn subscribe(&self, symbols: &[String], _first: bool) -> String {
        format!("sub {}", symbols.join(","))
    }
    fn unsubscribe(&self, symbols: &[String]) -> String {
        format!("unsub {}", symbols.join(","))
    }
    fn heartbeat(&self) -> Option<&'static str> {
        None
    }
    fn decode(&self, frame: &str, out: &mut Vec<Event>) -> anyhow::Result<()> {
        let mut parts = frame.split(' ');
        let (kind, symbol, qty) = (
            parts.next(),
            parts.next().unwrap_or_default().to_owned(),
            parts.next(),
        );
        let qty: i64 = qty.unwrap_or("0").parse()?;
        let exchange_ts = Utc::now();
        match kind {
            Some("S") => out.push(Event::Snapshot {
                symbol,
                exchange_ts,
                bids: vec![Level { price: 1, qty }],
                asks: vec![],
            }),
            Some("L") => out.push(Event::Level {
                symbol,
                exchange_ts,
                side: Side::Buy,
                price: 1,
                qty,
                id: 0,
            }),
            _ => {}
        }
        Ok(())
    }
    async fn discover(
        &self,
        _: &reqwest::Client,
        _: Scope,
        out: &mpsc::UnboundedSender<Vec<Listing>>,
    ) -> anyhow::Result<()> {
        out.send(vec![Listing {
            symbol: "X".into(),
            activity: 0.0,
        }])?;
        Ok(())
    }
}

/// Connection 0 dies shortly after its snapshot; connection 1 survives and
/// later publishes an update; reconnections just hold the book.
async fn serve(listener: TcpListener) {
    let accepted = Arc::new(AtomicUsize::new(0));
    loop {
        let (tcp, _) = listener.accept().await.unwrap();
        let n = accepted.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(async move {
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let Some(Ok(_subscribe)) = ws.next().await else {
                return;
            };
            ws.send(Message::text("S X 5")).await.unwrap();
            match n {
                0 => {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    return; // dropped without a close frame, like a crash
                }
                1 => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    ws.send(Message::text("L X 8")).await.unwrap();
                }
                _ => {}
            }
            while let Some(Ok(_)) = ws.next().await {}
        });
    }
}

#[tokio::test]
async fn output_survives_a_leg_dying() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    tokio::spawn(serve(listener));

    let (out_tx, out) = crossbeam::channel::unbounded();
    let (engine_tx, engine_rx) = mpsc::channel(1024);
    let (req_tx, req_rx) = mpsc::unbounded_channel();
    let cancel = CancellationToken::new();
    let engine =
        tokio::spawn(Engine::new(2, Sink::from_sender(out_tx), req_tx).run(engine_rx, None));
    let manager = tokio::spawn(manager::run(
        Arc::new(Mock { url }),
        ManagerConfig {
            legs: 2,
            full_discovery: Duration::from_secs(3600),
            new_discovery: Duration::from_secs(3600),
            leg: Arc::new(LegConfig {
                stall_timeout: Duration::from_secs(30),
                heartbeat: Duration::from_secs(10),
                max_lifetime: Duration::from_secs(3600),
                pacer: Pacer::new(Duration::ZERO),
            }),
        },
        engine_tx,
        req_rx,
        cancel.clone(),
    ));

    tokio::time::sleep(Duration::from_millis(1500)).await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), manager)
        .await
        .expect("legs shut down")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), engine)
        .await
        .expect("engine drained")
        .unwrap();

    let records: Vec<(bool, i64)> = out
        .try_iter()
        .map(|r| match r {
            Record::Snapshot(s) => (true, s.bids[0].quantity),
            Record::Update(u) => (false, u.quantity),
        })
        .collect();
    // Whichever leg won the race, the output starts with the book and carries
    // the surviving leg's update exactly once (never duplicated by the
    // standby, never lost in the failover), ending at quantity 8.
    assert_eq!(records.first(), Some(&(true, 5)), "{records:?}");
    assert_eq!(
        records.iter().filter(|r| **r == (false, 8)).count(),
        1,
        "{records:?}"
    );
    assert_eq!(records.last().map(|r| r.1), Some(8), "{records:?}");
}

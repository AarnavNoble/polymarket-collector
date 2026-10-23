//! Latency experiment: opens several websockets subscribed to the same busy
//! Polymarket tokens and races them. Every `price_change` entry is keyed by
//! (asset, timestamp, hash), which is identical on every connection, so the
//! connection that delivers a key first "wins" and the others' lag behind it
//! needs no clock sync. Also reports per-connection setup times and the
//! exchange-timestamp-to-receive delay (which does depend on the local clock).
//!
//! Usage: latency [--conns 8] [--tokens 20] [--secs 120] [--pin a|b|split]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpSocket, lookup_host};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

const HOST: &str = "ws-subscriptions-clob.polymarket.com";
const URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 8)]
    conns: usize,
    #[arg(long, default_value_t = 20)]
    tokens: usize,
    #[arg(long, default_value_t = 120)]
    secs: u64,
    /// Which resolved address to dial: the first, the second, alternate
    /// between them, or let each connection resolve on its own.
    #[arg(long, default_value = "split")]
    pin: String,
}

struct Setup {
    addr: SocketAddr,
    local_port: u16,
    tcp: Duration,
    handshake: Duration,
}

/// (asset, exchange timestamp ms, hash): the same logical update on every socket.
type Key = (String, i64, String);

async fn hot_tokens(n: usize) -> anyhow::Result<Vec<String>> {
    let url = format!(
        "https://gamma-api.polymarket.com/markets/keyset?closed=false&limit={n}&version=v1&version=v2&order=volume24hr&ascending=false"
    );
    let page: serde_json::Value = reqwest::get(url).await?.json().await?;
    let mut out = Vec::new();
    for m in page["markets"].as_array().into_iter().flatten() {
        if let Some(ids) = m["clobTokenIds"].as_str() {
            let ids: Vec<String> = serde_json::from_str(ids)?;
            out.extend(ids.into_iter().next());
        }
    }
    Ok(out)
}

async fn connect(
    addr: SocketAddr,
) -> anyhow::Result<(
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    Setup,
)> {
    let t0 = Instant::now();
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_nodelay(true)?;
    let stream = socket.connect(addr).await?;
    let tcp = t0.elapsed();
    let local_port = stream.local_addr()?.port();
    let (ws, _) = tokio_tungstenite::client_async_tls_with_config(URL, stream, None, None).await?;
    Ok((
        ws,
        Setup {
            addr,
            local_port,
            tcp,
            handshake: t0.elapsed() - tcp,
        },
    ))
}

async fn run_conn(
    i: usize,
    addr: SocketAddr,
    tokens: Vec<String>,
    deadline: Instant,
    tx: mpsc::UnboundedSender<(usize, Instant, i64, Vec<Key>)>,
) -> anyhow::Result<Setup> {
    let (ws, setup) = tokio::time::timeout(Duration::from_secs(15), connect(addr))
        .await
        .map_err(|_| anyhow::anyhow!("connection {i}: connect timed out"))??;
    let (mut sink, mut stream) = ws.split();
    sink.send(Message::text(
        serde_json::json!({ "assets_ids": tokens, "type": "market" }).to_string(),
    ))
    .await?;
    let mut ping = tokio::time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline.into()) => break,
            _ = ping.tick() => sink.send(Message::text("PING")).await?,
            frame = stream.next() => {
                let now = Instant::now();
                let wall_ms = chrono::Utc::now().timestamp_millis();
                let Some(frame) = frame else { anyhow::bail!("connection {i} closed") };
                let Message::Text(text) = frame? else { continue };
                let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) else { continue };
                if v["event_type"] != "price_change" {
                    continue;
                }
                let ts: i64 = v["timestamp"].as_str().and_then(|t| t.parse().ok()).unwrap_or(0);
                let keys = v["price_changes"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|c| (c["asset_id"].as_str().unwrap_or("").to_owned(), ts, c["hash"].as_str().unwrap_or("").to_owned()))
                    .collect();
                let _ = tx.send((i, now, wall_ms - ts, keys));
            }
        }
    }
    let _ = tokio::time::timeout(Duration::from_secs(2), sink.send(Message::Close(None))).await;
    Ok(setup)
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let tokens = hot_tokens(args.tokens).await?;
    let addrs: Vec<SocketAddr> = lookup_host((HOST, 443))
        .await?
        .filter(|a| a.is_ipv4())
        .collect();
    anyhow::ensure!(!addrs.is_empty(), "no IPv4 address for {HOST}");
    eprintln!("{} tokens, resolved {:?}", tokens.len(), addrs);

    let deadline = Instant::now() + Duration::from_secs(args.secs);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut handles = Vec::new();
    for i in 0..args.conns {
        let addr = match args.pin.as_str() {
            "a" => addrs[0],
            "b" => addrs[addrs.len().min(2) - 1],
            "split" => addrs[i % addrs.len()],
            _ => lookup_host((HOST, 443))
                .await?
                .find(|a| a.is_ipv4())
                .unwrap(),
        };
        handles.push(tokio::spawn(run_conn(
            i,
            addr,
            tokens.clone(),
            deadline,
            tx.clone(),
        )));
    }
    drop(tx);

    // first arrival per key, and every connection's arrival
    let mut arrivals: HashMap<Key, Vec<(usize, Instant)>> = HashMap::new();
    let mut exch_delay: Vec<Vec<f64>> = vec![Vec::new(); args.conns];
    while let Some((i, at, delay_ms, keys)) = rx.recv().await {
        exch_delay[i].push(delay_ms as f64);
        for k in keys {
            arrivals.entry(k).or_default().push((i, at));
        }
    }

    let mut setups = Vec::new();
    for (i, h) in handles.into_iter().enumerate() {
        match h.await? {
            Ok(s) => setups.push(Some(s)),
            Err(e) => {
                eprintln!("connection {i}: {e}");
                setups.push(None)
            }
        }
    }

    // Only keys every connection saw make a fair race.
    let mut wins = vec![0usize; args.conns];
    let mut lags: Vec<Vec<f64>> = vec![Vec::new(); args.conns];
    let complete: Vec<_> = arrivals
        .values()
        .filter(|a| a.len() == args.conns)
        .collect();
    for a in &complete {
        let first = a.iter().map(|(_, t)| *t).min().unwrap();
        let winner = a.iter().min_by_key(|(_, t)| *t).unwrap().0;
        wins[winner] += 1;
        for (i, t) in a.iter() {
            lags[*i].push((*t - first).as_secs_f64() * 1e3);
        }
    }

    println!(
        "keys seen: {}, seen on every connection: {}",
        arrivals.len(),
        complete.len()
    );
    println!(
        "| conn | address | src port | TCP ms | TLS+upgrade ms | wins | lag p50 ms | lag p99 ms | exch→recv p50 ms |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");
    for i in 0..args.conns {
        let Some(s) = &setups[i] else { continue };
        lags[i].sort_by(f64::total_cmp);
        exch_delay[i].sort_by(f64::total_cmp);
        println!(
            "| {i} | {} | {} | {:.1} | {:.1} | {:.1}% | {:.2} | {:.2} | {:.0} |",
            s.addr.ip(),
            s.local_port,
            s.tcp.as_secs_f64() * 1e3,
            s.handshake.as_secs_f64() * 1e3,
            100.0 * wins[i] as f64 / complete.len().max(1) as f64,
            pct(&lags[i], 0.5),
            pct(&lags[i], 0.99),
            pct(&exch_delay[i], 0.5),
        );
    }
    Ok(())
}

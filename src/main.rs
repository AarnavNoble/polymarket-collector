use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use engineering_challenge::connection::{LegConfig, Pacer};
use engineering_challenge::engine::{Engine, VerifyLink};
use engineering_challenge::exchange::{polymarket::Polymarket, vpx::Vpx};
use engineering_challenge::manager::{self, ManagerConfig};
use engineering_challenge::poll::{self, PollConfig};
use engineering_challenge::{dashboard, verify, writer};

/// Pause between full discovery passes (a pass itself takes about 20 min).
const FULL_DISCOVERY: Duration = Duration::from_secs(300);
/// Pause between polls for newly listed markets.
const NEW_DISCOVERY: Duration = Duration::from_secs(30);
/// Silence on a connection, pings included, before it counts as dead.
const STALL_TIMEOUT: Duration = Duration::from_secs(30);
/// Approximate lifetime before a connection is recycled.
const RECYCLE_AFTER: Duration = Duration::from_secs(6 * 3600);
/// Spacing of connection attempts across all legs (4/s; see LATENCY.md).
const CONNECT_INTERVAL: Duration = Duration::from_millis(250);
/// Books per REST verification round, stalest first.
const VERIFY_BATCH: usize = 200;
/// VPX is polled over REST: pause between rounds over every market, and
/// between market list refreshes.
const VPX_POLL: Duration = Duration::from_secs(2);
const VPX_DISCOVERY: Duration = Duration::from_secs(60);

/// Collects order book data for every live market on an exchange.
#[derive(Parser)]
struct Args {
    #[arg(long, value_enum, default_value_t = Venue::Polymarket)]
    exchange: Venue,
    /// Output file for SBE-encoded snapshots and updates.
    #[arg(long, default_value = "/dev/stdout")]
    output: PathBuf,
    /// Redundant connections per shard; each carries the same symbols.
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u16).range(1..))]
    legs: u16,
    /// Seconds between REST spot-checks of our books (0 disables).
    #[arg(long, default_value_t = 10)]
    verify_secs: u64,
    /// Cap symbols per discovery pass (for local testing).
    #[arg(long)]
    max_symbols: Option<usize>,
    /// Serve a live dashboard at this address, e.g. 127.0.0.1:8080.
    #[arg(long)]
    dashboard: Option<SocketAddr>,
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Venue {
    /// Full-depth websocket feed, redundant legs, REST verification.
    Polymarket,
    /// Vancouver Prediction Exchange: REST polling (needs VPX_API_KEY).
    Vpx,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();

    let vpx = match args.exchange {
        Venue::Vpx => Some(Arc::new(Vpx::from_env()?)),
        Venue::Polymarket => None,
    };
    // Polling is a single source, so redundancy and verification don't apply.
    let legs = if vpx.is_some() {
        1
    } else {
        usize::from(args.legs)
    };
    let exchange = Arc::new(Polymarket {
        max_symbols: args.max_symbols,
        ..Default::default()
    });
    let (sink, writer) = writer::spawn(args.output)?;
    let (engine_tx, engine_rx) = mpsc::channel(65_536);
    let (req_tx, req_rx) = mpsc::unbounded_channel();
    let cancel = CancellationToken::new();

    let verify_link = (vpx.is_none() && args.verify_secs > 0).then(|| {
        let (requests, verify_rx) = mpsc::channel(1);
        let (verify_tx, results) = mpsc::channel(1);
        tokio::spawn(verify::run(exchange.clone(), verify_rx, verify_tx));
        VerifyLink {
            interval: Duration::from_secs(args.verify_secs),
            batch: VERIFY_BATCH,
            requests,
            results,
        }
    });
    let mut engine = Engine::new(legs, sink, req_tx);
    if let Some(addr) = args.dashboard {
        let (status_tx, status_rx) = tokio::sync::watch::channel(Default::default());
        engine = engine.with_status(status_tx);
        let cancel = cancel.clone();
        tokio::spawn(async move {
            if let Err(e) = dashboard::serve(addr, status_rx, cancel).await {
                tracing::error!(error = %e, "dashboard stopped");
            }
        });
    }
    let engine = tokio::spawn(engine.run(engine_rx, verify_link));
    let source = if let Some(vpx) = vpx {
        let cfg = PollConfig {
            interval: VPX_POLL,
            discovery: VPX_DISCOVERY,
            concurrency: 8,
        };
        let cancel = cancel.clone();
        tokio::spawn(async move {
            if let Err(e) = poll::run(vpx, cfg, engine_tx, req_rx, cancel).await {
                tracing::error!(error = %e, "polling stopped");
            }
        })
    } else {
        tokio::spawn(manager::run(
            exchange,
            ManagerConfig {
                legs,
                full_discovery: FULL_DISCOVERY,
                new_discovery: NEW_DISCOVERY,
                leg: Arc::new(LegConfig {
                    stall_timeout: STALL_TIMEOUT,
                    heartbeat: Duration::from_secs(5),
                    max_lifetime: RECYCLE_AFTER,
                    pacer: Pacer::new(CONNECT_INTERVAL),
                }),
            },
            engine_tx,
            req_rx,
            cancel.clone(),
        ))
    };

    shutdown_signal().await;
    tracing::info!("shutting down; press Ctrl-C again to force exit");
    cancel.cancel();
    tokio::spawn(async {
        shutdown_signal().await;
        std::process::exit(130);
    });

    // Order matters: legs close (the source waits for them), then the engine
    // drains everything they sent, then the writer flushes and closes.
    // The writer is joined even if a task panicked, so its buffer is flushed.
    let tasks = source.await.and(engine.await);
    let records = tokio::task::spawn_blocking(move || writer.join()).await??;
    tracing::info!(records, "output flushed and closed");
    Ok(tasks?)
}

async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("installing SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

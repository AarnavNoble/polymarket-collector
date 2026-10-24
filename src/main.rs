use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use engineering_challenge::connection::{LegConfig, Pacer};
use engineering_challenge::engine::{Engine, VerifyLink};
use engineering_challenge::exchange::polymarket::Polymarket;
use engineering_challenge::manager::{self, ManagerConfig};
use engineering_challenge::{verify, writer};

/// Collects full-depth order book data for every live Polymarket market.
#[derive(Parser)]
struct Args {
    /// Output file for SBE-encoded snapshots and updates.
    #[arg(long, default_value = "/dev/stdout")]
    output: PathBuf,
    /// Redundant connections per shard; each carries the same symbols.
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u16).range(1..))]
    legs: u16,
    /// Seconds between full discovery passes (a pass itself takes minutes:
    /// Polymarket pages ~280k markets through sequential cursors).
    #[arg(long, default_value_t = 300)]
    full_discovery_secs: u64,
    /// Seconds between polls for newly listed markets.
    #[arg(long, default_value_t = 30)]
    new_discovery_secs: u64,
    /// Seconds without any frame before a connection is considered stalled.
    #[arg(long, default_value_t = 30)]
    stall_secs: u64,
    /// Approximate hours before a connection is recycled.
    #[arg(long, default_value_t = 6.0, value_parser = positive)]
    recycle_hours: f64,
    /// Maximum new connection attempts per second, across all legs.
    #[arg(long, default_value_t = 4.0, value_parser = positive)]
    connects_per_sec: f64,
    /// Seconds between REST spot-checks of our books (0 disables).
    #[arg(long, default_value_t = 10)]
    verify_secs: u64,
    /// Books per spot-check round, stalest first.
    #[arg(long, default_value_t = 200)]
    verify_batch: usize,
    /// Cap symbols per discovery pass (for local testing).
    #[arg(long)]
    max_symbols: Option<usize>,
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

    let exchange = Arc::new(Polymarket {
        max_symbols: args.max_symbols,
        ..Default::default()
    });
    let (sink, writer) = writer::spawn(args.output)?;
    let (engine_tx, engine_rx) = mpsc::channel(65_536);
    let (req_tx, req_rx) = mpsc::unbounded_channel();
    let cancel = CancellationToken::new();

    let verify_link = (args.verify_secs > 0).then(|| {
        let (requests, verify_rx) = mpsc::channel(1);
        let (verify_tx, results) = mpsc::channel(1);
        tokio::spawn(verify::run(exchange.clone(), verify_rx, verify_tx));
        VerifyLink {
            interval: Duration::from_secs(args.verify_secs),
            batch: args.verify_batch,
            requests,
            results,
        }
    });
    let engine =
        tokio::spawn(Engine::new(usize::from(args.legs), sink, req_tx).run(engine_rx, verify_link));
    let manager = tokio::spawn(manager::run(
        exchange,
        ManagerConfig {
            legs: usize::from(args.legs),
            full_discovery: Duration::from_secs(args.full_discovery_secs),
            new_discovery: Duration::from_secs(args.new_discovery_secs),
            leg: Arc::new(LegConfig {
                stall_timeout: Duration::from_secs(args.stall_secs),
                heartbeat: Duration::from_secs(5),
                max_lifetime: Duration::from_secs_f64(args.recycle_hours * 3600.0),
                pacer: Pacer::new(Duration::from_secs_f64(1.0 / args.connects_per_sec)),
            }),
        },
        engine_tx,
        req_rx,
        cancel.clone(),
    ));

    shutdown_signal().await;
    tracing::info!("shutting down; press Ctrl-C again to force exit");
    cancel.cancel();
    tokio::spawn(async {
        shutdown_signal().await;
        std::process::exit(130);
    });

    // Order matters: legs close (manager waits for them), then the engine
    // drains everything they sent, then the writer flushes and closes.
    // The writer is joined even if a task panicked, so its buffer is flushed.
    let tasks = manager.await.and(engine.await);
    let records = tokio::task::spawn_blocking(move || writer.join()).await??;
    tracing::info!(records, "output flushed and closed");
    Ok(tasks?)
}

fn positive(s: &str) -> Result<f64, String> {
    match s.parse::<f64>() {
        Ok(v) if v > 0.0 && v.is_finite() => Ok(v),
        _ => Err(format!("{s:?} is not a positive number")),
    }
}

async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("installing SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

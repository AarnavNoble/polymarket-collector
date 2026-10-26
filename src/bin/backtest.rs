//! Backtests the market-making strategy in `backtest.rs` on every market in a
//! collector output file, under both fill models, and reports P&L, volume,
//! inventory and markouts (how the mid moved after each fill).
//!
//! Usage: backtest <file> [--latency-ms 100] [--size 10] [--max-position 100]

use std::path::PathBuf;

use clap::Parser;
use rustc_hash::FxHashMap;
use serde::Serialize;

use engineering_challenge::backtest::{FillModel, Params, Sim};
use engineering_challenge::replay::Replay;

const SHARE: f64 = 1e6;
const HORIZONS_S: [i64; 3] = [1, 10, 60];

#[derive(Parser)]
struct Args {
    file: PathBuf,
    /// Order-entry latency.
    #[arg(long, default_value_t = 100)]
    latency_ms: i64,
    /// Shares per quote.
    #[arg(long, default_value_t = 10)]
    size: i64,
    /// Inventory limit, in shares.
    #[arg(long, default_value_t = 100)]
    max_position: i64,
    /// Markets to list per model, by P&L.
    #[arg(long, default_value_t = 5)]
    top: usize,
    /// Also write the results as JSON.
    #[arg(long)]
    json: Option<PathBuf>,
}

#[derive(Serialize)]
struct Summary {
    model: &'static str,
    markets_traded: usize,
    fills: u64,
    volume_usd: f64,
    pnl_usd: f64,
    /// P&L per dollar traded, in basis points.
    pnl_bps: f64,
    /// Mean markout per share, in cents, at each horizon in `HORIZONS_S`.
    markout_cents: Vec<(i64, f64)>,
    best: Vec<(String, f64)>,
    worst: Vec<(String, f64)>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let params = |model| Params {
        model,
        latency_us: args.latency_ms * 1_000,
        size: args.size * SHARE as i64,
        max_position: args.max_position * SHARE as i64,
        markouts_us: HORIZONS_S.map(|s| s * 1_000_000),
    };
    let models = [
        ("cross (lower bound)", FillModel::Cross),
        ("queue (upper bound)", FillModel::Queue),
    ];

    let mut replay = Replay::open(&args.file)?;
    let mut sims: FxHashMap<String, [Sim; 2]> = FxHashMap::default();
    while let Some(r) = replay.read()? {
        let Some(book) = replay.book(&r.symbol) else {
            continue;
        };
        let t = r.received_at.timestamp_micros();
        let pair = sims
            .entry(r.symbol)
            .or_insert_with(|| models.map(|(_, m)| Sim::new(params(m))));
        for sim in pair {
            sim.step(t, book);
        }
    }

    let summaries: Vec<_> = models
        .iter()
        .enumerate()
        .map(|(i, (name, _))| {
            let mut traded: Vec<(&String, &Sim)> = sims
                .iter()
                .map(|(s, pair)| (s, &pair[i]))
                .filter(|(_, sim)| sim.fills > 0)
                .collect();
            traded.sort_unstable_by(|a, b| b.1.pnl().total_cmp(&a.1.pnl()));
            let volume: f64 = traded.iter().map(|(_, s)| s.volume_usd).sum();
            let pnl: f64 = traded.iter().map(|(_, s)| s.pnl()).sum();
            let markout_cents = (0..HORIZONS_S.len())
                .map(|h| {
                    let (usd, qty) = traded.iter().fold((0.0, 0i64), |(u, q), (_, s)| {
                        (u + s.markout[h].0, q + s.markout[h].1)
                    });
                    (HORIZONS_S[h], 100.0 * usd / (qty.max(1) as f64 / SHARE))
                })
                .collect();
            let label = |v: &[(&String, &Sim)]| -> Vec<(String, f64)> {
                v.iter().map(|(s, sim)| ((*s).clone(), sim.pnl())).collect()
            };
            Summary {
                model: name,
                markets_traded: traded.len(),
                fills: traded.iter().map(|(_, s)| s.fills).sum(),
                volume_usd: volume,
                pnl_usd: pnl,
                pnl_bps: 1e4 * pnl / volume.max(f64::MIN_POSITIVE),
                markout_cents,
                best: label(&traded[..args.top.min(traded.len())]),
                worst: label(&traded[traded.len().saturating_sub(args.top)..]),
            }
        })
        .collect();

    println!(
        "Strategy: quote {} shares at the touch (one cent inside spreads over 2¢), \
         inventory limit {} shares, {} ms order latency.\n",
        args.size, args.max_position, args.latency_ms
    );
    println!(
        "| Fill model | Markets | Fills | Volume | P&L | P&L/volume | Markout 1s | 10s | 60s |"
    );
    println!("|---|--:|--:|--:|--:|--:|--:|--:|--:|");
    for s in &summaries {
        let m: Vec<String> = s
            .markout_cents
            .iter()
            .map(|(_, c)| format!("{c:+.2}¢"))
            .collect();
        println!(
            "| {} | {} | {} | ${:.0} | ${:+.2} | {:+.1} bps | {} |",
            s.model,
            s.markets_traded,
            s.fills,
            s.volume_usd,
            s.pnl_usd,
            s.pnl_bps,
            m.join(" | ")
        );
    }
    if let Some(path) = args.json {
        std::fs::write(path, serde_json::to_vec_pretty(&summaries)?)?;
    }
    Ok(())
}

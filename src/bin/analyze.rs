//! Market microstructure and feed-quality report for a collector output file.
//!
//! Replays every book and measures, per market: update rate, time-weighted
//! spread and touch depth, how often the mid moves, and how often a snapshot
//! corrected the replayed book. Across the feed it measures delivery latency
//! (local receive time minus exchange timestamp) and activity concentration.
//!
//! Usage: analyze <file> [--top N] [--json out.json] [--no-names]

use std::path::PathBuf;

use clap::Parser;
use rustc_hash::FxHashMap;
use serde::Serialize;

use engineering_challenge::book::OrderBook;
use engineering_challenge::exchange::polymarket;
use engineering_challenge::replay::{Kind, Replay};

#[derive(Parser)]
struct Args {
    file: PathBuf,
    /// Markets to list, busiest first.
    #[arg(long, default_value_t = 20)]
    top: usize,
    /// Also write the report as JSON.
    #[arg(long)]
    json: Option<PathBuf>,
    /// Skip looking up market questions on Polymarket.
    #[arg(long)]
    no_names: bool,
}

/// Top of book while two-sided.
#[derive(Clone, Copy)]
struct Top {
    spread: i64,
    mid: i64,
    /// Dollar notional resting at the best bid and best ask.
    depth: f64,
}

impl Top {
    fn of(book: &OrderBook) -> Option<Self> {
        let (bid, ask) = (book.best_bid()?, book.best_ask()?);
        Some(Self {
            spread: ask.price - bid.price,
            mid: (ask.price + bid.price) / 2,
            depth: (bid.price as f64 * bid.qty as f64 + ask.price as f64 * ask.qty as f64) / 1e12,
        })
    }
}

#[derive(Default)]
struct Market {
    updates: u64,
    corrections: u64,
    corrected_levels: u64,
    top: Option<Top>,
    /// Receive time (µs) of the last record; time-weighted stats start here.
    since: Option<i64>,
    observed_us: i64,
    two_sided_us: i64,
    spread_us: f64,
    depth_us: f64,
    mid_moves: u64,
    second: i64,
    in_second: u32,
    peak_per_second: u32,
}

impl Market {
    /// Accrues the state held since the last record up to `t`.
    fn accrue(&mut self, t: i64) {
        let dt = self.since.map_or(0, |s| (t - s).max(0));
        self.observed_us += dt;
        if let Some(top) = self.top {
            self.two_sided_us += dt;
            self.spread_us += top.spread as f64 * dt as f64;
            self.depth_us += top.depth * dt as f64;
        }
        self.since = Some(t);
    }

    fn observe(&mut self, book: &OrderBook) {
        let top = Top::of(book);
        if let (Some(old), Some(new)) = (self.top, top)
            && old.mid != new.mid
        {
            self.mid_moves += 1;
        }
        self.top = top;
    }
}

#[derive(Serialize)]
struct Row {
    symbol: String,
    name: Option<String>,
    updates: u64,
    updates_per_min: f64,
    peak_updates_per_sec: u32,
    /// Time-weighted, in cents.
    spread_cents: Option<f64>,
    two_sided_pct: f64,
    /// Time-weighted dollar notional at the best bid plus best ask.
    touch_depth_usd: Option<f64>,
    mid_moves: u64,
    corrections: u64,
}

#[derive(Serialize)]
struct Report {
    minutes: f64,
    markets: usize,
    active_markets: usize,
    updates: u64,
    updates_per_sec: f64,
    top_1pct_share: f64,
    snapshots_first: u64,
    snapshots_consistent: u64,
    snapshots_corrected: u64,
    corrected_levels: u64,
    orphan_updates: u64,
    /// Receive time minus exchange timestamp, in ms, at these percentiles.
    latency_ms: Vec<(f64, f64)>,
    median_spread_cents: Option<f64>,
    busiest: Vec<Row>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut replay = Replay::open(&args.file)?;
    let mut markets: FxHashMap<String, Market> = FxHashMap::default();
    let (mut first, mut last) = (i64::MAX, i64::MIN);
    let (mut updates, mut orphans) = (0u64, 0u64);
    let (mut snap_first, mut snap_same, mut snap_fixed) = (0u64, 0u64, 0u64);
    let mut latency_us = Vec::new();

    while let Some(r) = replay.read()? {
        let t = r.received_at.timestamp_micros();
        (first, last) = (first.min(t), last.max(t));
        let m = markets.entry(r.symbol.clone()).or_default();
        m.accrue(t);
        match r.kind {
            Kind::Snapshot { changed: None } => snap_first += 1,
            Kind::Snapshot { changed: Some(0) } => snap_same += 1,
            Kind::Snapshot { changed: Some(n) } => {
                snap_fixed += 1;
                m.corrections += 1;
                m.corrected_levels += n as u64;
            }
            Kind::Update { orphan, .. } => {
                updates += 1;
                orphans += u64::from(orphan);
                m.updates += 1;
                latency_us.push(t - r.exchange_ts.timestamp_micros());
                let second = t.div_euclid(1_000_000);
                if second != m.second {
                    (m.second, m.in_second) = (second, 0);
                }
                m.in_second += 1;
                m.peak_per_second = m.peak_per_second.max(m.in_second);
            }
        }
        if let Some(book) = replay.book(&r.symbol) {
            m.observe(book);
        }
    }
    anyhow::ensure!(first <= last, "no records in {}", args.file.display());
    for m in markets.values_mut() {
        m.accrue(last);
    }

    let minutes = (last - first) as f64 / 60e6;
    let mut ranked: Vec<_> = markets.iter().filter(|(_, m)| m.updates > 0).collect();
    ranked.sort_unstable_by_key(|(_, m)| std::cmp::Reverse(m.updates));
    let top_1pct: u64 = ranked
        .iter()
        .take(markets.len().div_ceil(100))
        .map(|(_, m)| m.updates)
        .sum();

    latency_us.sort_unstable();
    let latency_ms = [50.0, 90.0, 99.0, 99.9]
        .into_iter()
        .filter(|_| !latency_us.is_empty())
        .map(|p| {
            let i = ((p / 100.0 * latency_us.len() as f64) as usize).min(latency_us.len() - 1);
            (p, latency_us[i] as f64 / 1e3)
        })
        .collect();

    let spread =
        |m: &Market| (m.two_sided_us > 0).then(|| m.spread_us / m.two_sided_us as f64 / 1e4);
    let mut spreads: Vec<f64> = ranked.iter().filter_map(|(_, m)| spread(m)).collect();
    spreads.sort_unstable_by(f64::total_cmp);

    let busiest: Vec<_> = ranked.iter().take(args.top).collect();
    let names = if args.no_names {
        FxHashMap::default()
    } else {
        let symbols: Vec<String> = busiest.iter().map(|(s, _)| (*s).clone()).collect();
        polymarket::market_names(&reqwest::Client::new(), &symbols)
            .await
            .unwrap_or_else(|e| {
                eprintln!("market names unavailable: {e}");
                FxHashMap::default()
            })
    };

    let report = Report {
        minutes,
        markets: markets.len(),
        active_markets: ranked.len(),
        updates,
        updates_per_sec: updates as f64 / (minutes * 60.0),
        top_1pct_share: top_1pct as f64 / updates.max(1) as f64,
        snapshots_first: snap_first,
        snapshots_consistent: snap_same,
        snapshots_corrected: snap_fixed,
        corrected_levels: markets.values().map(|m| m.corrected_levels).sum(),
        orphan_updates: orphans,
        latency_ms,
        median_spread_cents: spreads.get(spreads.len() / 2).copied(),
        busiest: busiest
            .iter()
            .map(|(symbol, m)| Row {
                symbol: (*symbol).clone(),
                name: names.get(*symbol).cloned(),
                updates: m.updates,
                updates_per_min: m.updates as f64 / minutes,
                peak_updates_per_sec: m.peak_per_second,
                spread_cents: spread(m),
                two_sided_pct: 100.0 * m.two_sided_us as f64 / m.observed_us.max(1) as f64,
                touch_depth_usd: (m.two_sided_us > 0).then(|| m.depth_us / m.two_sided_us as f64),
                mid_moves: m.mid_moves,
                corrections: m.corrections,
            })
            .collect(),
    };
    print_markdown(&report);
    if let Some(path) = args.json {
        std::fs::write(path, serde_json::to_vec_pretty(&report)?)?;
    }
    Ok(())
}

fn print_markdown(r: &Report) {
    let opt = |v: Option<f64>, digits: usize| v.map_or("–".into(), |v| format!("{v:.digits$}"));
    println!("## Dataset\n");
    println!("| | |\n|---|---|");
    println!("| Duration | {:.1} min |", r.minutes);
    println!("| Markets with a book | {} |", r.markets);
    println!("| Markets that changed | {} |", r.active_markets);
    println!(
        "| Level updates | {} ({:.0}/s) |",
        r.updates, r.updates_per_sec
    );
    println!(
        "| Share of updates in the busiest 1% of markets | {:.1}% |",
        100.0 * r.top_1pct_share
    );
    println!(
        "| Median time-weighted spread, markets that changed | {}¢ |",
        opt(r.median_spread_cents, 1)
    );

    println!("\n## Feed quality\n");
    println!("| | |\n|---|---|");
    for (p, ms) in &r.latency_ms {
        println!("| Delivery latency p{p} (receive − exchange timestamp) | {ms:.0} ms |");
    }
    println!(
        "| Snapshots matching the replayed book | {} |",
        r.snapshots_consistent
    );
    println!(
        "| Snapshots correcting it | {} ({} levels) |",
        r.snapshots_corrected, r.corrected_levels
    );
    println!(
        "| Updates before their symbol's first snapshot | {} |",
        r.orphan_updates
    );

    println!("\n## Busiest markets\n");
    println!(
        "| Market | Updates/min | Peak/s | Spread | Two-sided | Touch depth | Mid moves | Corrections |"
    );
    println!("|---|--:|--:|--:|--:|--:|--:|--:|");
    for row in &r.busiest {
        let name = row
            .name
            .clone()
            .unwrap_or_else(|| format!("{}…", &row.symbol[..12.min(row.symbol.len())]));
        println!(
            "| {} | {:.0} | {} | {}¢ | {:.0}% | ${} | {} | {} |",
            name.replace('|', "/"),
            row.updates_per_min,
            row.peak_updates_per_sec,
            opt(row.spread_cents, 1),
            row.two_sided_pct,
            opt(row.touch_depth_usd, 0),
            row.mid_moves,
            row.corrections,
        );
    }
}

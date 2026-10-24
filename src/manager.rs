//! Subscription manager. Two discovery loops feed it listings: a periodic full
//! pass (the authoritative live set) and a frequent poll for newly listed
//! symbols. New symbols are subscribed as soon as their page arrives, packed
//! onto shards sized for their activity (each shard = `legs` redundant
//! connections carrying the same symbols). Symbols are unsubscribed once
//! absent from two consecutive full passes, and the engine's resync and
//! reconnect requests are routed to the right leg.

use std::sync::Arc;
use std::time::Duration;

use chrono::{TimeDelta, Utc};
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::connection::{self, Command, LegConfig};
use crate::engine::{EngineMsg, LegId, Request};
use crate::exchange::{Exchange, Listing, Scope};

pub struct ManagerConfig {
    pub legs: usize,
    /// Pause between full discovery passes.
    pub full_discovery: Duration,
    /// Pause between polls for newly listed symbols.
    pub new_discovery: Duration,
    pub leg: Arc<LegConfig>,
}

enum Discovery {
    Listed(Vec<Listing>),
    /// A full pass finished; these are every live symbol it saw.
    Complete(FxHashSet<String>),
}

struct Shard {
    cap: usize,
    len: usize,
    legs: Vec<mpsc::UnboundedSender<Command>>,
}

impl Shard {
    fn send(&self, cmd: Command) {
        for leg in &self.legs {
            let _ = leg.send(cmd.clone());
        }
    }
}

struct Manager<E: Exchange> {
    exchange: Arc<E>,
    cfg: ManagerConfig,
    engine: mpsc::Sender<EngineMsg>,
    cancel: CancellationToken,
    shards: Vec<Shard>,
    owner: FxHashMap<String, usize>,
    /// Subscribed symbols the last full pass did not list.
    missing: FxHashSet<String>,
    tasks: JoinSet<()>,
}

pub async fn run<E: Exchange>(
    exchange: Arc<E>,
    cfg: ManagerConfig,
    engine: mpsc::Sender<EngineMsg>,
    mut requests: mpsc::UnboundedReceiver<Request>,
    cancel: CancellationToken,
) {
    let (discovered_tx, mut discovered) = mpsc::unbounded_channel();
    let mut discovery = JoinSet::new();
    discovery.spawn(discover_loop(
        exchange.clone(),
        true,
        cfg.full_discovery,
        discovered_tx.clone(),
        cancel.clone(),
    ));
    discovery.spawn(discover_loop(
        exchange.clone(),
        false,
        cfg.new_discovery,
        discovered_tx,
        cancel.clone(),
    ));

    let mut m = Manager {
        exchange,
        cfg,
        engine,
        cancel: cancel.clone(),
        shards: Vec::new(),
        owner: FxHashMap::default(),
        missing: FxHashSet::default(),
        tasks: JoinSet::new(),
    };
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            Some(msg) = discovered.recv() => match msg {
                Discovery::Listed(listings) => m.add(listings).await,
                Discovery::Complete(live) => m.reconcile(live).await,
            },
            Some(req) = requests.recv() => match req {
                Request::Resync { leg, symbols } => m.resync(leg, symbols),
                Request::Reconnect(leg) => {
                    let _ = m.shards[leg.shard].legs[leg.leg].send(Command::Reconnect);
                }
                Request::Closed(symbols) => m.remove(symbols).await,
            },
        }
    }

    // Dropping the command senders lets every leg exit; wait for their close
    // handshakes so the engine sees every final message before it drains.
    discovery.shutdown().await;
    m.shards.clear();
    while m.tasks.join_next().await.is_some() {}
}

/// Runs discovery passes until cancelled, forwarding each page as it arrives.
/// A full pass ends with the set of everything it listed; the other loop
/// polls for symbols listed since its previous poll (with margin for the
/// listing API's caching).
async fn discover_loop<E: Exchange>(
    exchange: Arc<E>,
    full: bool,
    pause: Duration,
    tx: mpsc::UnboundedSender<Discovery>,
    cancel: CancellationToken,
) {
    const MARGIN: TimeDelta = TimeDelta::minutes(10);
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("http client");
    let mut since = Utc::now() - MARGIN;
    loop {
        let started = Utc::now();
        let scope = if full {
            Scope::Full
        } else {
            Scope::Since(since)
        };
        let (page_tx, mut pages) = mpsc::unbounded_channel::<Vec<Listing>>();
        let pass = async {
            let result = exchange.discover(&http, scope, &page_tx).await;
            drop(page_tx);
            result
        };
        let forward = async {
            let mut seen = FxHashSet::default();
            while let Some(page) = pages.recv().await {
                seen.extend(page.iter().map(|l| l.symbol.clone()));
                let _ = tx.send(Discovery::Listed(page));
            }
            seen
        };
        let (result, seen) = tokio::select! {
            _ = cancel.cancelled() => return,
            r = async { tokio::join!(pass, forward) } => r,
        };
        match result {
            Ok(()) => {
                tracing::info!(
                    full,
                    listed = seen.len(),
                    secs = (Utc::now() - started).num_seconds(),
                    "discovery pass done"
                );
                since = started - MARGIN;
                if full {
                    let _ = tx.send(Discovery::Complete(seen));
                }
            }
            // A failed pass is never treated as complete: unsubscribing what
            // it missed would be a far worse gap than a late new symbol.
            Err(e) => tracing::warn!(full, error = %e, "discovery pass failed"),
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(pause) => {}
        }
    }
}

impl<E: Exchange> Manager<E> {
    async fn add(&mut self, listings: Vec<Listing>) {
        let mut added: FxHashMap<usize, Vec<String>> = FxHashMap::default();
        for Listing { symbol, activity } in listings {
            self.missing.remove(&symbol);
            if self.owner.contains_key(&symbol) {
                continue;
            }
            let shard = self.pick_shard(self.exchange.symbols_per_connection(activity));
            self.shards[shard].len += 1;
            self.owner.insert(symbol.clone(), shard);
            added.entry(shard).or_default().push(symbol);
        }
        for (shard, symbols) in added {
            // The engine learns of the symbols before any leg can deliver
            // their books, so it can hold every leg to delivering one.
            let _ = self
                .engine
                .send(EngineMsg::Added {
                    shard,
                    symbols: symbols.clone(),
                })
                .await;
            self.shards[shard].send(Command::Subscribe(symbols));
        }
    }

    /// Unsubscribes symbols absent from this full pass and the previous one.
    /// One miss is tolerated because the listing API is cached and a symbol
    /// may be listed after the pass already went by its position.
    async fn reconcile(&mut self, live: FxHashSet<String>) {
        // A pass that "succeeded" with far fewer symbols than we hold is far
        // likelier an upstream incident (empty pages, a truncated cursor)
        // than half the exchange delisting at once.
        if live.len() < self.owner.len() / 2 {
            tracing::warn!(
                listed = live.len(),
                held = self.owner.len(),
                "full pass implausibly small; not removing anything"
            );
            return;
        }
        let absent: FxHashSet<String> = self
            .owner
            .keys()
            .filter(|s| !live.contains(*s))
            .cloned()
            .collect();
        let gone = absent.intersection(&self.missing).cloned().collect();
        self.missing = absent;
        self.remove(gone).await;
        tracing::info!(
            symbols = self.owner.len(),
            shards = self.shards.len(),
            missing = self.missing.len(),
            "reconciled"
        );
    }

    async fn remove(&mut self, symbols: Vec<String>) {
        let mut removed: FxHashMap<usize, Vec<String>> = FxHashMap::default();
        for symbol in symbols {
            if let Some(shard) = self.owner.remove(&symbol) {
                self.missing.remove(&symbol);
                self.shards[shard].len -= 1;
                removed.entry(shard).or_default().push(symbol);
            }
        }
        for (shard, symbols) in removed {
            self.shards[shard].send(Command::Unsubscribe(symbols.clone()));
            let _ = self.engine.send(EngineMsg::Removed(symbols)).await;
        }
    }

    /// Re-requests a snapshot for `symbols` on one leg only; the other legs
    /// keep serving them meanwhile. A plain re-subscribe is ignored by some
    /// exchanges (Polymarket answers `NO NEW ASSETS`), so unsubscribe first.
    fn resync(&self, leg: LegId, symbols: Vec<String>) {
        let symbols: Vec<String> = symbols
            .into_iter()
            .filter(|s| self.owner.get(s) == Some(&leg.shard))
            .collect();
        if symbols.is_empty() {
            return;
        }
        let tx = &self.shards[leg.shard].legs[leg.leg];
        let _ = tx.send(Command::Unsubscribe(symbols.clone()));
        let _ = tx.send(Command::Subscribe(symbols));
    }

    /// The least-loaded shard of this size with spare capacity, spawning a
    /// new one if all are full. Live symbols are never moved, so rebalancing
    /// never causes a resubscribe gap.
    fn pick_shard(&mut self, cap: usize) -> usize {
        let open = self
            .shards
            .iter()
            .enumerate()
            .filter(|(_, s)| s.cap == cap && s.len < cap);
        if let Some((i, _)) = open.min_by_key(|(_, s)| s.len) {
            return i;
        }
        let shard = self.shards.len();
        let legs = (0..self.cfg.legs)
            .map(|leg| {
                let (tx, rx) = mpsc::unbounded_channel();
                self.tasks.spawn(connection::run(
                    LegId { shard, leg },
                    self.exchange.clone(),
                    self.cfg.leg.clone(),
                    rx,
                    self.engine.clone(),
                    self.cancel.clone(),
                ));
                tx
            })
            .collect();
        self.shards.push(Shard { cap, len: 0, legs });
        shard
    }
}

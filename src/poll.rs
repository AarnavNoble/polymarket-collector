//! Collection from exchanges that only offer REST books (no websocket feed).
//!
//! Every market is polled each round, and consecutive books are diffed into
//! [`Event::Level`] updates, so the engine, merge and output format are the
//! same as for a streaming exchange. The poller acts as a single leg (shard 0,
//! leg 0); the engine's resync requests make the next poll emit a snapshot.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures_util::StreamExt;
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::Side;
use crate::book::{Level, OrderBook};
use crate::engine::{EngineMsg, LegId, Request};
use crate::exchange::Event;

pub trait PolledExchange: Send + Sync + 'static {
    /// Every live market.
    fn markets(
        &self,
        http: &reqwest::Client,
    ) -> impl Future<Output = anyhow::Result<Vec<String>>> + Send;

    /// One market's current book.
    fn book(
        &self,
        http: &reqwest::Client,
        symbol: &str,
    ) -> impl Future<Output = anyhow::Result<OrderBook>> + Send;
}

pub struct PollConfig {
    /// Pause between rounds over every market.
    pub interval: Duration,
    /// Pause between market list refreshes.
    pub discovery: Duration,
    /// Book requests in flight at once.
    pub concurrency: usize,
}

const LEG: LegId = LegId { shard: 0, leg: 0 };

pub async fn run<E: PolledExchange>(
    exchange: Arc<E>,
    cfg: PollConfig,
    engine: mpsc::Sender<EngineMsg>,
    mut requests: mpsc::UnboundedReceiver<Request>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let mut held: FxHashMap<String, OrderBook> = FxHashMap::default();
    let mut resync: FxHashSet<String> = FxHashSet::default();
    let mut listed: FxHashSet<String> = FxHashSet::default();
    let mut discovery = tokio::time::interval(cfg.discovery);
    let mut round = tokio::time::interval(cfg.interval);
    round.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    engine.send(EngineMsg::Connected(LEG)).await?;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            Some(req) = requests.recv() => match req {
                Request::Resync { symbols, .. } => resync.extend(symbols),
                Request::Reconnect(_) => resync.extend(listed.iter().cloned()),
                Request::Closed(_) => {}
            },
            _ = discovery.tick() => match exchange.markets(&http).await {
                Ok(markets) => {
                    let now: FxHashSet<String> = markets.into_iter().collect();
                    let added: Vec<_> = now.difference(&listed).cloned().collect();
                    let removed: Vec<_> = listed.difference(&now).cloned().collect();
                    for s in &removed {
                        held.remove(s);
                    }
                    if !removed.is_empty() {
                        engine.send(EngineMsg::Removed(removed)).await?;
                    }
                    if !added.is_empty() {
                        tracing::info!(markets = now.len(), added = added.len(), "markets listed");
                        engine.send(EngineMsg::Added { shard: 0, symbols: added }).await?;
                    }
                    listed = now;
                }
                Err(e) => tracing::warn!(error = %e, "market list failed"),
            },
            _ = round.tick() => {
                let symbols: Vec<String> = listed.iter().cloned().collect();
                let mut books = futures_util::stream::iter(symbols)
                    .map(|s| {
                        let (exchange, http) = (exchange.clone(), &http);
                        async move { let book = exchange.book(http, &s).await; (s, book) }
                    })
                    .buffer_unordered(cfg.concurrency);
                let mut events = Vec::new();
                while let Some((symbol, book)) = books.next().await {
                    match book {
                        Ok(book) => {
                            let fresh = resync.remove(&symbol);
                            let old = held.insert(symbol.clone(), book.clone());
                            events.extend(diff(&symbol, old.filter(|_| !fresh).as_ref(), &book));
                        }
                        Err(e) => tracing::debug!(error = %e, symbol, "book poll failed"),
                    }
                }
                if !events.is_empty() {
                    engine.send(EngineMsg::Events { leg: LEG, recv_ts: Utc::now(), events }).await?;
                }
            }
        }
    }
}

/// The events that take a holder of `old` to `new`: a snapshot if there is
/// no `old`, else one absolute-quantity update per changed level.
pub fn diff(symbol: &str, old: Option<&OrderBook>, new: &OrderBook) -> Vec<Event> {
    let exchange_ts = Utc::now();
    let Some(old) = old else {
        return vec![Event::Snapshot {
            symbol: symbol.to_owned(),
            exchange_ts,
            bids: new.bids().collect(),
            asks: new.asks().collect(),
        }];
    };
    let mut events = Vec::new();
    for side in [Side::Buy, Side::Sell] {
        let levels = |b: &OrderBook| -> Vec<Level> {
            match side {
                Side::Buy => b.bids().collect(),
                Side::Sell => b.asks().collect(),
            }
        };
        let prices: std::collections::BTreeSet<i64> = levels(old)
            .into_iter()
            .chain(levels(new))
            .map(|l| l.price)
            .collect();
        for price in prices {
            let qty = new.qty_at(side, price);
            if qty != old.qty_at(side, price) {
                events.push(Event::Level {
                    symbol: symbol.to_owned(),
                    exchange_ts,
                    side,
                    price,
                    qty,
                    id: 0,
                });
            }
        }
    }
    events
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_turns_consecutive_books_into_level_updates() {
        let l = |price, qty| Level { price, qty };
        let a = OrderBook::from_levels(&[l(40, 5), l(39, 2)], &[l(42, 1)]);
        let b = OrderBook::from_levels(&[l(40, 7)], &[l(42, 1), l(43, 4)]);
        assert!(matches!(diff("m", None, &b)[..], [Event::Snapshot { .. }]));
        let changes: Vec<_> = diff("m", Some(&a), &b)
            .into_iter()
            .map(|e| match e {
                Event::Level {
                    side, price, qty, ..
                } => (side, price, qty),
                e => panic!("unexpected {e:?}"),
            })
            .collect();
        assert_eq!(
            changes,
            [(Side::Buy, 39, 0), (Side::Buy, 40, 7), (Side::Sell, 43, 4)]
        );
        // Replaying the updates onto `a` yields `b`.
        let mut replayed = a.clone();
        for (side, price, qty) in changes {
            replayed.set(side, price, qty);
        }
        assert_eq!(replayed, b);
    }
}

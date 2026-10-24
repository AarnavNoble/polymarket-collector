//! The engine owns every order book. Each leg keeps its own copy; the
//! *output* book, which is what gets written, applies every update once, from
//! whichever leg delivered it first. It is dropped only when no synced leg
//! remains, so an update is never written without the book it applies to.
//!
//! It also polices completeness: overdue books are resynced, frozen legs
//! reconnected, and output books spot-checked against REST (see README).

use std::hash::{Hash, Hasher};
use std::time::Duration;

use chrono::{DateTime, DurationRound, TimeDelta, Utc};
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::Side;
use crate::book::OrderBook;
use crate::exchange::Event;
use crate::util::dtos::challenge_orderbook::PriceLevelUpdateDomain;
use crate::writer::{Record, Sink};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LegId {
    pub shard: usize,
    pub leg: usize,
}

#[derive(Debug)]
pub enum EngineMsg {
    /// The manager subscribed these symbols on every leg of `shard`.
    Added { shard: usize, symbols: Vec<String> },
    /// The manager unsubscribed these symbols; forget them.
    Removed(Vec<String>),
    /// The leg has (re)connected; its books are unknown until each symbol's
    /// next snapshot arrives.
    Connected(LegId),
    /// The leg lost its connection; its books are stale.
    Disconnected(LegId),
    Events {
        leg: LegId,
        recv_ts: DateTime<Utc>,
        events: Vec<Event>,
    },
}

/// Requests from the engine to the subscription manager.
#[derive(Debug)]
pub enum Request {
    /// These symbols need a fresh snapshot on `leg`.
    Resync { leg: LegId, symbols: Vec<String> },
    /// Drop and re-establish this leg's connection.
    Reconnect(LegId),
    /// The exchange reported these symbols closed.
    Closed(Vec<String>),
}

/// How long a leg may hold a subscribed symbol without its book before the
/// engine asks again. Retries back off exponentially up to 16x.
pub const BOOK_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a leg may receive no data while a sibling leg does.
pub const FREEZE_TIMEOUT: Duration = Duration::from_secs(60);

const HEALTH_INTERVAL: Duration = Duration::from_secs(5);

/// A reference whose last change is newer than ours proves a gap only once
/// that change can no longer be in flight to us.
const VERIFY_IN_FLIGHT: TimeDelta = TimeDelta::seconds(2);

struct LegBook {
    book: OrderBook,
    synced: bool,
    /// While unsynced: when we last asked for this book, and how many times.
    waiting_since: Instant,
    attempts: u32,
}

impl LegBook {
    fn stale(now: Instant) -> Self {
        Self {
            book: OrderBook::default(),
            synced: false,
            waiting_since: now,
            attempts: 0,
        }
    }
}

/// The book as written. Updates set absolute quantities, so order matters only
/// within a level: a copy is stale if its level already applied something
/// newer, or the same update in the same millisecond.
struct Output {
    book: OrderBook,
    /// Newest exchange timestamp applied.
    exchange_ts: DateTime<Utc>,
    /// Timestamp of the book the output was last (re)based on.
    floor: DateTime<Utc>,
    /// Per level: the exchange timestamp of the last update applied, and the
    /// keys of every update applied at that timestamp.
    levels: FxHashMap<(Side, i64), (DateTime<Utc>, Vec<u64>)>,
}

impl Output {
    fn based_on(book: OrderBook, exchange_ts: DateTime<Utc>) -> Self {
        Self {
            book,
            exchange_ts,
            floor: exchange_ts,
            levels: FxHashMap::default(),
        }
    }

    /// Records an update unless it was already applied or superseded.
    fn first_sight(
        &mut self,
        exchange_ts: DateTime<Utc>,
        side: Side,
        price: i64,
        key: u64,
    ) -> bool {
        if exchange_ts < self.floor {
            return false;
        }
        let (ts, keys) = self
            .levels
            .entry((side, price))
            .or_insert((exchange_ts, Vec::new()));
        if exchange_ts < *ts || exchange_ts == *ts && keys.contains(&key) {
            return false;
        }
        if exchange_ts > *ts {
            *ts = exchange_ts;
            keys.clear();
        }
        keys.push(key);
        true
    }
}

struct Market {
    shard: usize,
    legs: Vec<LegBook>,
    /// `None` until a leg delivers a book, and again whenever no synced leg
    /// remains.
    output: Option<Output>,
    verified_at: Option<Instant>,
    /// The output's timestamp when it last disagreed with a reference. REST
    /// can capture the middle of a same-millisecond burst, so a mismatch is
    /// confirmed only if the next check disagrees at the same timestamp.
    suspect: Option<DateTime<Utc>>,
}

/// Connects the engine to a task that fetches reference books (see
/// [`crate::verify`]).
pub struct VerifyLink {
    pub interval: Duration,
    pub batch: usize,
    pub requests: mpsc::Sender<Vec<String>>,
    pub results: mpsc::Receiver<Vec<Event>>,
}

#[derive(Default, Debug)]
struct Stats {
    /// Updates written, and how many of them a non-first leg also delivered.
    written: u64,
    duplicates: u64,
    verified: u64,
    suspects: u64,
    mismatches: u64,
    /// Confirmed mismatches where a leg held the same book as the output.
    legs_agreed: u64,
    resyncs: u64,
    reconnects: u64,
    /// A synced leg was lost while another stayed synced: nothing lost.
    failovers: u64,
    /// The last synced leg was lost: the symbol is unserved until a fresh
    /// snapshot arrives.
    outages: u64,
}

pub struct Engine {
    markets: FxHashMap<String, Market>,
    last_data: FxHashMap<LegId, Instant>,
    legs_per_shard: usize,
    sink: Sink,
    requests: mpsc::UnboundedSender<Request>,
    stats: Stats,
}

impl Engine {
    pub fn new(
        legs_per_shard: usize,
        sink: Sink,
        requests: mpsc::UnboundedSender<Request>,
    ) -> Self {
        Self {
            markets: FxHashMap::default(),
            last_data: FxHashMap::default(),
            legs_per_shard,
            sink,
            requests,
            stats: Stats::default(),
        }
    }

    /// Runs until every sender of `rx` is dropped, writing a snapshot of every
    /// served book at each wall-clock minute.
    pub async fn run(mut self, mut rx: mpsc::Receiver<EngineMsg>, mut verify: Option<VerifyLink>) {
        let mut verify_timer = tokio::time::interval(
            verify
                .as_ref()
                .map_or(Duration::from_secs(3600), |v| v.interval),
        );
        let mut health_timer = tokio::time::interval(HEALTH_INTERVAL);
        let next_minute = || {
            let now = Utc::now();
            let boundary =
                now.duration_trunc(TimeDelta::minutes(1)).unwrap() + TimeDelta::minutes(1);
            Instant::now() + (boundary - now).to_std().unwrap_or(Duration::ZERO)
        };
        let snapshot_timer = tokio::time::sleep_until(next_minute());
        tokio::pin!(snapshot_timer);
        loop {
            tokio::select! {
                msg = rx.recv() => match msg {
                    Some(msg) => self.handle(msg),
                    None => break,
                },
                _ = &mut snapshot_timer => {
                    self.snapshot_all();
                    snapshot_timer.as_mut().reset(next_minute());
                }
                _ = health_timer.tick() => self.health_check(Instant::now()),
                _ = verify_timer.tick(), if verify.is_some() => {
                    let link = verify.as_ref().unwrap();
                    // Skip a round rather than queue if the fetcher is behind;
                    // checking first keeps skipped books first in line.
                    if link.requests.capacity() > 0 {
                        let batch = self.verify_candidates(link.batch);
                        if !batch.is_empty() {
                            let _ = link.requests.try_send(batch);
                        }
                    }
                }
                Some(books) = async { verify.as_mut().unwrap().results.recv().await }, if verify.is_some() => {
                    for b in books {
                        self.verify(b);
                    }
                }
            }
        }
        tracing::info!(markets = self.markets.len(), stats = ?self.stats, "engine drained");
    }

    pub fn handle(&mut self, msg: EngineMsg) {
        let now = Instant::now();
        match msg {
            EngineMsg::Added { shard, symbols } => {
                for s in symbols {
                    self.markets.entry(s).or_insert_with(|| Market {
                        shard,
                        legs: (0..self.legs_per_shard)
                            .map(|_| LegBook::stale(now))
                            .collect(),
                        output: None,
                        verified_at: None,
                        suspect: None,
                    });
                }
            }
            EngineMsg::Removed(symbols) => {
                for s in symbols {
                    self.markets.remove(&s);
                }
            }
            EngineMsg::Connected(id) => {
                self.last_data.insert(id, now);
                self.leg_reset(id, now);
            }
            EngineMsg::Disconnected(id) => self.leg_reset(id, now),
            EngineMsg::Events {
                leg,
                recv_ts,
                events,
            } => {
                // Only book changes prove a leg is live: initial dumps after a
                // (re)subscribe would otherwise make a quiet shard's legs
                // take turns looking frozen to each other.
                if events.iter().any(|e| matches!(e, Event::Level { .. })) {
                    self.last_data.insert(leg, now);
                }
                for event in events {
                    self.apply(leg, recv_ts, event);
                }
            }
        }
    }

    /// Marks every book on `id` stale.
    fn leg_reset(&mut self, id: LegId, now: Instant) {
        for m in self.markets.values_mut().filter(|m| m.shard == id.shard) {
            invalidate_leg(m, id.leg, now, &mut self.stats);
        }
    }

    fn apply(&mut self, leg: LegId, recv_ts: DateTime<Utc>, event: Event) {
        match event {
            Event::Snapshot {
                symbol,
                exchange_ts,
                bids,
                asks,
            } => {
                // Books for symbols we did not subscribe (e.g. the other
                // outcome of a market) are ignored.
                let Some(m) = self.markets.get_mut(&symbol) else {
                    return;
                };
                // In flight from a shard the symbol has since left.
                if m.shard != leg.shard {
                    return;
                }
                let lb = &mut m.legs[leg.leg];
                lb.book = OrderBook::from_levels(&bids, &asks);
                lb.synced = true;
                lb.attempts = 0;
                // A mid-stream book rebases the output only if strictly newer
                // and different; older or same-instant ones may be a lagging
                // leg's or mid-burst.
                let rebase = m
                    .output
                    .as_ref()
                    .is_none_or(|o| exchange_ts > o.exchange_ts && o.book != lb.book);
                if rebase {
                    self.sink.send(Record::Snapshot(lb.book.to_snapshot(
                        &symbol,
                        recv_ts,
                        exchange_ts,
                    )));
                    m.output = Some(Output::based_on(lb.book.clone(), exchange_ts));
                }
            }
            Event::Level {
                symbol,
                exchange_ts,
                side,
                price,
                qty,
                id,
            } => {
                // An update for a book this leg holds no snapshot of cannot be
                // applied correctly; the leg's next snapshot will cover it.
                let Some(m) = self.markets.get_mut(&symbol) else {
                    return;
                };
                let lb = &mut m.legs[leg.leg];
                if !lb.synced || m.shard != leg.shard {
                    return;
                }
                lb.book.set(side, price, qty);

                let Some(out) = &mut m.output else { return };
                let key = update_key(exchange_ts, side, price, qty, id);
                if !out.first_sight(exchange_ts, side, price, key) {
                    self.stats.duplicates += 1;
                    return;
                }
                out.exchange_ts = out.exchange_ts.max(exchange_ts);
                if out.book.set(side, price, qty) {
                    self.stats.written += 1;
                    self.sink.send(Record::Update(PriceLevelUpdateDomain {
                        backtest_timestamp: recv_ts,
                        exchange_timestamp: exchange_ts,
                        price_level: price,
                        quantity: qty.max(0),
                        side,
                        symbol,
                    }));
                }
            }
            Event::Closed { symbol } => {
                let _ = self.requests.send(Request::Closed(vec![symbol]));
            }
            Event::Gap { symbols } => {
                let symbols: Vec<String> = if symbols.is_empty() {
                    self.markets
                        .iter()
                        .filter(|(_, m)| m.shard == leg.shard)
                        .map(|(s, _)| s.clone())
                        .collect()
                } else {
                    symbols
                        .into_iter()
                        .filter(|s| self.markets.get(s).is_some_and(|m| m.shard == leg.shard))
                        .collect()
                };
                if symbols.is_empty() {
                    return;
                }
                let now = Instant::now();
                for s in &symbols {
                    invalidate_leg(
                        self.markets.get_mut(s).unwrap(),
                        leg.leg,
                        now,
                        &mut self.stats,
                    );
                }
                self.stats.resyncs += 1;
                let _ = self.requests.send(Request::Resync { leg, symbols });
            }
        }
    }

    /// Re-requests overdue books and reconnects legs that are frozen or keep
    /// failing to deliver books their siblings receive.
    fn health_check(&mut self, now: Instant) {
        let mut resync: FxHashMap<LegId, Vec<String>> = FxHashMap::default();
        let mut reconnect: FxHashSet<LegId> = FxHashSet::default();
        for (symbol, m) in &mut self.markets {
            let served = m.legs.iter().any(|l| l.synced);
            for (leg, lb) in m.legs.iter_mut().enumerate() {
                if lb.synced || now < lb.waiting_since + BOOK_TIMEOUT * (1 << lb.attempts.min(4)) {
                    continue;
                }
                let id = LegId {
                    shard: m.shard,
                    leg,
                };
                // A retry while another leg serves the symbol proves this leg
                // is broken; otherwise the symbol itself may be dead, and
                // reconnecting would only disturb its neighbours.
                if lb.attempts > 0 && served {
                    reconnect.insert(id);
                } else {
                    resync.entry(id).or_default().push(symbol.clone());
                }
                lb.attempts += 1;
                lb.waiting_since = now;
            }
        }

        for (&id, &last) in &self.last_data {
            let sibling_alive = (0..self.legs_per_shard).filter(|&l| l != id.leg).any(|l| {
                self.last_data
                    .get(&LegId { leg: l, ..id })
                    .is_some_and(|&t| now < t + FREEZE_TIMEOUT)
            });
            if now >= last + FREEZE_TIMEOUT && sibling_alive {
                tracing::warn!(
                    ?id,
                    "leg silent while its sibling receives data; treating as frozen"
                );
                reconnect.insert(id);
            }
        }

        for id in reconnect {
            resync.remove(&id);
            self.last_data.insert(id, now);
            self.stats.reconnects += 1;
            let _ = self.requests.send(Request::Reconnect(id));
        }
        for (leg, symbols) in resync {
            tracing::debug!(?leg, n = symbols.len(), "books overdue; resyncing");
            self.stats.resyncs += 1;
            let _ = self.requests.send(Request::Resync { leg, symbols });
        }
    }

    /// Up to `n` served symbols, never-verified first, then stalest.
    fn verify_candidates(&mut self, n: usize) -> Vec<String> {
        let mut served: Vec<(&String, &mut Market)> = self
            .markets
            .iter_mut()
            .filter(|(_, m)| m.output.is_some())
            .collect();
        let n = n.min(served.len());
        if n == 0 {
            return Vec::new();
        }
        let key = |m: &Market| (m.verified_at, m.output.as_ref().map(|o| o.exchange_ts));
        served.select_nth_unstable_by(n - 1, |a, b| key(a.1).cmp(&key(b.1)));
        let now = Instant::now();
        served[..n]
            .iter_mut()
            .map(|(s, m)| {
                m.verified_at = Some(now);
                (*s).clone()
            })
            .collect()
    }

    /// Compares a reference book from the exchange with the output.
    fn verify(&mut self, reference: Event) {
        let Event::Snapshot {
            symbol,
            exchange_ts: reference_ts,
            bids,
            asks,
        } = reference
        else {
            return;
        };
        let Some(m) = self.markets.get_mut(&symbol) else {
            return;
        };
        let Some(out) = &m.output else { return };
        let ours = out.exchange_ts;
        // An older reference (REST is served from a cache) or one whose last
        // change may still be on its way to us proves nothing.
        if reference_ts < ours
            || reference_ts > ours && reference_ts + VERIFY_IN_FLIGHT > Utc::now()
        {
            return;
        }
        self.stats.verified += 1;
        if out.book == OrderBook::from_levels(&bids, &asks) {
            m.suspect = None;
            return;
        }
        if m.suspect != Some(ours) {
            // First disagreement at this state of the output: recheck next round.
            self.stats.suspects += 1;
            m.suspect = Some(ours);
            m.verified_at = None;
            return;
        }
        self.stats.mismatches += 1;
        m.suspect = None;
        // The exchange's feed can omit a change identically on every
        // connection, so every leg is resynced.
        let legs_agree = m.legs.iter().any(|l| l.synced && l.book == out.book);
        self.stats.legs_agreed += u64::from(legs_agree);
        tracing::warn!(%symbol, legs_agree, "book differs from exchange reference; resyncing all legs");
        m.output = None;
        let now = Instant::now();
        for (leg, lb) in m.legs.iter_mut().enumerate() {
            *lb = LegBook::stale(now);
            self.stats.resyncs += 1;
            let leg = LegId {
                shard: m.shard,
                leg,
            };
            let _ = self.requests.send(Request::Resync {
                leg,
                symbols: vec![symbol.clone()],
            });
        }
    }

    fn snapshot_all(&self) {
        let now = Utc::now();
        let mut n = 0usize;
        for (symbol, m) in &self.markets {
            let Some(out) = &m.output else { continue };
            self.sink.send(Record::Snapshot(out.book.to_snapshot(
                symbol,
                now,
                out.exchange_ts,
            )));
            n += 1;
        }
        tracing::info!(
            snapshots = n,
            unserved = self.markets.len() - n,
            backlog = self.sink.backlog(),
            stats = ?self.stats,
            "minute snapshot"
        );
    }
}

/// Marks one leg's copy of `m` stale. If that leaves no synced leg, the
/// output can no longer be kept current and is dropped until a fresh
/// snapshot arrives.
fn invalidate_leg(m: &mut Market, leg: usize, now: Instant, stats: &mut Stats) {
    let was_synced = m.legs[leg].synced;
    m.legs[leg] = LegBook::stale(now);
    if !was_synced || m.output.is_none() {
        return;
    }
    if m.legs.iter().any(|l| l.synced) {
        stats.failovers += 1;
    } else {
        stats.outages += 1;
        m.output = None;
    }
}

/// Identity of an update for cross-leg deduplication.
fn update_key(exchange_ts: DateTime<Utc>, side: Side, price: i64, qty: i64, id: u64) -> u64 {
    let mut h = FxHasher::default();
    (exchange_ts.timestamp_micros(), side as u8, price, qty, id).hash(&mut h);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::Level;
    use crate::writer::Record;

    const A: LegId = LegId { shard: 0, leg: 0 };
    const B: LegId = LegId { shard: 0, leg: 1 };

    fn ts(ms: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(ms).unwrap()
    }

    fn book(bid_qty: i64, ms: i64) -> Event {
        Event::Snapshot {
            symbol: "X".into(),
            exchange_ts: ts(ms),
            bids: vec![Level {
                price: 10,
                qty: bid_qty,
            }],
            asks: vec![],
        }
    }

    fn snapshot(bid_qty: i64) -> Event {
        book(bid_qty, 1)
    }

    /// The update "the level now holds `qty`", stamped at `qty` ms so that
    /// distinct updates have distinct identities.
    fn level(qty: i64) -> Event {
        Event::Level {
            symbol: "X".into(),
            exchange_ts: ts(qty),
            side: Side::Buy,
            price: 10,
            qty,
            id: 0,
        }
    }

    struct Harness {
        engine: Engine,
        out: crossbeam::channel::Receiver<Record>,
        requests: mpsc::UnboundedReceiver<Request>,
    }

    impl Harness {
        fn new() -> Self {
            let (tx, out) = crossbeam::channel::unbounded();
            let (req, requests) = mpsc::unbounded_channel();
            let mut engine = Engine::new(2, Sink::from_sender(tx), req);
            engine.handle(EngineMsg::Added {
                shard: 0,
                symbols: vec!["X".into()],
            });
            Self {
                engine,
                out,
                requests,
            }
        }

        fn send(&mut self, leg: LegId, event: Event) {
            self.engine.handle(EngineMsg::Events {
                leg,
                recv_ts: ts(3),
                events: vec![event],
            });
        }

        /// Drains output as (is_snapshot, top bid qty) pairs.
        fn drain(&self) -> Vec<(bool, i64)> {
            self.out
                .try_iter()
                .map(|r| match r {
                    Record::Snapshot(s) => (true, s.bids.first().map_or(0, |l| l.quantity)),
                    Record::Update(u) => (false, u.quantity),
                })
                .collect()
        }

        fn requests(&mut self) -> Vec<String> {
            std::iter::from_fn(|| self.requests.try_recv().ok())
                .map(|r| match r {
                    Request::Resync { leg, symbols } => format!("resync {} {symbols:?}", leg.leg),
                    Request::Reconnect(leg) => format!("reconnect {}", leg.leg),
                    Request::Closed(s) => format!("closed {s:?}"),
                })
                .collect()
        }
    }

    #[test]
    fn each_update_is_written_once_from_whichever_leg_is_first() {
        let mut h = Harness::new();
        h.send(A, snapshot(5));
        h.send(B, snapshot(5));
        h.send(A, level(6)); // A first
        h.send(B, level(6));
        h.send(B, level(7)); // B first
        h.send(A, level(7));
        assert_eq!(h.drain(), [(true, 5), (false, 6), (false, 7)]);
        assert_eq!((h.engine.stats.written, h.engine.stats.duplicates), (2, 2));
    }

    #[test]
    fn a_lagging_leg_never_rewinds_the_output() {
        let mut h = Harness::new();
        h.send(A, snapshot(5));
        h.send(B, snapshot(5));
        for q in [6, 7, 8] {
            h.send(A, level(q));
        }
        for q in [6, 7, 8, 9] {
            h.send(B, level(q)); // replays 6..8, then is first with 9
        }
        assert_eq!(
            h.drain(),
            [(true, 5), (false, 6), (false, 7), (false, 8), (false, 9)]
        );
    }

    #[test]
    fn a_rebase_is_never_followed_by_older_updates() {
        let mut h = Harness::new();
        h.send(A, snapshot(5));
        h.send(B, snapshot(5));
        h.send(B, book(8, 50)); // B resyncs: a newer, different book
        h.send(A, level(7)); // A lags: an update from before B's book
        h.send(A, book(9, 50)); // same instant as the output: may be mid-burst
        assert_eq!(h.drain(), [(true, 5), (true, 8)]);
    }

    #[test]
    fn same_millisecond_replays_never_rewind_a_level() {
        let at = |price, qty| Event::Level {
            symbol: "X".into(),
            exchange_ts: ts(20),
            side: Side::Buy,
            price,
            qty,
            id: qty as u64,
        };
        let mut h = Harness::new();
        h.send(A, snapshot(5));
        h.send(B, snapshot(5));
        h.send(A, at(10, 6));
        h.send(A, at(10, 7));
        for q in 0..100 {
            h.send(A, at(11, 1_000 + q)); // a burst elsewhere in the book
        }
        h.drain();
        h.send(B, at(10, 6)); // the lagging leg's copies, long after
        h.send(B, at(10, 7));
        assert_eq!(h.drain(), []);
        let out = &h.engine.markets["X"].output.as_ref().unwrap().book;
        assert!(out.bids().any(|l| l.price == 10 && l.qty == 7));
    }

    #[test]
    fn gaps_for_unknown_symbols_are_ignored() {
        let mut h = Harness::new();
        h.send(
            A,
            Event::Gap {
                symbols: vec!["NO".into()],
            },
        );
        h.send(
            A,
            Event::Gap {
                symbols: vec!["X".into()],
            },
        );
        assert_eq!(h.requests(), ["resync 0 [\"X\"]"]);
    }

    #[test]
    fn a_newer_differing_book_replaces_the_output_once() {
        let mut h = Harness::new();
        h.send(A, snapshot(5));
        h.send(B, snapshot(5));
        h.send(A, book(4, 20)); // e.g. a trade, pushed as a full book
        h.send(B, book(4, 20)); // same book from the other leg
        h.send(B, book(3, 10)); // an older book from a lagging leg
        assert_eq!(h.drain(), [(true, 5), (true, 4)]);
    }

    #[test]
    fn unsubscribed_symbols_are_ignored() {
        let mut h = Harness::new();
        h.send(
            A,
            Event::Snapshot {
                symbol: "NO".into(),
                exchange_ts: ts(1),
                bids: vec![],
                asks: vec![],
            },
        );
        assert_eq!(h.drain(), []);
    }

    #[test]
    fn losing_one_leg_costs_nothing() {
        let mut h = Harness::new();
        h.send(A, snapshot(5));
        h.send(B, snapshot(5));
        h.engine.handle(EngineMsg::Disconnected(A));
        h.send(B, level(8));
        h.send(A, snapshot(8)); // A resyncs: re-arms A, writes nothing
        h.send(A, level(9));
        assert_eq!(h.drain(), [(true, 5), (false, 8), (false, 9)]);
        assert_eq!((h.engine.stats.failovers, h.engine.stats.outages), (1, 0));
    }

    #[test]
    fn updates_before_a_snapshot_are_not_written() {
        let mut h = Harness::new();
        h.send(A, level(6));
        h.send(A, snapshot(5));
        h.send(A, level(7));
        assert_eq!(h.drain(), [(true, 5), (false, 7)]);
    }

    #[test]
    fn losing_every_leg_waits_for_a_fresh_snapshot() {
        let mut h = Harness::new();
        h.send(A, snapshot(5));
        h.engine.handle(EngineMsg::Disconnected(A));
        h.send(A, level(9)); // stale leg: must not be written
        h.send(B, snapshot(4));
        assert_eq!(h.drain(), [(true, 5), (true, 4)]);
        assert_eq!(h.engine.stats.outages, 1);
    }

    #[test]
    fn overdue_books_are_resynced_then_the_leg_reconnected() {
        let mut h = Harness::new();
        let t0 = Instant::now();
        h.send(A, snapshot(5)); // B never delivers
        h.engine.health_check(t0 + BOOK_TIMEOUT);
        assert_eq!(h.requests(), ["resync 1 [\"X\"]"]);
        h.engine.health_check(t0 + BOOK_TIMEOUT * 2); // backoff not yet elapsed
        assert_eq!(h.requests(), Vec::<String>::new());
        h.engine.health_check(t0 + BOOK_TIMEOUT * 4);
        assert_eq!(h.requests(), ["reconnect 1"]);
    }

    #[test]
    fn a_symbol_no_leg_can_get_is_retried_without_reconnecting() {
        let mut h = Harness::new();
        let t0 = Instant::now();
        for i in 1..=6 {
            h.engine.health_check(t0 + BOOK_TIMEOUT * (1 << i));
        }
        assert!(h.requests().iter().all(|r| r.starts_with("resync")));
    }

    #[test]
    fn a_silent_leg_with_a_live_sibling_is_reconnected() {
        let mut h = Harness::new();
        let t0 = Instant::now();
        h.engine.handle(EngineMsg::Connected(A));
        h.engine.handle(EngineMsg::Connected(B));
        h.engine.last_data.insert(B, t0 + FREEZE_TIMEOUT);
        h.engine
            .health_check(t0 + FREEZE_TIMEOUT + Duration::from_secs(1));
        assert!(h.requests().contains(&"reconnect 0".to_string()));
    }

    #[test]
    fn a_confirmed_disagreement_resyncs_every_leg() {
        let mut h = Harness::new();
        h.send(A, snapshot(5));
        h.send(B, snapshot(5));
        h.engine.verify(book(6, 10_000));
        assert_eq!(
            h.engine.stats.mismatches, 0,
            "one disagreement is only a suspicion"
        );
        h.engine.verify(book(6, 10_000));
        assert_eq!(
            (h.engine.stats.mismatches, h.engine.stats.legs_agreed),
            (1, 1)
        );
        assert_eq!(h.requests(), ["resync 0 [\"X\"]", "resync 1 [\"X\"]"]);
        h.send(A, level(7)); // stale until resynced: dropped
        h.send(B, snapshot(6));
        assert_eq!(h.drain(), [(true, 5), (true, 6)]);
    }

    #[test]
    fn a_suspicion_clears_when_the_reference_catches_up() {
        let mut h = Harness::new();
        h.send(A, snapshot(5));
        h.engine.verify(book(6, 1)); // REST caught mid-burst
        h.engine.verify(book(5, 1));
        h.engine.verify(book(6, 1));
        assert_eq!((h.engine.stats.suspects, h.engine.stats.mismatches), (2, 0));
    }

    #[test]
    fn matching_stale_or_in_flight_references_are_left_alone() {
        let mut h = Harness::new();
        h.send(A, snapshot(5));
        h.engine.verify(book(5, 1)); // same last change, same book
        h.engine.verify(book(9, 0)); // older than ours: stale cache
        let now = Utc::now().timestamp_millis();
        h.engine.verify(book(9, now)); // newer, but may still be in flight
        assert_eq!((h.engine.stats.verified, h.engine.stats.mismatches), (1, 0));
        assert_eq!(h.drain(), [(true, 5)]);
    }
}

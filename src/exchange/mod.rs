//! The exchange boundary. Everything outside this module (connections,
//! subscription management, books, output) is exchange-agnostic; an exchange
//! only describes how to discover symbols, encode commands and decode frames.

pub mod polymarket;
pub mod vpx;

use std::future::Future;

use chrono::{DateTime, Utc};

use crate::Side;
use crate::book::Level;

/// A normalized market-data event decoded from one websocket frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// The full book for `symbol`; replaces whatever we held before.
    Snapshot {
        symbol: String,
        exchange_ts: DateTime<Utc>,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    /// One level now holds `qty` (absolute).
    Level {
        symbol: String,
        exchange_ts: DateTime<Utc>,
        side: Side,
        price: i64,
        qty: i64,
        /// Exchange-provided identity of the update, equal on every connection
        /// (0 if none). Together with the other fields it lets the engine
        /// recognise the same update arriving on redundant connections.
        id: u64,
    },
    /// The exchange reports `symbol` stopped trading.
    Closed { symbol: String },
    /// Data for these symbols was lost on this connection (all of its symbols
    /// when empty), e.g. a sequence break or an undecodable frame; resync them.
    Gap { symbols: Vec<String> },
}

/// A tradable symbol found by discovery, with a hint of how busy it is.
#[derive(Debug, Clone, PartialEq)]
pub struct Listing {
    pub symbol: String,
    /// Exchange-specific activity measure (e.g. 24h volume), used only to
    /// size connections.
    pub activity: f64,
}

/// Which listings a discovery pass should produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// Every live symbol; the pass's union is the authoritative live set.
    Full,
    /// Only symbols listed since roughly the given time (fast path).
    Since(DateTime<Utc>),
}

pub trait Exchange: Send + Sync + 'static {
    fn ws_url(&self) -> &str;

    /// How many symbols one connection should carry, given their activity.
    /// Busy symbols get small connections, since exchanges drop connections
    /// whose message rate they cannot push fast enough.
    fn symbols_per_connection(&self, activity: f64) -> usize;

    /// Largest batch of symbols in a single subscribe/unsubscribe command.
    fn max_batch(&self) -> usize;

    /// Commands that subscribe `symbols`. `first` is true for the first
    /// command on a fresh connection, for exchanges whose initial
    /// subscription differs from later additions.
    fn subscribe(&self, symbols: &[String], first: bool) -> String;

    fn unsubscribe(&self, symbols: &[String]) -> String;

    /// Application-level keepalive, if the exchange wants one.
    fn heartbeat(&self) -> Option<&'static str>;

    /// Decodes one text frame, appending events to `out`. Frames that carry no
    /// book data (acks, pongs) decode to nothing.
    fn decode(&self, frame: &str, out: &mut Vec<Event>) -> anyhow::Result<()>;

    /// Streams live listings in `scope` to `out`, page by page, so callers can
    /// subscribe while a long pass is still running.
    fn discover(
        &self,
        http: &reqwest::Client,
        scope: Scope,
        out: &tokio::sync::mpsc::UnboundedSender<Vec<Listing>>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Fetches reference books (as [`Event::Snapshot`]s) for spot-checking
    /// ours. Exchanges without a REST book endpoint return nothing, which
    /// disables verification.
    fn fetch_books(
        &self,
        _http: &reqwest::Client,
        _symbols: &[String],
    ) -> impl Future<Output = anyhow::Result<Vec<Event>>> + Send {
        async { Ok(Vec::new()) }
    }
}

/// Hashes an exchange-provided update identifier (e.g. a book hash) into an
/// [`Event::Level`] `id`.
pub fn update_id(s: &str) -> u64 {
    use std::hash::Hasher;
    let mut h = rustc_hash::FxHasher::default();
    h.write(s.as_bytes());
    h.finish()
}

/// Converts an exchange's millisecond epoch timestamp.
pub fn ts_from_millis(ms: i64) -> anyhow::Result<DateTime<Utc>> {
    DateTime::from_timestamp_millis(ms)
        .ok_or_else(|| anyhow::anyhow!("timestamp {ms} out of range"))
}

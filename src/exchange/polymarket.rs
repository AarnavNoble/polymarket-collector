//! Polymarket CLOB market channel.
//!
//! Every outcome token has its own two-sided book, and a binary market's NO
//! book is an exact mirror of its YES book (bid p <=> ask 1-p, same size), so
//! one symbol per market suffices: the YES token id, whose book is emitted
//! as-is. Entries for the NO token are ignored by the engine.

use anyhow::Context;
use serde::Deserialize;

use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::{DateTime, TimeDelta, Utc};
use futures_util::StreamExt;

use super::{Event, Exchange, Listing, Scope, ts_from_millis, update_id};
use crate::Side;
use crate::book::Level;
use crate::fixed;

const GAMMA: &str = "https://gamma-api.polymarket.com/markets/keyset";
/// Concurrent Gamma cursors; Gamma allows 300 listing requests per 10 s.
const DISCOVERY_PARALLELISM: usize = 8;
const CLOB_BOOKS: &str = "https://clob.polymarket.com/books";

pub struct Polymarket {
    pub ws_url: String,
    /// Cap on symbols per discovery pass, for local testing.
    pub max_symbols: Option<usize>,
}

impl Default for Polymarket {
    fn default() -> Self {
        Self {
            ws_url: "wss://ws-subscriptions-clob.polymarket.com/ws/market".into(),
            max_symbols: None,
        }
    }
}

#[derive(Deserialize)]
struct WireLevel<'a> {
    price: &'a str,
    size: &'a str,
}

#[derive(Deserialize)]
struct WireChange<'a> {
    asset_id: &'a str,
    price: &'a str,
    size: &'a str,
    side: &'a str,
    /// SHA-1 of the asset's book after this change: identical on every
    /// connection, so it identifies the update.
    #[serde(default)]
    hash: &'a str,
}

/// A full book, as pushed on the websocket and returned by REST.
#[derive(Deserialize)]
struct WireBook<'a> {
    asset_id: &'a str,
    #[serde(borrow)]
    bids: Vec<WireLevel<'a>>,
    #[serde(borrow)]
    asks: Vec<WireLevel<'a>>,
    timestamp: &'a str,
}

#[derive(Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
enum Wire<'a> {
    #[serde(borrow)]
    Book(WireBook<'a>),
    PriceChange {
        #[serde(borrow)]
        price_changes: Vec<WireChange<'a>>,
        timestamp: &'a str,
    },
    MarketResolved {
        #[serde(borrow, default)]
        assets_ids: Vec<&'a str>,
    },
    #[serde(other)]
    Other,
}

fn level(price: &str, size: &str) -> anyhow::Result<Level> {
    Ok(Level {
        price: fixed::parse(price).with_context(|| format!("bad price {price:?}"))?,
        qty: fixed::parse(size).with_context(|| format!("bad size {size:?}"))?,
    })
}

fn levels(wire: &[WireLevel]) -> anyhow::Result<Vec<Level>> {
    wire.iter().map(|l| level(l.price, l.size)).collect()
}

fn book_event(b: WireBook) -> anyhow::Result<Event> {
    Ok(Event::Snapshot {
        symbol: b.asset_id.to_owned(),
        exchange_ts: ts_from_millis(b.timestamp.parse()?)?,
        bids: levels(&b.bids)?,
        asks: levels(&b.asks)?,
    })
}

fn change_event(c: &WireChange, exchange_ts: DateTime<Utc>) -> anyhow::Result<Event> {
    let Level { price, qty } = level(c.price, c.size)?;
    let side = match c.side {
        "BUY" => Side::Buy,
        "SELL" => Side::Sell,
        other => anyhow::bail!("bad side {other:?}"),
    };
    Ok(Event::Level {
        symbol: c.asset_id.to_owned(),
        exchange_ts,
        side,
        price,
        qty,
        id: update_id(c.hash),
    })
}

fn gap(symbols: impl IntoIterator<Item = impl Into<String>>) -> Event {
    Event::Gap {
        symbols: symbols.into_iter().map(Into::into).collect(),
    }
}

/// Decodes one message. Book data that fails to decode is never skipped
/// silently: its symbols get an [`Event::Gap`], and the first error is
/// returned once the rest of the message is decoded.
fn decode_one(msg: Wire, out: &mut Vec<Event>) -> anyhow::Result<()> {
    match msg {
        Wire::Book(b) => {
            let symbol = b.asset_id;
            book_event(b)
                .map(|e| out.push(e))
                .inspect_err(|_| out.push(gap([symbol])))
        }
        Wire::PriceChange {
            price_changes,
            timestamp,
        } => {
            let exchange_ts = match timestamp
                .parse()
                .map_err(anyhow::Error::from)
                .and_then(ts_from_millis)
            {
                Ok(ts) => ts,
                Err(e) => {
                    out.push(gap(price_changes.iter().map(|c| c.asset_id)));
                    return Err(e);
                }
            };
            let mut result = Ok(());
            for c in &price_changes {
                match change_event(c, exchange_ts) {
                    Ok(e) => out.push(e),
                    Err(e) => {
                        out.push(gap([c.asset_id]));
                        result = result.and(Err(e));
                    }
                }
            }
            result
        }
        Wire::MarketResolved { assets_ids } => {
            out.extend(assets_ids.into_iter().map(|s| Event::Closed {
                symbol: s.to_owned(),
            }));
            Ok(())
        }
        Wire::Other => Ok(()),
    }
}

/// Every `asset_id` in a frame that failed to parse, so its symbols can be
/// resynced. A frame without any carried no book data.
fn asset_ids(frame: &str) -> Vec<String> {
    fn collect(v: &serde_json::Value, out: &mut Vec<String>) {
        match v {
            serde_json::Value::Object(m) => {
                for (k, v) in m {
                    match v {
                        serde_json::Value::String(s) if k == "asset_id" => out.push(s.clone()),
                        _ => collect(v, out),
                    }
                }
            }
            serde_json::Value::Array(a) => a.iter().for_each(|v| collect(v, out)),
            _ => {}
        }
    }
    let mut out = Vec::new();
    if let Ok(v) = serde_json::from_str(frame) {
        collect(&v, &mut out);
    }
    out
}

/// Gamma sends explicit `null`s, which `#[serde(default)]` alone rejects.
fn null_as_default<'de, D: serde::Deserializer<'de>, T: Default + Deserialize<'de>>(
    d: D,
) -> Result<T, D::Error> {
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    #[serde(default, deserialize_with = "null_as_default")]
    active: bool,
    #[serde(default, deserialize_with = "null_as_default")]
    closed: bool,
    #[serde(default, deserialize_with = "null_as_default")]
    archived: bool,
    #[serde(default, deserialize_with = "null_as_default")]
    enable_order_book: bool,
    version: Option<String>,
    volume24hr: Option<f64>,
    created_at: Option<String>,
    /// Token ids for v1 (CTF) markets, as a JSON-encoded string array.
    clob_token_ids: Option<String>,
    /// Token ids for v2 markets. Per Polymarket's migration guide the id field
    /// is chosen by `version`, even when both are present. Often `null`.
    position_ids: Option<Vec<String>>,
    /// Outcome labels aligned with the ids, as a JSON-encoded string array.
    outcomes: Option<String>,
}

impl GammaMarket {
    fn is_live(&self) -> bool {
        // `acceptingOrders` is deliberately not required: pre-open and paused
        // markets keep their books and resume without notice.
        self.active && !self.closed && !self.archived && self.enable_order_book
    }

    /// The token whose book represents the market: "Yes" if labelled, else
    /// the first outcome (e.g. the home team).
    fn yes_token(&self) -> anyhow::Result<Option<String>> {
        let mut ids = match self.version.as_deref() {
            Some("v2") => self.position_ids.clone().unwrap_or_default(),
            _ => match &self.clob_token_ids {
                Some(ids) => serde_json::from_str(ids)?,
                None => Vec::new(),
            },
        };
        let outcomes: Vec<String> = match &self.outcomes {
            Some(o) => serde_json::from_str(o)?,
            None => Vec::new(),
        };
        let yes = outcomes
            .iter()
            .position(|o| o.eq_ignore_ascii_case("yes"))
            .unwrap_or(0);
        Ok((yes < ids.len()).then(|| ids.swap_remove(yes)))
    }
}

#[derive(Deserialize)]
struct GammaPage {
    markets: Vec<GammaMarket>,
    next_cursor: Option<String>,
}

impl Exchange for Polymarket {
    fn ws_url(&self) -> &str {
        &self.ws_url
    }

    /// About 1% of markets carry 95% of volume, and Polymarket closes sockets
    /// it cannot push fast enough (1013 "slow consumer") even with few busy
    /// tokens, so busy markets get small sockets and the long quiet tail
    /// shares large ones.
    fn symbols_per_connection(&self, volume_24h: f64) -> usize {
        match volume_24h {
            v if v >= 10_000.0 => 100,
            v if v > 0.0 => 1_000,
            _ => 4_000,
        }
    }

    fn max_batch(&self) -> usize {
        500
    }

    fn subscribe(&self, symbols: &[String], first: bool) -> String {
        if first {
            serde_json::json!({
                "assets_ids": symbols,
                "type": "market",
                "initial_dump": true,
                "custom_feature_enabled": true,
            })
        } else {
            serde_json::json!({ "assets_ids": symbols, "operation": "subscribe", "custom_feature_enabled": true })
        }
        .to_string()
    }

    fn unsubscribe(&self, symbols: &[String]) -> String {
        serde_json::json!({ "assets_ids": symbols, "operation": "unsubscribe" }).to_string()
    }

    fn heartbeat(&self) -> Option<&'static str> {
        Some("PING")
    }

    fn decode(&self, frame: &str, out: &mut Vec<Event>) -> anyhow::Result<()> {
        let parsed = match frame.as_bytes().first() {
            // The initial dump after a subscribe arrives as an array of books.
            Some(b'[') => serde_json::from_str::<Vec<Wire>>(frame),
            Some(b'{') => serde_json::from_str(frame).map(|msg| vec![msg]),
            // "PONG" and other non-JSON keepalive text.
            _ => return Ok(()),
        };
        match parsed {
            Ok(msgs) => {
                // Keep decoding past a bad message: one bad book in a dump
                // must not cost the books after it.
                let mut result = Ok(());
                for msg in msgs {
                    result = result.and(decode_one(msg, out));
                }
                result
            }
            Err(e) => {
                let symbols = asset_ids(frame);
                if !symbols.is_empty() {
                    out.push(gap(symbols));
                }
                Err(e.into())
            }
        }
    }

    /// A full pass splits the market list into end-date windows paged in
    /// parallel, since one Gamma cursor takes ~45 min for ~280k markets.
    /// Each window pages newest-first by id, an immutable key.
    /// `Since` pages newest-first and stops at the first older market.
    async fn discover(
        &self,
        http: &reqwest::Client,
        scope: Scope,
        out: &tokio::sync::mpsc::UnboundedSender<Vec<Listing>>,
    ) -> anyhow::Result<()> {
        let remaining = AtomicUsize::new(self.max_symbols.unwrap_or(usize::MAX));
        match scope {
            Scope::Since(since) => {
                paginate(http, "createdAt", None, Some(since), out, &remaining).await
            }
            Scope::Full => {
                let mut passes = futures_util::stream::iter(end_date_windows(Utc::now()))
                    // Page by an immutable key: ordering by a live figure
                    // such as volume lets markets move behind the cursor
                    // mid-pass, go unlisted twice, and be removed while live.
                    .map(|window| paginate(http, "id", Some(window), None, out, &remaining))
                    .buffer_unordered(DISCOVERY_PARALLELISM);
                while let Some(result) = passes.next().await {
                    result?;
                }
                Ok(())
            }
        }
    }

    async fn fetch_books(
        &self,
        http: &reqwest::Client,
        symbols: &[String],
    ) -> anyhow::Result<Vec<Event>> {
        let mut out = Vec::with_capacity(symbols.len());
        for batch in symbols.chunks(100) {
            let body: Vec<_> = batch
                .iter()
                .map(|id| serde_json::json!({ "token_id": id }))
                .collect();
            let text = http
                .post(CLOB_BOOKS)
                .json(&body)
                .send()
                .await?
                .error_for_status()?
                .text()
                .await?;
            for b in serde_json::from_str::<Vec<WireBook>>(&text)? {
                // A book we cannot decode is simply not checked.
                out.extend(book_event(b).ok());
            }
        }
        Ok(out)
    }
}

/// An end-date range; `None` is unbounded.
type Window = (Option<DateTime<Utc>>, Option<DateTime<Utc>>);

/// Contiguous `[min, max)` end-date windows covering all time, denser where
/// markets cluster (most end within days). Open-ended at both extremes.
fn end_date_windows(now: DateTime<Utc>) -> Vec<Window> {
    let mut edges = vec![now - TimeDelta::days(1)];
    edges.extend((0..14).map(|d| now + TimeDelta::hours(12 * d + 12)));
    edges.extend((1..=6).map(|w| now + TimeDelta::days(7 + 7 * w)));
    edges.extend((2..=6).map(|m| now + TimeDelta::days(30 * m)));
    edges.push(now + TimeDelta::days(365));
    let mut windows = vec![(None, Some(edges[0]))];
    windows.extend(edges.windows(2).map(|w| (Some(w[0]), Some(w[1]))));
    windows.push((edges.last().copied(), None));
    windows
}

/// Fetches one page, retrying transient failures so one flaky request does
/// not abort a pass that takes minutes.
async fn get_page(req: reqwest::RequestBuilder) -> anyhow::Result<GammaPage> {
    const ATTEMPTS: u32 = 4;
    let mut delay = std::time::Duration::from_millis(500);
    let mut attempt = 1;
    loop {
        let result = async {
            let resp = req
                .try_clone()
                .context("unclonable request")?
                .send()
                .await?
                .error_for_status()?;
            Ok::<_, anyhow::Error>(serde_json::from_slice::<GammaPage>(&resp.bytes().await?)?)
        }
        .await;
        match result {
            Err(e) if attempt < ATTEMPTS => {
                tracing::debug!(error = %e, attempt, "gamma page failed; retrying");
                tokio::time::sleep(delay).await;
                delay *= 2;
                attempt += 1;
            }
            result => return result,
        }
    }
}

/// Pages one keyset query to the end (or, for `since`, to the first market
/// created before it), streaming live listings to `out`.
async fn paginate(
    http: &reqwest::Client,
    order: &str,
    end_window: Option<Window>,
    since: Option<DateTime<Utc>>,
    out: &tokio::sync::mpsc::UnboundedSender<Vec<Listing>>,
    remaining: &AtomicUsize,
) -> anyhow::Result<()> {
    let mut cursor: Option<String> = None;
    loop {
        // Without explicit versions Gamma lists only v1 markets.
        let mut req = http.get(GAMMA).query(&[
            ("closed", "false"),
            ("limit", "100"),
            ("version", "v1"),
            ("version", "v2"),
            ("order", order),
            ("ascending", "false"),
        ]);
        if let Some((min, max)) = end_window {
            // Gamma treats both bounds as inclusive; a market on a boundary is
            // listed twice, which is harmless.
            if let Some(min) = min {
                req = req.query(&[("end_date_min", min.to_rfc3339())]);
            }
            if let Some(max) = max {
                req = req.query(&[("end_date_max", max.to_rfc3339())]);
            }
        }
        if let Some(c) = &cursor {
            req = req.query(&[("after_cursor", c)]);
        }
        let page = get_page(req).await?;
        let mut done = page.markets.is_empty();
        let mut listings = Vec::with_capacity(page.markets.len());
        for m in &page.markets {
            if let Some(since) = since {
                let created = m
                    .created_at
                    .as_deref()
                    .and_then(|t| DateTime::parse_from_rfc3339(t).ok());
                if created.is_some_and(|c| c.with_timezone(&Utc) < since) {
                    done = true;
                    break;
                }
            }
            let symbol = match m.yes_token() {
                Ok(symbol) => symbol,
                // One malformed market must not cost the rest of the page.
                Err(e) => {
                    tracing::warn!(error = %e, "skipping malformed market");
                    None
                }
            };
            if m.is_live()
                && let Some(symbol) = symbol
            {
                listings.push(Listing {
                    symbol,
                    activity: m.volume24hr.unwrap_or(0.0),
                });
            }
        }
        let left = remaining
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |r| {
                Some(r - r.min(listings.len()))
            })
            .unwrap();
        let exhausted = left <= listings.len();
        listings.truncate(left);
        if !listings.is_empty() {
            out.send(listings)?;
        }
        match page.next_cursor {
            Some(c) if !c.is_empty() && !done && !exhausted => cursor = Some(c),
            _ => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(frame: &str) -> Vec<Event> {
        let mut out = Vec::new();
        Polymarket::default().decode(frame, &mut out).unwrap();
        out
    }

    #[test]
    fn decodes_book_dump() {
        let events = decode(
            r#"[{"event_type":"book","asset_id":"A","market":"0x1","timestamp":"1700000000123",
                "hash":"h","bids":[{"price":"0.48","size":"30"}],"asks":[{"price":"0.52","size":"12.5"}]}]"#,
        );
        assert_eq!(
            events,
            [Event::Snapshot {
                symbol: "A".into(),
                exchange_ts: ts_from_millis(1_700_000_000_123).unwrap(),
                bids: vec![Level {
                    price: 480_000,
                    qty: 30_000_000
                }],
                asks: vec![Level {
                    price: 520_000,
                    qty: 12_500_000
                }],
            }]
        );
    }

    #[test]
    fn decodes_price_changes() {
        let events = decode(
            r#"{"event_type":"price_change","market":"0x1","timestamp":"1700000000456","price_changes":[
                {"asset_id":"A","price":"0.5","size":"0","side":"BUY","hash":"x","best_bid":"0.48","best_ask":"0.52"},
                {"asset_id":"B","price":"0.5","size":"7","side":"SELL","hash":"y","best_bid":"0.48","best_ask":"0.5"}]}"#,
        );
        let ts = ts_from_millis(1_700_000_000_456).unwrap();
        assert_eq!(
            events,
            [
                Event::Level {
                    symbol: "A".into(),
                    exchange_ts: ts,
                    side: Side::Buy,
                    price: 500_000,
                    qty: 0,
                    id: update_id("x"),
                },
                Event::Level {
                    symbol: "B".into(),
                    exchange_ts: ts,
                    side: Side::Sell,
                    price: 500_000,
                    qty: 7_000_000,
                    id: update_id("y"),
                },
            ]
        );
    }

    #[test]
    fn undecodable_book_data_resyncs_its_symbols_without_losing_the_rest() {
        let mut out = Vec::new();
        let result = Polymarket::default().decode(
            r#"{"event_type":"price_change","market":"0x1","timestamp":"1700000000456","price_changes":[
                {"asset_id":"A","price":"0.5","size":"0.1234567","side":"BUY","hash":"x"},
                {"asset_id":"B","price":"0.5","size":"7","side":"SELL","hash":"y"}]}"#,
            &mut out,
        );
        assert!(result.is_err());
        assert_eq!(out[0], gap(["A"]));
        assert!(matches!(&out[1], Event::Level { symbol, .. } if symbol == "B"));

        let mut out = Vec::new();
        let result = Polymarket::default().decode(
            r#"[{"event_type":"book","asset_id":"A","timestamp":"1","bids":"oops","asks":[]}]"#,
            &mut out,
        );
        assert!(result.is_err());
        assert_eq!(out, [gap(["A"])]);
    }

    #[test]
    fn picks_the_yes_token_by_version() {
        let market = |json: &str| {
            serde_json::from_str::<GammaMarket>(json)
                .unwrap()
                .yes_token()
                .unwrap()
        };
        assert_eq!(
            market(
                r#"{"version":"v1","clobTokenIds":"[\"y\",\"n\"]","positionIds":["p","q"],"outcomes":"[\"Yes\",\"No\"]"}"#
            ),
            Some("y".into())
        );
        assert_eq!(
            market(
                r#"{"version":"v2","clobTokenIds":"[\"y\",\"n\"]","positionIds":["n2","y2"],"outcomes":"[\"No\",\"Yes\"]"}"#
            ),
            Some("y2".into())
        );
        assert_eq!(
            market(r#"{"clobTokenIds":"[\"a\",\"b\"]","outcomes":"[\"Lakers\",\"Celtics\"]"}"#),
            Some("a".into())
        );
        assert_eq!(market(r#"{"outcomes":"[\"Yes\",\"No\"]"}"#), None);
        // Gamma sends explicit nulls, not just missing fields.
        assert_eq!(
            market(
                r#"{"version":"v1","volume24hr":null,"positionIds":null,"clobTokenIds":"[\"y\",\"n\"]","outcomes":null}"#
            ),
            Some("y".into())
        );
    }

    #[test]
    fn end_date_windows_are_contiguous_and_cover_everything() {
        let w = end_date_windows(Utc::now());
        assert_eq!(w.first().unwrap().0, None);
        assert_eq!(w.last().unwrap().1, None);
        assert!(w.windows(2).all(|p| p[0].1 == p[1].0 && p[0].1.is_some()));
        assert!(w.iter().all(|(a, b)| a.zip(*b).is_none_or(|(a, b)| a < b)));
    }

    #[test]
    fn ignores_keepalives_and_unknown_events() {
        assert!(decode("PONG").is_empty());
        assert!(decode(r#"{"event_type":"last_trade_price","asset_id":"A"}"#).is_empty());
    }
}

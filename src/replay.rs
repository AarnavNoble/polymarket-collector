//! Replays a collector output file: decodes each record with the template's
//! reader and applies it to that symbol's book, so research tools see every
//! book exactly as the collector wrote it.

use std::path::Path;

use chrono::{DateTime, Utc};
use rustc_hash::FxHashMap;

use crate::Side;
use crate::book::{Level, OrderBook};
use crate::util::dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain};
use crate::util::{AlignedSbeReader, OrderbookSchema};

/// One decoded record, already applied to [`Replay::book`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub symbol: String,
    pub received_at: DateTime<Utc>,
    pub exchange_ts: DateTime<Utc>,
    pub kind: Kind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A snapshot. `changed` counts the levels where it differs from the book
    /// replayed since the previous snapshot (`None` for a symbol's first).
    Snapshot { changed: Option<usize> },
    /// A level update. `orphan` means no snapshot of its symbol came first,
    /// which a correct file never contains.
    Update {
        side: Side,
        price: i64,
        qty: i64,
        orphan: bool,
    },
}

pub struct Replay {
    reader: AlignedSbeReader<OrderbookSchema>,
    books: FxHashMap<String, OrderBook>,
}

impl Replay {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(Self {
            reader: AlignedSbeReader::open(path)?,
            books: FxHashMap::default(),
        })
    }

    /// Reads the next record and applies it to its book.
    pub fn read(&mut self) -> anyhow::Result<Option<Record>> {
        let Some(msg) = self.reader.read_msg()? else {
            return Ok(None);
        };
        Ok(Some(match msg {
            AnyMessage::Snapshot(d) => {
                let s = SnapshotDomain::try_from_decoder(d)?;
                let bids: Vec<_> = s
                    .bids
                    .iter()
                    .map(|l| Level {
                        price: l.price_level,
                        qty: l.quantity,
                    })
                    .collect();
                let asks: Vec<_> = s
                    .asks
                    .iter()
                    .map(|l| Level {
                        price: l.price_level,
                        qty: l.quantity,
                    })
                    .collect();
                let book = OrderBook::from_levels(&bids, &asks);
                let changed = self
                    .books
                    .insert(s.symbol.clone(), book.clone())
                    .map(|old| old.diff(&book));
                Record {
                    symbol: s.symbol,
                    received_at: s.received_at,
                    exchange_ts: s.exchange_timestamp,
                    kind: Kind::Snapshot { changed },
                }
            }
            AnyMessage::PriceLevelUpdate(d) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(d)?;
                let orphan = match self.books.get_mut(&u.symbol) {
                    Some(book) => {
                        book.set(u.side, u.price_level, u.quantity);
                        false
                    }
                    None => true,
                };
                Record {
                    symbol: u.symbol,
                    received_at: u.backtest_timestamp,
                    exchange_ts: u.exchange_timestamp,
                    kind: Kind::Update {
                        side: u.side,
                        price: u.price_level,
                        qty: u.quantity,
                        orphan,
                    },
                }
            }
            _ => anyhow::bail!("unknown message type"),
        }))
    }

    /// The book of `symbol` as of the last record read.
    pub fn book(&self, symbol: &str) -> Option<&OrderBook> {
        self.books.get(symbol)
    }

    pub fn books(&self) -> &FxHashMap<String, OrderBook> {
        &self.books
    }
}

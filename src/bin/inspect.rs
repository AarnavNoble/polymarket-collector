//! Reads a collector output file back with the template's reader, replays
//! every update onto its book, and checks each snapshot against the book
//! replayed since the previous one. Snapshots that differ are corrections:
//! a newer book pushed by the exchange or a verification resync.
//!
//! Usage: inspect <file> [symbol]
//! With a symbol, prints that symbol's records instead, one JSON object per
//! line (timestamps in µs since the epoch, prices and sizes at 1e6 scale).

use engineering_challenge::book::{Level, OrderBook};
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};
use rustc_hash::FxHashMap;

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("usage: inspect <file> [symbol]"))?;
    let wanted = args.next();

    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path)?;
    let mut books: FxHashMap<String, OrderBook> = FxHashMap::default();
    let (mut snapshots, mut updates, mut consistent, mut corrections, mut orphans) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    while let Some(msg) = reader.read_msg()? {
        match msg {
            AnyMessage::Snapshot(d) => {
                let s = SnapshotDomain::try_from_decoder(d)?;
                snapshots += 1;
                if wanted.as_deref() == Some(s.symbol.as_str()) {
                    println!(
                        "{}",
                        serde_json::json!({
                            "kind": "snapshot",
                            "recv": s.received_at.timestamp_micros(),
                            "exch": s.exchange_timestamp.timestamp_micros(),
                            "bids": s.bids.iter().map(|l| [l.price_level, l.quantity]).collect::<Vec<_>>(),
                            "asks": s.asks.iter().map(|l| [l.price_level, l.quantity]).collect::<Vec<_>>(),
                        })
                    );
                }
                let book = OrderBook::from_levels(
                    &s.bids
                        .iter()
                        .map(|l| Level {
                            price: l.price_level,
                            qty: l.quantity,
                        })
                        .collect::<Vec<_>>(),
                    &s.asks
                        .iter()
                        .map(|l| Level {
                            price: l.price_level,
                            qty: l.quantity,
                        })
                        .collect::<Vec<_>>(),
                );
                match books.insert(s.symbol, book.clone()) {
                    Some(replayed) if replayed == book => consistent += 1,
                    Some(_) => corrections += 1,
                    None => {}
                }
            }
            AnyMessage::PriceLevelUpdate(d) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(d)?;
                updates += 1;
                if wanted.as_deref() == Some(u.symbol.as_str()) {
                    println!(
                        "{}",
                        serde_json::json!({
                            "kind": "update",
                            "recv": u.backtest_timestamp.timestamp_micros(),
                            "exch": u.exchange_timestamp.timestamp_micros(),
                            "side": u.side.to_string(),
                            "price": u.price_level,
                            "qty": u.quantity,
                        })
                    );
                }
                match books.get_mut(&u.symbol) {
                    Some(book) => {
                        book.set(u.side, u.price_level, u.quantity);
                    }
                    None => orphans += 1,
                }
            }
            _ => anyhow::bail!("unknown message type"),
        }
    }

    if wanted.is_some() {
        return Ok(());
    }

    println!("symbols:            {}", books.len());
    println!("snapshots:          {snapshots}");
    println!("updates:            {updates}");
    println!("updates before any snapshot of their symbol: {orphans}");
    println!("later snapshots matching the replayed book:  {consistent}");
    println!("later snapshots correcting the replayed book: {corrections}");
    Ok(())
}

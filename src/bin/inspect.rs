//! Reads a collector output file back with the template's reader, replays
//! every update onto its book, and checks each snapshot against the book
//! replayed since the previous one. Snapshots that differ are corrections:
//! a newer book pushed by the exchange or a verification resync.
//!
//! Usage: inspect <file> [symbol]
//! With a symbol, prints that symbol's records instead, one JSON object per
//! line (timestamps in µs since the epoch, prices and sizes at 1e6 scale).

use engineering_challenge::replay::{Kind, Replay};

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("usage: inspect <file> [symbol]"))?;
    let wanted = args.next();

    let mut replay = Replay::open(&path)?;
    let (mut snapshots, mut updates, mut consistent, mut corrections, mut orphans) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    while let Some(r) = replay.read()? {
        let shown = wanted.as_deref() == Some(r.symbol.as_str());
        let (recv, exch) = (
            r.received_at.timestamp_micros(),
            r.exchange_ts.timestamp_micros(),
        );
        match r.kind {
            Kind::Snapshot { changed } => {
                snapshots += 1;
                match changed {
                    Some(0) => consistent += 1,
                    Some(_) => corrections += 1,
                    None => {}
                }
                if shown {
                    let book = replay.book(&r.symbol).expect("just inserted");
                    println!(
                        "{}",
                        serde_json::json!({
                            "kind": "snapshot", "recv": recv, "exch": exch,
                            "bids": book.bids().map(|l| [l.price, l.qty]).collect::<Vec<_>>(),
                            "asks": book.asks().map(|l| [l.price, l.qty]).collect::<Vec<_>>(),
                        })
                    );
                }
            }
            Kind::Update {
                side,
                price,
                qty,
                orphan,
            } => {
                updates += 1;
                orphans += u64::from(orphan);
                if shown {
                    println!(
                        "{}",
                        serde_json::json!({
                            "kind": "update", "recv": recv, "exch": exch,
                            "side": side.to_string(), "price": price, "qty": qty,
                        })
                    );
                }
            }
        }
    }

    if wanted.is_some() {
        return Ok(());
    }

    println!("symbols:            {}", replay.books().len());
    println!("snapshots:          {snapshots}");
    println!("updates:            {updates}");
    println!("updates before any snapshot of their symbol: {orphans}");
    println!("later snapshots matching the replayed book:  {consistent}");
    println!("later snapshots correcting the replayed book: {corrections}");
    Ok(())
}

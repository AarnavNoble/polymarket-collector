//! Output sink. File I/O runs on a dedicated OS thread behind an unbounded
//! channel so a slow disk or pipe can never stall a websocket read loop; the
//! engine watches the backlog instead (see [`Sink::backlog`]).

use std::path::PathBuf;
use std::thread::JoinHandle;

use anyhow::Context;
use crossbeam::channel::{Sender, unbounded};

use crate::util::AlignedSbeWriter;
use crate::util::dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain};

/// The reader in `util` hard-codes 64 MiB chunks, so the writer must match.
pub const CHUNK_SIZE: usize = 64 << 20;

pub enum Record {
    Snapshot(SnapshotDomain),
    Update(PriceLevelUpdateDomain),
}

#[derive(Clone)]
pub struct Sink(Sender<Record>);

impl Sink {
    pub fn from_sender(tx: Sender<Record>) -> Self {
        Self(tx)
    }

    pub fn send(&self, record: Record) {
        // The writer thread only exits after every sender is dropped, or on an
        // I/O error that `WriterHandle::join` reports, so a failed send is
        // already surfaced there.
        let _ = self.0.send(record);
    }

    pub fn backlog(&self) -> usize {
        self.0.len()
    }
}

pub struct WriterHandle(JoinHandle<anyhow::Result<u64>>);

impl WriterHandle {
    /// Waits for the writer to drain, flush and close the file. Returns the
    /// number of records written. Every [`Sink`] must be dropped first.
    pub fn join(self) -> anyhow::Result<u64> {
        self.0
            .join()
            .map_err(|_| anyhow::anyhow!("writer thread panicked"))?
    }
}

pub fn spawn(path: PathBuf) -> anyhow::Result<(Sink, WriterHandle)> {
    let mut writer = AlignedSbeWriter::create(&path, CHUNK_SIZE)
        .with_context(|| format!("opening output {}", path.display()))?;
    let (tx, rx) = unbounded();
    let handle = std::thread::Builder::new()
        .name("writer".into())
        .spawn(move || {
            let mut written = 0u64;
            for record in rx {
                let result = match &record {
                    Record::Snapshot(m) => writer.write_msg(m),
                    Record::Update(m) => writer.write_msg(m),
                };
                match result {
                    Ok(()) => written += 1,
                    // A record that cannot be encoded is dropped, not the run.
                    Err(e) if e.downcast_ref::<std::io::Error>().is_none() => {
                        tracing::error!(error = %e, "dropping unencodable record");
                    }
                    // I/O failure: keep what is buffered and stop.
                    Err(e) => {
                        let _ = writer.flush();
                        return Err(e);
                    }
                }
            }
            // `flush` pads the final chunk; there is no flush-on-drop, so this is
            // the only point where the tail of the data reaches the file.
            writer.flush()?;
            Ok(written)
        })?;
    Ok((Sink(tx), WriterHandle(handle)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Side;
    use crate::util::dtos::challenge_orderbook::AnyMessage;
    use crate::util::{AlignedSbeReader, OrderbookSchema};
    use chrono::DateTime;

    #[test]
    fn records_survive_a_roundtrip() -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let (sink, handle) = spawn(file.path().to_owned())?;
        let ts = DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap();
        let update = PriceLevelUpdateDomain {
            backtest_timestamp: ts,
            exchange_timestamp: ts,
            price_level: 450_000,
            quantity: 10_000_000,
            side: Side::Sell,
            symbol: "TOKEN".into(),
        };
        for _ in 0..3 {
            sink.send(Record::Update(update.clone()));
        }
        drop(sink);
        assert_eq!(handle.join()?, 3);

        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path())?;
        let mut n = 0;
        while let Some(msg) = reader.read_msg()? {
            let AnyMessage::PriceLevelUpdate(decoded) = msg else {
                anyhow::bail!("unexpected message");
            };
            assert_eq!(PriceLevelUpdateDomain::try_from_decoder(decoded)?, update);
            n += 1;
        }
        assert_eq!(n, 3);
        Ok(())
    }
}

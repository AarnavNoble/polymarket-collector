# Polymarket order book collector

Records the full-depth order book of every live Polymarket market. It writes level updates, plus a snapshot of every book at each wall-clock minute, to stdout or a file through the template's `AlignedSbeWriter`. Research tools replay those files to measure the market and backtest a market maker; see [FINDINGS.md](FINDINGS.md) for what 20 minutes of the whole exchange shows.

```sh
cargo run --release -- --output data.sbe          # all live markets; logs on stderr
cargo run --release -- --max-symbols 500          # small run (writes to stdout)
cargo run --release -- --dashboard 127.0.0.1:8080 # live view of the collector in a browser
cargo run --release -- --exchange vpx             # Vancouver Prediction Exchange (needs VPX_API_KEY)
cargo run --release --bin inspect -- data.sbe     # read a file back and self-check it
cargo run --release --bin analyze -- data.sbe     # spreads, depth, update rates, feed quality
cargo run --release --bin backtest -- data.sbe    # market-making backtest under two fill models
cargo run --release --bin latency -- --conns 8    # connection race experiment (LATENCY.md)
cargo test
```

On Ctrl-C (or SIGTERM), every websocket sends a close frame, the engine drains what it has received, and the writer flushes and closes the file. A second Ctrl-C forces an exit.

**Why Polymarket:** Kalshi's sign-up requires a SIN from Canadian residents. The exchange-specific code is one file behind a trait (see [Exchange boundary](#exchange-boundary)).

## Output

- **Symbol:** the market's YES token id. A binary market's NO book mirrors its YES book exactly, so collecting both would add nothing. Markets without a "Yes" outcome (sports) use their first outcome.
- **Prices and sizes:** parsed from decimal strings straight into `i64` at 1e6 scale, never through floats. A value that can't be represented exactly is rejected, and its symbol is resynced.
- **`PriceLevelUpdate`:** the absolute quantity at a level (0 removes it). No-op updates are suppressed.
- **Timestamps:** `backtest_timestamp`/`received_at` is the local clock, read as soon as the websocket library returns a frame. For an update that arrives on both connections, it is the earlier arrival. `exchange_timestamp` is Polymarket's, in ms. For a book, Polymarket's timestamp is the book's last-change time.
- **Snapshots** are written on a symbol's first book, when the exchange pushes a newer book that differs from ours, after an outage or resync, and every minute. An update is always preceded by the book it applies to.

## Architecture

```
      discovery ──► manager ── subscribe / unsubscribe ──► shard: leg A, leg B (websockets)
                       ▲                                          │ timestamped events
                       └── resync / reconnect ──── engine (all books) ◄── REST verification
                                                          │
                                                   writer thread → AlignedSbeWriter
```

| Module | Role |
|---|---|
| `exchange/` | The `Exchange` trait and the Polymarket implementation. |
| `connection.rs` | One websocket leg: paced connect, batched subscribe, keepalive, stall detection, write deadlines, jittered backoff, periodic recycling. |
| `manager.rs` | Discovery loops, sharding, and routing of resync and reconnect requests. |
| `engine.rs` | Owns every book: merges the legs, writes snapshots, polices completeness. Single task, no locks. |
| `writer.rs` | Dedicated OS thread, so file I/O never stalls a socket read. |
| `book.rs`, `fixed.rs` | Sorted-`Vec` book (3x smaller than a `BTreeMap` across ~280k books) and exact decimal parsing. |
| `dashboard.rs` | Optional local web view: connections, throughput, verification, and the busiest books with live depth. |
| `poll.rs`, `exchange/vpx.rs` | REST-only exchanges: consecutive polled books are diffed into the same level updates a websocket produces. |
| `replay.rs`, `backtest.rs`, `bin/` | Research side: replay a file book by book, analyze it, backtest a strategy on it. |

**Discovery.** Polymarket lists about 280k markets through a paged cursor, and one cursor takes about 45 min. A full pass therefore splits the list into 26 end-date windows paged by 8 parallel cursors, ordered by id so markets can't slip behind the cursor mid-pass. Each page is subscribed as soon as it arrives. A second loop polls for new listings every 30 s. A symbol is removed only after two consecutive completed passes miss it, and a pass that lists fewer than half the held symbols removes nothing.

**Sharding.** Polymarket drops sockets it can't push fast enough (`1013 slow consumer`), so shards are sized by 24h volume: 100 markets per socket at ≥ $10k, 1,000 for any volume, and 4,000 for none. About 1% of markets carry 95% of the volume. All connection attempts share one pacer (4/s), because Polymarket throttles bursts of upgrades from one IP (`LATENCY.md`).

## Redundancy: first arrival wins

Every shard runs two connections ("legs") with identical subscriptions, and each leg keeps its own copy of each book. The engine keeps one more book per symbol, the *output*, which is what gets written. Every update is applied to it once, from whichever leg delivered it first, so each update gets the faster connection's latency. Losing a leg costs nothing while its sibling is synced. Only when both legs are lost is the output dropped until the next snapshot.

The merge is safe because Polymarket updates set a level's *absolute* quantity: updates to different levels commute, so order only matters within a level. Each level remembers the exchange time of its last update, plus the identities (side, price, size, Polymarket's book hash) of every update applied in that millisecond. A copy that is older, or already seen, is dropped. A book pushed mid-stream replaces the output only if it is strictly newer and differs.

Measured on the 2,000 busiest markets over about 3 minutes: 562,351 updates written, 553,191 slower copies discarded.

## Completeness policing

Polymarket has no sequence numbers, so gaps must be inferred:

1. **Missing books.** Every subscribed symbol must get a book on every leg within 10 s. If it doesn't, it is resynced. That means unsubscribe then subscribe, because a plain re-subscribe returns `NO NEW ASSETS`. If a leg keeps failing while its sibling serves the symbol, the leg is reconnected.
2. **Frozen sockets.** Some Polymarket sockets keep answering `PING` but stop delivering data. A leg that sees no book changes for 60 s while its sibling does is reconnected.
3. **REST verification.** Every 10 s, the 200 books that have gone longest unverified are compared with REST `POST /books`. Quiet books are where a lost update would hide longest, so this doubles as the low-activity monitor.
   - REST is cached, so an older reference proves nothing.
   - A newer reference only counts once its change is more than 2 s old.
   - A first mismatch is only a suspicion, since REST can capture the middle of a same-millisecond burst. It is confirmed only if it persists on the next check.

**Finding: Polymarket's update stream drifts from its own snapshots.** Confirmed mismatches were 0 in 14,400 checks across all markets, but about 0.4–0.5% of checks on the busiest ones. In every case (17 of 17), independent connections agreed with each other and a fresh snapshot sided with REST. So the stream itself occasionally omits a change, identically for every subscriber, and only REST verification can catch it. On confirmation, every leg is resynced and the fresh snapshot corrects the record.

## Results

12 minutes against the whole live universe (2026-10-23, home connection, Toronto):

| | |
|---|---|
| Markets subscribed | 156,683 (discovery still streaming toward ~280k) |
| Connections | 131 |
| Records written | 4.97M (832 MB) |
| Memory | 353 MB (~2.3 KB per market) |
| CPU | ~25% of one core |
| REST checks | 14,400, 0 confirmed mismatches |
| Server-side resets | 59, all recovered |
| Ctrl-C to file closed | 1 s |

This run predates first-arrival merging and connection pacing, which were measured separately on the busiest 2,000 markets (above).

## Exchange boundary

Everything outside `src/exchange/` is exchange-agnostic:

```rust
trait Exchange {
    fn ws_url(&self) -> &str;
    fn symbols_per_connection(&self, activity: f64) -> usize;
    fn subscribe(&self, symbols: &[String], first: bool) -> String;
    fn unsubscribe(&self, symbols: &[String]) -> String;
    fn heartbeat(&self) -> Option<&'static str>;
    fn decode(&self, frame: &str, out: &mut Vec<Event>) -> Result<()>;
    async fn discover(&self, http, scope: Scope, out: &Sender<Vec<Listing>>) -> Result<()>;
    async fn fetch_books(&self, http, symbols: &[String]) -> Result<Vec<Event>>; // optional
}
```

VPX has no websocket, so it implements a smaller `PolledExchange` trait (list markets, fetch a book) instead; the poller turns its books into the same events, and everything downstream is unchanged.

An exchange decodes frames into `Event`s: `Snapshot`, `Level` (an absolute quantity), `Closed`, and `Gap` (lost data, such as a sequence break or an undecodable frame). Each `Level` carries an `id` equal on every connection, which the merge uses to recognise copies. A Kalshi adapter would add handshake signing, map `seq` breaks to `Gap`, and add a delta event for Kalshi's signed changes. The integration test runs the real manager, legs and engine against a mock exchange with a different wire format.

## Testing

`cargo test` runs 37 unit tests and 1 integration test. They cover:
- property tests for decimal parsing and the book;
- Polymarket decoding, including malformed entries;
- a writer round trip through the template's reader;
- the merge: first arrival wins, a lagging leg never rewinds a level, rebasing, losing one or both legs;
- completeness policing: resync escalation, frozen legs, and every REST verification rule;
- the backtest's latency, both fill models and markouts; VPX decoding and book diffing.

`tests/failover.rs` kills one leg of a local websocket server mid-stream and checks that the surviving leg's update is written exactly once and that shutdown completes.

## Known limitations

- **Crash durability:** the template's reader requires 64 MiB chunks, so the writer flushes only at shutdown, and a hard crash loses the unflushed tail. Run under a supervisor with a new output path per run. Every file starts with a snapshot of each book.
- **Bandwidth:** the whole exchange is 90–130 Mbps of uncompressed JSON at peak, which saturates a home connection and pushes the delivery-latency tail to tens of seconds (see [FINDINGS.md](FINDINGS.md#delivery-latency-the-bottleneck-was-the-house-not-the-exchange)). Run it on a datacenter link.
- **Per-update verification:** Polymarket's per-update book hash could verify every update, not just REST samples. That requires the original price strings plus trade events, and is the natural next step.
- **Writer queue** is monitored (`backlog` in the logs) but unbounded.
- **Full discovery pass** takes about 20 min, limited by the densest end-date windows.

`src/util/`, `sbe_schemas/` and `src/side.rs` are the challenge template, unmodified.

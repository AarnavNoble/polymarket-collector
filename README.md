# Polymarket order book collector

This collector records every live Polymarket market's full-depth order book. It writes level updates, plus a snapshot of every book at each wall-clock minute, to stdout or a file, in the challenge's SBE format, using the provided `AlignedSbeWriter`.

```sh
cargo run --release -- --output data.sbe          # all live markets; logs on stderr
cargo run --release -- --max-symbols 500          # small run for testing (writes to stdout)
cargo run --release --bin inspect -- data.sbe     # read a file back: counts and samples
cargo run --release --bin latency -- --conns 8    # connection race experiment (LATENCY.md)
cargo test
```

Ctrl-C (or SIGTERM) shuts down in order:
1. Every websocket sends a close frame.
2. The engine drains everything already received.
3. The writer flushes the final chunk and closes the file.

The writer is joined even if a task panicked. A second Ctrl-C forces an exit.

**Why Polymarket:** Kalshi's sign-up requires a SIN from Canadian residents, so this uses the challenge's accepted alternative. The exchange-specific code is one file (`src/exchange/polymarket.rs`) behind a trait; see [Exchange boundary](#exchange-boundary).

## Output

- **Symbol:** one per market, the market's **YES token id**.
  - Every Polymarket outcome token has its own two-sided book.
  - A binary market's NO book is an exact mirror of its YES book (bid `p` ⇔ ask `1−p`, same size), so collecting both would double the data and add nothing.
  - The YES book is emitted as-is: bids are `Buy` and asks are `Sell`. Sports markets have no "Yes" outcome, so their first outcome is used.
- **Prices and sizes:** parsed straight from decimal strings into `i64` at 1e6 scale, never through floats (`src/fixed.rs`). A string that cannot be represented exactly is rejected, and its symbol is resynced rather than left with a missing update; the rest of the frame is still decoded.
- **`PriceLevelUpdate`:** carries the absolute quantity at a level (0 removes it). No-op updates are suppressed.
- **Timestamps:**
  - `backtest_timestamp` / `received_at` is the local wall clock, read as the first statement after the websocket library returns a frame, before our own decoding.
  - For an update merged from redundant connections it is the *earliest* arrival (see below).
  - `exchange_timestamp` is Polymarket's event timestamp in ms. For a `book` this is the time of the book's last change, so an initial snapshot's exchange time can be minutes old.
  - Minute snapshots use the newest exchange timestamp applied to the book.
- **Snapshots:** a snapshot is written:
  - when a symbol's book is first received;
  - when the exchange pushes a newer book that differs from ours (Polymarket does after trades);
  - after an outage or a verification resync;
  - at every wall-clock minute.

  So an update is always preceded by the book it applies to.

## Architecture

```
          discovery (full pass + new-listings poll)
                          │ listings
                          ▼
      manager ── Subscribe/Unsubscribe/Reconnect ──► shard k: leg A, leg B   (websocket tasks)
          ▲                                                    │ timestamped, decoded events
          │ Resync / Reconnect / Closed                        ▼
          └──────────────────────────────────────────── engine (all books)  ◄── verify (REST)
                                                               │ records
                                                               ▼
                                                  writer thread → AlignedSbeWriter
```

| Module | Role |
|---|---|
| `exchange/` | The `Exchange` trait (discover, encode commands, decode frames, fetch reference books) and the Polymarket implementation. |
| `connection.rs` | One websocket leg: paced connect, subscribe in batches, timestamp and decode frames, keepalive, dead-connection detection, 5 s deadline on every write, jittered exponential backoff, periodic recycling. If the engine's queue stays full for 500 ms, the leg gives up its connection and resyncs rather than dropping frames silently. |
| `manager.rs` | Discovery loops, activity-tiered sharding, and routing of resync and reconnect requests. |
| `engine.rs` | Owns every book: merges the legs, writes minute snapshots, polices completeness (below). Single task, no locks. |
| `verify.rs` | Fetches REST reference books for the engine off the hot path. |
| `writer.rs` | A dedicated OS thread behind a channel, so file I/O never stalls a socket read. An unencodable record is skipped; an I/O error flushes what it can and ends the writer. |
| `book.rs`, `fixed.rs` | Price-level book (a sorted `Vec` per side: 3x less memory than a `BTreeMap` across ~280k books, and cache-friendly) and exact decimal parsing. |

### Discovery

Polymarket lists about 280k live markets through a sequential cursor (Gamma `markets/keyset`, 100 per page). One cursor takes about 45 min for a full pass, so:
- **Full pass:** split into 26 end-date windows paged by 8 parallel cursors, which stays well under Gamma's limit of 300 requests per 10 s. Every page is subscribed as soon as it arrives. Failed pages are retried with backoff, and a malformed market is skipped rather than failing its page.
  - Windows are paged by market id, never by a live figure such as volume. A market whose ranking changes mid-pass could otherwise slip behind the cursor, go unlisted, and be removed while live.
- **New listings:** a separate loop polls newest-first every 30 s until it reaches markets it has already seen. New markets are picked up within about a minute instead of waiting for the next full pass.
- **Removals:** a symbol is unsubscribed only after it is absent from two consecutive *completed* full passes, because Gamma is cached for up to 5 min. A failed pass, or one that lists fewer than half the symbols held, removes nothing.
- **Versions:** queries always pass `version=v1&version=v2`. Without it, Gamma silently omits every Protocol V2 market.

### Sharding and connection pacing

Polymarket has no per-connection subscription cap, but it disconnects sockets whose message rate it can't push (`1013 slow consumer`), even when they carry only a few busy markets. Shards are therefore sized by 24h volume:

| 24h volume | Markets per socket |
|---|---|
| ≥ $10k | 100 |
| > 0 | 1,000 |
| none | 4,000 |

- About 1% of markets carry 95% of the volume.
- Live symbols are never moved between shards, so rebalancing never opens a gap.
- Every connection attempt, across all legs, goes through one pacer (default 4/s). Polymarket throttles bursts of websocket upgrades from one IP (measured, see `LATENCY.md`), and both startup and a mass disconnect would otherwise produce exactly such a burst.

### Redundancy: first arrival wins

- Every shard runs `--legs` (default 2) connections with identical subscriptions. Each leg keeps its own copy of every book.
- The engine keeps one more book per symbol, the *output*, which is what is written: every leg's updates merged, each applied **once, from whichever leg delivered it first**. Connections differ by several ms (`LATENCY.md`), so every update takes the fastest connection's latency.
- Losing a leg costs nothing while a sibling is synced: the survivor keeps feeding the same output, and no extra snapshot is needed because the output never stopped being current. Only when the last synced leg is lost is the output dropped and restarted from the next snapshot.

**Why the merge is safe.** Polymarket updates set a level's *absolute* quantity, so updates to different levels commute; merge order can only matter within a level. The output records, per level, the exchange time of the last update it applied:
- A copy older than that, or older than the book the output was last rebased on, is stale and dropped.
- A copy from the same millisecond is recognised by identity (side, price, size, and Polymarket's per-update book hash), checked against every update applied at that level in that millisecond.
- Books pushed mid-stream rebase the output only if they are strictly newer and differ. Then a lagging leg's copy of the same book, or an older one, can't rewind it.

Measured (busiest 2,000 markets, about 3 min): 562,351 updates written, 553,191 copies from the slower leg discarded. Every confirmed REST mismatch (17 of 17) matched the book one leg had built from its own stream alone, so the merge introduced none of them; see below.

Legs reconnect with jittered exponential backoff (up to 30 s). Each leg is also recycled after about 6 h, with ±25% jitter so the two legs of a shard rarely recycle together; even when they do, the merge needs only one synced leg.

### Completeness policing

Polymarket has no sequence numbers, so gaps must be inferred. The engine checks four things:

1. **Every subscribed symbol must get a book on every leg within 10 s.** If it doesn't, it is resynced (unsubscribe + subscribe, because a plain re-subscribe returns `NO NEW ASSETS` with no book). Retries back off exponentially. If a leg still fails while a sibling *does* serve the symbol, the leg is broken and is reconnected. If no leg can get it, the symbol is retried without disturbing its neighbours.
2. **Frozen sockets.** Polymarket has a known failure mode where a socket keeps answering `PING` but stops delivering data. A leg that receives no book changes for 60 s while its sibling (same symbols) does is treated as frozen and reconnected. Book dumps don't count as activity, so a quiet shard's legs can't take turns looking frozen to each other. Separately, any frame at all, PONGs included, resets the 30 s dead-connection timer.
3. **REST verification, stalest first.** Every 10 s, the 200 served books that have gone longest unverified are fetched from REST `POST /books` and compared level by level. Quiet books are exactly where a lost update would go unnoticed longest, so this doubles as the low-activity monitor (extension 2). The comparison rules account for how Polymarket behaves, as measured:
   - A book's `timestamp` is its last-change time, identical on the websocket and REST. Equal timestamps must mean equal books.
   - REST is served from a cache, so an older reference proves nothing. A newer reference only proves a gap once its change is more than 2 s old, beyond any in-flight window.
   - Several changes share one millisecond and REST can capture the middle of such a burst, so a first disagreement is only a *suspicion*. It is rechecked next round and confirmed only if it persists while our book is unchanged.
4. **Discovery safety:** see above.

**Finding: Polymarket's incremental feed drifts from its own snapshots.** Confirmed mismatches are rare on quiet markets (0 in 14,400 checks across the full universe) but reach about 0.4–0.5% of checks on the 2,000 busiest markets. In those cases:
- The book built from updates agrees across *independent* connections but not with REST.
- A fresh websocket snapshot sides with REST.

So the update stream occasionally omits a change that the snapshots include, identically for every subscriber. Socket redundancy cannot catch this; REST verification can. On confirmation, every leg is resynced, and nothing is written for that symbol until the fresh snapshot arrives, which then corrects the record.

Counters are logged with every minute snapshot:
- `written` and `duplicates`
- `verified`, `suspects`, `mismatches` and `legs_agreed`
- `resyncs` and `reconnects`
- `failovers`: a leg was lost while a sibling stayed synced.
- `outages`: the last synced leg was lost.

## Results from live runs

A 12-minute run against the whole live universe (2026-10-23, home connection in Toronto):

| | |
|---|---|
| Markets subscribed after 12 min | 156,683 (discovery still streaming; about 280k total) |
| Connections | 131 (2 legs per shard) |
| Records written | 4.97M (832 MB) |
| Memory | 353 MB, about 2.3 KB per market (about 650 MB projected at 280k) |
| CPU | about 25% of one core |
| Books verified against REST | 14,400, 0 confirmed mismatches |
| Server-side connection resets | 59, all recovered automatically |
| Overdue-book resyncs / leg reconnects | 158 / 23 |
| Ctrl-C to file closed | 1 s |

That run predates first-arrival merging and connection pacing. Those were measured separately on the 2,000 busiest markets (above).

## Exchange boundary

Everything outside `src/exchange/` is exchange-agnostic. An exchange implements:

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

- **`Event` variants:** `Level` covers exchanges that publish absolute levels (Polymarket) and `Delta` those that publish signed changes (Kalshi). `Gap` covers exchange-signalled gaps, such as a Kalshi `seq` break.
- **Update identity:** `Level` and `Delta` carry an `id`, an identity that is equal on every connection (Polymarket: the book hash; Kalshi: its cross-connection `clock`). The merge uses it to recognise copies.
- **The boundary is tested:** the integration test (`tests/failover.rs`) runs the real manager, legs and engine against a mock exchange with a different wire format.
- **A Kalshi adapter** would add RSA-PSS signing of the websocket handshake, map `seq` breaks to `Event::Gap`, and fill `id` from `clock`.

## Testing

`cargo test` runs 30 unit tests and 1 integration test:
- **Fixed-point parsing:** exact cases, rejected inputs, and a property test that round-trips any value.
- **Books:** a property test against a naive `BTreeMap` model, best-first ordering, and delta clamping.
- **Polymarket decoding:** book dumps, batched `price_change` entries, undecodable entries resyncing only their symbol, keepalive and unknown events, YES-token selection by market version (including Gamma's explicit `null`s, which once stalled a full pass), and end-date window coverage.
- **Writer:** a round trip through the template's reader.
- **Engine merge:**
  - each update is written once, from whichever leg is first;
  - a lagging leg never rewinds the output, including same-millisecond copies arriving after a long burst;
  - a rebase is never followed by older updates;
  - newer books rebase once, while older or same-instant books don't;
  - losing one leg costs nothing;
  - losing every leg waits for a fresh snapshot;
  - updates before a snapshot are never written.
- **Engine policing:**
  - overdue-book resync escalating to reconnect;
  - dead symbols never cause reconnects;
  - frozen-leg detection;
  - the REST verification rules: suspicion, confirmation, and stale or in-flight references.
- **`tests/failover.rs`:** a local websocket server kills one leg mid-stream. It asserts that the output starts with the book, that the surviving leg's update is written exactly once, and that shutdown completes.

## Known limitations

- **Crash durability:** the template's reader requires 64 MiB chunks, and `flush()` pads to a full chunk, so the writer flushes only at shutdown. A hard crash loses the unflushed tail (up to 64 MiB, typically minutes of data). Run under a supervisor such as systemd `Restart=always` and give every run a new output path, because `create` truncates. Each run starts with a snapshot of every book, so files are self-contained.
- **Shard assignment:** based on 24h volume at discovery time. A market that suddenly turns busy stays on its large shard. A 1013 disconnect is covered by the sibling leg and a reconnect, but no symbols are moved.
- **Per-update hash verification:** Polymarket's per-update SHA-1 book hash could verify every update as it arrives, not just REST samples. That needs the original price strings, plus `last_trade_price`, which can reach the hash before its own event arrives, and updates split across messages. It is the natural next step.
- **Writer backpressure:** the writer's queue is only monitored (`backlog` in the logs), not bounded. A consumer that permanently reads stdout slower than the feed would grow memory.
- **Output volume:** about 280k books × a minute snapshot each is several GB/hour. This follows the spec, but `--max-symbols` exists for local runs.
- **Discovery pass time:** about 20 min, limited by the densest end-date windows. Splitting dense windows further would shorten it.

## Repository notes

`src/util/`, `sbe_schemas/` and `src/side.rs` are the challenge template, unmodified. The schema is in `sbe_schemas/engineering_challenge/challenge_orderbook.yaml`.

# Latency findings (Polymarket)

All measurements were taken on 2026-10-23 from a home connection in Toronto (macOS, Wi-Fi), with the `latency` binary (`src/bin/latency.rs`) and curl. Each item is labelled **measured** or **hypothesis**.

## Where the feed comes from (measured)

```
client ─► Cloudflare anycast (104.18.34.205 / 172.64.153.51, colo YYZ)
       ─► AWS CloudFront (POP IAD for websocket upgrades)
       ─► Envoy/Istio origin (region unconfirmed; community folklore says eu-west-2)
```

- TCP connect to the Cloudflare edge takes 9–17 ms, and the TLS 1.3 handshake about 15–40 ms more.
- The **websocket upgrade takes 0.5–0.9 s.** It is dominated by the edge→origin hop (Cloudflare's `cfOrigin` timing showed about 380 ms on one upgrade).
- Opening a connection is therefore expensive. The collector keeps connections for hours and recycles each leg on its own jittered timer. While one leg reconnects, its sibling keeps the merged output current.

## How to measure without synchronized clocks

- Every `price_change` entry carries `(asset_id, timestamp, hash)`, and the hash is the SHA-1 of the book after the update. That key identifies the same logical update on every connection.
- So we open K websockets to the same tokens and, for each key, record which connection delivered it first and how far behind the others were.
- This uses only the local monotonic clock, so it needs no clock sync. This machine's wall clock was 70–90 ms behind NTP, ±47 ms (`sntp`), which makes absolute "exchange → receive" figures meaningless here.

## Experiment 1: 8 identical connections, alternating between the two Cloudflare IPs (measured)

20 most-traded markets, 180 s, 108,620 updates seen by all 8 connections:

| conn | IP | TCP ms | TLS+upgrade ms | first to deliver | lag behind fastest, p50 | p99 |
|---|---|---|---|---|---|---|
| 0 | 104.18.34.205 | 11.2 | 801 | **48.0%** | 0.00 ms | 704 ms |
| 1 | 172.64.153.51 | 9.0 | 856 | 3.8% | 3.99 ms | 396 ms |
| 2 | 104.18.34.205 | 11.3 | 854 | 6.4% | 7.00 ms | 244 ms |
| 3 | 172.64.153.51 | 12.8 | 820 | **33.4%** | 0.04 ms | 397 ms |
| 4 | 104.18.34.205 | 12.8 | 852 | 3.0% | 7.39 ms | 361 ms |
| 5 | 172.64.153.51 | 12.9 | 856 | 1.0% | 9.65 ms | 425 ms |
| 6 | 104.18.34.205 | 12.6 | 852 | 1.3% | 8.38 ms | 537 ms |
| 7 | 172.64.153.51 | 12.8 | 856 | 3.1% | 6.59 ms | 282 ms |

Findings:
- **Connections are not equal.** Two of the eight won 81% of the races, and the other six typically trailed by 4–10 ms. The difference is in the connection itself, not in this machine's path to the edge: TCP connect times are within 4 ms of each other.
- **The fast connections were on different Cloudflare IPs.** So the IP you dial is not the lever; which origin backend (or CloudFront route) the connection lands on is. That fits the path above, where all the variance is past the edge, and it means TCP source-port tricks can't do much here. The source port only affects the client→Cloudflare hop, which is anycast and short.
- **The tails are large** (p99 of 250–700 ms behind the fastest) on every connection, including the fast ones. Bursts on a busy market are queued per connection.

## Experiment 2: connecting again right after a burst (measured)

- I opened 8 more connections, all pinned to one IP, minutes after the collector had run 131 connections and experiment 1 had opened 8.
- **6 of the 8 upgrades did not complete within 15 s**, and the 2 that did took about 6.2 s.
- So the service (Cloudflare or the origin) throttles bursts of websocket upgrades from one IP.
- Of the two survivors, one again won most races (76%).

Consequences for the collector:
- Reconnect backoff is jittered and grows to 30 s, so a mass disconnect doesn't become a reconnect storm.
- Recycling happens one leg at a time on about 6 h jittered timers.
- Every connection attempt goes through one process-wide pacer (`--connects-per-sec`, default 4), so startup and mass reconnects never burst.

## What this means for the collector design

1. **Racing redundant connections pays off, and the collector does it.** The engine writes each update from whichever leg delivers it first and drops the other legs' copies (README, "Redundancy: first arrival wins"). The output's receive time is therefore the minimum over legs.
   - Per experiment 1, that saves several ms at the median versus a fixed leg, and far more at the tail.
   - On the 2,000 busiest markets, the slower leg's copy of 553,191 out of 562,351 updates was discarded. Both legs deliver almost everything, and only the first arrival counts.
2. **Re-roll slow connections (hypothesis, supported by experiment 1).** Since connection quality varies per connection and persists while it stays open, a collector can run N > 2 legs, score each by its win rate, and periodically reconnect the worst. Experiment 2 shows this must be rate-limited, or the reconnects get throttled.
3. **Connecting right after maintenance (hypothesis).** Polymarket announces matching-engine restarts (status page, Discord). After one, every client reconnects and the origin's connection-to-backend assignment is reset. Connecting first could land on the least-loaded backends, but it would be competing with the reconnect storm that experiment 2 shows is throttled. To test it: race connections opened in the first minute after a restart against connections opened an hour later, using the method above.
4. **Hosting.**
   - The edge is anycast, so any region reaches a nearby Cloudflare POP. What matters is distance to the origin.
   - The WS upgrade was routed via CloudFront IAD (us-east-1), which suggests running the collector in us-east-1. The origin might really be in eu-west-2, as rumoured.
   - Experiment to run: the same race from EC2 in us-east-1 and eu-west-2, comparing first-arrival shares across the two hosts. It needs no clock sync if both legs stream into one process over a private link. Otherwise use chrony with Amazon Time Sync (±0.1 ms), and the absolute `exchange_ts → recv` figure becomes meaningful.
5. **Cheap socket settings (already applied).**
   - `TCP_NODELAY` is set on every collector connection; it only affects what we send (PINGs and subscribes).
   - permessage-deflate is not negotiated (tungstenite has no support), so there is no inflate cost per frame.
   - The read loop only timestamps, decodes and forwards. The engine and file I/O run elsewhere, so the receive timestamp is taken as soon as the websocket library returns the frame, before our decoding.

## Reproduce

```sh
cargo run --release --bin latency -- --conns 8 --tokens 20 --secs 180 --pin split   # alternate IPs
cargo run --release --bin latency -- --conns 8 --tokens 20 --secs 120 --pin a       # one IP
```

`--pin` accepts `a` (first resolved IP), `b` (second), `split` (alternate between them), or anything else (resolve per connection).

## Next steps

- A connection scorer: run N > 2 legs, track each leg's first-arrival share, and recycle the slowest through the pacer.
- The same race from AWS us-east-1 vs eu-west-2, with chrony, to place the origin and measure true one-way latency.

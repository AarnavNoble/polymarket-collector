# Findings: 20 minutes of every Polymarket book

One recording of the whole exchange (2026-10-26, 13:25–13:45 UTC, home connection in Toronto), replayed with `analyze` and `backtest`. Every number below comes from those two tools on that file.

- **Activity is concentrated and hard to predict.** 1% of markets carry 59% of all book updates, led by 15-minute crypto contracts and live esports, and 24-hour volume doesn't predict which.
- **Latency tails were the link, not the exchange.** Delivery latency reached minutes. Four experiments ruled out the collector and Polymarket and pinned it on a saturated downlink. One plausible fix (rate-based re-sharding) was tested, made it worse, and was reverted.
- **Book data can't evaluate a market maker.** Without trade prints, the same strategy on the same data lands anywhere from −$542 to +$20k depending on how cancels are interpreted. Recording trades is the next step.

## The market

| | |
|---|---|
| Books recorded | 195,606 |
| Books that changed at all | 76,864 (39%) |
| Level updates | 9.59M (8,150/s) |
| Share of updates in the busiest 1% of markets | 58.8% |
| Median time-weighted spread, books that changed | 6.0¢ |

Activity is extremely concentrated, and the busiest markets are not the ones with the most 24-hour volume. They are short-dated crypto contracts ("Bitcoin Up or Down, 9:30–9:45 AM ET") and live esports maps, whose books reprice on every tick of the underlying:

| Market | Updates/min | Peak/s | Spread | Touch depth | Mid moves |
|---|--:|--:|--:|--:|--:|
| Bitcoin Up or Down, 9:30–9:45 AM ET | 6,568 | 2,357 | 1.1¢ | $342 | 582 |
| Bitcoin between $78k and $80k on Oct 26? | 4,243 | 517 | 3.4¢ | $1,162 | 8,894 |
| Bitcoin Up or Down, 9:30–9:35 AM ET | 3,992 | 3,112 | 1.1¢ | $306 | 584 |
| Brest: Hemery vs Catry (tennis) | 3,453 | 667 | 1.4¢ | $4,466 | 720 |
| Ethereum Up or Down, 9:30–9:45 AM ET | 3,111 | 977 | 1.3¢ | $91 | 1,365 |
| Dota 2: Yandex vs Liquid, Game 1 | 3,107 | 624 | 1.3¢ | $3,224 | 632 |

Two shapes stand out. The 15-minute crypto markets have tight spreads but thin touches ($90–$350) and update in bursts of thousands per second around the window open. The daily price-range ladders ("between $78k and $80k") have fewer updates but a mid that moves over ten times as often, because they track the underlying continuously. 24-hour volume predicts neither: "XRP Up or Down" carried 2,000 updates/min on under $1,000 of 24-hour volume.

## Feed integrity

| | |
|---|---|
| Updates written before their book | 0 |
| Minute snapshots matching the book replayed from updates | 2,959,136 |
| Snapshots that corrected it | 28,567 (0.96%) |
| REST spot-checks / confirmed mismatches | 23,600 / 0 |

A correction is any snapshot that differs from the replayed book: a resync after an outage, a newer book pushed by Polymarket, or a REST-confirmed drift. Between those points every book replays exactly. The drift found earlier (Polymarket's stream occasionally omitting a change that its own snapshots include) is one source of corrections; reconnects are the other.

## Delivery latency: the bottleneck was the house, not the exchange

`analyze` measures each update's receive time minus Polymarket's timestamp. On this recording the median is fine and the tail is not:

| | p50 | p90 | p99 |
|---|--:|--:|--:|
| All updates | 356 ms | 29 s | 211 s |
| Busiest 1% of markets | 391 ms | 29 s | 179 s |

Updates arriving minutes late would make the collector useless to a trader, so this needed a root cause, not a caveat. (The local clock was 70–90 ms off NTP when last measured, which matters for the median, not for seconds.)

1. **Is the collector falling behind?** No. During a full run it used 31% of one core, the writer backlog was empty, and a leg never waits on the engine for more than 500 ms.
2. **Is Polymarket slow?** Not for a light client. While the collector was lagging by 0.6–33 s on its busiest market, the `latency` tool, on two sockets of its own, received the 30 hottest markets with a 450 ms median from the same machine.
3. **Is it how markets are packed onto sockets?** The laggards were bursty markets (1,000–3,100 updates/s at peak), and 24-hour volume, which sizes the shards, is a poor predictor of message rate. So I built rate-based promotion: the engine reports markets above 3 updates/s, and the manager moves them to 10-market sockets. **It made things worse:** the median rose from 356 ms to 1.3 s and p90 from 29 s to 100 s. I reverted it.
4. **Is it the link?** Yes. Sampling the network interface every 10 s during a full run, inbound traffic sat at 90–100 Mbps for minutes at a time (peaks of 130), the most this connection delivers (`networkQuality` measured 67 Mbps with nothing else running). Polymarket offers no websocket compression (the server ignores `permessage-deflate`), and halving the traffic with `--legs 1` changed nothing: the initial full-depth books for 200k markets alone fill the link.

This explains every observation. TCP shares a saturated link roughly equally per connection, so a socket carrying a bursty market needs more than its share and queues upstream. The probe's two light sockets fit within theirs. Promotion added a hundred sockets and their resubscription dumps, which raised demand on a link that was already full.

The fixes are infrastructure, not code. Run the whole-exchange collector where bandwidth is cheap (a cloud VM with a gigabit link), or point a home connection at the busiest few thousand markets, which a light client receives with a 450 ms median. The dashboard now shows inbound Mbps next to throughput, so a saturated link is visible live instead of only in a later analysis.

## Backtest: market making without trade prints

`backtest` runs a deliberately simple strategy on every market: quote 10 shares on each side at the touch (one cent inside if the spread is wider than 2¢), stop quoting one side at 100 shares of inventory, stay out of prices under 2¢ or over 98¢, and pay 100 ms of order latency on every quote change. The orders it places are never in the recorded book, so fills must be inferred, and the file holds books but not trades. Two fill models bracket the truth:

- **Cross (lower bound):** filled only when the recorded book trades through our price. Real, but only the adverse fills: someone crossing us is usually right.
- **Queue (upper bound):** also filled from the queue: every size decrease at our price is assumed to be a trade, consuming the size ahead of us first. Many decreases are cancels.

| Fill model | Markets | Fills | Volume | P&L | Markout 1s | 10s | 60s |
|---|--:|--:|--:|--:|--:|--:|--:|
| Cross (lower bound) | 579 | 4,750 | $16,177 | −$542 | −1.59¢ | −1.63¢ | −1.85¢ |
| Queue (upper bound) | 5,671 | 42,575 | $147,920 | +$20,317 | +5.22¢ | +5.42¢ | +5.64¢ |

Markout is the mid's move after each fill, per share, signed so that positive means the market moved our way. The cross model's negative markouts are adverse selection in its purest form: we only trade when someone is willing to cross the spread to reach us, and they are right on average.

The queue model's upper bound is not credible, and why is instructive. Its best market made $292 on 10-share clips in 20 minutes: bots there cancel and repost at the touch every few hundred milliseconds, and the model counts each cancel as a trade that hit us. **Book data alone cannot tell a cancel from a trade, so it cannot evaluate a market maker:** the plausible range here spans from losing money to a 14% return on volume. The fix is to record Polymarket's `last_trade_price` events alongside the books, so that only real trades advance the queue. That is the next thing to build, and it makes the backtest a real one.

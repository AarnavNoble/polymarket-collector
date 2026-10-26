//! Event-driven market-making backtest over recorded books.
//!
//! The file holds books but not trades, so fills must be inferred. Two models
//! bracket the truth:
//! - [`FillModel::Cross`] (lower bound) fills a resting order only when the
//!   recorded book crosses its price, i.e. someone posted liquidity that
//!   would have matched it. Taker flow that hits the touch is invisible here,
//!   so an order joining the touch is never filled by it.
//! - [`FillModel::Queue`] (upper bound) also fills from the queue: an order
//!   joins behind the size already at its price, and every decrease at that
//!   price is assumed to be a trade, consuming the queue ahead first. Some of
//!   those decreases are really cancels, so this overstates fills.
//!
//! Orders and cancels take effect `latency` after the strategy decides, and an
//! order stays live (and fillable) until its replacement lands.

use std::collections::VecDeque;

use crate::Side;
use crate::book::OrderBook;

/// One cent at the 1e6 price scale.
pub const CENT: i64 = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillModel {
    Cross,
    Queue,
}

#[derive(Debug, Clone, Copy)]
pub struct Params {
    pub model: FillModel,
    /// Order-entry latency, µs.
    pub latency_us: i64,
    /// Shares per quote, at 1e6 scale.
    pub size: i64,
    /// Absolute inventory, at 1e6 scale, beyond which only the reducing side
    /// is quoted.
    pub max_position: i64,
    /// Markout horizons, µs.
    pub markouts_us: [i64; 3],
}

/// The strategy: quote both sides at the touch, one cent inside when the
/// spread leaves room, never crossing, and only away from the 0/1 extremes
/// where tick sizes shrink and prices stop moving.
pub fn quotes(book: &OrderBook, position: i64, p: &Params) -> [Option<i64>; 2] {
    let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) else {
        return [None, None];
    };
    if bid.price < 2 * CENT || ask.price > 98 * CENT {
        return [None, None];
    }
    let room = ask.price - bid.price > 2 * CENT;
    let buy = bid.price + if room { CENT } else { 0 };
    let sell = ask.price - if room { CENT } else { 0 };
    [
        (position < p.max_position).then_some(buy),
        (position > -p.max_position).then_some(sell),
    ]
}

#[derive(Debug, Clone, Copy)]
struct Order {
    price: i64,
    left: i64,
    queue_ahead: i64,
    /// Recorded size at our price when last seen.
    seen: i64,
}

#[derive(Debug, Clone, Copy)]
struct Pending {
    at: i64,
    price: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
pub struct Fill {
    pub t: i64,
    pub side: Side,
    pub price: i64,
    pub qty: i64,
}

/// One market's simulated account.
#[derive(Debug)]
pub struct Sim {
    p: Params,
    /// Indexed by side: 0 = bid, 1 = ask.
    live: [Option<Order>; 2],
    pending: [Option<Pending>; 2],
    pub position: i64,
    /// Dollars.
    pub cash: f64,
    pub fills: u64,
    pub volume_usd: f64,
    pub max_abs_position: i64,
    /// Per horizon: (sum of signed markout in dollars, shares).
    pub markout: [(f64, i64); 3],
    open_markouts: VecDeque<(Fill, usize)>,
    last_mid: Option<i64>,
}

const SIDES: [Side; 2] = [Side::Buy, Side::Sell];

impl Sim {
    pub fn new(p: Params) -> Self {
        Self {
            p,
            live: [None; 2],
            pending: [None; 2],
            position: 0,
            cash: 0.0,
            fills: 0,
            volume_usd: 0.0,
            max_abs_position: 0,
            markout: [(0.0, 0); 3],
            open_markouts: VecDeque::new(),
            last_mid: None,
        }
    }

    /// Advances to time `t` (µs) with `book` as the market's current state.
    pub fn step(&mut self, t: i64, book: &OrderBook) {
        let mid = book
            .best_bid()
            .zip(book.best_ask())
            .map(|(b, a)| (b.price + a.price) / 2);
        if let Some(mid) = mid {
            self.settle_markouts(t, mid);
            self.last_mid = Some(mid);
        }
        for (i, side) in SIDES.into_iter().enumerate() {
            self.match_order(i, t, book);
            if let Some(pending) = self.pending[i].filter(|p| p.at <= t) {
                self.pending[i] = None;
                self.live[i] = pending.price.map(|price| {
                    let seen = book.qty_at(side, price);
                    Order {
                        price,
                        left: self.p.size,
                        queue_ahead: seen,
                        seen,
                    }
                });
                self.match_order(i, t, book);
            }
        }
        let want = quotes(book, self.position, &self.p);
        for (i, want) in want.into_iter().enumerate() {
            let target = match self.pending[i] {
                Some(p) => p.price,
                None => self.live[i].map(|o| o.price),
            };
            if target != want {
                self.pending[i] = Some(Pending {
                    at: t + self.p.latency_us,
                    price: want,
                });
            }
        }
    }

    fn match_order(&mut self, i: usize, t: i64, book: &OrderBook) {
        let Some(mut o) = self.live[i] else { return };
        let side = SIDES[i];
        // Liquidity on the other side at or through our price would have
        // matched us. What's left is cancelled, so the same recorded
        // liquidity can't fill us twice.
        let crossing: i64 = match side {
            Side::Buy => book
                .asks()
                .take_while(|l| l.price <= o.price)
                .map(|l| l.qty)
                .sum(),
            Side::Sell => book
                .bids()
                .take_while(|l| l.price >= o.price)
                .map(|l| l.qty)
                .sum(),
        };
        if crossing > 0 {
            self.fill(t, side, o.price, o.left.min(crossing));
            self.live[i] = None;
            return;
        }
        if self.p.model == FillModel::Queue {
            let now = book.qty_at(side, o.price);
            let traded = (o.seen - now).max(0);
            o.seen = now;
            let through = (traded - o.queue_ahead).max(0);
            o.queue_ahead = (o.queue_ahead - traded).max(0);
            let qty = through.min(o.left);
            if qty > 0 {
                self.fill(t, side, o.price, qty);
                o.left -= qty;
            }
        }
        self.live[i] = (o.left > 0).then_some(o);
    }

    fn fill(&mut self, t: i64, side: Side, price: i64, qty: i64) {
        let usd = price as f64 * qty as f64 / 1e12;
        let sign = if side == Side::Buy { 1 } else { -1 };
        self.position += sign * qty;
        self.cash -= sign as f64 * usd;
        self.fills += 1;
        self.volume_usd += usd;
        self.max_abs_position = self.max_abs_position.max(self.position.abs());
        let fill = Fill {
            t,
            side,
            price,
            qty,
        };
        for h in 0..self.p.markouts_us.len() {
            self.open_markouts.push_back((fill, h));
        }
    }

    /// Scores fills whose horizon has passed against the mid at that time:
    /// positive means the market moved our way after we traded.
    fn settle_markouts(&mut self, t: i64, mid: i64) {
        let horizons = self.p.markouts_us;
        let mut keep = VecDeque::with_capacity(self.open_markouts.len());
        while let Some((f, h)) = self.open_markouts.pop_front() {
            if t < f.t + horizons[h] {
                keep.push_back((f, h));
                continue;
            }
            let edge = if f.side == Side::Buy {
                mid - f.price
            } else {
                f.price - mid
            };
            self.markout[h].0 += edge as f64 * f.qty as f64 / 1e12;
            self.markout[h].1 += f.qty;
        }
        self.open_markouts = keep;
    }

    /// Cash plus inventory marked at the last mid, in dollars.
    pub fn pnl(&self) -> f64 {
        self.cash + self.position as f64 * self.last_mid.unwrap_or(0) as f64 / 1e12
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::Level;

    const SHARE: i64 = 1_000_000;

    fn params(model: FillModel) -> Params {
        Params {
            model,
            latency_us: 100,
            size: 10 * SHARE,
            max_position: 100 * SHARE,
            markouts_us: [1_000, 10_000, 100_000],
        }
    }

    fn book(bids: &[(i64, i64)], asks: &[(i64, i64)]) -> OrderBook {
        let l = |v: &[(i64, i64)]| -> Vec<Level> {
            v.iter()
                .map(|&(c, s)| Level {
                    price: c * CENT,
                    qty: s * SHARE,
                })
                .collect()
        };
        OrderBook::from_levels(&l(bids), &l(asks))
    }

    #[test]
    fn quotes_inside_wide_spreads_and_join_tight_ones() {
        let p = params(FillModel::Cross);
        assert_eq!(
            quotes(&book(&[(40, 1)], &[(50, 1)]), 0, &p),
            [Some(41 * CENT), Some(49 * CENT)]
        );
        assert_eq!(
            quotes(&book(&[(40, 1)], &[(42, 1)]), 0, &p),
            [Some(40 * CENT), Some(42 * CENT)]
        );
        assert_eq!(
            quotes(&book(&[(40, 1)], &[(42, 1)]), p.max_position, &p),
            [None, Some(42 * CENT)]
        );
        assert_eq!(quotes(&book(&[(1, 1)], &[(3, 1)]), 0, &p), [None, None]);
    }

    #[test]
    fn orders_land_after_latency() {
        let mut sim = Sim::new(params(FillModel::Cross));
        let b = book(&[(40, 5)], &[(42, 5)]);
        sim.step(0, &b);
        assert!(sim.live[0].is_none());
        sim.step(99, &b);
        assert!(sim.live[0].is_none());
        sim.step(100, &b);
        assert_eq!(sim.live[0].map(|o| o.price), Some(40 * CENT));
    }

    #[test]
    fn cross_model_fills_only_when_the_book_trades_through() {
        let mut sim = Sim::new(params(FillModel::Cross));
        sim.step(0, &book(&[(40, 5)], &[(42, 5)]));
        sim.step(100, &book(&[(40, 5)], &[(42, 5)]));
        // The touch is eaten away: invisible to the cross model.
        sim.step(200, &book(&[(39, 5)], &[(42, 5)]));
        assert_eq!(sim.fills, 0);
        // An ask lands at our bid: we'd have matched it.
        sim.step(300, &book(&[(39, 5)], &[(40, 3)]));
        assert_eq!(sim.fills, 1);
        assert_eq!(sim.position, 3 * SHARE);
        assert!((sim.cash + 1.2).abs() < 1e-9);
    }

    #[test]
    fn queue_model_fills_after_the_size_ahead_trades() {
        let mut sim = Sim::new(params(FillModel::Queue));
        let at_40 = |n| book(&[(39, 5), (40, n)], &[(42, 50)]);
        sim.step(0, &at_40(5));
        sim.step(100, &at_40(5)); // join behind 5
        sim.step(200, &at_40(2)); // 3 of the 5 ahead trade
        sim.step(300, &at_40(0)); // the last 2 trade; none of ours yet
        assert_eq!(sim.fills, 0);
        sim.step(400, &at_40(8)); // others join behind us
        sim.step(500, &at_40(4)); // 4 trade: ours
        assert_eq!(sim.position, 4 * SHARE);
    }

    #[test]
    fn markouts_score_fills_against_the_later_mid() {
        let mut sim = Sim::new(params(FillModel::Cross));
        sim.step(0, &book(&[(40, 5)], &[(42, 5)]));
        sim.step(100, &book(&[(40, 5)], &[(42, 5)]));
        sim.step(200, &book(&[(38, 5)], &[(40, 10)])); // bought 10 at 40
        sim.step(1_200, &book(&[(36, 5)], &[(38, 5)])); // mid 37: lost 3¢ a share
        let (usd, qty) = sim.markout[0];
        assert_eq!(qty, 10 * SHARE);
        assert!((usd + 0.3).abs() < 1e-9);
    }
}

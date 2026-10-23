//! A single instrument's price-level order book.

use chrono::{DateTime, Utc};

use crate::Side;
use crate::util::dtos::challenge_orderbook::{
    SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};

/// A price level: fixed-point price and absolute quantity (see [`crate::fixed`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    pub price: i64,
    pub qty: i64,
}

/// Aggregated price levels for one symbol, each side a `Vec` sorted by
/// ascending price. Books have at most a few hundred levels and most changes
/// land near the top, so a sorted vector beats a tree on memory (the
/// collector holds hundreds of thousands of books) and cache locality.
/// Zero-quantity levels are never stored, so two books are equal exactly when
/// they show the same depth.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OrderBook {
    bids: Vec<Level>,
    asks: Vec<Level>,
}

impl OrderBook {
    pub fn from_levels(bids: &[Level], asks: &[Level]) -> Self {
        let mut book = Self::default();
        for &Level { price, qty } in bids {
            book.set(Side::Buy, price, qty);
        }
        for &Level { price, qty } in asks {
            book.set(Side::Sell, price, qty);
        }
        book
    }

    fn side_mut(&mut self, side: Side) -> &mut Vec<Level> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    /// Sets the absolute quantity at a level. Returns whether anything changed,
    /// so callers can suppress no-op updates.
    pub fn set(&mut self, side: Side, price: i64, qty: i64) -> bool {
        let levels = self.side_mut(side);
        match (levels.binary_search_by_key(&price, |l| l.price), qty > 0) {
            (Ok(i), true) => std::mem::replace(&mut levels[i].qty, qty) != qty,
            (Ok(i), false) => {
                levels.remove(i);
                true
            }
            (Err(i), true) => {
                levels.insert(i, Level { price, qty });
                true
            }
            (Err(_), false) => false,
        }
    }

    /// Applies a signed quantity change (for exchanges that publish deltas)
    /// and returns the resulting absolute quantity.
    pub fn apply_delta(&mut self, side: Side, price: i64, delta: i64) -> i64 {
        let levels = self.side_mut(side);
        let current = levels
            .binary_search_by_key(&price, |l| l.price)
            .map_or(0, |i| levels[i].qty);
        let qty = (current + delta).max(0);
        self.set(side, price, qty);
        qty
    }

    /// Bids best (highest) first.
    pub fn bids(&self) -> impl Iterator<Item = Level> + '_ {
        self.bids.iter().rev().copied()
    }

    /// Asks best (lowest) first.
    pub fn asks(&self) -> impl Iterator<Item = Level> + '_ {
        self.asks.iter().copied()
    }

    pub fn to_snapshot(
        &self,
        symbol: &str,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    ) -> SnapshotDomain {
        SnapshotDomain {
            received_at,
            exchange_timestamp,
            bids: self
                .bids()
                .map(|l| SnapshotBidsEntryDomain {
                    price_level: l.price,
                    quantity: l.qty,
                })
                .collect(),
            asks: self
                .asks()
                .map(|l| SnapshotAsksEntryDomain {
                    price_level: l.price,
                    quantity: l.qty,
                })
                .collect(),
            symbol: symbol.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_reports_changes_and_drops_empty_levels() {
        let mut book = OrderBook::default();
        assert!(book.set(Side::Buy, 400_000, 5));
        assert!(!book.set(Side::Buy, 400_000, 5));
        assert!(book.set(Side::Buy, 400_000, 0));
        assert!(!book.set(Side::Buy, 400_000, 0));
        assert_eq!(book, OrderBook::default());
    }

    #[test]
    fn levels_are_best_first() {
        let book = OrderBook::from_levels(
            &[Level { price: 1, qty: 1 }, Level { price: 3, qty: 1 }],
            &[Level { price: 9, qty: 1 }, Level { price: 5, qty: 1 }],
        );
        assert_eq!(book.bids().map(|l| l.price).collect::<Vec<_>>(), [3, 1]);
        assert_eq!(book.asks().map(|l| l.price).collect::<Vec<_>>(), [5, 9]);
    }

    proptest::proptest! {
        /// The book agrees with a naive model under any sequence of updates.
        #[test]
        fn matches_a_naive_model(ops in proptest::collection::vec((0i64..20, 0i64..4, proptest::bool::ANY), 0..200)) {
            let mut book = OrderBook::default();
            let mut model = std::collections::BTreeMap::new();
            for (price, qty, bid) in ops {
                let side = if bid { Side::Buy } else { Side::Sell };
                let changed = book.set(side, price, qty);
                let before = if qty > 0 { model.insert((bid, price), qty) } else { model.remove(&(bid, price)) };
                proptest::prop_assert_eq!(changed, before != (qty > 0).then_some(qty));
            }
            let bids: Vec<_> = model.iter().filter(|((b, _), _)| *b).rev().map(|((_, p), q)| (*p, *q)).collect();
            let asks: Vec<_> = model.iter().filter(|((b, _), _)| !*b).map(|((_, p), q)| (*p, *q)).collect();
            proptest::prop_assert_eq!(book.bids().map(|l| (l.price, l.qty)).collect::<Vec<_>>(), bids);
            proptest::prop_assert_eq!(book.asks().map(|l| (l.price, l.qty)).collect::<Vec<_>>(), asks);
        }
    }

    #[test]
    fn deltas_accumulate_and_clamp() {
        let mut book = OrderBook::default();
        assert_eq!(book.apply_delta(Side::Sell, 7, 10), 10);
        assert_eq!(book.apply_delta(Side::Sell, 7, -4), 6);
        assert_eq!(book.apply_delta(Side::Sell, 7, -6), 0);
        assert_eq!(book, OrderBook::default());
    }
}

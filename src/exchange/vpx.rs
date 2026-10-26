//! Vancouver Prediction Exchange (vancouverpx.ca), polled over its REST API.
//!
//! YES and NO share one book on VPX (a NO bid at X is a YES ask at 100 − X),
//! so the YES side is the whole book. Prices are integer cents and the API
//! returns the top 10 levels per side, so a level that drops below tenth
//! place reads as removed.

use anyhow::Context;
use serde::Deserialize;

use crate::book::{Level, OrderBook};
use crate::fixed;
use crate::poll::PolledExchange;

pub struct Vpx {
    pub base: String,
    pub api_key: String,
}

impl Vpx {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            base: "https://www.vancouverpx.ca/api/v1".into(),
            api_key: std::env::var("VPX_API_KEY")
                .ok()
                .filter(|k| !k.is_empty())
                .context("set VPX_API_KEY (vancouverpx.ca → Developer Settings)")?,
        })
    }

    async fn get<T: serde::de::DeserializeOwned>(
        &self,
        http: &reqwest::Client,
        path: &str,
        query: &[(&str, &str)],
    ) -> anyhow::Result<T> {
        Ok(http
            .get(format!("{}{path}", self.base))
            .bearer_auth(&self.api_key)
            .query(query)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }
}

#[derive(Deserialize)]
struct Markets {
    markets: Vec<Market>,
}

#[derive(Deserialize)]
struct Market {
    id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Books {
    order_book_yes: Book,
}

#[derive(Deserialize)]
struct Book {
    bids: Vec<WireLevel>,
    asks: Vec<WireLevel>,
}

#[derive(Deserialize)]
struct WireLevel {
    /// Cents.
    price: serde_json::Number,
    quantity: serde_json::Number,
}

impl Book {
    fn into_book(self) -> anyhow::Result<OrderBook> {
        let levels = |v: Vec<WireLevel>| -> anyhow::Result<Vec<Level>> {
            v.into_iter()
                .map(|l| {
                    let cents = fixed::parse(&l.price.to_string());
                    Ok(Level {
                        price: cents
                            .and_then(|c| c.checked_div(100))
                            .context("bad price")?,
                        qty: fixed::parse(&l.quantity.to_string()).context("bad quantity")?,
                    })
                })
                .collect()
        };
        Ok(OrderBook::from_levels(
            &levels(self.bids)?,
            &levels(self.asks)?,
        ))
    }
}

impl PolledExchange for Vpx {
    async fn markets(&self, http: &reqwest::Client) -> anyhow::Result<Vec<String>> {
        let m: Markets = self.get(http, "/markets", &[("status", "OPEN")]).await?;
        Ok(m.markets.into_iter().map(|m| m.id).collect())
    }

    async fn book(&self, http: &reqwest::Client, symbol: &str) -> anyhow::Result<OrderBook> {
        let b: Books = self
            .get(http, "/market/order-book", &[("marketId", symbol)])
            .await?;
        b.order_book_yes.into_book()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_the_documented_book() {
        let b: Books = serde_json::from_str(
            r#"{"orderBookYes":{"bids":[{"price":65,"quantity":10,"total":10},{"price":60,"quantity":25,"total":35}],
                "asks":[{"price":70,"quantity":5,"total":5}],"lastTradePrice":66},
                "orderBookNo":{"bids":[],"asks":[],"lastTradePrice":null}}"#,
        )
        .unwrap();
        let book = b.order_book_yes.into_book().unwrap();
        let l = |price, qty| Level { price, qty };
        assert_eq!(book.best_bid(), Some(l(650_000, 10_000_000)));
        assert_eq!(book.best_ask(), Some(l(700_000, 5_000_000)));
        assert_eq!(book.bids().count(), 2);
    }
}

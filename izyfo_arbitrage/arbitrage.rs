use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

use chrono::{TimeZone, Utc};
use chrono::{NaiveDate, NaiveDateTime};
use chrono::prelude::*;
use chrono::prelude::DateTime;
use log::{debug, error, info, trace, warn};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::izyfo_arbitrage::arbitrage_transaction::{ArbitrageTransaction, ArbitrageTransactionResult};
use crate::izyfo_events::exchange::market_bbo::MarketBBO;

// Arbitrage Profit
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ArbitrageProfit {
    name: String,
    tick_timestamp: f64,
    tick_received_timestamp_ms: i64,
    transaction_result_list: Vec<ArbitrageTransactionResult>,
    create_at: DateTime<Utc>,
    uuid: Uuid,
}

impl fmt::Display for ArbitrageProfit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let millis = self.tick_timestamp * 1000_f64;
        write!(f, "{} - {} - {} - {}", self.name, self.get_profit(), Utc.timestamp_millis(millis as i64).to_string(), self.get_distance())
    }
}

impl ArbitrageProfit {
    // return profit
    pub fn get_profit(&self) -> f32 {
        return self.transaction_result_list[2].get_qty_out() - self.transaction_result_list[0].get_qty_in();
    }

    // return distance
    pub fn get_distance(&self) -> f64 {
        return self.transaction_result_list[2].get_tick_timestamp() - self.transaction_result_list[0].get_tick_timestamp();
    }

    // return name
    pub fn get_timestamp(&self) -> f64 {
        return self.tick_timestamp;
    }

    // return name
    pub fn get_name(&self) -> &String {
        return &self.name;
    }

    // return qty in
    pub fn get_qty_in(&self) -> f32 {
        return self.transaction_result_list[0].get_qty_in();
    }

    // return qty out
    pub fn get_qty_out(&self) -> f32 {
        return self.transaction_result_list[2].get_qty_out();
    }

    // return transaction result list
    pub fn get_transaction_result_list(&self) -> &Vec<ArbitrageTransactionResult> {
        return &self.transaction_result_list;
    }

    // return asset list
    pub fn get_asset_list(&self) -> Vec<String> {
        let mut asset_list: Vec<String> = Vec::new();

        for transaction in &self.transaction_result_list {
            let source = transaction.get_source().to_string().replace("BINANCE_", "");
            let target = transaction.get_target().to_string().replace("BINANCE_", "");


            // insert base
            if !asset_list.contains(&source) {
                asset_list.push(source);
            }

            // insert base
            if !asset_list.contains(&target) {
                asset_list.push(target);
            }
        }

        return asset_list;
    }

    // return latency
    pub fn get_latency_ms(&self) -> i64 {
        return self.create_at.timestamp_millis() - self.tick_received_timestamp_ms;
    }

    // return uuid
    pub fn get_uuid(&self) -> Uuid {
        return self.uuid;
    }

    // check valid
    pub fn is_valid_ordering(&self) -> bool {
        for t in &self.transaction_result_list {
            if !t.is_valid_ordering() {
                warn!("arbitrage_profit - invalid transaction. {:?}", t);
                return false;
            } else {
                info!("arbitrage_profit - valid transaction. {:?}", t);
            }
        }
        return true;
    }
}

// Arbitrage
pub struct Arbitrage {
    name: String,
    transaction_list: Vec<ArbitrageTransaction>,
    pub instrument_list: Vec<String>,
    markets: HashMap<String, MarketBBO>,
}

impl Arbitrage {
    // transaction list
    pub fn from_transaction_list(transaction_hash_list: &Vec<HashMap<String, String>>) -> Arbitrage {
        let mut transaction_list: Vec<ArbitrageTransaction> = Vec::new();

        for transaction in transaction_hash_list {
            let source = transaction.get("source").unwrap().to_string();
            let target = transaction.get("target").unwrap().to_string();
            let operation = transaction.get("operation").unwrap().to_string();
            let instrument = transaction.get("instrument").unwrap().to_string();
            let exchange_code = transaction.get("exchange_code").unwrap().to_string();
            let arbitrage_transaction = ArbitrageTransaction::new(source, target, operation, instrument, exchange_code);
            transaction_list.push(arbitrage_transaction);
        }

        let name: String = format!(
            "{}:{}:{}", transaction_list[0].get_name(),
            transaction_list[1].get_name(),
            transaction_list[2].get_name()
        );

        let mut instrument_list: Vec<String> = Vec::new();
        for transaction in &transaction_list {
            let value = transaction.get_instrument().to_string();
            if !instrument_list.contains(&value) {
                instrument_list.push(value);
            }
        }

        Arbitrage {
            name: name,
            transaction_list: transaction_list,
            instrument_list: instrument_list,
            markets: HashMap::new(),
        }
    }

    // execute market bbo
    pub fn execute(&mut self, market_bbo: &MarketBBO, qty_initial: f32, scale: bool) -> Option<ArbitrageProfit> {
        // initialize out
        let mut qty_in: f32 = qty_initial;
        let start_date = Instant::now();


        // iterate over transaction
        let mut last_transaction_result: ArbitrageTransactionResult;

        let mut transaction_result_list: Vec<ArbitrageTransactionResult> = Vec::new();

        // list over transaction
        for transaction in &mut self.transaction_list {

            // update tick
            if transaction.get_instrument().to_string() == market_bbo.get_instrument() {
                transaction.update(market_bbo);
            }

            // transaction
            let valid_message = transaction.is_valid();
            match valid_message {
                Ok(s) => {
                    last_transaction_result = transaction.execute(qty_in);
                    qty_in = last_transaction_result.get_qty_out();
                    transaction_result_list.push(last_transaction_result);
                }
                Err(e) => {}
            }
        }

        if transaction_result_list.len() == 3 {
            if scale {
                let mut ratio_list: Vec<f32> = Vec::new();

                // ratio list
                for t in transaction_result_list {
                    // ratio list
                    if !t.is_valid_ordering() {
                        let ratio = t.get_qty_to_execute() / t.get_market_qty();
                        ratio_list.push(ratio);
                    }
                }

                // max value
                let mut max_value = ratio_list.iter().fold(0.0f32, |mut max, &val| {
                    if val > max {
                        max = val;
                    }
                    max
                });

                if max_value > 1.0 {
                    let qty_initial_scaled = qty_initial / max_value;
                    let arbitrage_profit = self.execute(market_bbo, qty_initial_scaled, false);
                    return arbitrage_profit;
                }
            } else {
                let arbitrage_profit = ArbitrageProfit {
                    name: self.name.clone(),
                    transaction_result_list: transaction_result_list,
                    tick_timestamp: market_bbo.get_marketdata_timestamp(),
                    tick_received_timestamp_ms: market_bbo.get_created_timestamp_ms(),
                    create_at: Utc::now(),
                    uuid: Uuid::new_v4(),
                };
                return Some(arbitrage_profit);
            }
        }


        debug!("arbitrage - executed. duration: {:?}", start_date.elapsed());

        return None;
    }

    // return name
    pub fn get_name(&self) -> &String {
        return &self.name;
    }

    // return instruments
    pub fn get_instruments(&self) -> Vec<String> {
        let mut instruments: Vec<String> = Vec::new();
        for transaction in &self.transaction_list {
            let value = transaction.get_instrument().to_string();
            if !instruments.contains(&value) {
                instruments.push(value);
            }
        }
        instruments
    }

    pub fn get_start_asset(&self) -> &String {
        return self.transaction_list[0].get_source();
    }
}
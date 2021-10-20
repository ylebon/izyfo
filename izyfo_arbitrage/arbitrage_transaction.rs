use std::collections::HashMap;
use std::ptr::null;
use uuid::Uuid;

use serde::{Deserialize, Serialize};

use crate::izyfo_events::exchange::market_bbo::MarketBBO;
use crate::izyfo_utils::math;

#[derive(Serialize, Deserialize, Debug)]
pub struct ArbitrageTransaction {
    name: String,
    source: String,
    target: String,
    operation: String,
    instrument: String,
    exchange_code: String,
    ask_price: f32,
    bid_price: f32,
    min_price: f32,
    max_price: f32,
    ask_qty: f32,
    bid_qty: f32,
    min_qty: f32,
    max_qty: f32,
    step_size: f32,
    tick_size: f32,
    tick_timestamp: f64,
    trade_fee: (f32, String),
    ready: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ArbitrageTransactionResult {
    name: String,
    source: String,
    target: String,
    operation: String,
    instrument: String,
    exchange_code: String,
    qty_in: f32,
    qty_out: f32,
    qty_out_r: f32,
    qty_to_execute: f32,
    price: f32,
    fee: f32,
    step_size: f32,
    tick_size: f32,
    min_price: f32,
    max_price: f32,
    min_qty: f32,
    max_qty: f32,
    tick_timestamp: f64,
    market_qty: f32,
    uuid: Uuid,
}

impl ArbitrageTransactionResult {
    // return transaction result
    pub fn get_qty_in(&self) -> f32 {
        return self.qty_in;
    }

    // return transaction result
    pub fn get_qty_out(&self) -> f32 {
        return self.qty_out;
    }

    // return tick timestamp
    pub fn get_tick_timestamp(&self) -> f64 {
        return self.tick_timestamp;
    }

    // return operation
    pub fn get_operation(&self) -> &String {
        return &self.operation;
    }

    // return instrument symbol
    pub fn get_instrument_symbol(&self) -> String {
        return self.instrument.replace("BINANCE_", "");
    }

    pub fn get_price(&self) -> f32 {
        return self.price;
    }

    pub fn get_exchange_code(&self) -> &String {
        return &self.exchange_code;
    }

    pub fn get_qty_to_execute(&self) -> f32 {
        return self.qty_to_execute;
    }

    pub fn get_market_qty(&self) -> f32 {
        return self.market_qty;
    }

    // check transaction is valid for ordering
    pub fn is_valid_ordering(&self) -> bool {
        return (self.qty_to_execute <= self.market_qty) & (self.min_qty <= self.qty_to_execute) & (self.qty_to_execute <= self.max_qty);
    }

    // return source
    pub fn get_source(&self) -> &String {
        return &self.source;
    }

    // return target
    pub fn get_target(&self) -> &String {
        return &self.target;
    }

    // return uuid
    pub fn get_uuid(&self) -> Uuid {
        return self.uuid;
    }
}

impl ArbitrageTransaction {
    // create new instance
    pub fn new(source: String, target: String, operation: String, instrument: String, exchange_code: String) -> ArbitrageTransaction {
        ArbitrageTransaction {
            name: format!("{}-({})->{}", source, operation, target),
            source: source,
            target: target,
            operation: operation,
            instrument: instrument,
            bid_price: 0.0,
            ask_price: 0.0,
            min_price: 0.0,
            max_price: 0.0,
            bid_qty: 0.0,
            ask_qty: 0.0,
            min_qty: 0.0,
            max_qty: 0.0,
            step_size: 0.0,
            tick_size: 0.0,
            trade_fee: (0.001, "%".to_string()),
            ready: false,
            tick_timestamp: 0.0,
            exchange_code: exchange_code,
        }
    }

    // update
    pub fn update(&mut self, tick: &MarketBBO) {
        self.ask_price = tick.get_ask_price();
        self.bid_price = tick.get_bid_price();
        self.min_price = tick.get_min_price();
        self.max_price = tick.get_max_price();

        self.ask_qty = tick.get_ask_qty();
        self.bid_qty = tick.get_bid_qty();
        self.min_qty = tick.get_min_qty();
        self.max_qty = tick.get_max_qty();

        self.step_size = tick.get_step_size();
        self.tick_size = tick.get_tick_size();
        self.tick_timestamp = tick.get_marketdata_timestamp();
        self.ready = true;
    }

    // update
    pub fn is_valid(&self) -> Result<bool, String> {
        if self.ask_price <= 0.0 {
            Err(format!("invalid ask price: '{}'", self.ask_price))
        } else if self.bid_price <= 0.0 {
            Err(format!("invalid bid price: '{}'", self.bid_price))
        } else if self.ask_qty <= 0.0 {
            Err(format!("invalid bid price: '{}'", self.ask_qty))
        } else if self.bid_qty <= 0.0 {
            Err(format!("invalid bid price: '{}'", self.bid_qty))
        } else {
            Ok(true)
        }
    }

    // execute transaction
    pub fn execute(&self, qty_in: f32) -> ArbitrageTransactionResult {
        let mut qty_out: f32;


        if self.operation == "BUY" {
            // get price
            let mut price = self.ask_price;
            price = self.normalize_price(price);

            // calculate qty
            let mut qty_to_execute = qty_in / price;
            qty_to_execute = self.normalize_qty(qty_to_execute);

            // calculate fee
            let mut fee: f32 = 0.0;
            if self.trade_fee.1 == "%" {
                fee = qty_to_execute * self.trade_fee.0;
            }

            // remove fee
            qty_out = qty_to_execute - fee;

            // arbitrage transaction result
            ArbitrageTransactionResult {
                name: self.name.clone(),
                source: self.source.clone(),
                target: self.target.clone(),
                instrument: self.instrument.clone(),
                operation: self.operation.clone(),
                tick_timestamp: self.tick_timestamp.clone(),
                qty_in: qty_in,
                qty_out: qty_out,
                qty_out_r: 0.0,
                qty_to_execute: qty_to_execute,
                fee: fee,
                price: price,
                step_size: self.step_size.clone(),
                tick_size: self.tick_size.clone(),
                min_price: self.min_price.clone(),
                max_price: self.max_price.clone(),
                min_qty: self.min_qty.clone(),
                max_qty: self.max_qty.clone(),
                exchange_code: self.exchange_code.clone(),
                market_qty: self.ask_qty,
                uuid: Uuid::new_v4(),
            }
        } else if self.operation == "SELL" {
            // normalize input
            let normalize_qty = self.normalize_qty(qty_in);

            // price
            let mut price = self.bid_price;
            price = self.normalize_price(price);

            // round out
            qty_out = normalize_qty * price;

            // calculate fee
            let mut fee: f32 = 0.0;
            if self.trade_fee.1 == "%" {
                fee = qty_out * self.trade_fee.0;
            }
            // remove fee
            qty_out = qty_out - fee;

            // arbitrage transaction result
            ArbitrageTransactionResult {
                name: self.name.clone(),
                source: self.source.clone(),
                target: self.target.clone(),
                instrument: self.instrument.clone(),
                operation: self.operation.clone(),
                tick_timestamp: self.tick_timestamp.clone(),
                qty_in: qty_in,
                qty_out_r: 0.0,
                qty_out: qty_out,
                qty_to_execute: normalize_qty,
                fee: fee,
                price: price,
                step_size: self.step_size.clone(),
                tick_size: self.tick_size.clone(),
                min_price: self.min_price.clone(),
                max_price: self.max_price.clone(),
                min_qty: self.min_qty.clone(),
                max_qty: self.max_qty.clone(),
                exchange_code: self.exchange_code.clone(),
                market_qty: self.bid_qty,
                uuid: Uuid::new_v4(),
            }
        } else {
            println!("UNKNOWN");
            ArbitrageTransactionResult {
                name: self.name.clone(),
                source: self.source.clone(),
                target: self.target.clone(),
                instrument: self.instrument.clone(),
                operation: self.operation.clone(),
                tick_timestamp: self.tick_timestamp.clone(),
                qty_in: qty_in,
                qty_out: qty_in,
                qty_to_execute: 0.0,
                fee: 0.0,
                price: 0.0,
                step_size: self.step_size.clone(),
                tick_size: self.tick_size.clone(),
                min_price: self.min_price.clone(),
                max_price: self.max_price.clone(),
                min_qty: self.min_qty.clone(),
                max_qty: self.max_qty.clone(),
                qty_out_r: 0.0,
                exchange_code: self.exchange_code.clone(),
                market_qty: self.ask_qty,
                uuid: Uuid::new_v4(),
            }
        }
    }

    pub fn get_instrument(&self) -> &String {
        return &self.instrument;
    }

    pub fn get_exchange_code(&self) -> &String {
        return &self.exchange_code;
    }

    pub fn get_name(&self) -> &String {
        return &self.name;
    }

    pub fn get_source(&self) -> &String {
        return &self.source;
    }

    fn normalize_qty(&self, qty: f32) -> f32 {
        //check step size
        if !self.step_size.is_nan() {
            if self.step_size == 1.0 {
                return qty.trunc();
            } else {
                let round_count: usize = self.step_size.to_string().len() - 2;
                return math::round_down(qty, round_count);
            }
        } else {
            return qty;
        }
    }

    fn normalize_price(&self, price: f32) -> f32 {
        //check step size
        if !self.tick_size.is_nan() {
            if self.tick_size == 1.0 {
                return price.trunc();
            } else {
                let round_count: usize = self.tick_size.to_string().len() - 2;
                return math::round_down(price, round_count);
            }
        } else {
            return price;
        }
    }
}
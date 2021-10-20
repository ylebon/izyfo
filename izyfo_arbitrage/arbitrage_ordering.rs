use std::{thread, time};
use std::collections::HashMap;
use std::time::{Duration, Instant};

use binance::account::*;
use binance::api::*;
use binance::errors::Error;
use binance::errors::ErrorKind as BinanceLibErrorKind;
use binance::market::*;
use binance::model::Transaction;
use bus::BusReader;
use crossbeam_channel::{Receiver, Sender};
use futures::future::lazy;
use log::{debug, error, info, trace, warn};
use simplelog::*;
use std::sync::{Arc, Mutex};

use crate::izyfo_arbitrage::arbitrage::ArbitrageProfit;
use crate::izyfo_arbitrage::arbitrage_executor::ExecutionMode;
use crate::izyfo_arbitrage::arbitrage_transaction::ArbitrageTransactionResult;
use crate::izyfo_configs::services;
use crate::izyfo_connectors::referencedata::{ReferencedataConnector, Referencedata};
use crate::izyfo_events::exchange::instrument::Instrument;
use crate::izyfo_utils::math;
use std::env;

pub struct ArbitrageOrdering {
    exchange: Arc<Account>,
    balances: HashMap<String, f32>,
    referencedata: Referencedata,
    busy: bool,
    arbitrage_profit_receiver: Receiver<ArbitrageProfit>,
    mode: ExecutionMode,
}

pub struct ArbitrageOrderingTransaction {
    index: u32,
    transaction_result: ArbitrageTransactionResult,
    transaction_ordering: Transaction,
}


impl ArbitrageOrdering {
    // new arbitrage
    pub fn new(arbitrage_profit: Receiver<ArbitrageProfit>, mode: ExecutionMode) -> ArbitrageOrdering {
        let api_key = Some("".to_string());
        let secret_key = Some("".to_string());
        let account: Account = Binance::new(api_key, secret_key);

        let url = String::from("");
        let referencedata_connector = ReferencedataConnector::from_url(url);
        let referencedata = referencedata_connector.get_referencedata("BINANCE");

        let mut arbitrage_ordering = ArbitrageOrdering {
            exchange: Arc::new(account),
            balances: HashMap::new(),
            referencedata: referencedata,
            busy: false,
            arbitrage_profit_receiver: arbitrage_profit,
            mode: mode,
        };
        arbitrage_ordering
    }

    pub fn start(&mut self) {
        info!("arbitrage_ordering - started.");

        // create thread counter
        let thread_counter = Arc::new(Mutex::new(0));

        // display error
        fn display_error(err: Error) {
            match err.0 {
                BinanceLibErrorKind::BinanceError(code, msg, response) => match code {
                    _ => error!("arbitrage_ordering - binance error. error code: {}, msg: {}", code, msg),
                },
                BinanceLibErrorKind::Msg(msg) => {
                    error!("arbitrage_ordering - lib error. error: {}", msg)
                }
                _ => error!("arbitrage_ordering - other error. error: {}.", err.0),
            };
        }

        loop {
            let arbitrage_profit = self.arbitrage_profit_receiver.recv();

            // continue if thread counter equal zero
            let mut num = thread_counter.lock().unwrap();
            if (*num != 0){
                continue;
            }

            match arbitrage_profit {
                Ok(p) => {
                    info!("arbitrage_ordering - parallel execution started.");
                    let start_date = Instant::now();
                    let mut children = vec![];

                    for t in p.get_transaction_result_list() {
                        let transaction = t.clone();
                        let thread_counter = Arc::clone(&thread_counter);

                        children.push(thread::spawn(move || {
                            info!("arbitrage_ordering - executing transaction. transaction: {:?}", transaction);

                            // increment counter
                            let mut num = thread_counter.lock().unwrap();
                            *num += 1;

                            let api_key = Some("".to_string());
                            let secret_key = Some("".to_string());
                            let account: Account = Binance::new(api_key, secret_key);

                            let instrument_symbol = transaction.get_exchange_code().to_string();

                            // buy transaction
                            let operation = transaction.get_operation();

                            // uuid
                            let uuid = transaction.get_uuid();

                            if operation == "BUY" {

                                // setup order parameters
                                let price = transaction.get_price();
                                let qty = transaction.get_qty_to_execute();

                                info!("arbitrage_ordering - running. uuid: {}, side: {},symbol: {},price: {}, qty:{}", uuid.to_string(), operation, instrument_symbol, price, qty);

                                // run exchange ordering
                                match account.limit_buy_fok(instrument_symbol.clone(), qty, price) {
                                    Ok(answer) => {
                                        info!("arbitrage_ordering - executed. uuid: {}, side: {},symbol: {},price: {}, qty:{}", uuid.to_string(), operation, instrument_symbol, price, qty);
                                        info!("arbitrage_ordering - order transaction. {:?}", answer);
                                        *num -= 1;
                                        Ok(answer)
                                    }
                                    Err(err) => {
                                        error!("arbitrage_ordering - failed. uuid: {}, side: {},symbol: {},price: {}, qty:{}", uuid.to_string(), operation, instrument_symbol, price, qty);
                                        display_error(err);
                                        *num -= 1;
                                        Err("failed".to_string())
                                    }
                                }
                            } else if operation == "SELL" {

                                // setup order parameters
                                let price = transaction.get_price();
                                let qty = transaction.get_qty_to_execute();

                                info!("arbitrage_ordering - running. uuid: {}, side: {},symbol: {},price: {}, qty:{}", uuid.to_string(), operation, instrument_symbol, price, qty);

                                // run exchange ordering
                                match account.limit_sell_fok(instrument_symbol.clone(), qty, price) {
                                    Ok(answer) => {
                                        info!("arbitrage_ordering - executed. uuid: {}, side: {},symbol: {},price: {}, qty:{}", uuid.to_string(), operation, instrument_symbol, price, qty);
                                        info!("arbitrage_ordering - order transaction. {:?}", answer);
                                        *num -= 1;
                                        Ok(answer)
                                    }
                                    Err(err) => {
                                        error!("arbitrage_ordering - failed. uuid: {}, side: {},symbol: {},price: {}, qty:{}", uuid.to_string(), operation, instrument_symbol, price, qty);
                                        error!("arbitrage_ordering - order transaction. error: {:?}", err);
                                        *num -= 1;
                                        display_error(err);
                                        Err("failed".to_string())
                                    }
                                }
                            } else {
                                error!("{} failed to recognize transaction", instrument_symbol);
                                *num -= 1;
                                Err("failed".to_string())
                            }
                        }));

                        // sleep between transactions
                        let mut sleep_duration = time::Duration::from_micros(10);
                        match env::var("SLEEP_BETWEEN_TRANSACTIONS") {
                            Ok(s) => {
                                sleep_duration = time::Duration::from_micros(s.parse::<u64>().unwrap());
                            }
                            Err(e) => ()
                        };
                        thread::sleep(sleep_duration);

                    };

                    // arbitrage info
                    info!("arbitrage_ordering - parallel executions finished. duration: {:?}", start_date.elapsed());
                }
                Err(err) => {
                    error!("arbitrage_ordering - failed to recv transaction. error: {:?}", err);
                }
            }
        }
    }

    // parallel execution
    pub fn execute_parallel(&mut self, arbitrage_profit: &ArbitrageProfit) {
    }

    // sequential execution
    pub fn execute_sequential(&mut self, arbitrage_profit: &ArbitrageProfit) {
        info!("arbitrage_ordering - executing.");

        self.busy = true;
        let start_date = Instant::now();

        // results
        let mut results: HashMap<u32, (Transaction, f32)> = HashMap::new();
        let mut transaction_nbr: u32 = 0;

        // run all transactions
        for arbitrage_transaction in arbitrage_profit.get_transaction_result_list() {
            let result = self.execute_transaction(arbitrage_transaction);

            // result
            match result {
                Ok(order_transaction) => {
                    let symbol = &order_transaction.symbol;
                    let order_id = &order_transaction.order_id;

                    // sleep
                    let sleep_duration = time::Duration::from_millis(300);
                    thread::sleep(sleep_duration);

                    // check order status
                    match self.exchange.order_status(symbol, *order_id) {
                        Ok(order_status) => {
                            if order_status.status == "NEW" {

                                // cancel order
                                match self.exchange.cancel_order(symbol, *order_id) {
                                    Ok(order_cancelled) => {
                                        warn!("arbitrage_ordering - cancelling order. {:?}", order_cancelled);
                                    }
                                    Err(err) => {
                                        error!("arbitrage_ordering - cancel order error. error: {}", err)
                                    }
                                }
                            } else if order_status.status == "FILLED" {
                                continue;
                            }
                        }
                        Err(err_1) => {
                            break;
                        }
                    }
                }
                Err(err) => {
                    error!("arbitrage_ordering - order status error. error: {}", err);
                    break;
                }
            }
        }

        info!("arbitrage_ordering - executed. duration: {:?}", start_date.elapsed());

        // clean
        //self.cancel_pending_transactions(results);

        // revert
        self.clean_balances(arbitrage_profit);

        // update balances after transactions complete
        self.update_balances();

        // remove busy
        self.busy = false;
    }

    // execute transaction
    pub fn execute_transaction(&self, transaction: &ArbitrageTransactionResult) -> Result<Transaction, String> {
        let instrument_symbol = transaction.get_exchange_code().to_string();

        // buy transaction
        let operation = transaction.get_operation();

        // uuid
        let uuid = transaction.get_uuid();

        if operation == "BUY" {

            // setup order parameters
            let price = transaction.get_price();
            let qty = transaction.get_qty_to_execute();

            info!("arbitrage_ordering - running. uuid: {}, side: {},symbol: {},price: {}, qty:{}", uuid.to_string(), operation, instrument_symbol, price, qty);

            // run exchange ordering
            match self.exchange.limit_buy_fok(instrument_symbol.clone(), qty, price) {
                Ok(answer) => {
                    info!("arbitrage_ordering - executed. uuid: {}, side: {},symbol: {},price: {}, qty:{}", uuid.to_string(), operation, instrument_symbol, price, qty);
                    info!("arbitrage_ordering - order transaction. {:?}", answer);
                    Ok(answer)
                }
                Err(err) => {
                    error!("arbitrage_ordering - failed. uuid: {}, side: {},symbol: {},price: {}, qty:{}", uuid.to_string(), operation, instrument_symbol, price, qty);
                    error!("arbitrage_ordering - arbitrage transaction. {:?}", transaction);
                    self.display_error(err);
                    Err("failed".to_string())
                }
            }
        } else if operation == "SELL" {

            // setup order parameters
            let price = transaction.get_price();
            let qty = transaction.get_qty_to_execute();

            info!("arbitrage_ordering - running. uuid: {}, side: {},symbol: {},price: {}, qty:{}", uuid.to_string(), operation, instrument_symbol, price, qty);

            // run exchange ordering
            match self.exchange.limit_sell_fok(instrument_symbol.clone(), qty, price) {
                Ok(answer) => {
                    info!("arbitrage_ordering - executed. uuid: {}, side: {},symbol: {},price: {}, qty:{}", uuid.to_string(), operation, instrument_symbol, price, qty);
                    info!("arbitrage_ordering - order transaction. {:?}", answer);
                    Ok(answer)
                }
                Err(err) => {
                    error!("arbitrage_ordering - failed. uuid: {}, side: {},symbol: {},price: {}, qty:{}", uuid.to_string(), operation, instrument_symbol, price, qty);
                    error!("arbitrage_ordering - arbitrage transaction. {:?}", transaction);
                    self.display_error(err);
                    Err("failed".to_string())
                }
            }
        } else {
            error!("{} failed to recognize transaction", instrument_symbol);
            Err("failed".to_string())
        }
    }

    // reset arbitrage execution
    pub fn reset(&self, arbitrage_execution: &ArbitrageTransactionResult) {}

    // update balance
    pub fn update_balances(&mut self) {
        info!("arbitrage_ordering - updating balances ...");

        match self.exchange.get_account() {
            Ok(answer) => {
                for balance in answer.balances {
                    debug!("balance: {:?}", balance);
                    let mut amount = balance.free.parse::<f32>().unwrap_or_default();
                    self.balances.insert(balance.asset, amount);
                }
            }
            Err(err) => {
                self.display_error(err);
            }
        }
    }

    pub fn get_balance(&self, asset: &String) -> Option<&f32> {
        let a = asset.replace("BINANCE_", "");
        let balance = self.balances.get(&a);
        match balance {
            Some(b) => return Some(b),
            None => {
                warn!("arbitrage_ordering - asset {} not found!", a);
                return None;
            }
        };
    }

    fn display_error(&self, err: Error) {
        match err.0 {
            BinanceLibErrorKind::BinanceError(code, msg, response) => match code {
                _ => error!("arbitrage_ordering - binance error. error code: {}, msg: {}", code, msg),
            },
            BinanceLibErrorKind::Msg(msg) => {
                error!("arbitrage_ordering - lib error. error: {}", msg)
            }
            _ => error!("arbitrage_ordering - other error. error: {}.", err.0),
        };
    }

    fn cancel_pending_transactions(&self, results: HashMap<u32, (Transaction, f32)>) {
        // cancelling pending transactions
        info!("arbitrage_ordering - cancelling pending transactions ...");
        let start_date = Instant::now();


        for (key, v) in results.iter() {
            let (transaction, qty) = v;

            debug!("{:?}", transaction);
            let symbol = &transaction.symbol;
            let order_id = &transaction.order_id;

            /// get order status
            match self.exchange.order_status(symbol, *order_id) {
                Ok(order) => {
                    debug!("{:?}", order);
                    let side = order.side;

                    // cancel order if it is pending
                    if order.status == "NEW" {
                        match self.exchange.cancel_order(symbol, *order_id) {
                            Ok(order_canceled) => {
                                warn!("arbitrage_ordering - {} : {:?}", key, order_canceled);
                            }
                            Err(e) => {
                                error!("arbitrage_ordering - {}", e)
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("{}", e);
                }
            }
        }
        info!("arbitrage_ordering - orders transactions cancelled. elapsed_time: {:?}", start_date.elapsed());
    }

    // Clean balances
    fn clean_balances(&self, arbitrage_profit: &ArbitrageProfit) {
        info!("arbitrage_ordering - cleaning balances ...");
        let start_date = Instant::now();

        for asset in &arbitrage_profit.get_asset_list() {
            if asset != "BTC" {
                info!("arbitrage_ordering - getting balance. asset: {}", asset);
                match self.exchange.get_balance(asset) {
                    Ok(balance) => {
                        // balance
                        info!("arbitrage_ordering - {:?}", balance);

                        // amount to sell
                        let amount = balance.free.parse::<f32>().unwrap_or_default();

                        // instrument to use
                        let instrument_id_str = format!("BINANCE_{}_BTC", asset).to_string();

                        // normalize qty
                        let qty = self.normalize_qty(instrument_id_str, amount);

                        let symbol = format!("{}{}", asset, "BTC");

                        // sell if qty > 0
                        if qty > 0.0 {
                            match self.exchange.market_sell(symbol, qty) {
                                Ok(answer) => {
                                    debug!("{:?}", answer);
                                }
                                Err(err) => {
                                    error!("arbitrage_ordering - market sell failure. {:?}", err.1);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("failed to get balance {:?}", e);
                    }
                }
            }
        }
        info!("arbitrage_ordering - balances cleaned. elapsed_time: {:?}", start_date.elapsed());
    }

    fn normalize_qty(&self, instrument_id: String, qty: f32) -> f32 {

        // find instrument
        let instrument: &Instrument = self.referencedata.get_instrument_by_id(instrument_id).unwrap();
        let step_size = instrument.get_step_size();

        //check step size
        if !step_size.is_nan() {
            if step_size == 1.0 {
                return qty.trunc();
            } else {
                let round_count: usize = step_size.to_string().len() - 2;
                return math::round_down(qty, round_count);
            }
        } else {
            return qty;
        }
    }

    pub fn is_busy(&self) -> bool {
        return self.busy;
    }

    // revert to start asset
    pub fn revert_to_start_asset(&self, symbol: String, side: String, qty: f32) {
        let start_asset = "BTC".to_string();

        // parameters
        let qty_ex = qty;
        let (base, quote) = symbol.split_at(symbol.len() - 3);

        if (quote == start_asset) & (side == "SELL") {
            // sell base to btc
            let symbol = format!("{}{}", base, quote);
            info!("arbitrage_ordering - market sell. symbol: {}, qty:{}", symbol, qty);
            self.exchange.market_sell(symbol, qty_ex);
        } else if (quote != start_asset) & (side == "BUY") {
            // sell quote to btc
            let symbol = format!("{}{}", quote, "BTC");
            info!("arbitrage_ordering - market sell. symbol: {}, qty:{}", symbol, qty);
            self.exchange.market_sell(symbol, qty_ex);
        } else if (quote != start_asset) & (side == "SELL") {
            // sell base to btc
            let symbol = format!("{}{}", base, "BTC");
            info!("arbitrage_ordering - market sell. symbol: {}, qty:{}", symbol, qty);
            self.exchange.market_sell(symbol, qty_ex);
        }
    }
}
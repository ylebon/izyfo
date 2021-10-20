use std::{thread, time};
use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::time::{Duration, Instant};
use std::env;
use binance::account::*;
use binance::api::*;
use binance::errors::Error;
use binance::errors::ErrorKind as BinanceLibErrorKind;
use binance::market::*;
use binance::model::Transaction;
use bus::Bus;
use crossbeam_channel;
use itertools::Itertools;
use log::{debug, error, info, trace, warn};
use serde::{Deserialize, Serialize};
use simplelog::*;

use crate::izyfo_arbitrage::arbitrage::{Arbitrage, ArbitrageProfit};
use crate::izyfo_arbitrage::arbitrage_database::ArbitrageDatabase;
use crate::izyfo_arbitrage::arbitrage_ordering::ArbitrageOrdering;
use crate::izyfo_arbitrage::arbitrage_transaction::ArbitrageTransactionResult;
use crate::izyfo_connectors;
use crate::izyfo_events::exchange::market_bbo::MarketBBO;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ExecutionMode {
    PARALLEL,
    SEQUENTIAL,
}


pub struct ArbitrageExecutor {
    profit_thresold: f32,
    qty_in: f32,
    exchange: String,
    start_asset: String,
    arbitrage_database: ArbitrageDatabase,
    transactions_list: Vec<Vec<HashMap<String, String>>>,
    symbol_list: Vec<String>,
    ordering: bool,
    market_bbo_bus: Bus<MarketBBO>,
}

impl ArbitrageExecutor {
    // return instruments
    pub fn get_instruments(&self) -> Vec<String> {
        let mut r: Vec<String> = Vec::new();
        r
    }

    // create new instance
    pub fn new(exchange: String, start_asset: String, symbol_list: &Vec<String>, qty_in: f32, profit_threshold: f32, ordering: bool) -> ArbitrageExecutor {
        // create bus
        let mut market_bbo_bus: Bus<MarketBBO> = Bus::new(1000);

        // arbitrage database
        let arbitrage_database = ArbitrageDatabase::new("".to_string());
        arbitrage_database.connect();


        // return arbitrage executor
        ArbitrageExecutor {
            profit_thresold: profit_threshold,
            qty_in: qty_in,
            arbitrage_database: arbitrage_database,
            ordering: ordering,
            transactions_list: Vec::new(),
            market_bbo_bus: market_bbo_bus,
            symbol_list: symbol_list.clone(),
            exchange: exchange,
            start_asset: start_asset,
        }
    }

    pub fn initialize(&mut self) {
        // initialize
        info!("arbitrage_executor - initializing.");

        // database instrument list
        let database_instrument_list = izyfo_connectors::database::list();

        // referencedata
        let url = String::from("");
        let referencedata_connector = izyfo_connectors::referencedata::ReferencedataConnector::from_url(url);
        let referencedata = referencedata_connector.get_referencedata(&self.exchange);

        // referencedata instrument list
        let referencedata_instrument_list = referencedata.get_instrument_list();

        // symbol list
        let combinations = self.symbol_list.iter().combinations(3);

        for c in combinations {
            let mut permutations: Vec<Vec<String>> = vec![
                vec![c[0].to_string(), c[1].to_string(), c[2].to_string()],
                vec![c[0].to_string(), c[2].to_string(), c[1].to_string()],
                vec![c[1].to_string(), c[0].to_string(), c[2].to_string()],
                vec![c[1].to_string(), c[2].to_string(), c[0].to_string()],
                vec![c[2].to_string().to_string(), c[0].to_string(), c[1].to_string()],
                vec![c[2].to_string(), c[1].to_string(), c[0].to_string()]
            ];


            for p in &permutations {
                let symbol_1 = &p[0];
                let symbol_2 = &p[1];
                let symbol_3 = &p[2];

                if symbol_1.to_string() == self.start_asset {
                    // transaction 1
                    let mut transaction_1: HashMap<String, String> = HashMap::new();
                    transaction_1.insert("source".to_string(), format!("{}_{}", self.exchange, symbol_1));
                    transaction_1.insert("target".to_string(), format!("{}_{}", self.exchange, symbol_2));

                    let mut instrument_a = format!("{}_{}_{}", self.exchange, symbol_1, symbol_2);
                    let mut instrument_b = format!("{}_{}_{}", self.exchange, symbol_2, symbol_1);

                    if database_instrument_list.contains(&instrument_a) {
                        transaction_1.insert("operation".to_string(), "SELL".to_string());
                        transaction_1.insert("instrument".to_string(), instrument_a);
                        transaction_1.insert("exchange_code".to_string(), format!("{}{}", symbol_1, symbol_2));
                    } else if database_instrument_list.contains(&instrument_b) {
                        transaction_1.insert("operation".to_string(), "BUY".to_string());
                        transaction_1.insert("instrument".to_string(), instrument_b);
                        transaction_1.insert("exchange_code".to_string(), format!("{}{}", symbol_2, symbol_1));
                    } else {
                        break;
                    }

                    // transaction 2
                    let mut transaction_2: HashMap<String, String> = HashMap::new();
                    transaction_2.insert("source".to_string(), format!("{}_{}", self.exchange, symbol_2));
                    transaction_2.insert("target".to_string(), format!("{}_{}", self.exchange, symbol_3));

                    instrument_a = format!("{}_{}_{}", self.exchange, symbol_2, symbol_3);
                    instrument_b = format!("{}_{}_{}", self.exchange, symbol_3, symbol_2);


                    if database_instrument_list.contains(&instrument_a) {
                        transaction_2.insert("operation".to_string(), "SELL".to_string());
                        transaction_2.insert("instrument".to_string(), instrument_a);
                        transaction_2.insert("exchange_code".to_string(), format!("{}{}", symbol_2, symbol_3));
                    } else if database_instrument_list.contains(&instrument_b) {
                        transaction_2.insert("operation".to_string(), "BUY".to_string());
                        transaction_2.insert("instrument".to_string(), instrument_b);
                        transaction_2.insert("exchange_code".to_string(), format!("{}{}", symbol_3, symbol_2));
                    } else {
                        break;
                    }

                    // transaction 3
                    let mut transaction_3: HashMap<String, String> = HashMap::new();
                    transaction_3.insert("source".to_string(), format!("{}_{}", self.exchange, symbol_3));
                    transaction_3.insert("target".to_string(), format!("{}_{}", self.exchange, symbol_1));

                    instrument_a = format!("{}_{}_{}", self.exchange, symbol_3, symbol_1);
                    instrument_b = format!("{}_{}_{}", self.exchange, symbol_1, symbol_3);

                    if database_instrument_list.contains(&instrument_a) {
                        transaction_3.insert("operation".to_string(), "SELL".to_string());
                        transaction_3.insert("instrument".to_string(), instrument_a);
                        transaction_3.insert("exchange_code".to_string(), format!("{}{}", symbol_3, symbol_1));
                    } else if database_instrument_list.contains(&instrument_b) {
                        transaction_3.insert("operation".to_string(), "BUY".to_string());
                        transaction_3.insert("instrument".to_string(), instrument_b);
                        transaction_3.insert("exchange_code".to_string(), format!("{}{}", symbol_1, symbol_3));
                    } else {
                        break;
                    }

                    // check instruments in referencedata
                    if referencedata_instrument_list.contains(&transaction_1.get("instrument").unwrap())
                        & referencedata_instrument_list.contains(&transaction_2.get("instrument").unwrap()) &
                        &referencedata_instrument_list.contains(&transaction_3.get("instrument").unwrap()) {
                        let transactions = vec![transaction_1, transaction_2, transaction_3];
                        self.transactions_list.push(transactions);
                    }
                }
            }
        }

        info!("arbitrage_executor - initialization finished. total arbitrage:{}", self.transactions_list.len());
    }

    // start arbitrage
    pub fn start(&mut self) {
        // receiver
        info!("arbitrage_executor - starting.");

        // qty initial
        let mut qty_initial: f32 = self.qty_in.clone();

        // mode
        let mode = ExecutionMode::PARALLEL;

        // arbitrage profit channel
        let (arbitrage_profit_sender, arbitrage_profit_receiver): (crossbeam_channel::Sender<ArbitrageProfit>, crossbeam_channel::Receiver<ArbitrageProfit>) = crossbeam_channel::unbounded();

        // arbitrage ordering
        let mut arbitrage_ordering: ArbitrageOrdering = ArbitrageOrdering::new(
            arbitrage_profit_receiver.clone(), mode.clone(),
        );

        // arbitrage ordering
        if self.ordering {
            // update balances
            arbitrage_ordering.update_balances();

            // balance
            let balance = arbitrage_ordering.get_balance(&self.start_asset);
            match balance {
                Some(b) => {
                    if b > &0.0 {
                        qty_initial = b.clone() / 3.0;
                    }
                }
                None => ()
            }
        }

        // start arbitrage ordering
        let c_arbitrage_profit_receiver = arbitrage_profit_receiver.clone();
        let c_mode = mode.clone();
        thread::spawn(move || {
            let mut arbitrage_ordering = ArbitrageOrdering::new(
                c_arbitrage_profit_receiver, c_mode,
            );
            arbitrage_ordering.start();
        });


        // arbitrage executor
        info!("arbitrage_executor - initial balance. balance:{}", qty_initial);

        // transactions
        for transactions in &self.transactions_list {

            // qty in
            let c_qty_in: f32 = qty_initial.clone();

            // arbitrage profit sender clone
            let c_arbitrage_profit_sender = arbitrage_profit_sender.clone();

            // scale
            let scale = true;

            // transactions clone
            let c_transactions = transactions.clone();

            // market bbo bus receiver
            let mut market_bbo_receiver = self.market_bbo_bus.add_rx();

            // ordering
            let c_ordering = self.ordering.clone();

            // arbitrage profit thread
            thread::spawn(move || {

                // arbitrage
                let mut arbitrage = Arbitrage::from_transaction_list(&c_transactions);
                info!("arbitrage_executor - arbitrage. name:{}, scale:{}, qty_in:{}", arbitrage.get_name(), scale, c_qty_in);

                // loop
                loop {
                    // receive market bbo
                    let market_bbo = market_bbo_receiver.recv().unwrap();

                    // get market bbo feed
                    let feed = market_bbo.get_feed();

                    // check arbitrage contains feed
                    if arbitrage.instrument_list.contains(&feed) {

                        // execute arbitrage
                        let arbitrage_profit = arbitrage.execute(&market_bbo, c_qty_in, scale);

                        match arbitrage_profit {
                            Some(p) => {
                                if p.get_profit() > 0.0 {
                                    info!("arbitrage_executor - arbitrage profit. profit:{}, latency:{}(ms)", p, p.get_latency_ms());
                                    if c_ordering & & p.is_valid_ordering(){
                                        c_arbitrage_profit_sender.send(p);
                                    }
                                }
                            }
                            None => {}
                        }
                    }
                }
            });
        }
    }


    // execute arbitrage
    pub fn execute(&mut self, market_bbo: MarketBBO) {
        self.market_bbo_bus.broadcast(market_bbo);
    }
}
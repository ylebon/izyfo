use postgres::{Connection, TlsMode};

use simplelog::*;
use log::{info, trace, warn};
use crate::izyfo_arbitrage::arbitrage::ArbitrageProfit;


pub struct ArbitrageDatabase{
    address: String
}

impl ArbitrageDatabase{
    pub fn new(address: String) -> ArbitrageDatabase{
        ArbitrageDatabase{
            address: address
        }
    }

    pub fn connect(&self){
        let mut connection = match Connection::connect(self.address.clone(), TlsMode::None){
            Ok(conn) => {
                info!("arbitrage_database connected=True");
                let trans = conn.transaction().unwrap();
                match trans.execute("create table if not exists triangle_arbitrage_binance (
                    id serial primary key,
                    name varchar(255),
                    date timestamp(3) with time zone,
                    profit DOUBLE PRECISION", &[]){
                        Ok(result) => println!("{}", result),
                        Err(err) => trace!("{}", format!("{}", err))
                    }
                trans.commit().unwrap();
            },
            Err(err) => warn!("{}", format!("failed connection error={}", err))
        };
    }

    pub fn add_profit(&self, arbitrage_profit: &ArbitrageProfit){

    }
}
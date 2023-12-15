/// config file
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug,Clone)]
pub struct AppConfig {
    pub name: String,
    pub http: HttpConfig,
    pub data: Data,
}

#[derive(Deserialize, Serialize, Debug,Clone)]
pub struct HttpConfig {
   pub  listen: String,
}

#[derive(Deserialize, Serialize, Debug,Clone)]
pub struct Data {
    pub db: DbConfig, // db set
}

#[derive(Deserialize, Serialize, Debug,Clone)]
pub struct DbConfig {
    pub dsn: String,          // db conn info
    pub log_level: String,    // log level
    pub idle_sec: u64,        //
    pub life_time: u64,       //
    pub connection: u32,      //
    pub conn_timeout: u64,    // connect timeout
    pub logging: bool,        // sqlx logging
    pub acquire_timeout: u64, // acquire
}

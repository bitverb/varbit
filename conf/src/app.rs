/// config file
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct AppConfig {
    pub name: String,
    pub http: HttpConfig,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct HttpConfig {
    listen: String,
}

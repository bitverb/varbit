pub mod task;

use std::time::Duration;

use log::{error, info};
use once_cell::sync::OnceCell;
use sqlx::{mysql::MySqlPoolOptions, MySql, Pool};

pub static DB_INSTANCE: OnceCell<Pool<MySql>> = OnceCell::new();

pub async fn init_database(db: conf::app::DbConfig) -> Pool<MySql> {
    info!("dsn {}", db.dsn);
    info!("max connections {}", db.connection);
    info!("show sqlx logging {}", db.logging);
    info!("connect timeout({})s", db.conn_timeout);
    info!("acquire_timeout timeout({})s", db.acquire_timeout);
    info!("idle_timeout timeout({})s", db.idle_sec);
    info!("max_lifetime timeout({})s", db.life_time);

    let opt: sqlx::pool::PoolOptions<MySql> = MySqlPoolOptions::new()
        .max_connections(db.connection.clone())
        .acquire_timeout(Duration::from_secs(db.acquire_timeout))
        .idle_timeout(Duration::from_secs(db.idle_sec))
        .max_lifetime(Duration::from_secs(db.life_time));

    match opt.connect(&db.dsn.to_owned()).await {
        Ok(pool) => {
            info!("✅Connection to the database is successful!");
            if let Err(err) = DB_INSTANCE.set(pool.clone()) {
                error!("unable to init db instance error {:?}", err);
            }
            pool
        }
        Err(err) => {
            panic!(
                "🔥 Failed to connect to the  database dsn {:?}: {:?}",
                db.dsn, err
            );
        }
    }
}

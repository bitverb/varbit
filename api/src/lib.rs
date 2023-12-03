/// api mod
pub mod animal;
pub mod flash;

use std:: time::Duration;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::get,
     Json, Router,
};
use log::{error, info};
use serde::{Deserialize, Serialize};
use sqlx::{mysql::MySqlPoolOptions,MySql, Pool};

pub async fn start(app_conf: conf::app::AppConfig) -> anyhow::Result<()> {
    let state = AppState {
        conn: build_db(app_conf.data.db.clone()).await,
    };

    let app = Router::new().route("/", get(index)).with_state(state);

    let listener = tokio::net::TcpListener::bind(&app_conf.http.listen.as_str())
        .await
        .unwrap();
    axum::serve(listener, app).await?;
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Data {
    key: String,
    value: String,
}

async fn index(state: State<AppState>, header: HeaderMap) -> (StatusCode, Json<Vec<Data>>) {
    // state.conn.
    let _ = &state.conn;
    info!("query {:?}", header);
    let mut v = vec![];
    for ele in &header {
        info!("value = {:?}", ele.1);
        v.push(Data {
            key: String::from(ele.0.to_string()),
            value: format!("{:#?}", ele.1),
        })
    }
    error!("");
    (StatusCode::OK, Json(v))
}

#[derive(Clone)]
struct AppState {
    conn: Pool<MySql>, // 数据库链接信息
}

async fn build_db(db: conf::app::DbConfig) -> Pool<MySql> {
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

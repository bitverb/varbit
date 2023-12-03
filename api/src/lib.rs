/// api mod
pub mod animal;
pub mod flash;

use axum::{
    extract::State,
    http::{HeaderMap, Method},
    routing::{get, post},
    Json, Router,
};

use ::chrono::Local;
use axum::BoxError;
use flash::Whortleberry;
use log::{error, info};
use serde::{Deserialize, Serialize};
use sqlx::{
    mysql::MySqlPoolOptions,
    types::chrono::{self},
    MySql, Pool,
};

use std::{collections::HashMap, net::SocketAddr, time::Duration};

use std::sync::{Arc, Mutex, MutexGuard};

use lazy_static::lazy_static;
lazy_static! {
    pub static ref W_LOCK: Arc<Mutex<i64>> = Arc::new(Mutex::new(0));
}

use tower_http::{
    cors::{self, CorsLayer},
    limit::RequestBodyLimitLayer,
};

pub async fn start(app_conf: conf::app::AppConfig) -> anyhow::Result<()> {
    let state: AppState = AppState {
        conn: build_db(app_conf.data.db.clone()).await,
    };

    let cors: CorsLayer = CorsLayer::new()
        .allow_methods(vec![Method::GET, Method::POST, Method::PUT])
        .allow_origin(cors::Any);
    let limit: RequestBodyLimitLayer = RequestBodyLimitLayer::new(1024 * 10);

    let app = Router::new()
        .layer(limit)
        .layer(cors)
        .route("/", get(index))
        .route("/task/start", post(start_task))
        .fallback(handler_404)
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
    Ok(())
}

pub async fn handler_404() -> Json<Whortleberry<HashMap<String, String>>> {
    let mut data = HashMap::new();
    data.insert(
        String::from("ts"),
        chrono::Utc::now().timestamp().to_string(),
    );
    data.insert(String::from("info"), "欢迎使用".to_owned());

    Json(Whortleberry {
        err_no: 404,
        err_msg: "a he, 404 not found!".to_string(),
        data,
    })
}

pub async fn time_out_handler(err: BoxError) -> Json<Whortleberry<HashMap<String, String>>> {
    let mut data: HashMap<String, String> = HashMap::new();
    data.insert(
        String::from("ts"),
        chrono::Utc::now().timestamp().to_string(),
    );
    data.insert(String::from("info"), "欢迎使用".to_owned());
    error!("error info {:?}", err);
    info!("error info {:?}", err);
    let v: Json<Whortleberry<HashMap<String, String>>> =
        if err.is::<tower::timeout::error::Elapsed>() {
            Json(Whortleberry {
                err_no: 404,
                err_msg: "time out".to_string(),
                data,
            })
        } else {
            Json(Whortleberry {
                err_no: 404,
                err_msg: "time out".to_string(),
                data,
            })
        };
    return v;
}

/// add background task
async fn start_task(_state: State<AppState>) -> Json<Whortleberry<(String, String)>> {
    tokio::task::spawn(async {
        let mut v = W_LOCK.lock().unwrap();
        *v += 1;
        info!("start a new task....... {v}",);
        let cry = service::task::json::ChrysaetosBit::new("_".to_owned(), 32);
        let obj = serde_json::from_str(r###"{"foo":"baz","complex":[1,2,3]}"###).unwrap();
        let res = cry.parse(&obj);
        info!("res {:?}", serde_json::to_string(&res).unwrap());
    });
    let _cnt: MutexGuard<'_, i64> = W_LOCK.lock().unwrap();
    Json(Whortleberry {
        err_no: 10000,
        err_msg: format!("success",).to_owned(),
        data: (
            Local::now().format("%Y-%m-%d %H:%M:%S.%f").to_string(),
            format!("success {}", *_cnt + 1),
        ),
    })
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Data {
    key: String,
    value: String,
}

async fn index(state: State<AppState>, header: HeaderMap) -> Json<Whortleberry<Vec<Data>>> {
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
    Json(Whortleberry {
        err_no: 10000,
        err_msg: "success".to_string(),
        data: v,
    })
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

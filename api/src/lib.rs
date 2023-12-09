/// api mod
pub mod animal;
pub mod flash;
pub mod kafka;
pub mod task;

use axum::{
    extract::{Query, State},
    http::{HeaderMap, Method},
    routing::{get, post},
    Json, Router,
};

use axum::BoxError;
use flash::Whortleberry;

use log::{error, info};
use serde::{Deserialize, Serialize};
use sqlx::{
    mysql::MySqlPoolOptions,
    types::chrono::{self},
    MySql, Pool,
};
use task::NewTaskRequest;

use std::{collections::HashMap, net::SocketAddr, time::Duration};

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
        .route("/task/start2", post(start_task2))
        .route("/task/cancel", post(cancel_task))
        .route("/task/new", post(create_task))
        .fallback(handler_404)
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
    Ok(())
}

pub async fn handler_404() -> Whortleberry<HashMap<String, String>> {
    let mut data = HashMap::new();
    data.insert(
        String::from("ts"),
        chrono::Utc::now().timestamp().to_string(),
    );
    data.insert(String::from("info"), "欢迎使用".to_owned());

    Whortleberry {
        err_no: 404,
        err_msg: "a he, 404 not found!".to_string(),
        data,
    }
}

pub async fn time_out_handler(err: BoxError) -> Whortleberry<HashMap<String, String>> {
    let mut data: HashMap<String, String> = HashMap::new();
    data.insert(
        String::from("ts"),
        chrono::Utc::now().timestamp().to_string(),
    );
    data.insert(String::from("info"), "欢迎使用".to_owned());
    error!("error info {:?}", err);
    info!("error info {:?}", err);
    if err.is::<tower::timeout::error::Elapsed>() {
        Whortleberry {
            err_no: 404,
            err_msg: "time out".to_string(),
            data,
        }
    } else {
        Whortleberry {
            err_no: 404,
            err_msg: "time out".to_string(),
            data,
        }
    }
}

// #[derive(Debug, Serialize, Default, Deserialize)]
// pub struct NewTaskRequest {
//     pub task_id: String,
// }

#[derive(Debug, Serialize, Default, Deserialize)]
pub struct CancelTaskReq {
    pub task_id: String,
}
async fn cancel_task(_state: State<AppState>, query: Query<CancelTaskReq>) -> Whortleberry<String> {
    let remove_task = pubg::task::remove_tasking(query.task_id.to_owned()).await;
    // debug is close
    info!("task id {:?} close task {:?}", query.task_id, remove_task);

    Whortleberry {
        err_no: 10000,
        err_msg: format!("success",).to_owned(),
        data: format!(
            "task_id:{} remove {:?}",
            query.task_id.to_owned(),
            remove_task
        ),
    }
}

#[derive(Debug, Serialize, Default, Deserialize)]
pub struct NewTaskRequest2 {
    pub task_id: String,
}
async fn start_task2(
    _state: State<AppState>,
    query: Query<NewTaskRequest2>,
) -> Whortleberry<(String, bool)> {
    info!("task id {:?}", query.task_id);
    let ok = pubg::task::dispatch_tasking(
        query.task_id.clone(),
        "kafka".to_owned(),
        &serde_json::json!({
           "broker":"localhost:9092",
           "topic":"my-topic",
           "group_id":format!("verb-{}",query.task_id.to_owned()),
           "encoder":"json",
           "meta":{
            "task_id":query.task_id.to_owned(),
           }
        }),
        "kafka".to_owned(),
        &serde_json::json!({
           "broker":"localhost:9092",
           "topic":"my-topic",
           "group_id":format!("verb-{}",query.task_id.to_owned()),
           "decoder":"json",
           "meta":{
            "task_id":query.task_id.to_owned(),
           }
        }),
    )
    .await;

    Whortleberry {
        err_no: 10000,
        err_msg: format!("success",).to_owned(),
        data: (query.task_id.to_owned(), ok),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Data {
    key: String,
    value: String,
}

async fn index(state: State<AppState>, header: HeaderMap) -> Whortleberry<Vec<Data>> {
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
    Whortleberry {
        err_no: 10000,
        err_msg: "success".to_string(),
        data: v,
    }
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

async fn create_task(
    state: State<AppState>,
    Json(req): Json<NewTaskRequest>,
) -> Whortleberry<Option<NewTaskRequest>> {
    info!("create task req {:?}", req);
    let src_cfg: Result<serde_json::Value, serde_json::Error> =
        serde_json::from_str(&req.src_cfg.to_owned().as_str());
    if src_cfg.is_err() {
        let err_msg = format!(
            "src_cfg error expected json format data but {:?} value is {:?}",
            src_cfg.err(),
            req.src_cfg
        );
        return Whortleberry {
            err_msg: err_msg,
            err_no: 400,
            data: None,
        };
    }

    let src_cfg_val: serde_json::Value = match req.src_type.as_str() {
        "kafka" => serde_json::from_str(req.src_cfg.to_owned().as_str()).unwrap(),
        _a @ _ => {
            info!("default patch {}", _a);
            serde_json::from_str("{}").unwrap()
        }
    };
    info!("src_cfg {:?}", src_cfg_val.to_owned().to_string());

    Whortleberry {
        err_msg: "success".to_owned(),
        err_no: 10_000,
        data: Some(req),
    }
}

/// api mod
pub mod animal;
pub mod flash;
pub mod kafka;
pub mod task;

use axum::{
    extract::{Query, State},
    http::{HeaderMap, Method},
    routing::{get, post},
    Router,
};

use axum::BoxError;
use flash::Whortleberry;

use log::{error, info};
use pubg::{input::Src, sink::Dst};
use serde::{Deserialize, Serialize};
use sqlx::{
    mysql::MySqlPoolOptions,
    types::chrono::{self},
    MySql, Pool,
};

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use tokio::sync::mpsc;
use tokio_context::context;

use tower_http::{
    cors::{self, CorsLayer},
    limit::RequestBodyLimitLayer,
};

use crate::task::TaskDetail;

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
        .route("/task/start2", post(start_task2))
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

#[derive(Debug, Serialize, Default, Deserialize)]
pub struct NewTaskRequest {
    pub task_id: String,
}
async fn start_task2(
    _state: State<AppState>,
    query: Query<NewTaskRequest>,
) -> Whortleberry<String> {
    info!("task id {:?}", query.task_id);
    let task_id = query.task_id.clone();

    {
        let mut _data: std::sync::MutexGuard<'_, HashMap<String, Arc<Box<dyn Src + Send + Sync>>>> =
            pubg::SRC_PLUGIN.lock().unwrap();
        let source = _data.get("kafka").unwrap();

        let (_, mut handle) = context::Context::new();
        let (rx, mut _tx) = mpsc::channel::<serde_json::Value>(20);

        let ctx = handle.spawn_ctx();
        let source = source.clone();
        let id = task_id.clone();
        let mut _dst: std::sync::MutexGuard<'_, HashMap<String, Arc<Box<dyn Dst + Send + Sync>>>> =
            pubg::DST_PLUGIN.lock().unwrap();
        let _dst = _dst.get("kafka").unwrap().clone();
        tokio::task::spawn(async move {
            _dst.to_dst(
                id.to_owned(),
                serde_json::json!({
                   "broker":"localhost:9092",
                   "topic":"my-topic",
                   "group_id":format!("verb-{}",id.to_owned()),
                   "encoder":"json",
                   "meta":{
                    "task_id":id.to_owned(),
                   }
                }),
                _tx,
            )
            .await;
        });
        let _ctx = handle.spawn_ctx();
        tokio::task::spawn(async move {
            source
                .from_src(
                    task_id.to_owned(),
                    &serde_json::json!({
                       "broker":"localhost:9092",
                       "topic":"my-topic",
                       "group_id":format!("verb-{}",task_id.to_owned()),
                       "decoder":"json",
                       "meta":{
                        "task_id":task_id.to_owned(),
                       }
                    }),
                    rx,
                )
                .await;
        });
    }

    // source.from_src(ctx, task_id, conf, sender)
    // }
    Whortleberry {
        err_no: 10000,
        err_msg: format!("success",).to_owned(),
        data: query.task_id.to_owned(),
    }
}
/// add background task
async fn start_task(
    _state: State<AppState>,
    query: Query<NewTaskRequest>,
) -> Whortleberry<Vec<task::Task>> {
    info!("task id {:?}", query.task_id);
    let contain = {
        task::GLOBAL_TASK_POOL
            .lock()
            .unwrap()
            .contain_task(&query.task_id)
    };
    let group_id = format!("verb-{}", query.task_id).clone();

    if !contain {
        let handler = tokio::task::spawn(async move {
            // let task = qu
            tokio::task::spawn(async move {
                let group_id = group_id.as_str();
                kafka::consume_and_print("localhost:9092", group_id, &vec!["my-topic"]).await;
            });
        });

        {
            let mut task_pool = task::GLOBAL_TASK_POOL.lock().unwrap();
            let _insert = task_pool.insert_task(TaskDetail::new(
                query.task_id.clone(),
                "demo".to_owned(),
                "kafka".to_owned(),
                "{}".to_owned(),
                "{}".to_owned(),
                "{}".to_owned(),
                handler,
            ));
        }
    } else {
        info!("task have already starting...");
    }

    let list = task::GLOBAL_TASK_POOL.lock().unwrap().task_list();
    Whortleberry {
        err_no: 10000,
        err_msg: format!("success",).to_owned(),
        data: list,
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

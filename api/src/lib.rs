/// api mod
pub mod animal;
pub mod flash;

use axum::{
    extract::{Query, State},
    http::{HeaderMap, Method},
    routing::{get, post},
    Json, Router,
};

use ::chrono::Local;
use axum::BoxError;
use flash::Whortleberry;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use sqlx::{
    mysql::MySqlPoolOptions,
    types::chrono::{self},
    MySql, Pool,
};

use std::{collections::HashMap, net::SocketAddr, thread, time::Duration};

use std::sync::{Arc, Mutex, MutexGuard};

use lazy_static::lazy_static;

use tower_http::{
    cors::{self, CorsLayer},
    limit::RequestBodyLimitLayer,
};

use crate::flash::Task;

lazy_static! {
    pub static ref W_LOCK: Arc<Mutex<i64>> = Arc::new(Mutex::new(0));
    pub static ref GLOBAL_TASK_POOL: Arc<Mutex<HashMap<String, Task>>> =
        Arc::new(Mutex::new(HashMap::new()));
}

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

#[derive(Debug, Serialize, Default, Deserialize)]
pub struct NewTaskRequest {
    pub task_id: String,
}
/// add background task
async fn start_task(
    _state: State<AppState>,
    query: Query<NewTaskRequest>,
) -> Json<Whortleberry<(String, String, HashMap<String, Task>)>> {
    info!("task id {:?}", query.task_id);
    let task_id = Arc::new(query.task_id.clone());
    let handler = tokio::task::spawn(async move {
        let mut task_p: MutexGuard<'_, HashMap<String, Task>> = GLOBAL_TASK_POOL.lock().unwrap();

        if (*task_p).contains_key(task_id.as_str()) {
            info!("task is running {}", task_id);
            return;
        }
        // insert task
        tokio::task::spawn(async {});
        task_p.insert(
            task_id.clone().to_string(),
            Task {
                id: task_id.to_owned().to_string(),
                name: "foo".to_owned(),
                last_heartbeat: 0,
            },
        );
    });

    let _cnt: MutexGuard<'_, i64> = W_LOCK.lock().unwrap();
    let v: MutexGuard<'_, HashMap<String, Task>> = GLOBAL_TASK_POOL.lock().unwrap();
    let mut data: HashMap<String, Task> = HashMap::new();
    for ele in v.iter() {
        data.insert(ele.0.to_string(), ele.1.to_owned().clone());
    }
    Json(Whortleberry {
        err_no: 10000,
        err_msg: format!("success",).to_owned(),
        data: (
            Local::now().format("%Y-%m-%d %H:%M:%S.%f").to_string(),
            format!("success {}", *_cnt + 1),
            data,
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

use kafka::{
    consumer::{Consumer, FetchOffset, GroupOffsetStorage, MessageSets},
    producer::AsBytes,
};

async fn start_clean_task(task_id: String, group: String, topic: String, brokers: Vec<String>) {
    // todo
    {
        let tasks = GLOBAL_TASK_POOL.lock().unwrap();
        if (*tasks).contains_key(task_id.to_owned().as_str()) {
            info!("task{} is already in pool", task_id);
            let before = Duration::from_secs(
                (*tasks)
                    .get(task_id.to_owned().as_str())
                    .unwrap()
                    .last_heartbeat as u64,
            );
            if std::time::Instant::now().elapsed() - before < Duration::from_secs(60) {
                return;
            }
        }
    }
    let mut conn = Consumer::from_hosts(brokers)
        .with_topic(topic.to_owned())
        .with_group(group.to_owned())
        .with_fallback_offset(FetchOffset::Earliest)
        .with_fetch_max_bytes_per_partition(1_000_000)
        .with_offset_storage(Some(GroupOffsetStorage::Kafka))
        .create()
        .unwrap();
    info!("链接 kafka 成功 {}", conn.client().client_id());

    let mut cnt = 0;
    loop {
        let m = conn.poll();
        if m.is_err() {
            error!("poll {} msg error {:?}", topic, m.err());
            thread::sleep(Duration::from_millis(500));

            continue;
        }

        let msgs = m.unwrap();

        if msgs.is_empty() {
            debug!("empty msg {}", topic);
            thread::sleep(Duration::from_millis(500));
            continue;
        }
        for cmsg in msgs.iter() {
            for msg in cmsg.messages() {
                cnt += 1;
                info!("msg {:?}", msg.value.as_bytes());
            }
        }
        conn.commit_consumed().unwrap();
        if cnt % 1000 == 0 {
            {
                let mut l = GLOBAL_TASK_POOL.lock().unwrap();
                if (*l).contains_key(task_id.to_owned().as_str()) {
                    let v: &Task = (*l).get(task_id.to_owned().as_str()).unwrap();
                    let mut v = v.clone();
                    v.last_heartbeat = 1;
                    (*l).insert(task_id.to_owned(), v);
                }
            }
        }
    }
}

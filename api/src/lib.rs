/// api mod
pub mod animal;
pub mod flash;
pub mod kafka;
pub mod task;

use axum::{
    extract::{Query, State},
    http::{HeaderMap, Method},
    routing::{get, post, put},
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
use task::{NewTaskRequest, Task};

use std::{collections::HashMap, fmt::format, net::SocketAddr, time::Duration};

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
        .route("/connect_testing", post(connect_testing))
        .route("/task/list", get(fetch_task_list))
        .route("/task/update", put(update_task))
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

/// create task
/// 1. check src type is ok
/// 2. check src config is ok?
/// 3. check dst_type is ok?
/// 4. check dst_config is ok
/// 5. check tasking is ok
/// 6. save to database
async fn create_task(
    state: State<AppState>,
    Json(req): Json<NewTaskRequest>,
) -> Whortleberry<Option<task::NewTaskRequest>> {
    info!("create task req {:?}", req);

    // check  src type is  kafka
    if req.src_type != "kafka" {
        error!("not support src type {}", req.src_type);
        return Whortleberry {
            err_msg: format!("not support src type  {}", req.src_type),
            err_no: 10_006,
            data: None,
        };
    }

    // check dst _ type is kafka
    if req.dst_type != "kafka" {
        error!("not support dst type {}", req.dst_type);
        return Whortleberry {
            err_msg: format!("not support dst type  {}", req.dst_type),
            err_no: 10_006,
            data: None,
        };
    }

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

    if let Err(err) = serde_json::from_str::<serde_json::Value>(req.tasking_cfg.as_str()) {
        error!("invalid json format for tasking {:?}", err);
        return Whortleberry {
            err_msg: format!(
                "invalid json format for tasking {:?}",
                req.tasking_cfg.to_string()
            ),
            err_no: 400,
            data: None,
        };
    }

    if let Err(err) = serde_json::from_str::<serde_json::Value>(req.src_cfg.as_str()) {
        error!(
            "invalid json format for src config {:?} error:{:?}",
            req.src_cfg, err
        );
        return Whortleberry {
            err_msg: format!("invalid json format for src config {:?}", req.src_cfg),
            err_no: 400,
            data: None,
        };
    }

    if let Err(err) = serde_json::from_str::<serde_json::Value>(req.dst_cfg.as_str()) {
        error!(
            "invalid json format for dst config {:?} error:{:?}",
            req.dst_cfg, err
        );
        return Whortleberry {
            err_msg: format!("invalid json format for dst config {:?}", req.dst_cfg),
            err_no: 400,
            data: None,
        };
    }
    let t = task::Task::from_task_detail(&req);

    info!("task is {:?}", t);

    info!("src_cfg {:?}", src_cfg_val.to_owned().to_string());

    let v = sqlx::query(
        r###"INSERT INTO task (
        id,
        name,
        last_heartbeat,
        src_type,
        dst_type,
        src_cfg,
        dst_cfg,
        status,
        created_at,
        updated_at,
        deleted_at,
        tasking_cfg) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)"###,
    )
    .bind(t.id)
    .bind(t.name)
    .bind(t.last_heartbeat)
    .bind(t.src_type)
    .bind(t.dst_type)
    .bind(t.src_cfg)
    .bind(t.dst_cfg)
    .bind(t.status)
    .bind(t.created_at)
    .bind(t.updated_at)
    .bind(t.deleted_at)
    .bind(t.tasking_cfg)
    .execute(&state.conn)
    .await;
    if v.is_err() {
        error!("save task into database error {:?}", v.err());
        return Whortleberry {
            err_msg: "save task to database error".to_owned(),
            err_no: 10_005,
            data: None,
        };
    }
    info!("create new task success result {:?} ", Some(v).unwrap());
    Whortleberry {
        err_msg: "success".to_owned(),
        err_no: 10_000,
        data: Some(req),
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConnectTestingRequest {
    pub detect_type: String,
    pub cfg: serde_json::Value,
}

// connect
async fn connect_testing(Json(req): Json<ConnectTestingRequest>) -> Whortleberry<String> {
    info!("testing..... connect {:?}", req);
    if req.detect_type != "kafka" {
        return Whortleberry {
            err_msg: format!("not support {}", req.detect_type),
            err_no: 10_003,
            data: "".to_owned(),
        };
    }

    let res = kafka::kafka_test_connect(&req.cfg);
    if res.is_err() {
        Whortleberry {
            err_msg: "failed to find topic".to_owned(),
            err_no: 10_001,
            data: res.err().unwrap(),
        }
    } else {
        Whortleberry {
            err_msg: "success".to_owned(),
            err_no: 10_000,
            data: "success".to_owned(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct FetchTaskListReq {
    pub status: i32,
    pub page_size: i32,
    pub page: i32,
}

async fn fetch_task_list(
    state: State<AppState>,
    Query(mut req): Query<FetchTaskListReq>,
) -> Whortleberry<Vec<Task>> {
    // sqlx::query!("SELECT * FROM task where status = ?",).
    if req.page_size <= 0 || req.page_size >= 100 {
        req.page_size = 100
    }
    if req.page <= 0 {
        req.page = 0
    }
    // fetch task
    let res = sqlx::query_as::<MySql, Task>("SELECT * FROM task WHERE status = ? limit ? offset ?")
        .bind(req.status)
        .bind(req.page_size)
        .bind(req.page * req.page_size)
        .fetch_all(&state.conn)
        .await;
    if res.is_err() {
        let err = format!("error {:?}", res.err());
        error!("{}", err);
        return Whortleberry {
            err_msg: err,
            err_no: 10_003,
            data: vec![],
        };
    }

    info!("fetch task _list {:?}", req);
    Whortleberry {
        err_msg: "".to_owned(),
        err_no: 10000,
        data: res.unwrap(),
    }
}

// update task
async fn update_task(
    state: State<AppState>,
    Json(req): Json<task::UpdateTaskRequest>,
) -> Whortleberry<String> {
    info!("req ..{:?}", req);
    // sqlx::query("SELECT count(*) FROM task WHERE id = ?").bind(req.id).execute(state.conn).await
    // todo
    Whortleberry {
        err_msg: "success".to_owned(),
        err_no: 10_000,
        data: "".to_owned(),
    }
}

/// api mod
pub mod flash;
pub mod kafka;
pub mod task;

use async_trait::async_trait;

use axum::{
    extract::{Query, State},
    http::{HeaderValue, Method},
    routing::{get, post, put},
    Json, Router,
};

use ::chrono::Local;
use axum::BoxError;
use schema::{DB_INSTANCE, init_database};
use flash::Whortleberry;

use log::{error, info};
use pubg::{
    input::kafka::{KafkaSourceConfig, KafkaSourceMeta},
    sink::kafka::{check_dst_cfg, DstConfigReq, KafkaDstConfig, KafkaDstMeta},
    task::{dispatch_tasking, task_running},
    CloseTask,
};
use serde::{Deserialize, Serialize};
use service::task::json::check_chrysaetos_bit_cfg;
use sqlx::{
    types::chrono::{self},
    MySql, Pool,
};
use task::{NewTaskRequest, Task};

use std::{collections::HashMap, net::TcpListener};

use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    limit::RequestBodyLimitLayer,
};

use crate::task::{update_task_status, TaskStatus};

pub async fn start(app_conf: conf::app::AppConfig) -> anyhow::Result<()> {
    let state: AppState = AppState {
        conn: init_database(app_conf.data.db.clone()).await,
    };

    let cors: CorsLayer = CorsLayer::new()
        .allow_methods(vec![
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::OPTIONS,
        ])
        .allow_origin(AllowOrigin::list([
            "http://localhost:5173/*".parse::<HeaderValue>().unwrap(),
            "http://localhost:8080/*".parse::<HeaderValue>().unwrap(),
            "http://127.0.0.1:5173/*".parse::<HeaderValue>().unwrap(),
            "http://127.0.0.1:8080".parse::<HeaderValue>().unwrap(),
            "http://0.0.0.0:5173/*".parse::<HeaderValue>().unwrap(),
        ]));
    let limit: RequestBodyLimitLayer = RequestBodyLimitLayer::new(1024 * 1024 * 10);

    // let cors = CorsLayer::new()
    // .allow_methods(vec![Method::GET, Method::POST, Method::OPTIONS])
    // .allow_origin(Any)
    // .allow_credentials(false);

    let app = Router::new()
        .layer(cors.clone())
        .layer(limit)
        .route("/task/cancel", post(cancel_task))
        .route("/task/new", post(create_task))
        .route("/connect_testing", post(connect_testing))
        .route("/task/list", get(fetch_task_list).layer(cors.clone()))
        .route("/task/count", get(fetch_count).layer(cors.clone()))
        .route("/task/update", put(update_task))
        .route("/task/start", get(start_tasking))
        .fallback(handler_404)
        .with_state(state);

    info!("listen {}", app_conf.http.listen.to_owned());
    let listener = TcpListener::bind(app_conf.http.listen.to_owned())
        .expect(format!("unable to listen {}", app_conf.http.listen.to_owned()).as_str());
    axum::Server::from_tcp(listener)
        .expect("server bind  tcp error ")
        .serve(app.into_make_service())
        .await
        .unwrap();

    Ok(())
}

pub async fn handler_404() -> Whortleberry<HashMap<String, String>> {
    let mut data = HashMap::new();
    data.insert(
        String::from("ts"),
        Local::now().format("%Y-%m-%d %H:%M:%S.%3f").to_string(),
    );
    data.insert(String::from("info"), "welcome use freebit".to_owned());

    Whortleberry {
        err_no: 404,
        err_msg: "ê hə, not found the specific resource!".to_string(),
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

#[derive(Debug, Serialize, Deserialize)]
pub struct Data {
    key: String,
    value: String,
}

#[derive(Clone)]
struct AppState {
    conn: Pool<MySql>, // 数据库链接信息
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
) -> Whortleberry<Option<Task>> {
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

    // src cfg
    if let Err(err) = serde_json::from_value::<KafkaSrcCfg>(req.src_cfg.clone()) {
        error!("invalid src cfg expected json config {:?}", err);
        return Whortleberry {
            err_no: 400,
            err_msg: format!("invalid json format config {:?}", err),
            data: None,
        };
    }

    // dst config
    if let Err(err) = serde_json::from_value::<DstConfigReq>(req.dst_cfg.clone()) {
        error!(
            "invalid dst cfg expected {:?} json format {:?}",
            req.dst_cfg, err
        );
        return Whortleberry {
            err_msg: format!(
                "invalid dst cfg expected json format but get {:?} error{:?}",
                req.dst_cfg, err
            ),
            err_no: 400,
            data: None,
        };
    }

    if let Err(err) = check_chrysaetos_bit_cfg(&req.tasking_cfg) {
        error!("invalid json format for tasking {:?}", err);
        return Whortleberry {
            err_msg: format!(
                "invalid json format for tasking {:?}, error{:?}",
                req.tasking_cfg.to_string(),
                err
            ),
            err_no: 400,
            data: None,
        };
    }

    let mut task = task::Task::from_task_detail(&req);

    info!("task is {:?}", task);

    match task::create_task(&state.conn, &mut task).await {
        Ok(_) => {}
        Err(err) => {
            error!("save task into database error {:?}", err);
            return Whortleberry {
                err_msg: "save task to database error".to_owned(),
                err_no: 10_005,
                data: None,
            };
        }
    };

    Whortleberry {
        err_msg: "success".to_owned(),
        err_no: 10_000,
        data: Some(task),
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

#[derive(Debug, Deserialize)]
struct FetchCountReq {
    status: i32,
}
async fn fetch_count(
    state: State<AppState>,
    Query(req): Query<FetchCountReq>,
) -> Whortleberry<Option<i64>> {
    match task::count_task(&state.conn, req.status).await {
        Err(err) => {
            error!(
                "failed to count task, status:{}  error:{:?}",
                req.status, err
            );
            Whortleberry {
                err_msg: "success".to_owned(),
                err_no: 10_000,
                data: None,
            }
        }
        Ok(count) => Whortleberry {
            err_msg: "success".to_owned(),
            err_no: 10_000,
            data: Some(count),
        },
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
    if req.page_size <= 0 || req.page_size >= 100 {
        req.page_size = 100
    }
    if req.page <= 0 {
        req.page = 0
    }
    // fetch task
    let res = match task::fetch_task_list(&state.conn, req.status, req.page_size, req.page).await {
        Ok(v) => v,
        Err(err) => {
            let err = format!("error {:?}", err);
            error!("{}", err);
            return Whortleberry {
                err_msg: err,
                err_no: 10_003,
                data: vec![],
            };
        }
    };

    info!("fetch task _list {:?}", req);
    Whortleberry {
        err_msg: "".to_owned(),
        err_no: 10000,
        data: res,
    }
}

// update task
async fn update_task(
    state: State<AppState>,
    Json(req): Json<task::UpdateTaskRequest>,
) -> Whortleberry<Option<String>> {
    // check task is running....
    if pubg::task::task_running(&req.id).await {
        error!("task is running {}", req.id);
        return Whortleberry {
            err_msg: format!("task {} is running", req.id),
            err_no: 10_008,
            data: None,
        };
    }

    // src cfg error
    if let Err(err) = serde_json::from_value::<KafkaSrcCfg>(req.src_cfg.clone()) {
        error!("invalid src cfg expected json config {:?}", err);
        return Whortleberry {
            err_no: 400,
            err_msg: format!("invalid json format config {:?}", err),
            data: None,
        };
    }

    // check dst config
    match pubg::sink::kafka::check_dst_cfg(&req.dst_cfg) {
        Ok(_) => (),
        Err(err) => {
            error!("update task dst cfg {} error{:?}", &req.src_cfg, err);
            return Whortleberry {
                err_msg: format!("invalid dst cfg  {} error:{:?}", req.src_cfg, err),
                err_no: 400,
                data: None,
            };
        }
    };

    // check config is ok
    match check_chrysaetos_bit_cfg(&req.tasking_cfg) {
        Ok(_) => (),
        Err(err) => {
            error!("update task tasking cfg {} error{:?}", &req.src_cfg, err);
            return Whortleberry {
                err_msg: format!("invalid tasking cfg  {} error:{:?}", req.src_cfg, err),
                err_no: 400,
                data: None,
            };
        }
    }
    let mut task = req.to_task();
    match task::update_task(&state.conn, &mut task).await {
        Err(err) => {
            return Whortleberry {
                err_msg: format!("update task {}  error:{:?}", req.id, err),
                err_no: 400,
                data: None,
            };
        }
        _ => {}
    }
    // update task
    // check task is ok
    info!("req ...{:?}", req);
    Whortleberry {
        err_msg: "success".to_owned(),
        err_no: 10_000,
        data: Some("".to_owned()),
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct StartTaskingRequest {
    pub task_id: String,
}

async fn start_tasking(
    state: State<AppState>,
    Query(req): Query<StartTaskingRequest>,
) -> Whortleberry<String> {
    info!("start tasking.....{:?}", req);
    if task_running(&req.task_id).await {
        info!("task {} is running", req.task_id);
        return Whortleberry {
            err_msg: format!("task {} is running ", req.task_id),
            err_no: 10_000,
            data: "success".to_owned(),
        };
    }

    let task = match sqlx::query_as::<MySql, Task>("SELECT * FROM task where id = ?")
        .bind(&req.task_id)
        .fetch_one(&state.conn)
        .await
    {
        Ok(v) => v,
        Err(err) => {
            error!("failed to find {:?} error: {:?}", &req.task_id, err);
            return Whortleberry {
                err_msg: format!("failed to find task{}, error:{:?}", req.task_id, err),
                err_no: 10_001,
                data: "unable to find task".to_owned(),
            };
        }
    };

    info!("data is {:?}", task);
    let src_cfg: KafkaSrcCfg = match serde_json::from_str::<KafkaSrcCfg>(&task.src_cfg.as_str()) {
        Ok(v) => v,
        Err(err) => {
            error!(
                "failed to un marshal src cfg {:?} error{:?}",
                task.src_cfg, err
            );
            return Whortleberry {
                err_msg: format!("failed to find task{}, error:{:?}", req.task_id, err),
                err_no: 10_001,
                data: "unable to find task".to_owned(),
            };
        }
    };

    let kafka_src_cfg = KafkaSourceConfig {
        broker: src_cfg.broker.clone(),
        group_id: format!("verb-{}", task.id.to_owned()),
        decoder: src_cfg.decoder.to_owned(),
        topic: src_cfg.topic.to_owned(),
        meta: KafkaSourceMeta {
            task_id: task.id.to_owned(),
        },
    };

    let dst_cfg = match check_dst_cfg(&serde_json::from_str(&task.dst_cfg).unwrap()) {
        Ok(v) => v,
        Err(err) => {
            error!(
                "failed to un marshal dst cfg {:?} error{:?}",
                task.dst_cfg, err
            );
            return Whortleberry {
                err_msg: format!("failed to find task{}, error:{:?}", req.task_id, err),
                err_no: 10_001,
                data: "unable to find task".to_owned(),
            };
        }
    };
    let kafka_sink_cfg = KafkaDstConfig {
        broker: dst_cfg.broker.to_owned(),
        topic: dst_cfg.topic.to_owned(),
        encoder: "json".to_owned(),
        meta: KafkaDstMeta {
            task_id: task.id.to_owned(),
        },
    };

    info!(
        "task {} src_cfg {:#?} dst_cfg {:#?}",
        task.id, kafka_src_cfg, kafka_sink_cfg
    );
    dispatch_tasking(
        task.id.to_owned(),
        task.src_type.to_owned(),
        &serde_json::json!(&kafka_src_cfg),
        task.dst_type.to_owned(),
        &serde_json::json!(&kafka_sink_cfg),
        Box::new(CloseTaskImpl {}),
    )
    .await;
    // task_running(task_id)
    match update_task_status(
        &state.conn,
        task.id.clone(),
        TaskStatus::Running.get_status(),
    )
    .await
    {
        Ok(_) => (),
        Err(err) => {
            error!("update task {} status error {:?}", task.id.clone(), err);
            return Whortleberry {
                err_no: 10_109,
                err_msg: "failed".to_owned(),
                data: format!("unable to start task {:?}", err),
            };
        }
    }

    // check task is exists
    return Whortleberry {
        err_no: 10000,
        err_msg: "success".to_owned(),
        data: "success".to_owned(),
    };
}

struct CloseTaskImpl {}

#[async_trait]
impl CloseTask for CloseTaskImpl {
    async fn close_task(&self, task_id: String) {
        info!("update task {} status ", task_id.clone());
        let conn = DB_INSTANCE.get().unwrap();
        match update_task_status(conn, task_id.clone(), TaskStatus::Cancel.get_status()).await {
            Err(err) => {
                error!("update task {} error {:?}", task_id.clone(), err)
            }
            Ok(_) => {
                info!("update task {} success", task_id.to_owned())
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct KafkaSrcCfg {
    /// decoder
    pub decoder: String, // json format
    /// broker
    pub broker: String, // broker
    /// topic
    pub topic: String, // topic
}

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use axum::{
    extract::{Path, Query, State},
    Json,
};
use log::{error, info};
use pubg::{
    input::kafka::{KafkaSourceConfig, KafkaSourceMeta},
    sink::kafka::{check_dst_cfg, DstConfigReq, KafkaDstConfig, KafkaDstMeta},
    task::{dispatch_tasking, task_running},
    CloseTask,
};
use schema::{
    task::{get_running_task, update_task_status, Task, TaskStatus},
    DB_INSTANCE,
};
use serde::{Deserialize, Serialize};
use service::task::json::check_chrysaetos_bit_cfg;
use sqlx::{MySql, Pool};

use crate::{flash::Whortleberry, kafka};

#[derive(Clone)]
pub struct AppState {
    pub conn: Pool<MySql>, // 数据库链接信息
}

#[derive(Debug, Serialize, Default, Deserialize)]
pub struct CancelTaskReq {
    pub task_id: String,
}

pub async fn cancel_task(query: Query<CancelTaskReq>) -> Whortleberry<String> {
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

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NewTaskRequest {
    // task name
    pub name: String,
    // src type like kafka
    pub src_type: String,
    // src config json format
    pub src_cfg: serde_json::Value,
    // dst type like kafka
    pub dst_type: String,
    /// dst cfg json format
    pub dst_cfg: serde_json::Value,
    // json format
    pub tasking_cfg: serde_json::Value,
    // debug 文本
    pub debug_text: serde_json::Value,
    // 节点属性
    pub properties: serde_json::Value,
}

/// create task
/// 1. check src type is ok
/// 2. check src config is ok?
/// 3. check dst_type is ok?
/// 4. check dst_config is ok
/// 5. check tasking is ok
/// 6. save to database
pub async fn create_task(
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

    let mut task = schema::task::Task::from_task_detail(
        &req.name,
        &req.src_type,
        &req.dst_type,
        &req.src_cfg,
        &req.dst_cfg,
        &req.tasking_cfg,
        &req.debug_text,
        &req.properties,
    );

    info!("task is {:?}", task);

    match schema::task::create_task(&state.conn, &mut task).await {
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
pub async fn connect_testing(Json(req): Json<ConnectTestingRequest>) -> Whortleberry<String> {
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
pub struct FetchTaskListReq {
    pub status: i32,
    pub page_size: i32,
    pub page: i32,
}

pub async fn fetch_task_list(
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
    let res = match schema::task::fetch_task_list(&state.conn, req.status, req.page_size, req.page)
        .await
    {
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

#[derive(Debug, Deserialize)]
pub struct FetchTaskRequest {
    pub task_id: String,
}
pub async fn fetch_task(
    state: State<AppState>,
    Path(req): Path<FetchTaskRequest>,
) -> Whortleberry<Option<Task>> {
    match schema::task::fetch_task(&state.conn, &req.task_id).await {
        Err(err) => {
            error!("failed to find task {:?}", err);
            Whortleberry {
                err_msg: "failed to find task".to_owned(),
                err_no: 10_200,
                data: None,
            }
        }
        Ok(res) => Whortleberry {
            err_no: 10_000,
            err_msg: "success".to_owned(),
            data: Some(res),
        },
    }
}

#[derive(Debug, Deserialize)]
pub struct FetchCountReq {
    status: i32,
}

pub async fn fetch_count(
    state: State<AppState>,
    Query(req): Query<FetchCountReq>,
) -> Whortleberry<Option<i64>> {
    match schema::task::count_task(&state.conn, req.status).await {
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

#[derive(Debug, Deserialize)]
pub struct UpdateTaskRequest {
    /// task id
    pub id: String,
    // task name
    pub name: String,
    // src type like kafka
    pub src_type: String,
    // src config json format
    pub src_cfg: serde_json::Value,
    // dst type like kafka
    pub dst_type: String,
    /// dst cfg json format
    pub dst_cfg: serde_json::Value,
    /// tasking cfg json format
    pub tasking_cfg: serde_json::Value,

    pub debug_text: serde_json::Value,
    pub properties: serde_json::Value,
}

impl UpdateTaskRequest {
    pub fn to_task(&self) -> Task {
        let mut task = Task::default();
        task.id = self.id.clone();
        task.name = self.name.clone();
        task.dst_cfg = self.dst_cfg.to_string();
        task.src_cfg = self.src_cfg.to_string();
        task.src_type = self.src_type.to_string();
        task.dst_type = self.dst_type.to_string();
        task.tasking_cfg = self.tasking_cfg.to_string();
        task.debug_text = self.debug_text.to_string();
        task.properties = self.properties.to_string();
        return task;
    }
}

// update task
pub async fn update_task(
    state: State<AppState>,
    Json(req): Json<UpdateTaskRequest>,
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
    match schema::task::update_task(&state.conn, &mut task).await {
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
pub struct StartTaskingRequest {
    pub task_id: String,
}

pub async fn start_tasking(
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

pub async fn continue_running_task() {
    if let Ok(task_list) = get_running_task().await {
        for task in &task_list {
            info!("continue running task {}", task.id.clone());
            let src_cfg: KafkaSrcCfg =
                match serde_json::from_str::<KafkaSrcCfg>(&task.src_cfg.as_str()) {
                    Ok(v) => v,
                    Err(err) => {
                        error!(
                            "failed to un marshal src cfg {:?} error{:?}",
                            task.src_cfg, err
                        );
                        continue;
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
                    continue;
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
        }
    }
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

#[derive(Debug, Deserialize)]
pub struct DebugRequest {
    pub sep: String,
    pub debug: serde_json::Value,
}

pub async fn task_debug(
    Json(req): Json<DebugRequest>,
) -> Whortleberry<service::task::json::CRHRes> {
    let p = service::task::json::ChrysaetosBitFlow::from_sep(req.sep.to_owned());
    let v = p.property(req.debug.clone());
    Whortleberry {
        err_msg: "success".to_owned(),
        err_no: 10_000,
        data: v,
    }
}

#[derive(Debug, Deserialize)]
pub struct TaskDebugPreviewRequest {
    pub sep: String,
    pub debug: serde_json::Value,
    pub ignore: HashSet<String>,
    pub fold: HashSet<String>,
    pub max_depth: i32,
}
pub async fn task_debug_preview(
    Json(req): Json<TaskDebugPreviewRequest>,
) -> Whortleberry<Vec<HashMap<String, serde_json::Value>>> {
    let parser = service::task::json::ChrysaetosBit::new_cfg(
        "debug_preview".to_owned(),
        req.sep.to_owned(),
        req.max_depth.clone(),
        req.fold.clone(),
        req.ignore,
    );

    let res = parser.parse(&"debug_preview".to_owned(), &req.debug);
    Whortleberry {
        err_msg: "success".to_owned(),
        err_no: 10_000,
        data: res,
    }
}

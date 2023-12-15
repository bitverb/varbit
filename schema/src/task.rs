use log::{error, info};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use sqlx::{FromRow, MySql, Pool};

use crate::DB_INSTANCE;
pub enum TaskStatus {
    // task create but not starting
    Created,
    Running,
    Cancel,
    Error,
    Deleted,
}

impl TaskStatus {
    pub fn get_status(&self) -> i32 {
        match self {
            Self::Created => 1,
            Self::Running => 2,
            Self::Cancel => 3,
            Self::Error => 4,
            Self::Deleted => 5,
        }
    }
}

#[derive(Clone, Debug, Serialize, Default, FromRow)]
pub struct Task {
    // task id
    pub id: String,
    /// task name
    pub name: String,
    // latest heartbeat
    pub last_heartbeat: i64,
    // source type like kafka
    pub src_type: String,
    // sink type like kafka
    pub dst_type: String,
    /// json format src config
    pub src_cfg: String,
    // json format dst config
    pub dst_cfg: String,
    // task status
    pub status: i32,
    // task create time
    pub created_at: i64,
    // task updated at
    pub updated_at: i64,
    // task deleted at
    pub deleted_at: i64,
    // tasking config
    pub tasking_cfg: String,
}

pub async fn fetch_task_list(
    conn: &Pool<MySql>,
    status: i32,
    page_size: i32,
    page: i32,
) -> Result<Vec<Task>, String> {
    match sqlx::query_as::<MySql, Task>("SELECT * FROM task WHERE status = ? limit ? offset ?")
        .bind(status)
        .bind(page_size)
        .bind(page * page_size)
        .fetch_all(conn)
        .await
    {
        Err(err) => Err(format!("unable to find task error {:?}", err)),
        Ok(res) => Ok(res),
    }
}

pub async fn update_task(conn: &Pool<MySql>, task: &mut Task) -> Result<i32, String> {
    match sqlx::query(
        r#"UPDATE task SET name =?,
        src_type =?,
         src_cfg=?,
         dst_type=?,
         dst_cfg=?,
         tasking_cfg=?,
         updated_at = ?
         WHERE id =?"#,
    )
    .bind(&task.name)
    .bind(&task.src_type)
    .bind(&task.src_cfg)
    .bind(&task.dst_type)
    .bind(&task.dst_cfg)
    .bind(&task.tasking_cfg)
    .bind(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
    )
    .bind(&task.id)
    .execute(conn)
    .await
    {
        Ok(v) => Ok(v.rows_affected() as i32),
        Err(err) => Err(format!(
            "update task id {} error:{:?}",
            task.id.clone(),
            err
        )),
    }
}

pub async fn count_task(conn: &Pool<MySql>, status: i32) -> Result<i64, String> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task WHERE status = ?")
        .bind(status)
        .fetch_one(conn)
        .await
        .unwrap();

    Ok(count)
}

pub async fn get_running_task() -> Result<Vec<Task>, String> {
    let page_size = 100;
    let mut page = 0;
    let mut task_list: Vec<Task> = vec![];
    let conn = DB_INSTANCE.get().unwrap();
    loop {
        match fetch_task_list(
            conn,
            TaskStatus::Running.get_status(),
            page_size.clone(),
            page.clone(),
        )
        .await
        {
            Ok(v) => {
                let mut v = v;
                task_list.append(&mut v);
                if v.len() < (page_size as usize) {
                    break;
                }
                page += 1;
            }
            Err(err) => {
                error!("failed to get task {}", err);
                break;
            }
        }
    }

    Ok(task_list)
}

pub async fn update_task_status(
    conn: &Pool<MySql>,
    id: String,
    status: i32, // update
) -> Result<i64, String> {
    info!("update task status id:{}, status:{}", id.clone(), status);
    match sqlx::query(r#"UPDATE task SET status = ? , updated_at = ? WHERE id = ?"#)
        .bind(&status)
        .bind(
            &(SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64),
        )
        .bind(&id)
        .execute(conn)
        .await
    {
        Ok(v) => {
            info!(
                "update status task id {} rows affected {}",
                id.clone(),
                v.rows_affected()
            );
            Ok(v.rows_affected() as i64)
        }
        Err(err) => Err(format!("update task id {} error:{:?}", id.clone(), err)),
    }
}

pub async fn update_task_heartbeat(task_id: String) -> Result<(), String> {
    let conn = DB_INSTANCE.get().unwrap();
    match sqlx::query(
        r#"UPDATE task SET 
    last_heartbeat = ?,
    updated_at = ?
    WHERE id =?"#,
    )
    .bind(
        &(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64),
    )
    .bind(
        &(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64),
    )
    .bind(&task_id)
    .execute(conn)
    .await
    {
        Ok(_) => Ok(()),
        Err(err) => {
            error!("update task {} heartbeat error {:?}", task_id, err);
            Err(format!("error {:?}", err))
        }
    }
}
pub async fn create_task(conn: &Pool<MySql>, task: &mut Task) -> Result<(), String> {
    match sqlx::query(
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
    .bind(&task.id)
    .bind(&task.name)
    .bind(&task.last_heartbeat)
    .bind(&task.src_type)
    .bind(&task.dst_type)
    .bind(&task.src_cfg)
    .bind(&task.dst_cfg)
    .bind(&task.status)
    .bind(&task.created_at)
    .bind(&task.updated_at)
    .bind(&task.deleted_at)
    .bind(&task.tasking_cfg)
    .execute(conn)
    .await
    {
        Ok(_) => Ok(()),

        Err(err) => {
            error!("create task error {:?}", err);
            Err(format!("insert task {} error {:?}", task.id.clone(), err))
        }
    }
}

// json
impl Task {
    pub fn from_task_detail(
        name: &String,
        src_type: &String,
        dst_type: &String,
        src_cfg: &serde_json::Value,
        dst_cfg: &serde_json::Value,
        tasking_cfg: &serde_json::Value,
    ) -> Self {
        Self {
            id: mongodb::bson::oid::ObjectId::new().to_hex(),
            name: name.clone(),
            last_heartbeat: Instant::now().elapsed().as_secs() as i64,
            src_type: src_type.clone(),
            dst_type: dst_type.clone(),
            status: TaskStatus::Created.get_status(),
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            updated_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            deleted_at: 0,
            src_cfg: src_cfg.to_string(),
            dst_cfg: dst_cfg.to_string(),
            tasking_cfg: tasking_cfg.to_string(),
        }
    }
}

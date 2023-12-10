use std::time::{Instant, SystemTime, UNIX_EPOCH};
use validator::Validate;

use serde::{Deserialize, Serialize};
use sqlx::FromRow;
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
    pub last_heartbeat: u64,
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

#[derive(Debug, Deserialize, Serialize, Clone, Validate)]
pub struct NewTaskRequest {
    // task name
    #[validate(length(min = 1, message = "Can not be empty"))]
    pub name: String,
    // src type like kafka
    pub src_type: String,
    // src config json format
    pub src_cfg: String,
    // dst type like kafka
    pub dst_type: String,
    /// dst cfg json format
    pub dst_cfg: String,
    pub tasking_cfg: String,
}
// json
impl Task {
    pub fn from_task_detail(req: &NewTaskRequest) -> Self {
        Self {
            id: mongodb::bson::oid::ObjectId::new().to_hex(),
            name: req.name.clone(),
            last_heartbeat: Instant::now().elapsed().as_secs(),
            src_type: req.src_type.clone(),
            dst_type: req.dst_type.clone(),
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
            src_cfg: req.src_cfg.clone(),
            dst_cfg: req.dst_cfg.clone(),
            tasking_cfg: req.tasking_cfg.clone(),
        }
    }
}

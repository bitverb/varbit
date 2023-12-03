use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Whortleberry<T> {
    pub err_no: i64,
    pub err_msg: String,
    pub data: T,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub name: String,
    pub last_heartbeat: i64,
}

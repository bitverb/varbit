use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use lazy_static::lazy_static;
use log::{info, warn};
use serde::Serialize;

lazy_static! {
    pub static ref GLOBAL_TASK_POOL: Arc<Mutex<TaskPool>> = Arc::new(Mutex::new(TaskPool::new()));
}

#[derive(Clone, Debug, Serialize)]
pub struct Task {
    //
    pub id: String,
    //
    pub name: String,
    //
    pub last_heartbeat: u64,
    // source type like kafka
    pub source_type: String,
    // sink type like kafka
    pub sink_type: String,
    // task status
    pub status: i32,
}

impl Task {
    pub fn from_task_detail(task: &TaskDetail) -> Self {
        Self {
            id: task.id.to_owned(),
            name: task.name.to_owned(),
            last_heartbeat: task.last_heartbeat,
            source_type: task.source_type.to_owned(),
            sink_type: task.sink_type.to_owned(),
            status: task.status,
        }
    }
}
pub struct TaskDetail {
    pub id: String,
    pub name: String,
    pub source_type: String,
    pub source_config: String,
    pub sink_type: String,
    pub sink_config: String,
    pub last_heartbeat: u64,
    pub status: i32,
    pub handler: tokio::task::JoinHandle<()>,
}

impl TaskDetail {
    pub fn new(
        id: String,
        name: String,
        source_type: String,
        sink_type: String,
        source_config: String,
        sink_config: String,
        handler: tokio::task::JoinHandle<()>,
    ) -> Self {
        TaskDetail {
            id,
            name,
            source_type,
            source_config,
            sink_type,
            sink_config,
            last_heartbeat: 0,
            status: 0,
            handler,
        }
    }
}

pub struct TaskPool {
    task: HashMap<String, Box<TaskDetail>>,
}

impl TaskPool {
    pub fn new() -> Self {
        TaskPool {
            task: HashMap::new(),
        }
    }

    pub fn update_heartbeat(&mut self, task_id: &String) {
        if !self.task.contains_key(task_id) {
            warn!("not found task {}", task_id);
            return;
        }
        let task = self.task.get_mut(task_id).unwrap();
        task.last_heartbeat = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    pub fn contain_task(&self, task_id: &String) -> bool {
        return self.task.contains_key(task_id);
    }
    // insert task
    pub fn insert_task(&mut self, task: TaskDetail) -> bool {
        if self.task.contains_key(&task.id) {
            warn!("already create task {}", &task.id);
            return false;
        }

        self.task.insert(task.id.clone(), Box::new(task));
        return true;
    }

    // cancel task
    pub fn cancel_task(&mut self, task_id: &String) -> bool {
        if !self.task.contains_key(task_id) {
            warn!("not found task {}", task_id);
            return false;
        }
        info!("cancel task {:?}", task_id);
        let task_detail = self.task.get(task_id).unwrap();
        task_detail.handler.abort(); //
        self.task.remove(task_id);
        true
    }
    pub fn task_list(&self) -> Vec<Task> {
        let mut list: Vec<Task> = vec![];
        for ele in self.task.values() {
            list.push(Task::from_task_detail(&ele));
        }
        list
    }
}

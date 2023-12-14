use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use lazy_static::lazy_static;
use log::{error, info};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_context::context;

use crate::{core::Msg, input::Src, sink::Dst, CloseTask, DST_PLUGIN, SRC_PLUGIN};

pub struct Tasking {
    pub handle: context::Handle,
}

lazy_static! {
    /// link https://users.rust-lang.org/t/how-to-add-a-trait-value-into-hashmap/6542/3
    pub static ref GLOBAL_TASKING :Arc<Mutex<HashMap<String,Box<Tasking>>>> = {
        let  plugin:HashMap<String, Box<Tasking>> = HashMap::new();
        Arc::new(Mutex::new(plugin))
    };

}

pub async fn dispatch_tasking(
    task_id: String,
    src_type: String,
    src_conf: &serde_json::Value,
    dst_type: String,
    dst_conf: &serde_json::Value,
    after_close_task: Box<dyn CloseTask>,
) -> bool {
    let mut lock = GLOBAL_TASKING.lock().unwrap();
    if lock.contains_key(task_id.to_owned().as_str()) {
        error!("task {} is running", task_id.clone());
        return false;
    }

    let (rx, mut _tx) = mpsc::channel::<Msg>(20);

    let mut _dst: std::sync::MutexGuard<'_, HashMap<String, Arc<Box<dyn Dst + Send + Sync>>>> =
        DST_PLUGIN.lock().unwrap();
    if !_dst.contains_key(dst_type.as_str()) {
        error!("not found dst_type {}", dst_type);
        return false;
    }
    let _dst = _dst.get(dst_type.to_owned().as_str()).unwrap().clone();
    let dst_conf = dst_conf.clone();
    let task_id_2_dst = task_id.clone();
    // start dst task
    let dst_handler = tokio::task::spawn(async move {
        _dst.to_dst(task_id_2_dst.clone(), dst_conf.clone(), _tx)
            .await;
    });

    let mut _data: std::sync::MutexGuard<'_, HashMap<String, Arc<Box<dyn Src + Send + Sync>>>> =
        SRC_PLUGIN.lock().unwrap();
    if !_data.contains_key(src_type.as_str()) {
        error!("not found src_type {}", src_type);
        dst_handler.abort(); // close dst task
        return false;
    }
    let source = _data.get(src_type.to_owned().as_str()).unwrap();
    let source = source.clone();
    let task_id_2_src = task_id.clone();
    let src_conf: serde_json::Value = src_conf.clone();
    let src_handler = tokio::task::spawn(async move {
        source
            .from_src(task_id_2_src.clone(), &src_conf.clone(), rx)
            .await;
    });
    let (_, mut handle) = context::Context::new();
    let mut ctx = handle.spawn_ctx();
    let task_id_cp = task_id.clone();
    tokio::task::spawn(async move {
        tokio::select! {
            _ = ctx.done() =>{
                info!("remove task {}",task_id_cp.to_owned());
                src_handler.abort(); // cancel src
                remove_tasking(task_id_cp.to_owned()).await;
                after_close_task.close_task(task_id_cp).await;
                // update task status
            }
        }
    });
    //
    let task_id_cp = task_id.clone();
    tokio::task::spawn(async move {
        tokio::select! {
            _ = dst_handler => {
                info!("dst cancel task {}",task_id_cp);
                remove_tasking(task_id_cp).await;
            }
        }
    });
    lock.insert(task_id.to_owned(), Box::new(Tasking { handle: handle }));
    return true;
}

pub async fn remove_tasking(task_id: String) -> bool {
    let mut lock = GLOBAL_TASKING.lock().unwrap();
    if !lock.contains_key(task_id.to_owned().as_str()) {
        return false;
    }
    lock.remove(task_id.to_owned().as_str());
    return true;
}

// check task is running?
pub async fn task_running(task_id: &String) -> bool {
    let lock = GLOBAL_TASKING.lock().unwrap();
    return lock.contains_key(task_id);
}

#[derive(Deserialize)]
struct InputKafkaConfigMeta {
    /// broker
    pub broker: String,
    /// topic
    pub topic: String,
}

pub fn check_kafka_src(cfg: &String) -> Result<(), String> {
    match serde_json::from_str::<InputKafkaConfigMeta>(cfg.as_str()) {
        Ok(_) => Ok(()),
        Err(err) => Err(format!("invalid kafka input cfg {:?}", err)),
    }
}

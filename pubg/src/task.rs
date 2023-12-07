use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use lazy_static::lazy_static;
use log::info;
use tokio_context::context;

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

pub async fn dispatch_tasking(task_id: String) -> bool {
    let mut lock = GLOBAL_TASKING.lock().unwrap();
    if lock.contains_key(task_id.to_owned().as_str()) {
        return false;
    }

    let (_, mut handle) = context::Context::new();
    let mut ctx = handle.spawn_ctx();
    let dst_handler = tokio::task::spawn(async {});
    let src_handler = tokio::task::spawn(async {});
    let task_id_cp = task_id.clone();
    tokio::task::spawn(async move {
        tokio::select! {
            _ = ctx.done() =>{
                info!("remove task {}",task_id_cp);
                src_handler.abort(); // cancel src
                remove_tasking(task_id_cp).await;
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

pub async fn remove_tasking(task_id: String) {
    let mut lock = GLOBAL_TASKING.lock().unwrap();
    if !lock.contains_key(task_id.to_owned().as_str()) {
        return;
    }
    lock.remove(task_id.to_owned().as_str());
}

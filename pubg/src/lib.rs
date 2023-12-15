/// pubg mod is plugin
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub mod core;
pub mod input;
pub mod sink;
pub mod task;

use async_trait::async_trait;
use input::kafka::KafkaSrc;
use lazy_static::lazy_static;

use crate::sink::Dst;
use crate::{input::Src, sink::kafka::KafkaDst};

lazy_static! {
    /// link https://users.rust-lang.org/t/how-to-add-a-trait-value-into-hashmap/6542/3
    /// hashmap add dyn trait
    pub static ref SRC_PLUGIN: Arc<Mutex<HashMap<String,Arc<Box<dyn Src  +Send +Sync>>>>> =   {
        let mut plugin :HashMap<String,Arc<Box<dyn Src  +Send +Sync>>>= HashMap::new();
        plugin.insert(String::from("kafka"), Arc::new(Box::new(KafkaSrc{})));
        Arc::new(Mutex::new(plugin))
    };

    pub static ref DST_PLUGIN: Arc<Mutex<HashMap<String,Arc<Box<dyn Dst  +Send +Sync>>>>> =   {
        let mut plugin :HashMap<String,Arc<Box<dyn Dst  +Send +Sync>>>= HashMap::new();
        plugin.insert(String::from("kafka"), Arc::new(Box::new(KafkaDst{})));
        Arc::new(Mutex::new(plugin))
    };
}

#[async_trait]
pub trait CloseTask: Send + Sync {
    async fn close_task(&self, task_id: String);
}

// #[async_trait]
// pub trait HeartbeatHandler: Send + Sync {
//     async fn heartbeat_handler(task_id: String);
// }

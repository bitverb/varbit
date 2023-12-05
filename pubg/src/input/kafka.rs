use log::{error, info};
use serde::{de::value, Deserialize};
use tokio::sync::mpsc;
use tokio_context::context;

#[derive(Deserialize, Debug)]
struct KafkaSourceConfig {
    pub broker: String,
    pub topic: String,
    pub group_id: String,

    // json
    pub decoder: String,
    pub meta: KafkaSourceMeta,
}

#[derive(Deserialize, Debug)]
pub struct KafkaSourceMeta {
    pub task_id: String,
}

pub trait Src {
    fn from_src(
        &self,
        task_id: String,
        conf: &serde_json::Value,
        sender: mpsc::Sender<serde_json::Value>,
    );
    fn cfg(&self) -> serde_json::Value;
}

pub struct KafkaSrc {}

impl Src for KafkaSrc {
    fn from_src(
        &self,
        task_id: String,
        conf: &serde_json::Value,
        sender: mpsc::Sender<serde_json::Value>,
    ) {
        let raw_value = serde_json::from_value(conf.clone());
        if (raw_value).is_err() {
            let err: serde_json::Error = (raw_value).err().unwrap();
            error!(
                "task_id: {:?} un_marshal as KafkaSourceConfig error {:?}",
                task_id, err
            );
            return;
        }
        let sfc: KafkaSourceConfig = raw_value.unwrap();

        info!("task_id:{:?} sfc {:?}", task_id, sfc);
        let _ = async {
            sender.send(conf.clone()).await.unwrap();
            ()
        };
    }

    fn cfg(&self) -> serde_json::Value {
        todo!()
    }
}

pub async fn from_kafka(mut ctx: context::Context, sender: mpsc::Sender<String>) {
    info!("starting ....");
    let (_tx, _rx) = mpsc::channel::<String>(12);
    sender.send("value".to_owned()).await.unwrap();
    loop {
        tokio::select! {
            _ = ctx.done()=> {
                info!("close job");
                return ;
            }
              else =>  {
                _tx.send("233".to_owned()).await.unwrap();
             }

        }
    }
}

use async_trait::async_trait;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::mpsc;

use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};

use crate::core::Msg;

use super::Dst;

pub struct KafkaDst {}
#[async_trait]
impl Dst for KafkaDst {
    /// if producer is close please exit dst plugin
    async fn to_dst(
        &self,
        task_id: String,
        conf: serde_json::Value,
        mut receive: mpsc::Receiver<Msg>,
    ) {
        info!(
            "[dst] {} task_id:{} conf {:?}",
            self.dst_name(),
            task_id,
            serde_json::to_string(&conf.to_owned()).unwrap()
        );

        let sfc = match serde_json::from_value::<KafkaDstConfig>(conf.to_owned()) {
            Ok(v) => v,
            Err(err) => {
                error!(
                    "parser config {:?} error {:?}",
                    conf.to_owned().to_string(),
                    err
                );
                return;
            }
        };

        info!(
            "[dst] {} task_id {} config:{:?}",
            self.dst_name(),
            task_id.to_owned(),
            sfc
        );
        let producer = match ClientConfig::new()
            .set("bootstrap.servers", sfc.broker.as_str())
            .set("message.timeout.ms", "5000")
            .create::<FutureProducer>()
        {
            Ok(v) => v,
            Err(err) => {
                error!("create producer error {:?}", err);
                return;
            }
        };

        let mut ignore = HashSet::new();
        ignore.insert("ts".to_owned());
        let cry = service::task::json::ChrysaetosBit::new_cfg(
            task_id.clone(),
            "_".to_owned(),
            32,
            HashSet::new(),
            ignore,
        );

        while let Some(msg) = receive.recv().await {
            let res = cry.parse(&msg.g_id, &msg.value);

            debug!(
                "[dst] {} task_id:{} g_id:{} receive  data {:?} res{:?}",
                self.dst_name(),
                task_id,
                msg.g_id,
                msg.value.to_string(),
                serde_json::to_string(&serde_json::json!(res)).unwrap(),
            );

            for data in &res {
                let producer_status = match producer
                    .send(
                        FutureRecord::to(sfc.topic.as_str())
                            .key(&"".to_owned())
                            .payload(&format!("{}", serde_json::json!(data).to_string()))
                            .headers(OwnedHeaders::new()),
                        Duration::from_secs(0),
                    )
                    .await
                {
                    Ok(v) => v,
                    Err(err) => {
                        error!(
                            "[dst] task_id {}, g_id {} send data error {:?}",
                            task_id, msg.g_id, err
                        );
                        continue;
                    }
                };
                debug!(
                    "[dst] task_id {}, g_id {}, producer_status {:?}",
                    task_id, msg.g_id, producer_status
                );
            }
        }

        info!("exit...consumer....{:?}", receive.recv().await)
    }

    fn cfg(&self) -> serde_json::Value {
        serde_json::json!(KafkaDstConfig::default())
    }

    fn dst_name(&self) -> String {
        "kafka".to_owned()
    }
}
#[derive(Debug, Deserialize, Serialize, Default)]
pub struct KafkaDstConfig {
    pub broker: String,
    pub topic: String,
    pub encoder: String,
    pub meta: KafkaDstMeta,
}

#[derive(Debug, Deserialize, Serialize, Default)]

pub struct KafkaDstMeta {
    pub task_id: String,
}

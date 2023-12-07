use async_trait::async_trait;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc;

use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};

use super::Dst;

pub struct KafkaDst {}
#[async_trait]
impl Dst for KafkaDst {
    /// if producer is close please exit dst plugin
    async fn to_dst(
        &self,
        task_id: String,
        conf: serde_json::Value,
        mut receive: mpsc::Receiver<serde_json::Value>,
    ) {
        info!(
            " task_id:{} conf {:?}",
            task_id,
            serde_json::to_string(&conf.to_owned()).unwrap()
        );
        let dst_raw: Result<KafkaDstConfig, serde_json::Error> =
            serde_json::from_value(conf.to_owned());
        if dst_raw.is_err() {
            error!(
                "[dst] {} task_id {} un marshal error {:?}",
                self.dst_name(),
                task_id.to_owned(),
                dst_raw.err()
            );
            return;
        }
        let sfc: KafkaDstConfig = dst_raw.unwrap();
        info!(
            "[dst] {} task_id {} config:{:?}",
            self.dst_name(),
            task_id.to_owned(),
            sfc
        );

        let cry = service::task::json::ChrysaetosBit::new(task_id.clone(), "_".to_owned(), 32);

        while let Some(msg) = receive.recv().await {
            let res = cry.parse(&msg);

            debug!(
                "[dst] {} task_id:{} receive  data {:?} res{:?}",
                self.dst_name(),
                task_id,
                msg.to_string(),
                serde_json::to_string(&serde_json::json!(res)).unwrap(),
            );
        }

        println!("exit...consumer....")
    }

    fn cfg(&self) -> serde_json::Value {
        todo!()
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

async fn produce(brokers: &str, topic_name: &str) {
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    // This loop is non blocking: all messages will be sent one after the other, without waiting
    // for the results.
    let futures = (0..5)
        .map(|i| async move {
            // The send operation on the topic returns a future, which will be
            // completed once the result or failure from Kafka is received.
            let delivery_status = producer
                .send(
                    FutureRecord::to(topic_name)
                        .payload(&format!("Message {}", i))
                        .key(&format!("Key {}", i))
                        .headers(OwnedHeaders::new().add("header_key", "header_value")),
                    Duration::from_secs(0),
                )
                .await;

            // This will be executed when the result is received.
            info!("Delivery status for message {} received", i);
            delivery_status
        })
        .collect::<Vec<_>>();

    // This loop will wait until all delivery statuses have been received.
    for future in futures {
        info!("Future completed. Result: {:?}", future.await);
    }
}

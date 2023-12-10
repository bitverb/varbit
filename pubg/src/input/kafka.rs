use async_trait::async_trait;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use rdkafka::client::ClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::ClientConfig;
use uuid::Uuid;

use crate::core::Msg;

use super::Src;

struct CustomContext {
    pub task_id: String,
}

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!(
            "task_id:{:?} pre rebalance client{} {:?}",
            self.task_id.to_owned(),
            self.task_id.to_owned(),
            rebalance
        );
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!(
            "task_id:{:?} post rebalance {:?}",
            self.task_id.to_owned(),
            rebalance
        );
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        debug!(
            "task_id:{:?} committing offsets: {:?}",
            self.task_id.to_owned(),
            result
        );
    }
}
// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

#[derive(Deserialize, Debug, Serialize, Default)]
struct KafkaSourceConfig {
    pub broker: String,
    pub topic: String,
    pub group_id: String,

    // json
    pub decoder: String,
    pub meta: KafkaSourceMeta,
}

#[derive(Deserialize, Debug, Serialize, Default)]
pub struct KafkaSourceMeta {
    pub task_id: String,
}

pub struct KafkaSrc {}
#[async_trait]
impl Src for KafkaSrc {
    async fn from_src(&self, task_id: String, conf: &serde_json::Value, sender: mpsc::Sender<Msg>) {
        // String::from("").to_string()
        let raw_value = serde_json::from_value(conf.clone());
        if raw_value.is_err() {
            let err: serde_json::Error = (raw_value).err().unwrap();
            error!(
                "task_id: {:?} un_marshal as KafkaSourceConfig error {:?}",
                task_id, err
            );
            return;
        }

        let sfc: KafkaSourceConfig = raw_value.unwrap();
        info!("task_id:{:?} sfc {:?}", task_id, sfc);

        let context = CustomContext {
            task_id: task_id.to_owned(),
        };
        let consumer_res = ClientConfig::new()
            .set("group.id", sfc.group_id.to_owned().as_str())
            .set("bootstrap.servers", sfc.broker.to_owned().as_str())
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(context);
        if consumer_res.is_err() {
            error!(
                "task_id {task_id} build kafka consumer creation failed : {:?}",
                consumer_res.err()
            );
            return;
        }

        let consumer: LoggingConsumer = consumer_res.unwrap();

        let stream_consumer_res = consumer.subscribe(&vec![sfc.topic.to_owned().as_str()]);
        if stream_consumer_res.is_err() {
            error!(
                "task_id {task_id} subscribe to specified topic{:?} failed {:?}",
                sfc.topic.to_owned(),
                stream_consumer_res.err()
            );
            return;
        }
        loop {
            match consumer.recv().await {
                Err(e) => warn!("Kafka error: {}", e),
                Ok(m) => {
                    let payload = match m.payload_view::<str>() {
                        None => {
                            warn!(
                                "task_id:{task_id} topic:{:?} receive payload",
                                sfc.topic.to_owned()
                            );
                            ""
                        }

                        Some(Ok(s)) => s,
                        Some(Err(e)) => {
                            warn!(" task_id:{task_id} topic{:?} Error while deserializing message payload: {:?}",sfc.topic.to_owned(), e);
                            ""
                        }
                    };

                    if payload == "" {
                        warn!(
                            "task_id:{task_id} topic:{:?} receive payload",
                            sfc.topic.to_owned()
                        );
                        continue;
                    }

                    // get key id
                    let g_id = if !m.key_len() == 0 {
                        format!("{:?}", m.key())
                    } else {
                        Uuid::new_v4().to_string()
                    };
                    debug!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?} {:?}",
                              m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp(),payload);

                    if let Some(headers) = m.headers() {
                        for i in 0..headers.count() {
                            let header = headers.get(i).unwrap();
                            info!("  Header {:#?}: {:?}", header.0, header.1);
                        }
                    }

                    let mut value: serde_json::Value = serde_json::Value::Null;
                    if sfc.decoder == "json".to_owned() {
                        let value_res = serde_json::from_str(payload);
                        if value_res.is_err() {
                            warn!(
                                "task_id:{task_id} json payload{:?} decoder get error {:?}",
                                payload,
                                value_res.err()
                            );
                            continue;
                        }
                        value = value_res.unwrap();
                    }
                    if value == serde_json::Value::Null {
                        warn!("task_id:{task_id} null value continue",);
                        continue;
                    }

                    let msg = Msg::new(g_id, value);

                    let _ = sender.send(msg).await;
                    consumer.commit_message(&m, CommitMode::Async).unwrap();
                }
            }
        }
    }

    // config info
    fn cfg(&self) -> serde_json::Value {
        serde_json::to_value(KafkaSourceConfig::default()).unwrap()
    }

    fn src_name(&self) -> String {
        "kafka".to_owned()
    }
}

use std::time::Duration;

use log::info;

use rdkafka::consumer::{
    BaseConsumer, Consumer,
};

use rdkafka::ClientConfig;
use serde::Deserialize;

pub fn kafka_test_connect(cfg: &serde_json::Value) -> anyhow::Result<(), String> {
    let kcc = match serde_json::from_value::<KafkaConnectReqCfg>(cfg.clone()) {
        Ok(v) => v,
        Err(err) => {
            return Err(format!("{:?}", err));
        }
    };

    info!("kcc is config {:?}", kcc);
    let consumer = match ClientConfig::new()
        .set("bootstrap.servers", kcc.broker)
        .create::<BaseConsumer>()
    {
        Ok(v) => v,
        Err(err) => {
            return Err(format!("connect to broker error {:?}", err));
        }
    };

    let metadata = match consumer.fetch_metadata(Some(kcc.topic.as_str()), Duration::from_secs(10))
    {
        Ok(v) => v,
        Err(err) => {
            return Err(format!("fetch topic {} error {:?}", kcc.topic, err));
        }
    };

    if metadata.topics().len() == 1 && metadata.topics()[0].error().is_some() {
        return Err(format!("not found topic:{}", kcc.topic));
    }

    return Ok(());
}

#[derive(Debug, Deserialize)]
struct KafkaConnectReqCfg {
    broker: String,
    topic: String,
}

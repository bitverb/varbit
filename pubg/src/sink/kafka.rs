use async_trait::async_trait;
use log::info;
use tokio::sync::mpsc;
use tokio_context::context;

use super::Dst;

pub struct KafkaDst {}
#[async_trait]
impl Dst for KafkaDst {
    async fn to_dst(
        &self,
        _ctx: context::Context,
        task_id: String,
        conf: serde_json::Value,
        mut receive: mpsc::Receiver<serde_json::Value>,
    ) {
        info!(" task_id:{} conf {:?}", task_id, conf.to_owned());
        while let Some(msg) = receive.recv().await {
            info!(
                "[dst] task_id: {} receive  data {:?}",
                task_id,
                msg.to_string()
            );
        }
        println!("exit...consumer....")
    }

    fn cfg(&self) -> serde_json::Value {
        todo!()
    }

    fn src_name(&self) -> String {
        todo!()
    }
}

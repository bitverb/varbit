use async_trait::async_trait;
use tokio::sync::mpsc;

pub mod kafka;

#[async_trait]

pub trait Dst: Send + Sync {
    async fn to_dst(
        &self,
        task_id: String,
        conf: serde_json::Value,
        mut receive: mpsc::Receiver<serde_json::Value>,
    );
    fn cfg(&self) -> serde_json::Value;
    fn dst_name(&self) -> String;
}

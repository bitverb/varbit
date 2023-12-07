use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_context::context;

pub mod kafka;

#[async_trait]

pub trait Dst: Send + Sync {
    async fn to_dst(
        &self,
        mut ctx: context::Context,
        task_id: String,
        conf: serde_json::Value,
        mut receive: mpsc::Receiver<serde_json::Value>,
    );
    fn cfg(&self) -> serde_json::Value;
    fn dst_name(&self) -> String;
}

pub mod kafka;

use async_trait::async_trait;
use tokio::sync::mpsc;

#[async_trait]

pub trait Src: Send + Sync {
    async fn from_src(
        &self,
        task_id: String,
        conf: &serde_json::Value,
        sender: mpsc::Sender<serde_json::Value>,
    );
    fn cfg(&self) -> serde_json::Value;
    fn src_name(&self) -> String;
}

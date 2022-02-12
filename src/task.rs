use async_trait::async_trait;

use crate::zk_mng::{TaskSliceId, WorkerAddress};

pub enum TaskResult {
    Completed,
    InCompleted,
}
#[async_trait]
pub trait TaskBehaviour {
    async fn get_address(&self) -> WorkerAddress;
    async fn query_pressure(&self) -> anyhow::Result<usize>;
    async fn try_work(&self, task_slice: TaskSliceId, worker: WorkerAddress) ->anyhow::Result<TaskResult>;
    async fn start_serve(&self) -> anyhow::Result<()>;
    async fn stop_serve(&self) -> anyhow::Result<()>;
}
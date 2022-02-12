use std::time::Duration;

use async_trait::async_trait;
use libp2p::PeerId;

use crate::{task::{TaskBehaviour, TaskResult}, zk_mng::{TaskSliceId, WorkerAddress}};
pub struct DummyTaskBehaviour{}

#[async_trait]
impl TaskBehaviour for DummyTaskBehaviour {
    async fn try_work(&self, task_slice: TaskSliceId, worker: WorkerAddress) ->anyhow::Result<TaskResult> {
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok(TaskResult::Completed)
    }
    async fn get_address(&self) -> WorkerAddress {
        WorkerAddress::new(PeerId::random(), "/ip4/127.0.0.1/tcp/0".parse().unwrap())
    }
    async fn query_pressure(&self) -> anyhow::Result<usize> {
        Ok(0)
    }
    async fn start_serve(&self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn stop_serve(&self) -> anyhow::Result<()> {
        Ok(())
    }
}


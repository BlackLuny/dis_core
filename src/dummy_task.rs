use std::time::Duration;

use async_trait::async_trait;
use libp2p::PeerId;
use rand::Rng;

use crate::{task::{TaskBehaviour, TaskResult}, zk_mng::{TaskSliceId, WorkerAddress}};
pub struct DummyTaskBehaviour{}

#[async_trait]
impl TaskBehaviour for DummyTaskBehaviour {
    async fn try_work(&self, task_slice: TaskSliceId, worker: WorkerAddress) ->anyhow::Result<TaskResult> {
        //tokio::time::sleep(Duration::from_secs(2)).await;
        // let mut rng = rand::thread_rng();
        // if rng.gen_range(0, 10) > 9 {
        //     Ok(TaskResult::InCompleted)
        // } else{
        //     Ok(TaskResult::Completed)
        // }
        Ok(TaskResult::Completed)
    }
    async fn get_address(&self) -> WorkerAddress {
        WorkerAddress::new(PeerId::random(), "/ip4/127.0.0.1/tcp/0".parse().unwrap())
    }
    async fn query_pressure(&self) -> anyhow::Result<usize> {
        // 变化太频繁会导致zookeeper性能下降
        let mut rng = rand::thread_rng();
        let r = rng.gen_range(0, 1);
        Ok(r)
        // Ok(0)
    }
    async fn start_serve(&self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn stop_serve(&self) -> anyhow::Result<()> {
        Ok(())
    }
}


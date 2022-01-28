use std::fmt::format;
use std::{collections::HashMap, sync::Arc};

use anyhow::anyhow;
use const_str::concat;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use tokio::sync::Mutex;
use zookeeper_async::recipes::cache::PathChildrenCache;
use zookeeper_async::{Acl, CreateMode, ZooKeeper, ZooKeeperExt};

const TASKS_ROOT: &str = "/tasks";
const TASKS_PATH: &str = concat!(TASKS_ROOT, "/task");
const TASKS_QUEUE_ROOT: &str = concat!(TASKS_ROOT, "/queue");
const TASKS_QUEUE_PATH: &str = concat!(TASKS_QUEUE_ROOT, "/task");
const WORKER_ROOT: &str = "/workers";
const WORKER_PATH: &str = concat!(WORKER_ROOT, "/worker");
const WORKER_LEN: usize = const_str::len!("worker");
const SLICE_LEN: usize = const_str::len!("slice");
const TASK_LEN: usize = const_str::len!("task");

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
struct TaskId(usize);
impl TaskId {
    fn from_path(p: &str) -> anyhow::Result<Self> {
        let id: usize = p
            .split("/")
            .last()
            .and_then(|x| x[TASK_LEN..].parse().ok())
            .ok_or(anyhow!("task from path error"))?;
        Ok(Self(id))
    }
    pub fn id(&self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
struct SliceId(usize);

impl SliceId {
    fn from_path(p: &str) -> anyhow::Result<Self> {
        let id: usize = p
            .split("/")
            .last()
            .and_then(|x| x[SLICE_LEN..].parse().ok())
            .ok_or(anyhow!("slice from path error"))?;
        Ok(Self(id))
    }
    pub fn id(&self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
struct TaskSliceId(TaskId, SliceId);

impl TaskSliceId {
    fn from_task_slice(task_id: TaskId, slice_id: SliceId) -> Self {
        Self(task_id, slice_id)
    }
    pub fn task_id(&self) -> TaskId {
        self.0
    }
    pub fn slice_id(&self) -> SliceId {
        self.1
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
struct WorkerId(usize);

impl WorkerId {
    fn from_path(p: &str) -> anyhow::Result<Self> {
        let id: usize = p
            .split("/")
            .last()
            .and_then(|x| x[WORKER_LEN..].parse().ok())
            .ok_or(anyhow!("worker from path error"))?;
        Ok(Self(id))
    }
    pub fn id(&self) -> usize {
        self.0
    }
}

struct TaskData {
    worker_completed: (PathChildrenCache, Vec<WorkerId>), // 完成了所有切片的worker
    slice_completed: HashMap<SliceId, (PathChildrenCache, Vec<WorkerId>)>, // 完成了部分切片的worker
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum WorkerStatus {
    Idle,
    Working(usize),
    FullLoaded,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum TaskStatus {
    Initing,
    Working,
    Done,
}
struct TaskControlInfo {
    slice_num: usize,
    status: TaskStatus,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct WorkerData {
    status: WorkerStatus,
}

impl WorkerData {
    fn new_idle() -> Self {
        Self {
            status: WorkerStatus::Idle,
        }
    }
}

pub struct ZkMng {
    zk: Arc<ZooKeeper>,
    self_id: WorkerId,
    task_queue_cache: PathChildrenCache,
    tasks_data_cache: Arc<Mutex<HashMap<TaskId, TaskData>>>,
    worker_data_cache: Arc<Mutex<HashMap<WorkerId, WorkerData>>>,
    opr_tx: UnboundedSender<OperationEvent>,
}
enum OperationEvent {
    AddTaskDataWatcher(TaskId),
}
enum ZkEvent {
    TaskAdded(TaskId),
}

impl ZkMng {
    pub async fn connect(zk_addr: &str) -> anyhow::Result<Self> {
        let zk = Arc::new(
            ZooKeeper::connect(zk_addr, std::time::Duration::from_secs(10), |_| {}).await?,
        );
        zk.ensure_path(WORKER_ROOT).await?;
        let self_id = {
            let p = zk
                .create(
                    WORKER_PATH,
                    serde_json::to_vec(&WorkerData::new_idle())?,
                    Acl::open_unsafe().clone(),
                    CreateMode::EphemeralSequential,
                )
                .await?;
            WorkerId::from_path(&p)?
        };

        let (opr_tx, opr_rx) = unbounded_channel();

        let task_queue_cache = Self::create_queue_cache_watch(zk.clone(), opr_tx.clone()).await?;
        let tasks_data_cache = Arc::new(Mutex::new(HashMap::new()));
        let worker_data_cache = Arc::new(Mutex::new(HashMap::new()));


        tokio::spawn(Self::opr_event_proc(opr_rx));

        Ok(Self {
            zk,
            self_id,
            task_queue_cache,
            tasks_data_cache,
            worker_data_cache,
            opr_tx,
        })
    }
    async fn opr_event_proc(mut opr_rx: UnboundedReceiver<OperationEvent>) {
        while let Some(opr) = opr_rx.recv().await {
            match opr {
                _ => {

                }
            }
        }
    }
    async fn create_queue_cache_watch(zk: Arc<ZooKeeper>, opr_tx: UnboundedSender<OperationEvent> ) -> anyhow::Result<PathChildrenCache> {
        let cache = PathChildrenCache::new(zk, &TASKS_QUEUE_ROOT).await?;
        cache.add_listener(|event| {
            match event {
                _ => {
                    
                }
            }
        });
        Ok(cache)
    }
    async fn operation_sender(&self) ->UnboundedSender<OperationEvent> {
        self.opr_tx.clone()
    }
}


mod test_zk_mng {
    use crate::ZkMng;

    const ZK_ADDR: &str = "127.0.0.1:2181";
    #[tokio::main]
    #[test]
    async fn test_connect() {
        let zk_mng = ZkMng::connect(ZK_ADDR).await.unwrap();
    }
}
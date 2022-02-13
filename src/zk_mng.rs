use anyhow::anyhow;
use const_str::concat;
use futures::stream::iter;
use futures::StreamExt;
use libp2p::{Multiaddr, PeerId};
use log::info;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::HashSet;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, Mutex};
use zookeeper_async::recipes::cache::{PathChildrenCache, PathChildrenCacheEvent};
use zookeeper_async::{Acl, CreateMode, Stat, ZkError, ZooKeeper, ZooKeeperExt};

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
pub struct TaskId(usize);
impl TaskId {
    fn from_path(p: &str) -> anyhow::Result<Self> {
        let id: usize = p
            .split("/")
            .last()
            .and_then(|x| x[TASK_LEN..].parse().ok())
            .ok_or(anyhow!("task from path error"))?;
        Ok(Self(id))
    }
    #[inline(always)]
    fn to_path(&self) -> String {
        format!("{}{:0width$}", TASKS_PATH, self.id(), width = 10)
    }
    #[inline(always)]
    pub fn id(&self) -> usize {
        self.0
    }
}
impl From<usize> for TaskId {
    fn from(i: usize) -> Self {
        Self(i)
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct SliceId(usize);

impl SliceId {
    fn from_path(p: &str) -> anyhow::Result<Self> {
        let id: usize = p
            .split("/")
            .last()
            .and_then(|x| x[SLICE_LEN..].parse().ok())
            .ok_or(anyhow!("slice from path error"))?;
        Ok(Self(id))
    }

    fn to_path(&self) -> String {
        format!("slice{:0width$}", self.id(), width = 10)
    }

    pub fn id(&self) -> usize {
        self.0
    }
}

impl From<usize> for SliceId {
    fn from(i: usize) -> Self {
        Self(i)
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct TaskSliceId(TaskId, SliceId);

impl TaskSliceId {
    pub fn from_task_slice(task_id: TaskId, slice_id: SliceId) -> Self {
        Self(task_id, slice_id)
    }
    #[inline(always)]
    pub fn task_id(&self) -> TaskId {
        self.0
    }
    #[inline(always)]
    pub fn slice_id(&self) -> SliceId {
        self.1
    }
    #[inline(always)]
    pub fn to_path(&self) -> String {
        format!("{}/{}", self.task_id().to_path(), self.slice_id().to_path())
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct WorkerId(usize);

impl WorkerId {
    fn from_path(p: &str) -> anyhow::Result<Self> {
        let id: usize = p
            .split("/")
            .last()
            .and_then(|x| x[WORKER_LEN..].parse().ok())
            .ok_or(anyhow!("worker from path error"))?;
        Ok(Self(id))
    }
    #[inline(always)]
    pub fn to_path(&self) -> String {
        format!("{}{:0width$}", WORKER_PATH, self.id(), width = 10)
    }
    #[inline(always)]
    pub fn id(&self) -> usize {
        self.0
    }
}

struct TaskData {
    slice_completed: HashMap<SliceId, PathChildrenCache>, // 完成了部分切片的worker
}

impl TaskData {
    fn new() -> Self {
        Self {
            slice_completed: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum WorkerStatus {
    Idle,
    Working(usize),
    FullLoaded,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum TaskStatus {
    Initing,
    Working,
    Done,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum TaskType {
    Spread, // 传播型，可以从其他worker获取任务
    Hosted, // 只能从Host获取任务
}

#[derive(Serialize, Deserialize, Debug)]
pub enum TaskWorkerWanted {
    AllWorker,
    Explicit {
        workers_wanted: usize,
        worker_lst: HashSet<WorkerId>,
    },
}
#[derive(Serialize, Deserialize, Debug)]
pub struct TaskControlInfo {
    publisher: WorkerId,
    slice_num: usize,
    task_type: TaskType,
    workers_wanted: TaskWorkerWanted,
    status: TaskStatus,
}

impl TaskControlInfo {
    fn new_explict(
        publisher: WorkerId,
        slice_num: usize,
        task_type: TaskType,
        workers_wanted: usize,
    ) -> Self {
        Self {
            publisher,
            slice_num,
            task_type,
            workers_wanted: TaskWorkerWanted::Explicit {
                workers_wanted: workers_wanted,
                worker_lst: Default::default(),
            },
            status: TaskStatus::Initing,
        }
    }
    fn new_all_workers(publisher: WorkerId, slice_num: usize, task_type: TaskType) -> Self {
        Self {
            publisher,
            slice_num,
            task_type,
            workers_wanted: TaskWorkerWanted::AllWorker,
            status: TaskStatus::Initing,
        }
    }
    pub fn slice_num(&self) -> usize {
        self.slice_num
    }
    pub fn task_type(&self) -> TaskType {
        self.task_type
    }
    pub fn workers_wanted(&self) -> Option<usize> {
        match self.workers_wanted {
            TaskWorkerWanted::AllWorker => None,
            TaskWorkerWanted::Explicit{workers_wanted, ..} => Some(workers_wanted)
        }
    }
    pub fn is_all_worker_do(&self) -> bool {
        matches!(self.workers_wanted, TaskWorkerWanted::AllWorker)
    }
    pub fn add_worker(&mut self, worker_id: WorkerId) {
        match self.workers_wanted {
            TaskWorkerWanted::Explicit{ref mut worker_lst, ..} => {worker_lst.insert(worker_id);}
            _ => {}
        }
    }
    pub fn get_worker_list(&self) -> Option<&HashSet<WorkerId>> {
        match self.workers_wanted {
            TaskWorkerWanted::Explicit{ref worker_lst, ..} => {Some(worker_lst)}
            _ => {None}
        }
    }
    pub fn cur_worker_list(&self) -> Option<usize> {
        self.get_worker_list().map(|x| x.len())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerAddress(Vec<u8>, Multiaddr);

impl WorkerAddress {
    pub fn new(peer_id: PeerId, addr: Multiaddr) -> Self {
        Self(peer_id.to_bytes(), addr)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkerData {
    status: WorkerStatus,
    address: WorkerAddress,
}

impl WorkerData {
    fn new_idle() -> Self {
        Self {
            status: WorkerStatus::Idle,
            address: WorkerAddress(vec![], Multiaddr::empty()),
        }
    }
    fn new(address: WorkerAddress) -> Self {
        Self {
            status: WorkerStatus::Idle,
            address,
        }
    }
}

pub struct ZkMng {
    zk: Arc<ZooKeeper>,
    self_id: WorkerId,
    task_queue_cache: Arc<Mutex<(PathChildrenCache, HashMap<TaskId, String>)>>,
    tasks_data_cache: Arc<Mutex<HashMap<TaskId, TaskData>>>,
    worker_data_cache: Arc<Mutex<(PathChildrenCache, HashMap<WorkerId, WorkerData>)>>,
    opr_tx: UnboundedSender<OperationEvent>,
}
enum OperationEvent {
    AddTaskDataWatcher(TaskId),
    RmvTaskDataWatcher(TaskId),
    TaskPublished(TaskId),
    TaskGetted(TaskId, TaskType),
    TaskCompleted(TaskId),
    TaskPublish(TaskId, oneshot::Sender<anyhow::Result<()>>),
    SliceCompleted(TaskSliceId, WorkerId),
    SliceCompletedDeleted(TaskSliceId, WorkerId),
    WorkerAdded(WorkerId),
    WorkerDeleted(WorkerId),
}

pub enum ZkEvent {
    TaskPublished(TaskId),
    TaskCompleted(TaskId),
    TaskSliceCompleted(TaskSliceId, WorkerId),
    WorkerAdded(WorkerId),
    WorkerDeleted(WorkerId),
}

impl ZkMng {
    pub async fn connect(
        zk_addr: &str,
        callback: impl FnMut(ZkEvent) + 'static + Send + Sync,
    ) -> anyhow::Result<Self> {
        let zk = Arc::new(
            ZooKeeper::connect(zk_addr, std::time::Duration::from_secs(10), |_| {}).await?,
        );
        zk.ensure_path(WORKER_ROOT).await?;
        zk.ensure_path(TASKS_ROOT).await?;
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

        let task_queue_cache = Arc::new(Mutex::new((
            Self::create_queue_cache_watch(zk.clone(), opr_tx.clone()).await?,
            HashMap::new(),
        )));
        let tasks_data_cache = Arc::new(Mutex::new(HashMap::new()));
        let worker_data_cache = Arc::new(Mutex::new((
            Self::create_worker_cache_watch(zk.clone(), opr_tx.clone()).await?,
            HashMap::new(),
        )));

        tokio::spawn(Self::opr_event_proc(
            zk.clone(),
            callback,
            opr_tx.clone(),
            opr_rx,
            tasks_data_cache.clone(),
            task_queue_cache.clone(),
        ));

        Ok(Self {
            zk,
            self_id,
            task_queue_cache,
            tasks_data_cache,
            worker_data_cache,
            opr_tx,
        })
    }
    pub fn self_id(&self) -> WorkerId {
        self.self_id
    }
    async fn task_in_queue(zk: Arc<ZooKeeper>, task_id: TaskId) -> anyhow::Result<String> {
        zk.create(
            TASKS_QUEUE_PATH,
            serde_json::to_vec(&task_id)?,
            Acl::open_unsafe().clone(),
            CreateMode::EphemeralSequential,
        )
        .await
        .map_err(|e| anyhow!("in queue error: {}", e))
    }
    async fn task_out_queue(zk: Arc<ZooKeeper>, task_queue_path: &str) -> anyhow::Result<()> {
        zk.delete(task_queue_path, None)
            .await
            .map_err(|e| anyhow!("out queue error: {}", e))
    }
    pub async fn get_worker_address(&self, worker_id: WorkerId) -> anyhow::Result<WorkerAddress> {
        if let Some(worker_data) = self.worker_data_cache.lock().await.1.get(&worker_id) {
            Ok(worker_data.address.clone())
        } else {
            let (d, _) = self.zk.get_data(&worker_id.to_path(), false).await?;
            let worker_data: WorkerData = serde_json::from_slice(&d)?;
            Ok(worker_data.address)
        }
    }
    async fn opr_event_proc(
        zk: Arc<ZooKeeper>,
        mut cb: impl FnMut(ZkEvent),
        opr_tx: UnboundedSender<OperationEvent>,
        mut opr_rx: UnboundedReceiver<OperationEvent>,
        tasks_data_cache: Arc<Mutex<HashMap<TaskId, TaskData>>>,
        task_queue_cache: Arc<Mutex<(PathChildrenCache, HashMap<TaskId, String>)>>,
    ) {
        while let Some(opr) = opr_rx.recv().await {
            match opr {
                OperationEvent::AddTaskDataWatcher(task_id) => {
                    info!("add task watch");
                    let _ = Self::create_task_slice_watch(
                        zk.clone(),
                        opr_tx.clone(),
                        task_id,
                        tasks_data_cache.clone(),
                    )
                    .await;
                }
                OperationEvent::RmvTaskDataWatcher(task_id) => {
                    info!("task_id: {} watcher removed", task_id.id());
                    tasks_data_cache
                        .lock()
                        .await
                        .remove(&task_id)
                        .expect("should existed");
                }
                OperationEvent::SliceCompleted(task_slice_id, worker_id) => {
                    cb(ZkEvent::TaskSliceCompleted(task_slice_id, worker_id))
                }
                OperationEvent::SliceCompletedDeleted(task_slice_id, worker_id) => {}
                OperationEvent::TaskGetted(task_id, task_type) => {
                    let mut lock = tasks_data_cache.lock().await;
                    if let Entry::Vacant(e) = lock.entry(task_id) {
                        // 自己是worker
                        e.insert(TaskData::new());
                        drop(lock);
                        // todo 逻辑移动到schedular
                        match task_type {
                            TaskType::Spread => {
                                let _ = opr_tx.send(OperationEvent::AddTaskDataWatcher(task_id));
                            }
                            TaskType::Hosted => {}
                        }
                    }
                }
                OperationEvent::TaskPublished(task_id) => {
                    cb(ZkEvent::TaskPublished(task_id));
                }
                OperationEvent::TaskCompleted(task_id) => {
                    tasks_data_cache.lock().await.remove(&task_id);
                    if let Some(p) = task_queue_cache.lock().await.1.remove(&task_id) {
                        let _ = Self::task_out_queue(zk.clone(), &p).await;
                        cb(ZkEvent::TaskCompleted(task_id));
                    }
                }
                OperationEvent::TaskPublish(task_id, tx) => {
                    tasks_data_cache
                        .lock()
                        .await
                        .insert(task_id, TaskData::new());
                    // 必须要先添加监控，然后再入队，否则可能重复添加
                    if let Err(e) = Self::create_task_slice_watch(
                        zk.clone(),
                        opr_tx.clone(),
                        task_id,
                        tasks_data_cache.clone(),
                    )
                    .await
                    {
                        tx.send(Err(anyhow!("add slice watch failed: {}", e)))
                            .unwrap();
                        continue;
                    }
                    if let Ok(p) = Self::task_in_queue(zk.clone(), task_id).await {
                        task_queue_cache.lock().await.1.insert(task_id, p);
                    }
                    tx.send(Ok(())).unwrap()
                }
                OperationEvent::WorkerAdded(worker_id) => {
                    cb(ZkEvent::WorkerAdded(worker_id));
                }
                OperationEvent::WorkerDeleted(worker_id) => {
                    cb(ZkEvent::WorkerDeleted(worker_id));
                }
            }
        }
    }
    pub async fn new_task(
        &self,
        slice_num: usize,
        task_type: TaskType,
        workers_wanted: Option<usize>,
    ) -> anyhow::Result<TaskId> {
        let task_ctrl_info = match workers_wanted {
            Some(workers_wanted) => TaskControlInfo::new_explict(self.self_id, slice_num, task_type, workers_wanted),
            None => TaskControlInfo::new_all_workers(self.self_id, slice_num, task_type)
        };
            
        let mode = match task_type {
            TaskType::Hosted => CreateMode::EphemeralSequential,
            TaskType::Spread => CreateMode::PersistentSequential,
        };
        let p = self
            .zk
            .create(
                TASKS_PATH,
                serde_json::to_vec(&task_ctrl_info)?,
                Acl::open_unsafe().clone(),
                mode,
            )
            .await?;
        let task_id = TaskId::from_path(&p)?;
        let task_path = task_id.to_path();
        // create slices
        iter(0..slice_num)
            .for_each_concurrent(0, |_| {
                let zk_clone = self.zk.clone();
                let task_path_clone = task_path.clone();
                async move {
                    let task_slice_path = format!("{}/slice", task_path_clone);
                    let _ = zk_clone
                        .create(&task_slice_path, vec![], Acl::open_unsafe().clone(), mode)
                        .await
                        .unwrap();
                }
            })
            .await;
        Ok(task_id)
    }
    pub async fn publish_task(&self, task_id: TaskId) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.opr_tx
            .send(OperationEvent::TaskPublish(task_id, tx))
            .map_err(|e| anyhow!("send error: {}", e))?;
        rx.await?
    }
    pub async fn remove_task_slice_watch(&self, task_id: TaskId) {
        let _ = self
            .opr_tx
            .send(OperationEvent::RmvTaskDataWatcher(task_id));
    }

    pub async fn report_task_slice_completed(
        &self,
        task_slice_id: TaskSliceId,
    ) -> anyhow::Result<()> {
        let p = format!(
            "{}/worker{:0width$}",
            task_slice_id.to_path(),
            self.self_id.id(),
            width = 10
        );
        self.zk
            .create(
                &p,
                serde_json::to_vec(&self.self_id)?,
                Acl::open_unsafe().clone(),
                CreateMode::Ephemeral,
            )
            .await?;
        Ok(())
    }

    pub async fn get_task_ctrl_info(
        &self,
        task_id: TaskId,
    ) -> anyhow::Result<(TaskControlInfo, i32)> {
        let (d, Stat { version, .. }) = self.zk.get_data(&task_id.to_path(), false).await?;
        Ok((
            serde_json::from_slice(&d).map_err(|e| anyhow!("deserialize failed: {}", e))?,
            version,
        ))
    }

    pub async fn try_set_task_ctrl_info(
        &self,
        task_id: TaskId,
        task_ctrl_info: &TaskControlInfo,
        version: i32,
    ) -> anyhow::Result<bool> {
        let r = self
            .zk
            .set_data(
                &task_id.to_path(),
                serde_json::to_vec(task_ctrl_info)?,
                Some(version),
            )
            .await;
        match r {
            Err(ZkError::BadVersion) => Ok(false),
            Ok(_) => Ok(true),
            Err(e) => Err(anyhow!("other error: {}", e)),
        }
    }

    async fn set_task_ctrl_info(
        &self,
        task_id: TaskId,
        task_ctrl_info: &TaskControlInfo,
    ) -> anyhow::Result<()> {
        let _ = self
            .zk
            .set_data(
                &task_id.to_path(),
                serde_json::to_vec(task_ctrl_info)?,
                None,
            )
            .await?;
        Ok(())
    }

    pub async fn get_task_status(&self, task_id: TaskId) -> anyhow::Result<TaskStatus> {
        let (ctrl_info, _) = self.get_task_ctrl_info(task_id).await?;
        Ok(ctrl_info.status)
    }
    pub async fn set_task_status(&self, task_id: TaskId, status: TaskStatus) -> anyhow::Result<()> {
        let (mut ctrl_info, _) = self.get_task_ctrl_info(task_id).await?;
        ctrl_info.status = status;
        self.set_task_ctrl_info(task_id, &ctrl_info).await
    }
    pub fn report_task_completed(&self, task_id: TaskId) {
        let _ = self.opr_tx.send(OperationEvent::TaskCompleted(task_id));
    }
    pub fn task_getted(&self, task_id: TaskId, task_type: TaskType) {
        let _ = self
            .opr_tx
            .send(OperationEvent::TaskGetted(task_id, task_type));
    }
    async fn create_one_slice_watch(
        zk: Arc<ZooKeeper>,
        opr_tx: UnboundedSender<OperationEvent>,
        task_slice_id: TaskSliceId,
        tasks_data_cache: Arc<Mutex<HashMap<TaskId, TaskData>>>,
    ) -> anyhow::Result<()> {
        let mut slice_completed_cache =
            PathChildrenCache::new(zk, &task_slice_id.to_path()).await?;
        let id_map = std::sync::Mutex::new(HashMap::new());
        slice_completed_cache.add_listener(move |event| match event {
            PathChildrenCacheEvent::ChildAdded(p, data) => {
                let worker_id: WorkerId = serde_json::from_slice(&data.0).expect("invalid data");
                id_map.lock().unwrap().insert(p, worker_id);
                let _ = opr_tx.send(OperationEvent::SliceCompleted(task_slice_id, worker_id));
            }
            PathChildrenCacheEvent::ChildRemoved(p) => {
                if let Some(worker_id) = id_map.lock().unwrap().remove(&p) {
                    let _ = opr_tx.send(OperationEvent::SliceCompletedDeleted(
                        task_slice_id,
                        worker_id,
                    ));
                }
            }
            _ => {}
        });
        slice_completed_cache.start()?;
        tasks_data_cache
            .lock()
            .await
            .entry(task_slice_id.task_id())
            .and_modify(|x| {
                x.slice_completed
                    .insert(task_slice_id.slice_id(), slice_completed_cache);
            });
        Ok(())
    }
    async fn create_task_slice_watch(
        zk: Arc<ZooKeeper>,
        opr_tx: UnboundedSender<OperationEvent>,
        task_id: TaskId,
        tasks_data_cache: Arc<Mutex<HashMap<TaskId, TaskData>>>,
    ) -> anyhow::Result<()> {
        let task_data: TaskControlInfo =
            serde_json::from_slice(&zk.get_data(&task_id.to_path(), false).await?.0)?;
        iter(0..task_data.slice_num)
            .for_each_concurrent(0, |i| {
                let opr_tx_clone = opr_tx.clone();
                let zk_clone = zk.clone();
                let tasks_data_cache_clone = tasks_data_cache.clone();
                async move {
                    let _ = Self::create_one_slice_watch(
                        zk_clone,
                        opr_tx_clone,
                        TaskSliceId::from_task_slice(task_id, i.into()),
                        tasks_data_cache_clone,
                    )
                    .await;
                }
            })
            .await;
        Ok(())
    }

    async fn create_queue_cache_watch(
        zk: Arc<ZooKeeper>,
        opr_tx: UnboundedSender<OperationEvent>,
    ) -> anyhow::Result<PathChildrenCache> {
        let mut cache = PathChildrenCache::new(zk, TASKS_QUEUE_ROOT).await?;
        let id_map = std::sync::Mutex::new(HashMap::new());
        cache.add_listener(move |event| match event {
            PathChildrenCacheEvent::ChildAdded(p, data) => {
                let task_id = serde_json::from_slice(&data.0).expect("invalid data");
                id_map.lock().unwrap().insert(p, task_id);
                let _ = opr_tx.send(OperationEvent::TaskPublished(task_id));
            }
            PathChildrenCacheEvent::ChildRemoved(p) => {
                if let Some(task_id) = id_map.lock().unwrap().remove(&p) {
                    let _ = opr_tx.send(OperationEvent::TaskCompleted(task_id));
                }
            }
            _ => {}
        });
        cache.start()?;
        Ok(cache)
    }

    async fn create_worker_cache_watch(
        zk: Arc<ZooKeeper>,
        opr_tx: UnboundedSender<OperationEvent>,
    ) -> anyhow::Result<PathChildrenCache> {
        let mut cache = PathChildrenCache::new(zk, WORKER_ROOT).await?;
        let id_map = std::sync::Mutex::new(HashMap::new());
        cache.add_listener(move |event| match event {
            PathChildrenCacheEvent::ChildAdded(p, _) => {
                let worker_id = WorkerId::from_path(&p).unwrap();
                id_map.lock().unwrap().insert(p, worker_id);
                let _ = opr_tx.send(OperationEvent::WorkerAdded(worker_id));
            }
            PathChildrenCacheEvent::ChildRemoved(p) => {
                if let Some(worker_id) = id_map.lock().unwrap().remove(&p) {
                    let _ = opr_tx.send(OperationEvent::WorkerDeleted(worker_id));
                }
            }
            _ => {}
        });
        //cache.start()?;
        Ok(cache)
    }
    async fn operation_sender(&self) -> UnboundedSender<OperationEvent> {
        self.opr_tx.clone()
    }
}

mod test_zk_mng {
    use std::time::Duration;

    use crate::{
        zk_mng::{TaskSliceId, TASKS_PATH},
        ZkMng,
    };

    use super::{SliceId, TaskId, TaskType};

    const ZK_ADDR: &str = "127.0.0.1:2181";
    #[tokio::main]
    #[test]
    async fn test_connect() {
        let zk_mng = ZkMng::connect(ZK_ADDR, |x| {}).await.unwrap();
        let task = zk_mng
            .new_task(10, super::TaskType::Spread, Some(5))
            .await
            .unwrap();
        zk_mng.publish_task(task).await.unwrap();
        for i in 0..10 {
            zk_mng
                .report_task_slice_completed(TaskSliceId::from_task_slice(task, i.into()))
                .await
                .unwrap();
        }
        zk_mng.report_task_completed(task);
        tokio::time::sleep(Duration::from_secs(30)).await;
    }

    #[tokio::main]
    #[test]
    async fn test_multi_clients() {
        let zk_mng = ZkMng::connect(ZK_ADDR, |x| {}).await.unwrap();
        let zk_mng2 = ZkMng::connect(ZK_ADDR, |x| {}).await.unwrap();
        let task = zk_mng
            .new_task(10, super::TaskType::Spread, None)
            .await
            .unwrap();
        zk_mng.publish_task(task).await.unwrap();
        for i in 0..10 {
            zk_mng
                .report_task_slice_completed(TaskSliceId::from_task_slice(task, i.into()))
                .await
                .unwrap();
            zk_mng2
                .report_task_slice_completed(TaskSliceId::from_task_slice(task, i.into()))
                .await
                .unwrap();
        }
        zk_mng.report_task_completed(task);
    }

    #[tokio::main]
    #[test]
    async fn test_task_slice_id() {
        let zk_mng = ZkMng::connect(ZK_ADDR, |x| {}).await.unwrap();
        let task_id = zk_mng.new_task(10, TaskType::Spread, None).await.unwrap();
    }
}

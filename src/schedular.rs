use crate::{
    task::{TaskBehaviour, TaskResult},
    zk_mng::{SliceId, TaskId, TaskSliceId, TaskType, WorkerAddress, WorkerId, ZkEvent},
    ZkMng,
};
use anyhow::anyhow;
use futures::{stream::iter, StreamExt};
use log::info;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::{mpsc, oneshot, Mutex};

struct TaskData {
    slices_data: HashMap<SliceId, SliceData>,
    progress_info: Option<HashMap<WorkerId, HashSet<SliceId>>>, // 哪些worker完成了多少切片
    slices_num: usize,
    wanted_workers: Option<usize>,
    completed_worker_num: usize,
}
impl TaskData {
    fn new(slices_num: usize, wanted_workers: Option<usize>) -> Self {
        Self {
            slices_num,
            wanted_workers,
            progress_info: Default::default(),
            slices_data: Default::default(),
            completed_worker_num: 0,
        }
    }
}
#[derive(Default, Debug)]
struct SliceData {
    status: SliceStatus,
    completed_workers: Option<HashSet<WorkerId>>,
}
impl Default for SliceStatus {
    fn default() -> Self {
        SliceStatus::InCompleted
    }
}

#[derive(Debug, Clone, Copy)]
enum SliceStatus {
    InCompleted,
    Working,
    Completed,
}
pub struct Schedular<B> {
    task_behaviour: Arc<B>,
    opr_tx: mpsc::UnboundedSender<OperationEvent>,
    zk_mng: Arc<ZkMng>,
    tasks_data: Arc<Mutex<HashMap<TaskId, TaskData>>>,
    published_tasks: Arc<Mutex<HashMap<TaskId, oneshot::Sender<anyhow::Result<()>>>>>,
}

enum OperationEvent {
    PublishTask(
        TaskId,
        usize,
        Option<usize>,
        oneshot::Sender<anyhow::Result<()>>,
    ),
    TaskInQueue(TaskId),
    TryProc(TaskSliceId, WorkerId),
}

pub struct TaskHandle {}
impl TaskHandle {}
impl<B: 'static + Send + Sync + TaskBehaviour> Schedular<B> {
    pub async fn try_new(zk_addr: &str, task_behaviour: B) -> anyhow::Result<Self> {
        let (evt_tx, evt_rx) = mpsc::unbounded_channel();
        let (opr_tx, opr_rx) = mpsc::unbounded_channel();
        let task_behaviour = Arc::new(task_behaviour);
        let zk_mng = Arc::new(
            ZkMng::connect(zk_addr, move |evt| {
                let _ = evt_tx.send(evt);
            })
            .await?,
        );
        let tasks_data = Arc::new(Mutex::new(HashMap::new()));
        let published_tasks = Arc::new(Mutex::new(HashMap::new()));
        tokio::spawn(Self::proc_zk_event(
            zk_mng.clone(),
            tasks_data.clone(),
            published_tasks.clone(),
            evt_rx,
            opr_tx.clone(),
        ));
        tokio::spawn(Self::opr_proc(
            opr_rx,
            zk_mng.clone(),
            tasks_data.clone(),
            published_tasks.clone(),
            task_behaviour.clone(),
        ));
        Ok(Self {
            tasks_data,
            task_behaviour,
            zk_mng,
            opr_tx,
            published_tasks,
        })
    }
    pub async fn spawn_task(
        &self,
        slice_num: usize,
        task_type: TaskType,
        workers_wanted: Option<usize>,
    ) -> anyhow::Result<()> {
        let task_id = self
            .zk_mng
            .new_task(slice_num, task_type, workers_wanted)
            .await?;
        let (tx, rx) = oneshot::channel();
        self.opr_tx
            .send(OperationEvent::PublishTask(
                task_id,
                slice_num,
                workers_wanted,
                tx,
            ))
            .map_err(|e| anyhow!("send error: {}", e))?;
        rx.await?
    }
    async fn proc_task_slice_success(
        zk_mng: Arc<ZkMng>,
        task_slice_id: TaskSliceId,
        tasks_data: Arc<Mutex<HashMap<TaskId, TaskData>>>,
    ) -> anyhow::Result<()> {
        // 报告切片完成
        zk_mng.report_task_slice_completed(task_slice_id).await?;
        let task_id = task_slice_id.task_id();
        let slice_id = task_slice_id.slice_id();
        let mut lock = tasks_data.lock().await;
        let task_data = lock
            .get_mut(&task_id)
            .ok_or(anyhow!("task_id:{} data is not existed", task_id.id()))?;
        let slice_data = task_data.slices_data.entry(slice_id).or_default();
        slice_data.status = SliceStatus::Completed;
        if task_data.slices_data.len() == task_data.slices_num
            && task_data
                .slices_data
                .iter()
                .all(|x| matches!(x.1.status, SliceStatus::Completed))
        {
            // 自己完成了所有的工作，可以不用监控了
            zk_mng.remove_task_slice_watch(task_id).await;
        }
        Ok(())
    }
    async fn proc_task_slice_failed(
        task_slice_id: TaskSliceId,
        worker_id: WorkerId,
        tasks_data: Arc<Mutex<HashMap<TaskId, TaskData>>>,
    ) -> anyhow::Result<()> {
        let task_id = task_slice_id.task_id();
        let slice_id = task_slice_id.slice_id();
        let mut lock = tasks_data.lock().await;
        let task_data = lock
            .get_mut(&task_id)
            .ok_or(anyhow!("task_id:{} data is not existed", task_id.id()))?;
        let slice_data = task_data.slices_data.entry(slice_id).or_default();
        slice_data.status = SliceStatus::InCompleted;
        let mut completed_workers = slice_data.completed_workers.take().unwrap_or_default();
        completed_workers.insert(worker_id);
        slice_data.completed_workers = Some(completed_workers);
        Ok(())
    }
    async fn proc_task(
        zk_mng: Arc<ZkMng>,
        task_behaviour: Arc<B>,
        task_slice_id: TaskSliceId,
        worker_id: WorkerId,
        tasks_data: Arc<Mutex<HashMap<TaskId, TaskData>>>,
    ) -> anyhow::Result<()> {
        // 判断task的状态和自身的压力
        let pressure = task_behaviour.query_pressure().await?;
        if pressure > 50 {
            return Ok(());
        }
        // 判断slice状态
        let mut lock = tasks_data.lock().await;
        let task_id = task_slice_id.task_id();
        let slice_id = task_slice_id.slice_id();
        let task_data = lock
            .get_mut(&task_id)
            .ok_or(anyhow!("task_id:{} data is not existed", task_id.id()))?;
        let slice_data = task_data.slices_data.entry(slice_id).or_default();
        if let (SliceStatus::InCompleted) = slice_data.status {
            slice_data.status = SliceStatus::Working;
            drop(lock);
            // 开始工作
            let worker_address = zk_mng.get_worker_address(worker_id).await?;
            if let Ok(TaskResult::Completed) =
                task_behaviour.try_work(task_slice_id, worker_address).await
            {
                // 执行成功
                let _ = Self::proc_task_slice_success(zk_mng, task_slice_id, tasks_data).await;
            } else {
                // 执行失败
                let _ = Self::proc_task_slice_failed(task_slice_id, worker_id, tasks_data).await;
            }
        }

        Ok(())
    }
    async fn report_all_slice_completed(zk_mng: Arc<ZkMng>, task_id: TaskId, slice_num: usize) {
        iter(0..slice_num)
            .for_each_concurrent(0, |x| {
                let zk_mng_clone = zk_mng.clone();
                async move {
                    let _ = zk_mng_clone
                        .report_task_slice_completed(TaskSliceId::from_task_slice(
                            task_id,
                            x.into(),
                        ))
                        .await;
                }
            })
            .await;
    }
    async fn try_get_task(zk_mng: Arc<ZkMng>, tasks_data: Arc<Mutex<HashMap<TaskId, TaskData>>>, task_id: TaskId) {
        let task_ctrl_info = loop {
            if let Ok((mut task_ctrl_info, version)) = zk_mng.get_task_ctrl_info(task_id).await {
                if let Some(worker_wanted) = task_ctrl_info.workers_wanted() {
                    if task_ctrl_info.cur_worker_list().unwrap() < worker_wanted {
                        task_ctrl_info.add_worker(zk_mng.self_id());
                        match zk_mng.try_set_task_ctrl_info(task_id, &task_ctrl_info, version).await {
                            Ok(false) => {continue;} // 写入失败，继续尝试
                            Ok(true) => {break Some(task_ctrl_info);}
                            _ => {break None;}
                        }
                    } else {
                        // 数量足够了
                        break None;
                    }
                } else {
                    // 没有数量限制，所有worker都执行
                    break Some(task_ctrl_info);
                }
            } else {
                break None;
            }
        };
        if let Some(task_ctrl_info) = task_ctrl_info {
            zk_mng.task_getted(task_id, task_ctrl_info.task_type());
            tasks_data.lock().await.insert(task_id, TaskData::new(
                task_ctrl_info.slice_num(),
                task_ctrl_info.workers_wanted(),
            ));
        }
    }
    async fn opr_proc(
        mut opr_rx: mpsc::UnboundedReceiver<OperationEvent>,
        zk_mng: Arc<ZkMng>,
        tasks_data: Arc<Mutex<HashMap<TaskId, TaskData>>>,
        published_tasks: Arc<Mutex<HashMap<TaskId, oneshot::Sender<anyhow::Result<()>>>>>,
        task_behaviour: Arc<B>,
    ) {
        while let Some(opr) = opr_rx.recv().await {
            match opr {
                OperationEvent::PublishTask(task_id, slice_num, wanted_works, sender) => {
                    tasks_data
                        .lock()
                        .await
                        .insert(task_id, TaskData::new(slice_num, wanted_works));
                    if let Err(e) = zk_mng.publish_task(task_id).await {
                        sender
                            .send(Err(anyhow!("publish task error: {}", e)))
                            .expect("send error");
                    } else {
                        published_tasks.lock().await.insert(task_id, sender);
                        Self::report_all_slice_completed(zk_mng.clone(), task_id, slice_num).await;
                    }
                }
                OperationEvent::TaskInQueue(task_id) => {
                    let mut lock = tasks_data.lock().await;
                    if let Entry::Vacant(e) = lock.entry(task_id) {
                        // 抢任务
                       tokio::spawn(Self::try_get_task(zk_mng.clone(), tasks_data.clone(), task_id));
                    }
                }
                OperationEvent::TryProc(task_slice_id, worker_id) => {
                    tokio::spawn(Self::proc_task(
                        zk_mng.clone(),
                        task_behaviour.clone(),
                        task_slice_id,
                        worker_id,
                        tasks_data.clone(),
                    ));
                }
            }
        }
    }
    async fn record_task_slice_completed(
        tasks_data: Arc<Mutex<HashMap<TaskId, TaskData>>>,
        task_slice_id: TaskSliceId,
        worker_id: WorkerId,
    ) -> anyhow::Result<bool> {
        let mut lock = tasks_data.lock().await;
        let task_data = lock
            .get_mut(&task_slice_id.task_id())
            .ok_or(anyhow!("Can't find tasks"))?;
        let mut progress_info = task_data.progress_info.take().unwrap_or_default();
        let worker_entry = progress_info.entry(worker_id).or_default();
        worker_entry.insert(task_slice_id.slice_id());
        if worker_entry.len() == task_data.slices_num {
            task_data.completed_worker_num += 1;
        }
        task_data.progress_info = Some(progress_info);
        // 判断是否所有的worker都完成了
        if let Some(wanted) = task_data.wanted_workers {
            if task_data.completed_worker_num == wanted {
                return Ok(true);
            }
        } else {
            // 所有都要完成
            unimplemented!()
        }
        Ok(false)
    }
    async fn proc_zk_event(
        zk_mng: Arc<ZkMng>,
        tasks_data: Arc<Mutex<HashMap<TaskId, TaskData>>>,
        published_tasks: Arc<Mutex<HashMap<TaskId, oneshot::Sender<anyhow::Result<()>>>>>,
        mut evt_rx: mpsc::UnboundedReceiver<ZkEvent>,
        opr_tx: mpsc::UnboundedSender<OperationEvent>,
    ) {
        while let Some(evt) = evt_rx.recv().await {
            match evt {
                ZkEvent::TaskPublished(task_id) => {
                    info!("task_published: {}", task_id.id());
                    let _ = opr_tx.send(OperationEvent::TaskInQueue(task_id));
                }
                ZkEvent::TaskCompleted(task_id) => {}
                ZkEvent::TaskSliceCompleted(task_slice_id, worker_id) => {
                    if worker_id == zk_mng.self_id() {
                        continue;
                    }
                    if published_tasks
                        .lock()
                        .await
                        .get(&task_slice_id.task_id())
                        .is_none()
                    {
                        info!("try work: {:?}, from {}", task_slice_id, worker_id.id());
                        let _ = opr_tx.send(OperationEvent::TryProc(task_slice_id, worker_id));
                    } else {
                        // 判断task是否完成
                        info!(
                            "recv task slice completed: {}, {:?}",
                            worker_id.id(),
                            task_slice_id
                        );
                        if Self::record_task_slice_completed(
                            tasks_data.clone(),
                            task_slice_id,
                            worker_id,
                        )
                        .await
                        .expect("record must success")
                        {
                            // 任务结束
                            if let Some(tx) = published_tasks
                                .lock()
                                .await
                                .remove(&task_slice_id.task_id())
                            {
                                tx.send(Ok(())).unwrap();
                            }
                        }
                    }
                }
                ZkEvent::WorkerAdded(worker_id) => {}
                ZkEvent::WorkerDeleted(worekr_id) => {}
            }
        }
    }
}

mod test_schedular {
    use std::{
        collections::HashMap,
        sync::Arc,
        time::{Duration, Instant},
    };

    use crate::{dummy_task::DummyTaskBehaviour, zk_mng::TaskType};
    use futures::{stream::iter, StreamExt};
    use libp2p::swarm::DummyBehaviour;
    use tokio::sync::Mutex;

    use super::Schedular;
    const ZK_ADDR: &str = "127.0.0.1:2181";
    #[tokio::main]
    #[test]
    async fn test1() {
        let all_client = Arc::new(Mutex::new(Vec::new()));
        for _ in 0..20 {
            iter(0..100)
                .for_each_concurrent(0, |i| {
                    let all_client_clone = all_client.clone();
                    async move {
                        let schedular = Schedular::try_new(&format!("127.0.0.1:{}", 2181), DummyTaskBehaviour {})
                            .await
                            .unwrap();
                        all_client_clone.lock().await.push(schedular);
                    }
                })
                .await;
        }

        let now = Instant::now();
        all_client
            .lock()
            .await
            .first()
            .unwrap()
            .spawn_task(1, TaskType::Spread, Some(500))
            .await
            .unwrap();
        println!("time used: {}", now.elapsed().as_millis());
        //tokio::time::sleep(Duration::from_secs(50)).await;
    }
}

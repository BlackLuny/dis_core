use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use crate::zk_mng::{TaskSliceId, WorkerId};
use async_trait::async_trait;
use libp2p::kad::store::RecordStore;
use tokio::sync::{mpsc, Mutex};
pub enum WorkerMetric {
    Pressure(usize),
    Latency(Duration),
}
pub enum Event {
    Success(TaskSliceId, WorkerId),
    Failure(TaskSliceId, WorkerId),
    Stop,
}
pub struct Token {
    task_slice_id: TaskSliceId,
    worker_id: WorkerId,
    tx: Option<mpsc::UnboundedSender<Event>>,
}

impl Token {
    fn new(
        task_slice_id: TaskSliceId,
        worker_id: WorkerId,
        tx: mpsc::UnboundedSender<Event>,
    ) -> Self {
        Self {
            task_slice_id,
            worker_id,
            tx: Some(tx),
        }
    }
    pub fn success(&mut self) {
        self.tx
            .take()
            .map(|tx| tx.send(Event::Success(self.task_slice_id, self.worker_id)));
    }
    pub fn failure(&mut self) {
        self.tx
            .take()
            .map(|tx| tx.send(Event::Failure(self.task_slice_id, self.worker_id)));
    }
    pub fn task_slice_id(&self) -> TaskSliceId {
        self.task_slice_id
    }
    pub fn worker_id(&self) -> WorkerId {
        self.worker_id
    }
}

/// 默认上报失败
impl Drop for Token {
    fn drop(&mut self) {
        self.failure()
    }
}
#[async_trait]
pub trait ScheduleStrategy: 'static + Send + Sync {
    fn get_token_tx(&self) -> mpsc::UnboundedSender<Event>;
    /// 添加task候选worker
    async fn add_task_worker(&self, task_slice_id: TaskSliceId, worker_id: WorkerId);

    /// 移除一个worker
    async fn rmv_worker(&self, worker_id: WorkerId);

    /// 选择task slice 最好的worker
    async fn select_worker_strategy(&self, task_slice_id: TaskSliceId) -> Option<WorkerId>;

    /// 选择task slice 最好的worker
    async fn select_task_strategy(&self, worker_id: WorkerId) -> Option<TaskSliceId>;

    /// 更新worker的metric
    fn update_worker_metric(&mut self, worker_id: WorkerId, metric: WorkerMetric);

    /// 更新自己的metric
    fn update_self_metric(&mut self, metric: WorkerMetric);

    /// 获取自身的压力
    fn get_self_pressure(&self) -> usize;

    /// 获取自身最大压力
    fn get_self_max_pressure(&self) -> usize;

    /// 获取worker的压力
    fn get_remote_worker_pressure(&self, remote_worker_id: WorkerId) -> Option<usize>;

    /// 评估是否可以做任务, 默认只比较压力，如果要覆盖，需要在找不到worker的时候返回true
    fn evaluate(&self, _task_slice_id: TaskSliceId, worker_id: WorkerId) -> bool {
        let self_max_pressure = self.get_self_max_pressure();
        self.get_self_pressure() <= self_max_pressure
            && self
                .get_remote_worker_pressure(worker_id)
                .map(|p| p <= self_max_pressure)
                .unwrap_or(true)
    }

    /// 选择task slice 最好的worker
    async fn select_best_worker(&self, task_slice_id: TaskSliceId) -> Option<Token> {
        match self
            .select_worker_strategy(task_slice_id)
            .await
            .map(|worker_id| (worker_id, self.evaluate(task_slice_id, worker_id)))
        {
            Some((worker_id, true)) => Some(Token::new(task_slice_id, worker_id, self.get_token_tx())),
            _ => None,
        }
    }
    /// 选择worker的最好的task slice
    async fn select_best_task_slice(&self, worker_id: WorkerId) -> Option<Token> {
        match self
        .select_task_strategy(worker_id)
        .await
        .map(|task_slice_id| (task_slice_id, self.evaluate(task_slice_id, worker_id)))
    {
        Some((task_slice_id, true)) => Some(Token::new(task_slice_id, worker_id, self.get_token_tx())),
        _ => None,
    }
    }
}

enum SliceStatus {
    InCompleted(HashSet<WorkerId>),       // 没有完成状态，候选WorkerId
    Working(WorkerId, HashSet<WorkerId>), // 当前正在工作, 候选WorkerID
    Completed,                            // 完成状态
}
impl Default for SliceStatus {
    fn default() -> Self {
        Self::InCompleted(Default::default())
    }
}
impl SliceStatus {
    fn incompleted_with_workers(&self) -> Option<&HashSet<WorkerId>> {
        match self {
            SliceStatus::InCompleted(w) => Some(w),
            _ => None,
        }
    }
    fn to_incompleted(self) -> Self {
        match self {
            Self::Working(_, w) => Self::InCompleted(w),
            _ => self,
        }
    }
    fn to_working(self, worker_id: WorkerId) -> Self {
        match self {
            Self::InCompleted(w) => Self::Working(worker_id, w),
            _ => self,
        }
    }
    fn to_completed(self) -> Self {
        match self {
            Self::Working(_, _) => Self::Completed,
            _ => self,
        }
    }
}

#[derive(Default, Debug)]
struct MetricData {
    pressure: Option<usize>,
    latency: Option<Duration>,
}
pub struct SimpleScheduleStrategy {
    max_pressure: usize,
    self_metric: MetricData,
    event_sender: mpsc::UnboundedSender<Event>,
    workers_metric: HashMap<WorkerId, MetricData>,
    tasks_slice_status: Arc<Mutex<HashMap<TaskSliceId, SliceStatus>>>,
}
impl Drop for SimpleScheduleStrategy {
    fn drop(&mut self) {
        let _ = self.event_sender.send(Event::Stop);
    }
}
impl SimpleScheduleStrategy {
    pub fn new(max_pressure: usize) -> Self {
        let (event_sender, mut event_receiver) = mpsc::unbounded_channel();
        let tasks_slice_status = Arc::new(Mutex::new(HashMap::<TaskSliceId, SliceStatus>::new()));
        let tasks_slice_status_clone = tasks_slice_status.clone();
        tokio::spawn(async move {
            while let Some(evt) = event_receiver.recv().await {
                match evt {
                    Event::Stop => break,
                    Event::Success(task_slice_id, _worker_id) => {
                        let mut l = tasks_slice_status_clone.lock().await;
                        let status = l.remove(&task_slice_id).map(|s| s.to_completed()).unwrap();
                        l.insert(task_slice_id, status);
                    }
                    Event::Failure(task_slice_id, _worker_id) => {
                        let mut l = tasks_slice_status_clone.lock().await;
                        let status = l
                            .remove(&task_slice_id)
                            .map(|s| s.to_incompleted())
                            .unwrap();
                        l.insert(task_slice_id, status);
                    }
                }
            }
        });
        Self {
            event_sender,
            max_pressure,
            self_metric: Default::default(),
            workers_metric: Default::default(),
            tasks_slice_status,
        }
    }
}

#[async_trait]
impl ScheduleStrategy for SimpleScheduleStrategy {
    fn get_token_tx(&self) ->mpsc::UnboundedSender<Event> {
        self.event_sender.clone()
    }
    /// 添加task候选worker
    async fn add_task_worker(&self, task_slice_id: TaskSliceId, worker_id: WorkerId) {
        let mut lock = self.tasks_slice_status.lock().await;
        match lock.entry(task_slice_id).or_default() {
            SliceStatus::InCompleted(w) => {
                w.insert(worker_id);
            }
            SliceStatus::Working(_, w) => {
                w.insert(worker_id);
            }
            _ => {}
        }
    }

    /// 移除一个worker
    async fn rmv_worker(&self, worker_id: WorkerId) {
        let mut lock = self.tasks_slice_status.lock().await;
        lock.iter_mut().for_each(|x| match x.1 {
            SliceStatus::InCompleted(v) => {
                v.remove(&worker_id);
            }
            SliceStatus::Working(_, v) => {
                v.remove(&worker_id);
            }
            _ => {}
        });
    }

    /// 选择task slice 最好的worker
    async fn select_worker_strategy(&self, task_slice_id: TaskSliceId) -> Option<WorkerId> {
        let mut lock = self.tasks_slice_status.lock().await;
        // 按照latency和压力选择的worker
        match lock
            .get(&task_slice_id)
            .and_then(|x| x.incompleted_with_workers())
            .map(|worker_lst| {
                worker_lst.iter().map(|w| match self.workers_metric.get(w) {
                    Some(m) => (
                        *w,
                        m.pressure.as_ref().map_or(0, |x| *x),
                        m.latency
                            .as_ref()
                            .map_or(Duration::default(), |x| x.clone()),
                    ),
                    None => (*w, 0, Duration::default()),
                })
            })
            .and_then(|x| {
                x.min_by(|(_, pressure1, latency1), (_, pressure2, latency2)| {
                    match latency1.cmp(&latency2) {
                        Ordering::Equal => pressure1.cmp(&pressure2),
                        r => r,
                    }
                })
            })
            .map(|x| x.0)
        {
            Some(worker) => {
                let status = lock
                    .remove(&task_slice_id)
                    .map(|x| x.to_working(worker))
                    .unwrap();
                lock.insert(task_slice_id, status);
                Some(worker)
            }
            None => None,
        }
    }

    /// 选择worker的最好的task slice
    async fn select_task_strategy(&self, worker_id: WorkerId) -> Option<TaskSliceId> {
        let mut lock = self.tasks_slice_status.lock().await;
        // 找到剩余切片数最少的task
        match lock
            .iter()
            .filter_map(|(task_slice_id, status)| match status {
                SliceStatus::InCompleted(w) if w.contains(&worker_id) => Some(task_slice_id),
                _ => None,
            })
            .fold(HashMap::new(), |mut h, w| {
                h.entry(w.task_id()).or_insert((0, w)).0 += 1;
                h
            })
            .iter()
            .min_by_key(|(_, (slice_cnt, _))| slice_cnt)
            .map(|x| *x.1 .1)
        {
            Some(task_slice_id) => {
                let status = lock
                    .remove(&task_slice_id)
                    .map(|x| x.to_working(worker_id))
                    .unwrap();
                lock.insert(task_slice_id, status);
                Some(task_slice_id)
            }
            None => None,
        }
    }

    /// 更新worker的metric
    fn update_worker_metric(&mut self, worker_id: WorkerId, metric: WorkerMetric) {
        let entry = self.workers_metric.entry(worker_id).or_default();
        match metric {
            WorkerMetric::Pressure(p) => entry.pressure = Some(p),
            WorkerMetric::Latency(l) => {
                entry.latency = Some(l);
            }
            _ => {}
        }
    }

    /// 更新自己的metric
    fn update_self_metric(&mut self, metric: WorkerMetric) {
        match metric {
            WorkerMetric::Pressure(p) => {
                self.self_metric.pressure = Some(p);
            }
            WorkerMetric::Latency(l) => {
                self.self_metric.latency = Some(l);
            }
            _ => {}
        }
    }

    /// 获取自身的压力
    fn get_self_pressure(&self) -> usize {
        self.self_metric
            .pressure
            .as_ref()
            .map(|x| *x)
            .unwrap_or_default()
    }

    /// 获取自身最大压力
    fn get_self_max_pressure(&self) -> usize {
        self.max_pressure
    }

    /// 获取worker的压力
    fn get_remote_worker_pressure(&self, remote_worker_id: WorkerId) -> Option<usize> {
        self.workers_metric
            .get(&remote_worker_id)
            .and_then(|x| x.pressure.as_ref())
            .map(|x| *x)
    }
}

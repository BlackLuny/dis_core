use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use crate::zk_mng::{TaskSliceId, WorkerId};
use async_trait::async_trait;
use replace_with::replace_with_or_default;
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
            Some((worker_id, true)) => {
                Some(Token::new(task_slice_id, worker_id, self.get_token_tx()))
            }
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
            Some((task_slice_id, true)) => {
                Some(Token::new(task_slice_id, worker_id, self.get_token_tx()))
            }
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
                        l.get_mut(&task_slice_id)
                            .map(|s| replace_with_or_default(s, |s| s.to_completed()));
                    }
                    Event::Failure(task_slice_id, _worker_id) => {
                        let mut l = tasks_slice_status_clone.lock().await;
                        l.get_mut(&task_slice_id)
                            .map(|s| replace_with_or_default(s, |s| s.to_incompleted()));
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
    fn get_token_tx(&self) -> mpsc::UnboundedSender<Event> {
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
                lock.get_mut(&task_slice_id)
                    .map(|s| replace_with_or_default(s, |s| s.to_working(worker)));
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
                lock.get_mut(&task_slice_id)
                    .map(|s| replace_with_or_default(s, |s| s.to_working(worker_id)));
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

mod test_strategy {
    use std::time::Duration;

    use crate::SimpleScheduleStrategy;

    use super::{Token, ScheduleStrategy, WorkerMetric};

    async fn report_success(mut token: Token) {
        token.success();
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    async fn report_failure(mut token: Token) {
        token.failure();
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    #[tokio::main]
    #[test]
    async fn test_select_best_worker_2worker_2tasks_4slices() {
        let worker0 = 0.into();
        let worker1 = 1.into();
        let task0_slice0 = (0, 0).into();
        let task0_slice1 = (0, 1).into();
        let task1_slice0 = (1, 0).into();
        let task1_slice1 = (1, 1).into();

        let mut st = SimpleScheduleStrategy::new(20);
        // add tasks and workers
        st.add_task_worker(task0_slice0, worker0).await;
        st.add_task_worker(task0_slice1, worker0).await;
        st.add_task_worker(task1_slice0, worker0).await;
        st.add_task_worker(task1_slice1, worker0).await;
        st.add_task_worker(task0_slice0, worker1).await;
        st.add_task_worker(task0_slice1, worker1).await;
        st.add_task_worker(task1_slice0, worker1).await;
        st.add_task_worker(task1_slice1, worker1).await;

        // update workers metric
        st.update_worker_metric(worker0, WorkerMetric::Pressure(10));
        st.update_worker_metric(worker0, WorkerMetric::Latency(Duration::from_millis(100)));

        st.update_worker_metric(worker1, WorkerMetric::Pressure(15));
        st.update_worker_metric(worker1, WorkerMetric::Latency(Duration::from_millis(90)));

        st.update_self_metric(WorkerMetric::Pressure(1));
        st.update_self_metric(WorkerMetric::Latency(Duration::from_millis(50)));

        let selected0 = st.select_best_worker(task0_slice0).await;
        assert_eq!(selected0.as_ref().unwrap().worker_id().id(), 1); // 选择1

        let selected1 = st.select_best_worker(task0_slice0).await;
        assert_eq!(selected1.is_some(), false); // 选择失败，因为已经被选择

        report_failure(selected0.unwrap()).await; // 上报失败

        //update latency
        st.update_worker_metric(worker0, WorkerMetric::Latency(Duration::from_millis(90)));


        let selected0 = st.select_best_worker(task0_slice1).await;
        assert_eq!(selected0.as_ref().unwrap().worker_id().id(), 0); // 选择0, 延迟相同时，选择压力小的

        report_success(selected0.unwrap()).await;

        // update pressure
        st.update_worker_metric(worker0, WorkerMetric::Pressure(16));

        let selected0 = st.select_best_worker(task0_slice0).await;
        assert_eq!(selected0.as_ref().unwrap().worker_id().id(), 1); // 选择1, worker0的压力大于1
        report_failure(selected0.unwrap()).await;

        // update worker1 pressure to max pressure
        st.update_worker_metric(worker1, WorkerMetric::Pressure(20));
        let selected0 = st.select_best_worker(task0_slice0).await;
        assert_eq!(selected0.as_ref().unwrap().worker_id().id(), 0); // 选择0, worker1的压力过大, 选择worker0
        report_success(selected0.unwrap()).await;

        // remove worker
        st.rmv_worker(worker0).await;
        st.rmv_worker(worker1).await;
    }

    #[tokio::main]
    #[test]
    async fn test_select_best_worker_2worker_2tasks_4slices_no_latency_metric() {
        let worker0 = 0.into();
        let worker1 = 1.into();
        let task0_slice0 = (0, 0).into();
        let task0_slice1 = (0, 1).into();
        let task1_slice0 = (1, 0).into();
        let task1_slice1 = (1, 1).into();

        let mut st = SimpleScheduleStrategy::new(20);
        // add tasks and workers
        st.add_task_worker(task0_slice0, worker0).await;
        st.add_task_worker(task0_slice1, worker0).await;
        st.add_task_worker(task1_slice0, worker0).await;
        st.add_task_worker(task1_slice1, worker0).await;
        st.add_task_worker(task0_slice0, worker1).await;
        st.add_task_worker(task0_slice1, worker1).await;
        st.add_task_worker(task1_slice0, worker1).await;
        st.add_task_worker(task1_slice1, worker1).await;

        // update workers metric
        st.update_worker_metric(worker0, WorkerMetric::Pressure(10));
        st.update_worker_metric(worker0, WorkerMetric::Latency(Duration::from_millis(100)));

        st.update_worker_metric(worker1, WorkerMetric::Pressure(15));

        st.update_self_metric(WorkerMetric::Pressure(1));

        let selected0 = st.select_best_worker(task0_slice0).await;
        assert_eq!(selected0.as_ref().unwrap().worker_id().id(), 1); // 选择1, 没有延迟认为latency为0

        let selected1 = st.select_best_worker(task0_slice0).await;
        assert_eq!(selected1.is_some(), false); // 选择失败，因为已经被选择

        report_failure(selected0.unwrap()).await; // 上报失败

        //update latency
        st.update_worker_metric(worker1, WorkerMetric::Latency(Duration::from_millis(200)));


        let selected0 = st.select_best_worker(task0_slice1).await;
        assert_eq!(selected0.as_ref().unwrap().worker_id().id(), 0); // 选择0, 选择延迟小的

        report_success(selected0.unwrap()).await;

        // update pressure
        st.update_worker_metric(worker0, WorkerMetric::Pressure(16));

        let selected0 = st.select_best_worker(task0_slice0).await;
        assert_eq!(selected0.as_ref().unwrap().worker_id().id(), 0); // 选择1, 延迟低
        report_failure(selected0.unwrap()).await;

        // update worker1 pressure to max pressure
        st.update_worker_metric(worker0, WorkerMetric::Pressure(25));
        st.update_worker_metric(worker1, WorkerMetric::Pressure(23));

        let selected0 = st.select_best_worker(task1_slice0).await;
        assert_eq!(selected0.is_some(), false); // 都选择失败，因为压力都过大

        // remove worker
        st.rmv_worker(worker0).await;
        st.rmv_worker(worker1).await;
    }
    #[tokio::main]
    #[test]
    async fn test_select_best_task() {
        let worker0 = 0.into();
        let worker1 = 1.into();
        let task0_slice0 = (0, 0).into();
        let task1_slice0 = (1, 0).into();
        let task1_slice1 = (1, 1).into();
        let task1_slice2 = (1, 2).into();

        let mut st = SimpleScheduleStrategy::new(20);
        // add tasks and workers
        st.add_task_worker(task0_slice0, worker0).await;
        st.add_task_worker(task1_slice0, worker0).await;
        st.add_task_worker(task1_slice1, worker0).await;
        st.add_task_worker(task0_slice0, worker1).await;
        st.add_task_worker(task1_slice0, worker1).await;
        st.add_task_worker(task1_slice1, worker1).await;
        st.add_task_worker(task1_slice2, worker1).await;
        

        // update workers metric
        st.update_worker_metric(worker0, WorkerMetric::Pressure(10));
        st.update_worker_metric(worker0, WorkerMetric::Latency(Duration::from_millis(100)));

        st.update_worker_metric(worker1, WorkerMetric::Pressure(15));
        st.update_worker_metric(worker0, WorkerMetric::Latency(Duration::from_millis(80)));

        st.update_self_metric(WorkerMetric::Pressure(1));

        let selected = st.select_best_task_slice(worker1).await;
        assert_eq!(selected.as_ref().unwrap().task_slice_id(), task0_slice0);

        // add task0 4 slice
        st.add_task_worker((0, 1).into(), worker1).await;
        st.add_task_worker((0, 2).into(), worker1).await;
        st.add_task_worker((0, 3).into(), worker1).await;
        st.add_task_worker((0, 4).into(), worker1).await;

        report_success(selected.unwrap()).await;

        let selected = st.select_best_task_slice(worker1).await;
        // select task1 because task1 has 3 slices, task0 has 4 slices incompleted
        assert_eq!(selected.as_ref().unwrap().task_slice_id().task_id(), 1.into());

        report_success(selected.unwrap()).await;

        let selected = st.select_best_task_slice(worker1).await;
        // select task1 because task1 has 2 slices, task0 has 4 slices incompleted
        assert_eq!(selected.as_ref().unwrap().task_slice_id().task_id(), 1.into());

        report_success(selected.unwrap()).await;


        let selected = st.select_best_task_slice(worker1).await;
        // select task1 because task1 has 1 slices, task0 has 4 slices incompleted
        assert_eq!(selected.as_ref().unwrap().task_slice_id().task_id(), 1.into());

        report_success(selected.unwrap()).await;

        let selected = st.select_best_task_slice(worker1).await;
        // select task0 has 4 slices incompleted, task1 all completed
        assert_eq!(selected.as_ref().unwrap().task_slice_id().task_id(), 0.into());

        report_success(selected.unwrap()).await;

        let selected = st.select_best_task_slice(worker1).await;
        // only task0
        assert_eq!(selected.as_ref().unwrap().task_slice_id().task_id(), 0.into());

        report_success(selected.unwrap()).await;

        let selected = st.select_best_task_slice(worker1).await;
        // only task0
        assert_eq!(selected.as_ref().unwrap().task_slice_id().task_id(), 0.into());

        report_success(selected.unwrap()).await;


        let selected = st.select_best_task_slice(worker1).await;
        // only task0
        assert_eq!(selected.as_ref().unwrap().task_slice_id().task_id(), 0.into());

        report_success(selected.unwrap()).await;

        let selected = st.select_best_task_slice(worker1).await;
        // no task
        assert_eq!(selected.is_some(), false);
        
        // remove worker
        st.rmv_worker(worker0).await;
        st.rmv_worker(worker1).await;
    }
}
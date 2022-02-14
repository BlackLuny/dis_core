use std::{sync::Arc};
use tokio::sync::{Mutex, mpsc};
use zookeeper_async::{ZooKeeper, ZooKeeperExt, Stat, ZkState, ZkResult, WatchedEvent, WatchedEventType, ZkError};

pub struct NodeCache {
    path: Arc<String>,
    zk: Arc<ZooKeeper>,
    data: Arc<Mutex<Option<Data>>>,
    channel: Option<mpsc::UnboundedSender<Operation>>,
}

/// Data contents of a znode and associated `Stat`.
pub type Data = Arc<(Vec<u8>, Stat)>;

#[allow(dead_code)]
#[derive(Debug)]
enum Operation {
    Initialize,
    Shutdown,
    Refresh(),
    Event(NodeCacheEvent),
    ZkStateEvent(ZkState),
}

#[derive(Debug)]
pub enum NodeCacheEvent {
    Initialized(Data),
    NodeDeleted,
    DataUpdated(Data),
}

impl NodeCache {
    pub async fn new(zk: Arc<ZooKeeper>, path: &str) -> ZkResult<Self> {
        let data = Arc::new(Mutex::new(None));

        zk.ensure_path(path).await?;

        Ok(Self {
            path: Arc::new(path.to_owned()),
            zk,
            data,
            channel: None,
        })
    }

    async fn handle_operation(
        op: Operation,
        zk: Arc<ZooKeeper>,
        path: &str,
        data: Arc<Mutex<Option<Data>>>,
        event_listener: Arc<Mutex<impl FnMut(NodeCacheEvent)>>,
        ops_chan_tx: mpsc::UnboundedSender<Operation>,
    ) -> bool {
        let mut done = false;
        match op {
            Operation::Initialize => {
                debug!("initialising...");
                let result = Self::get_data(
                    zk.clone(),
                    path,
                    data.clone(),
                    ops_chan_tx.clone()
                ).await;
                debug!("got children {:?}", result);

                match result {
                    Ok(d) => {
                        let d = Arc::new(d);
                        *data.lock().await = Some(d.clone());
                        event_listener.lock().await(NodeCacheEvent::Initialized(d));
                    }
                    Err(_) => {
                        *data.lock().await = None;
                    }
                }
                
            }
            Operation::Shutdown => {
                debug!("shutting down worker thread");
                done = true;
            }
            Operation::Refresh() => {
                debug!("getting children");
                let result = Self::get_data(zk.clone(), path, data.clone(), ops_chan_tx.clone()).await;
                match result {
                    Ok(d) => {
                        let d = Arc::new(d);
                        *data.lock().await = Some(d.clone());
                        event_listener.lock().await(NodeCacheEvent::DataUpdated(d));
                    }
                    Err(_) => {
                        *data.lock().await = None;
                    }
                }
            }
            Operation::Event(event) => {
                debug!("received event {:?}", event);
                event_listener.lock().await(event);
            }
            Operation::ZkStateEvent(state) => {
                done = Self::handle_state_change(state, ops_chan_tx.clone());
            }
        }

        done
    }

    /// Return the current data. There are no guarantees of accuracy. This is merely the most recent
    /// view of the data.
    pub async fn get_current_data(&self) -> Option<Data> {
        self.data.lock().await.clone()
    }

    fn handle_state_change(state: ZkState, ops_chan_tx: mpsc::UnboundedSender<Operation>) -> bool {
        let mut done = false;

        debug!("zk state change {:?}", state);
        if let ZkState::Connected = state {
            if let Err(err) = ops_chan_tx.send(Operation::Refresh())
            {
                warn!("error sending Refresh to op channel: {:?}", err);
                done = true;
            }
        }

        done
    }
    /// Start the cache. The cache is not started automatically. You must call this method.
    pub fn start(&mut self, mut event_listener: impl FnMut(NodeCacheEvent) + 'static + Send + Sync) -> ZkResult<()> {
        let (ops_chan_tx, mut ops_chan_rx) = mpsc::unbounded_channel();
        let ops_chan_rx_zk_events = ops_chan_tx.clone();

        let sub = self.zk.add_listener(move |s| {
            ops_chan_rx_zk_events
                .send(Operation::ZkStateEvent(s))
                .unwrap()
        });

        let zk = self.zk.clone();
        let path = self.path.clone();
        let data = self.data.clone();
        self.channel = Some(ops_chan_tx.clone());

        tokio::spawn(async move {
            let mut done = false;
            let event_listener = Arc::new(Mutex::new(event_listener));
            while !done {
                match ops_chan_rx.recv().await {
                    Some(operation) => {
                        done = Self::handle_operation(
                            operation,
                            zk.clone(),
                            &*path,
                            data.clone(),
                            event_listener.clone(),
                            ops_chan_tx.clone(),
                        )
                        .await;
                    }
                    None => {
                        info!("error receiving from operations channel. shutting down");
                        done = true;
                    }
                }
            }
        });

        self.offer_operation(Operation::Initialize)
    }

    fn offer_operation(&self, op: Operation) -> ZkResult<()> {
        match self.channel {
            Some(ref chan) => chan.send(op).map_err(|err| {
                warn!("error submitting op to channel: {:?}", err);
                ZkError::APIError
            }),
            None => Err(ZkError::APIError),
        }
    }

    async fn get_data(
        zk: Arc<ZooKeeper>,
        path: &str,
        data: Arc<Mutex<Option<Data>>>,
        ops_chan: mpsc::UnboundedSender<Operation>,
    ) -> ZkResult<(Vec<u8>, Stat)> {
        let path1 = path.to_owned();

        let data_watcher = move |event: WatchedEvent| {
            let data = data.clone();
            let ops_chan = ops_chan.clone();
            let path1 = path1.clone();

            tokio::spawn(async move {
                let mut data_locked = data.lock().await;
                match event.event_type {
                    WatchedEventType::NodeDeleted => {
                        *data_locked = None;

                        if let Err(err) = ops_chan.send(Operation::Event(
                            NodeCacheEvent::NodeDeleted,
                        )) {
                            warn!("error sending ChildRemoved event: {:?}", err);
                        }
                    }
                    WatchedEventType::NodeDataChanged => {
                        // Subscribe to new changes recursively
                        if let Err(err) = ops_chan.send(Operation::Refresh()) {
                            warn!("error sending GetData to op channel: {:?}", err);
                        }
                    }
                    _ => error!("Unexpected: {:?}", event),
                };

                trace!("New data: {:?}", *data_locked);
            });
        };

        zk.get_data_w(path, data_watcher).await
    }
}



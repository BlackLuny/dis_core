use std::{sync::Arc, collections::HashMap};
use log::{debug, trace, warn, info};
use tokio::sync::{Mutex, mpsc};
use zookeeper_async::{ZooKeeper, ZooKeeperExt, recipes::cache::PathChildrenCacheEvent, Stat, ZkState, ZkResult, WatchedEvent, WatchedEventType, ZkError, Subscription};

/// Combine two paths into a single path, possibly inserting a '/' between them.
pub fn make_path(parent: &str, child: &str) -> String {
    if parent.ends_with('/') {
        format!("{}{}", parent, child)
    } else {
        format!("{}/{}", parent, child)
    }
}
pub struct PullPathCache {
    path: Arc<String>,
    zk: Arc<ZooKeeper>,
    data: Arc<Mutex<Data>>,
    channel: Option<mpsc::UnboundedSender<Operation>>,
    listener_subscription: Option<Subscription>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum RefreshMode {
    Standard,
    ForceGetDataAndStat,
}

/// Data contents of a znode and associated `Stat`.
pub type ChildData = Arc<(Vec<u8>, Stat)>;

/// Data for all known children of a znode.
pub type Data = HashMap<String, ChildData>;

#[allow(dead_code)]
#[derive(Debug)]
enum Operation {
    Initialize,
    Shutdown,
    Refresh(RefreshMode),
    Event(PathChildrenCacheEvent),
    GetData(String /* path */),
    ZkStateEvent(ZkState),
}

impl PullPathCache {
    pub async fn new(zk: Arc<ZooKeeper>, path: &str) -> ZkResult<Self> {
        let data = Arc::new(Mutex::new(HashMap::new()));

        zk.ensure_path(path).await?;

        Ok(Self {
            path: Arc::new(path.to_owned()),
            zk,
            data,
            channel: None,
            listener_subscription: None,
        })
    }

    async fn handle_operation(
        op: Operation,
        zk: Arc<ZooKeeper>,
        path: Arc<String>,
        data: Arc<Mutex<Data>>,
        mut event_listener: Arc<Mutex<impl FnMut(PathChildrenCacheEvent)>>,
        ops_chan_tx: mpsc::UnboundedSender<Operation>,
    ) -> bool {
        let mut done = false;
        match op {
            Operation::Initialize => {
                debug!("initialising...");
                let result = tokio::spawn(Self::get_children(
                    zk.clone(),
                    path,
                    data.clone(),
                    ops_chan_tx.clone(),
                    RefreshMode::ForceGetDataAndStat,
                ));
                debug!("got children {:?}", result);

                // event_listener(&PathChildrenCacheEvent::Initialized(
                //     data.lock().await.clone(),
                // ));
            }
            Operation::Shutdown => {
                // debug!("shutting down worker thread");
                // done = true;
            }
            Operation::Refresh(mode) => {
                debug!("getting children");
                let result = tokio::spawn(Self::get_children(zk.clone(), path, data.clone(), ops_chan_tx.clone(), mode)
                );
                    
                debug!("got children {:?}", result);
            }
            Operation::GetData(path) => {
                // debug!("getting data");
                // let result =
                //     Self::update_data(zk.clone(), &*path, data.clone(), ops_chan_tx.clone()).await;
                // if let Err(err) = result {
                //     warn!("error getting child data: {:?}", err);
                // }
            }
            Operation::Event(event) => {
                // debug!("received event {:?}", event);
                // event_listener.lock().await(event);
            }
            Operation::ZkStateEvent(state) => {
                //done = Self::handle_state_change(state, ops_chan_tx.clone());
            }
        }

        done
    }

    /// Start the cache. The cache is not started automatically. You must call this method.
    pub fn start(&mut self, mut event_listener: impl FnMut(PathChildrenCacheEvent) + 'static + Send + Sync) -> ZkResult<()> {
        let (ops_chan_tx, mut ops_chan_rx) = mpsc::unbounded_channel();
        let ops_chan_rx_zk_events = ops_chan_tx.clone();

        let sub = self.zk.add_listener(move |s| {
            ops_chan_rx_zk_events
                .send(Operation::ZkStateEvent(s))
                .unwrap()
        });
        self.listener_subscription = Some(sub);

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
                            path.clone(),
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
    async fn get_children(
        zk: Arc<ZooKeeper>,
        path: Arc<String>,
        data: Arc<Mutex<Data>>,
        ops_chan: mpsc::UnboundedSender<Operation>,
        mode: RefreshMode,
    ) -> ZkResult<()> {
        let path = path.as_str();
        let ops_chan1 = ops_chan.clone();

        let watcher = move |event: WatchedEvent| {
            match event.event_type {
                WatchedEventType::NodeChildrenChanged => {
                    let _path = event.path.as_ref().expect("Path absent");

                    // Subscribe to new changes recursively
                    if let Err(err) = ops_chan1.send(Operation::Refresh(RefreshMode::Standard)) {
                        warn!("error sending Refresh operation to ops channel: {:?}", err);
                    }
                }
                _ => panic!("Unexpected: {:?}", event),
            };
        };
        //tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        let children = zk.get_children_w(path, watcher).await?;

        // let mut data_locked = data.lock().await;

        // for child in &children {
        //     let child_path = make_path(path, child);

        //     if mode == RefreshMode::ForceGetDataAndStat || !data_locked.contains_key(&child_path) {
        //         let child_data = Arc::new(
        //             Self::get_data(zk.clone(), &child_path, data.clone(), ops_chan.clone()).await?,
        //         );

        //         data_locked.insert(child_path.clone(), child_data.clone());

        //         // ops_chan
        //         //     .send(Operation::Event(PathChildrenCacheEvent::ChildAdded(
        //         //         child_path, child_data,
        //         //     )))
        //         //     .map_err(|err| {
        //         //         info!("error sending ChildAdded event: {:?}", err);
        //         //         ZkError::APIError
        //         //     })?;
        //     }
        // }

        // trace!("New data: {:?}", *data_locked);

        Ok(())
    }

    async fn get_data(
        zk: Arc<ZooKeeper>,
        path: &str,
        data: Arc<Mutex<Data>>,
        ops_chan: mpsc::UnboundedSender<Operation>,
    ) -> ZkResult<(Vec<u8>, Stat)> {
        // let path1 = path.to_owned();

        // let data_watcher = move |event: WatchedEvent| {
        //     let data = data.clone();
        //     let ops_chan = ops_chan.clone();
        //     let path1 = path1.clone();

        //     tokio::spawn(async move {
        //         let mut data_locked = data.lock().await;
        //         match event.event_type {
        //             WatchedEventType::NodeDeleted => {
        //                 data_locked.remove(&path1);

        //                 if let Err(err) = ops_chan.send(Operation::Event(
        //                     PathChildrenCacheEvent::ChildRemoved(path1.clone()),
        //                 )) {
        //                     warn!("error sending ChildRemoved event: {:?}", err);
        //                 }
        //             }
        //             WatchedEventType::NodeDataChanged => {
        //                 // Subscribe to new changes recursively
        //                 if let Err(err) = ops_chan.send(Operation::GetData(path1.clone())) {
        //                     warn!("error sending GetData to op channel: {:?}", err);
        //                 }
        //             }
        //             _ => error!("Unexpected: {:?}", event),
        //         };

        //         trace!("New data: {:?}", *data_locked);
        //     });
        // };

        //zk.get_data_w(path, data_watcher).await
        zk.get_data(path, false).await
    }
}



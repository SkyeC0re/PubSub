use std::{
    collections::{HashMap, HashSet},
    ops::{AddAssign, DerefMut},
    sync::Arc,
};

use async_trait::async_trait;
use futures_util::{stream::SplitSink, SinkExt, StreamExt};

use log::{error, info, warn};

use std::future::Future;
use tokio::{
    net::{TcpListener, TcpStream},
    runtime::Runtime,
    sync::RwLock,
};
use tokio_tungstenite::{
    accept_async,
    tungstenite::{Error, Message},
    WebSocketStream,
};

type UniqId = u128;
type TagSetId = u32;
type Stream = TcpStream;

pub trait TagSets {
    fn tag_sets(&self) -> Vec<HashSet<String>>;
}

#[async_trait]
pub trait ClientCallback {
    async fn callback(&self, ws_client: &mut WebSocketClient, message: Message);
}

#[async_trait]
impl<F, Fut> ClientCallback for F
where
    F: (Fn(&mut WebSocketClient, Message) -> Fut) + Send + Sync + Clone + 'static,
    Fut: Future<Output = ()> + Send,
{
    async fn callback(&self, ws_client: &mut WebSocketClient, message: Message) {
        self(ws_client, message).await
    }
}

pub struct WebSocketManager<F> {
    next_id: Arc<RwLock<UniqId>>,
    web_socket_maps: Arc<RwLock<HashMap<UniqId, Arc<RwLock<WebSocketClient>>>>>,
    tag_maps: Arc<RwLock<HashMap<String, Arc<RwLock<HashSet<(UniqId, TagSetId)>>>>>>,
    client_message_callback: Arc<F>,
    tcp_runtime: Runtime,
    client_runtime: Runtime,
}

impl<F> WebSocketManager<F>
where
    F: ClientCallback + Send + Sync + 'static,
{
    pub async fn new(
        listener: TcpListener,
        client_message_callback: F,
    ) -> Result<Arc<RwLock<Self>>, Error> {
        let tcp_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()?;

        let client_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(8)
            .build()?;
        let manager = Self {
            next_id: Default::default(),
            web_socket_maps: Default::default(),
            tag_maps: Default::default(),
            client_message_callback: Arc::new(client_message_callback),
            tcp_runtime,
            client_runtime,
        };
        info!("Creating Manager");

        let locked_manager = Arc::new(RwLock::new(manager));
        let locked_manager_clone = locked_manager.clone();
        locked_manager.read().await.tcp_runtime.spawn(async move {
            while let Ok((stream, addr)) = listener.accept().await {
                let ws = match accept_async(stream).await {
                    Ok(ws) => ws,
                    Err(_) => {
                        warn!("Invalid websocket handshake from: {}", addr);
                        continue;
                    }
                };

                locked_manager_clone
                    .read()
                    .await
                    .insert_new_client(ws)
                    .await;
            }
        });
        Ok(locked_manager)
    }
    /// Find all websockets that are authorized to receive a message.
    pub async fn find_authorized_clients(
        tag_maps: Arc<RwLock<HashMap<String, Arc<RwLock<HashSet<(UniqId, TagSetId)>>>>>>,
        authorized_tag_sets: &Vec<HashSet<String>>,
    ) -> HashSet<UniqId> {
        let tag_maps = tag_maps.read().await;
        let mut candidate_set = HashSet::new();
        for authorized_tag_set in authorized_tag_sets {
            let mut tag_iter = authorized_tag_set.iter();
            if let Some(tag) = tag_iter.next() {
                let mut candidates = match tag_maps.get(tag) {
                    Some(candidates) => candidates.read().await.clone(),
                    None => HashSet::new(),
                };

                for tag in tag_iter {
                    if let Some(intersection_candidates) = tag_maps.get(tag) {
                        let intersection_candidates = intersection_candidates.read().await;
                        candidates.retain(|v| intersection_candidates.contains(v))
                    } else {
                        continue;
                    };
                }

                candidate_set.extend(candidates.iter().map(|(ws_id, _)| *ws_id));
            }
        }
        candidate_set
    }

    pub async fn insert_new_client(&self, ws: WebSocketStream<Stream>) {
        let mut next_id = self.next_id.write().await;
        let id = next_id.clone();
        next_id.add_assign(1);

        let (ws_send, ws_recv) = ws.split();
        let ws_client = WebSocketClient {
            id,
            next_tag_set_id: 0,
            ws_send,
            tag_set_permissions: HashMap::new(),
            web_socket_maps: self.web_socket_maps.clone(),
            tag_maps: self.tag_maps.clone(),
        };

        let ws_client = Arc::new(RwLock::new(ws_client));

        self.web_socket_maps
            .write()
            .await
            .insert(id, ws_client.clone());

        let client_message_callback = self.client_message_callback.clone();

        // Client listener
        self.client_runtime.spawn(async move {
            ws_recv
                .for_each(|res| async {
                    let message = match res {
                        Ok(message) => message,
                        Err(e) => {
                            warn!("Invalid message from client {:?}", e);
                            return;
                        }
                    };

                    client_message_callback
                        .callback(ws_client.write().await.deref_mut(), message)
                        .await;
                })
                .await;

            ws_client.write().await.remove_client().await;

            todo!()
        });
    }

    pub fn send_message(&self, tag_sets: Vec<HashSet<String>>, message: Message) {
        let tag_maps = self.tag_maps.clone();
        let web_socket_maps = self.web_socket_maps.clone();
        self.client_runtime.spawn(async move {
            let candidates = Self::find_authorized_clients(tag_maps, &tag_sets).await;
            let read_guard = web_socket_maps.read().await;
            for candidate in candidates {
                if let Some(candidate) = read_guard.get(&candidate) {
                    if let Err(e) = candidate.write().await.send_message(message.clone()).await {
                        error!("Error sending message to client: {:?}", e);
                    };
                }
            }
        });
    }
}

pub struct WebSocketClient {
    id: UniqId,
    next_tag_set_id: TagSetId,
    ws_send: SplitSink<WebSocketStream<Stream>, Message>,
    tag_set_permissions: HashMap<TagSetId, HashSet<String>>,
    web_socket_maps: Arc<RwLock<HashMap<UniqId, Arc<RwLock<WebSocketClient>>>>>,
    tag_maps: Arc<RwLock<HashMap<String, Arc<RwLock<HashSet<(UniqId, TagSetId)>>>>>>,
}

impl WebSocketClient {
    pub async fn add_tag_set_permission(&mut self, new_tag_set: HashSet<String>) {
        let mut subset_ids = Vec::new();
        for (&id, existing_tag_set) in &self.tag_set_permissions {
            // Websocket already contains at least the new set's permissions, no need to add
            if new_tag_set.is_subset(existing_tag_set) {
                return;
            }

            if new_tag_set.is_superset(existing_tag_set) {
                subset_ids.push(id);
            }
        }

        self.insert_tag_set(new_tag_set).await;
        for subset_tag_set_id in subset_ids {
            self.remove_tag_set(subset_tag_set_id).await;
        }
    }

    async fn insert_tag_set(&mut self, new_tag_set: HashSet<String>) {
        let new_id = self.next_tag_set_id;
        self.next_tag_set_id += 1;

        for tag in &new_tag_set {
            let tag_maps = self.tag_maps.read().await;
            let tag_map = tag_maps.get(tag);
            if let Some(tag_map) = tag_map {
                tag_map.write().await.insert((self.id, new_id));
            } else {
                drop(tag_maps);
                self.tag_maps
                    .write()
                    .await
                    .entry(tag.clone())
                    .or_default()
                    .write()
                    .await
                    .insert((self.id, new_id));
            }
        }

        self.tag_set_permissions.insert(new_id, new_tag_set);
    }

    async fn remove_tag_set(&mut self, tag_set_id: TagSetId) {
        if let Some(old_tag_set) = self.tag_set_permissions.remove(&tag_set_id) {
            for tag in &old_tag_set {
                let tag_maps = self.tag_maps.read().await;
                if let Some(id_set) = tag_maps.get(tag) {
                    id_set.write().await.remove(&(self.id, tag_set_id));
                }
            }
        }
    }

    pub async fn remove_client(&mut self) {
        self.web_socket_maps.write().await.remove(&self.id);
        let tag_set_ids = self.tag_set_permissions.keys().copied().collect::<Vec<_>>();
        for tag_set_id in tag_set_ids {
            self.remove_tag_set(tag_set_id).await;
        }
        self.tag_set_permissions = HashMap::new();
    }

    pub async fn send_message(&mut self, message: Message) -> Result<(), Error> {
        self.ws_send.send(message).await
    }
}

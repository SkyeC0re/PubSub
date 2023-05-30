use std::{
    collections::{HashMap, HashSet},
    ops::{AddAssign, Deref, DerefMut},
    sync::{Arc, Weak},
};

use async_trait::async_trait;
use futures_util::{stream::SplitSink, SinkExt, StreamExt};

use log::{error, info, warn};

use std::future::Future;
use tokio::{
    net::{TcpListener, TcpStream},
    runtime::Runtime,
    sync::{Mutex, RwLock},
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

pub enum TopicSpecifier {
    TopicAndSubtopics,
    OnlySubtopics,
    Subtopic {
        topic: String,
        specifier: Box<TopicSpecifier>,
    },
}

#[derive(Clone, Default)]
struct PruneGuardedTopicNode {
    topic_node: Arc<RwLock<TopicNode>>,
    prune_guard: Arc<RwLock<()>>,
}

#[derive(Default)]
struct TopicNode {
    pub topic_subscribers: HashMap<UniqId, WebSocketClient>,
    wildcard_subtopic_subscribers: HashMap<UniqId, WebSocketClient>,
    subtopics: HashMap<String, PruneGuardedTopicNode>,
    parent: Option<PruneGuardedTopicNode>,
    topic: String,
}

impl TopicNode {
    pub fn is_empty_leaf(&self) -> bool {
        self.topic_subscribers.is_empty()
            && self.wildcard_subtopic_subscribers.is_empty()
            && self.subtopics.is_empty()
    }
}

#[derive(Clone, Default)]
pub struct TopicTree {
    tree: PruneGuardedTopicNode,
}

impl TopicTree {
    pub async fn find_subscribers(
        &self,
        topic: &TopicSpecifier,
        collector: &mut HashMap<UniqId, WebSocketClient>,
    ) {
        let mut current_branch = self.tree.clone();
        let mut current_topic = topic;

        loop {
            match current_topic {
                TopicSpecifier::TopicAndSubtopics => {
                    Self::collect_all_subscribers(vec![current_branch], collector).await;
                    break;
                }
                TopicSpecifier::OnlySubtopics => {
                    let current_branch = current_branch.topic_node.read().await;
                    for (id, client) in &current_branch.wildcard_subtopic_subscribers {
                        collector.insert(*id, client.clone());
                    }
                    Self::collect_all_subscribers(
                        current_branch.subtopics.values().cloned().collect(),
                        collector,
                    )
                    .await;
                    break;
                }
                TopicSpecifier::Subtopic { topic, specifier } => {
                    let current_branch_read_guard = current_branch.topic_node.read().await;
                    if let Some(subtopic) = current_branch_read_guard.subtopics.get(topic).cloned()
                    {
                        drop(current_branch_read_guard);
                        current_branch = subtopic;
                        current_topic = specifier;
                    } else {
                        break;
                    }
                }
            }
        }
    }

    async fn collect_all_subscribers(
        trees: Vec<PruneGuardedTopicNode>,
        collector: &mut HashMap<UniqId, WebSocketClient>,
    ) {
        let mut unvisited_branches = trees;
        while let Some(branch) = unvisited_branches.pop() {
            let branch = branch.topic_node.read().await;
            for (id, client) in &branch.topic_subscribers {
                collector.insert(*id, client.clone());
            }
            for (id, client) in &branch.wildcard_subtopic_subscribers {
                collector.insert(*id, client.clone());
            }
            unvisited_branches.extend(branch.subtopics.values().cloned());
        }
    }

    async fn subscribe_to_topic(&self, client: WebSocketClient, topic: &TopicSpecifier) {
        let client_id = client.id;
        let mut prune_guards = Vec::new();
        let mut next_branch = self.tree.clone();
        let mut next_topic = topic;

        loop {
            match next_topic {
                TopicSpecifier::TopicAndSubtopics => {
                    next_branch
                        .topic_node
                        .write()
                        .await
                        .topic_subscribers
                        .insert(client_id, client);
                    break;
                }
                TopicSpecifier::OnlySubtopics => {
                    next_branch
                        .topic_node
                        .write()
                        .await
                        .wildcard_subtopic_subscribers
                        .insert(client_id, client);
                    break;
                }
                TopicSpecifier::Subtopic { topic, specifier } => {
                    next_topic = specifier;
                    let current_branch_read_guard = next_branch.topic_node.read().await;

                    if let Some((subtopic_tree, prune_guard)) = current_branch_read_guard
                        .subtopics
                        .get(topic)
                        .cloned()
                        .and_then(|subtopic_tree| {
                            if let Ok(prune_guard) =
                                subtopic_tree.prune_guard.clone().try_read_owned()
                            {
                                Some((subtopic_tree, prune_guard))
                            } else {
                                None
                            }
                        })
                    {
                        drop(current_branch_read_guard);
                        // Ensure that lower branches cannot be pruned during insertion process
                        prune_guards.push(prune_guard);
                        next_branch = subtopic_tree;
                        continue;
                    }
                    drop(current_branch_read_guard);

                    // Branch did not exist previously, aquire write lock and create if still required

                    let mut current_branch_write_guard = next_branch.topic_node.write().await;
                    if let Some((subtopic_tree, prune_guard)) = current_branch_write_guard
                        .subtopics
                        .get(topic)
                        .cloned()
                        .and_then(|subtopic_tree| {
                            if let Ok(prune_guard) =
                                subtopic_tree.prune_guard.clone().try_read_owned()
                            {
                                Some((subtopic_tree, prune_guard))
                            } else {
                                None
                            }
                        })
                    {
                        drop(current_branch_write_guard);
                        // Ensure that lower branches cannot be pruned during insertion process
                        prune_guards.push(prune_guard);
                        next_branch = subtopic_tree;
                        continue;
                    }

                    let subtopic_tree = PruneGuardedTopicNode {
                        topic_node: Arc::new(RwLock::new(TopicNode {
                            parent: Some(next_branch.clone()),
                            ..Default::default()
                        })),
                        prune_guard: Default::default(),
                    };

                    prune_guards.push(subtopic_tree.prune_guard.clone().read_owned().await);

                    current_branch_write_guard
                        .subtopics
                        .insert(topic.clone(), subtopic_tree.clone());
                    drop(current_branch_write_guard);
                    next_branch = subtopic_tree;
                }
            }
        }
    }

    async fn unsubscribe_from_topic(&self, client: WebSocketClient, topic: &TopicSpecifier) {
        
    }

    /// Attempt to prune empty branches of a topic, starting at the furthermost branch and working back to the root.
    /// Here all terminal topic specifiers are treated as equal and the function will attempt pruning from that branch.
    async fn prune_branch(branch: PruneGuardedTopicNode) {
        let mut current_branch = branch;
        loop {
            let prune_guard = if let Ok(prune_guard) = current_branch.prune_guard.try_write_owned()
            {
                prune_guard
            } else {
                return;
            };
            let branch = current_branch.topic_node.read().await;

            if !branch.is_empty_leaf() {
                return;
            }

            let parent_branch = if let Some(parent_branch) = branch.parent.clone() {
                parent_branch
            } else {
                return;
            };

            parent_branch
                .topic_node
                .write()
                .await
                .subtopics
                .remove(&branch.topic);

            drop(branch);
            current_branch = parent_branch;
        }
    }
}

pub struct WebSocketManager<F> {
    next_id: Arc<RwLock<UniqId>>,
    web_socket_maps: Arc<RwLock<HashMap<UniqId, Arc<RwLock<WebSocketClient>>>>>,
    topic_tree: TopicTree,
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
    ) -> Result<Arc<Self>, Error> {
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
            topic_tree: Default::default(),
            tag_maps: Default::default(),
            client_message_callback: Arc::new(client_message_callback),
            tcp_runtime,
            client_runtime,
        };
        info!("Creating Manager");

        let locked_manager = Arc::new(manager);
        let locked_manager_clone = locked_manager.clone();
        locked_manager.tcp_runtime.spawn(async move {
            while let Ok((stream, addr)) = listener.accept().await {
                let ws = match accept_async(stream).await {
                    Ok(ws) => ws,
                    Err(_) => {
                        warn!("Invalid websocket handshake from: {}", addr);
                        continue;
                    }
                };

                locked_manager_clone.insert_new_client(ws).await;
            }
        });
        Ok(locked_manager)
    }
    /// Find all websockets that are authorized to receive a message.
    pub async fn find_authorized_clients(
        topic_tree: &TopicTree,
        topics: impl IntoIterator<Item = &TopicSpecifier>,
    ) -> HashMap<UniqId, WebSocketClient> {
        let mut candidate_set = HashMap::new();
        for topic in topics {
            topic_tree.find_subscribers(topic, &mut candidate_set).await;
        }
        candidate_set
    }

    pub async fn insert_new_client(&self, ws: WebSocketStream<Stream>) {
        let mut next_id = self.next_id.write().await;
        let id = next_id.clone();
        next_id.add_assign(1);

        let (ws_send, ws_recv) = ws.split();
        let ws_client = WebSocketClient {
            inner: Arc::new(WebSocketClientInner {
                id,
                ws_send: Mutex::new(ws_send),
                topic_tree: self.topic_tree.clone(),
                subscribed_topics: Default::default(),
            }),
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
        });
    }

    pub fn send_message(&self, topics: Vec<TopicSpecifier>, message: Message) {
        let topic_tree = self.topic_tree.clone();
        let web_socket_maps = self.web_socket_maps.clone();
        self.client_runtime.spawn(async move {
            let candidates = Self::find_authorized_clients(&topic_tree, &topics).await;
            for candidate in candidates.into_values() {
                if let Err(e) = candidate.send_message(message.clone()).await {
                    error!("Error sending message to client: {:?}", e);
                };
            }
        });
    }
}

#[derive(Clone)]
pub struct WebSocketClient {
    inner: Arc<WebSocketClientInner>,
}

impl Deref for WebSocketClient {
    type Target = WebSocketClientInner;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

pub struct WebSocketClientInner {
    id: UniqId,
    ws_send: Mutex<SplitSink<WebSocketStream<Stream>, Message>>,
    subscribed_topics: RwLock<HashSet<TopicSpecifier>>,
    topic_tree: TopicTree,
}

impl WebSocketClient {
    pub async fn subscribe_to_topic(&self, topic: TopicSpecifier) {
        self.topic_tree.subscribe_to_topic(self.clone(), &topic).await
    }

    pub async fn remove_client(&mut self) {
        self.web_socket_maps.write().await.remove(&self.id);
        let tag_set_ids = self.tag_set_permissions.keys().copied().collect::<Vec<_>>();
        for tag_set_id in tag_set_ids {
            self.remove_tag_set(tag_set_id).await;
        }
        self.tag_set_permissions = HashMap::new();
    }

    pub async fn send_message(&self, message: Message) -> Result<(), Error> {
        self.ws_send.lock().await.send(message).await
    }
}

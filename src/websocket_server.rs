use std::{
    collections::{HashMap, HashSet},
    ops::{AddAssign, Deref},
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
type Stream = TcpStream;
type TopicTreeNode<V> = Arc<RwLock<TopicNode<V>>>;
type WeakTopicTreeNode<V> = Weak<RwLock<TopicNode<V>>>;
pub trait TagSets {
    fn tag_sets(&self) -> Vec<HashSet<String>>;
}

#[async_trait]
pub trait ClientCallback<M, E> {
    async fn callback(&self, client: &dyn Client<M, E>, message: M);
}

#[async_trait]
pub trait Client<M, E>: Send + Sync {
    async fn send_message(&self, message: M) -> Result<(), E>;
}

#[async_trait]
impl<M, E, F, Fut> ClientCallback<M, E> for F
where
    F: (Fn(&dyn Client<M, E>, M) -> Fut) + Send + Sync + Clone + 'static,
    Fut: Future<Output = ()> + Send,
    M: Send + Sync + 'static,
{
    async fn callback(&self, client: &dyn Client<M, E>, message: M) {
        self(client, message).await;
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

#[derive(Default)]
struct TopicNode<V> {
    topic_subscribers: HashMap<UniqId, V>,
    wildcard_subtopic_subscribers: HashMap<UniqId, V>,
    subtopics: HashMap<String, TopicTreeNode<V>>,
    parent: Option<WeakTopicTreeNode<V>>,
    topic: String,
}

impl<V> TopicNode<V> {
    pub fn new() -> Self {
        Self {
            topic_subscribers: HashMap::new(),
            wildcard_subtopic_subscribers: HashMap::new(),
            subtopics: HashMap::new(),
            parent: None,
            topic: "".to_string(),
        }
    }
}

impl<V> TopicNode<V> {
    pub fn is_empty_leaf(&self) -> bool {
        self.topic_subscribers.is_empty()
            && self.wildcard_subtopic_subscribers.is_empty()
            && self.subtopics.is_empty()
    }
}

#[derive(Clone)]
pub struct TopicTree<V> {
    next_id: Arc<Mutex<UniqId>>,
    tree: TopicTreeNode<V>,
}

impl<V: Clone> TopicTree<V> {
    pub fn new() -> Self {
        Self {
            next_id: Arc::default(),
            tree: Arc::new(RwLock::new(TopicNode::new())),
        }
    }

    pub async fn next_id(&self) -> UniqId {
        let mut next_id = self.next_id.lock().await;
        let id = next_id.clone();
        next_id.add_assign(&1);
        id
    }

    pub async fn find_subscribers(
        &self,
        topic: &TopicSpecifier,
        collector: &mut HashMap<UniqId, V>,
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
                    let current_branch = current_branch.read().await;
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
                    let current_branch_read_guard = current_branch.read().await;
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
        trees: Vec<TopicTreeNode<V>>,
        collector: &mut HashMap<UniqId, V>,
    ) {
        let mut unvisited_branches = trees;
        while let Some(branch) = unvisited_branches.pop() {
            let branch = branch.read().await;
            for (id, client) in &branch.topic_subscribers {
                collector.insert(*id, client.clone());
            }
            for (id, client) in &branch.wildcard_subtopic_subscribers {
                collector.insert(*id, client.clone());
            }
            unvisited_branches.extend(branch.subtopics.values().cloned());
        }
    }

    async fn subscribe_to_topic(&self, client: V, client_id: UniqId, topic: &TopicSpecifier) {
        let mut current_branch = self.tree.clone();
        let mut current_topic = topic;

        loop {
            match current_topic {
                TopicSpecifier::TopicAndSubtopics => {
                    current_branch
                        .write()
                        .await
                        .topic_subscribers
                        .insert(client_id, client);
                    break;
                }
                TopicSpecifier::OnlySubtopics => {
                    current_branch
                        .write()
                        .await
                        .wildcard_subtopic_subscribers
                        .insert(client_id, client);
                    break;
                }
                TopicSpecifier::Subtopic { topic, specifier } => {
                    current_topic = specifier;
                    let current_branch_read_guard = current_branch.read().await;

                    if let Some(subtopic_tree) =
                        current_branch_read_guard.subtopics.get(topic).cloned()
                    {
                        drop(current_branch_read_guard);
                        current_branch = subtopic_tree;
                        continue;
                    }
                    drop(current_branch_read_guard);

                    // Branch did not exist previously, aquire write lock and create if still required

                    let mut current_branch_write_guard = current_branch.write().await;
                    if let Some(subtopic_tree) =
                        current_branch_write_guard.subtopics.get(topic).cloned()
                    {
                        drop(current_branch_write_guard);
                        current_branch = subtopic_tree;
                        continue;
                    }

                    let subtopic_leaf = Arc::new(RwLock::new(TopicNode {
                        parent: Some(Arc::downgrade(&current_branch)),
                        topic: topic.clone(),
                        topic_subscribers: HashMap::new(),
                        wildcard_subtopic_subscribers: HashMap::new(),
                        subtopics: HashMap::new(),
                    }));

                    current_branch_write_guard
                        .subtopics
                        .insert(topic.clone(), subtopic_leaf.clone());
                    drop(current_branch_write_guard);
                    current_branch = subtopic_leaf;
                }
            }
        }
    }

    async fn unsubscribe_from_topic(
        &self,
        client_id: &UniqId,
        topic: &TopicSpecifier,
    ) -> Option<TopicTreeNode<V>> {
        let mut current_branch = self.tree.clone();
        let mut current_topic = topic;

        loop {
            match current_topic {
                TopicSpecifier::TopicAndSubtopics => {
                    current_branch
                        .write()
                        .await
                        .topic_subscribers
                        .remove(client_id);
                    break;
                }
                TopicSpecifier::OnlySubtopics => {
                    current_branch
                        .write()
                        .await
                        .wildcard_subtopic_subscribers
                        .remove(client_id);
                    break;
                }
                TopicSpecifier::Subtopic { topic, specifier } => {
                    current_topic = specifier;
                    let current_branch_read_guard = current_branch.read().await;

                    if let Some(subtopic_tree) =
                        current_branch_read_guard.subtopics.get(topic).cloned()
                    {
                        drop(current_branch_read_guard);
                        current_branch = subtopic_tree;
                    } else {
                        // Nothing to do, client was never on that topic
                        return None;
                    }
                }
            }
        }

        return Some(current_branch);
    }

    /// Attempt to prune empty branches of a topic, starting at the furthermost branch and working back to the root.
    async fn prune_leaf(leaf: TopicTreeNode<V>) {
        let mut owned_read_guard = leaf.read_owned().await;
        while owned_read_guard.is_empty_leaf() {
            let parent_branch = if let Some(parent_branch) = owned_read_guard
                .parent
                .as_ref()
                .and_then(|parent| parent.upgrade())
            {
                parent_branch
            } else {
                // Reached root or has already been pruned
                return;
            };

            let sub_topic = owned_read_guard.topic.clone();

            drop(owned_read_guard);
            let mut parent_branch_write_guard = parent_branch.write_owned().await;

            let child_branch =
                if let Some(child_branch) = parent_branch_write_guard.subtopics.get(&sub_topic) {
                    child_branch
                } else {
                    // Another prune operation has already removed the branch
                    return;
                };

            if child_branch.read().await.is_empty_leaf() {
                parent_branch_write_guard.subtopics.remove(&sub_topic);
            } else {
                // The child branch has been populated in the meantime and is no longer empty
                return;
            }

            owned_read_guard = parent_branch_write_guard.downgrade();
        }
    }
}

pub struct WebSocketManager<F> {
    topic_tree: TopicTree<WebSocketClient<F>>,
    client_message_callback: F,
    tcp_runtime: Runtime,
    client_runtime: Runtime,
}

impl<F> WebSocketManager<F>
where
    F: ClientCallback<Message, Error> + Send + Sync + Clone + 'static,
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
            topic_tree: TopicTree::new(),
            client_message_callback,
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
        topic_tree: &TopicTree<WebSocketClient<F>>,
        topics: impl IntoIterator<Item = &TopicSpecifier>,
    ) -> HashMap<UniqId, WebSocketClient<F>> {
        let mut candidate_set = HashMap::new();
        for topic in topics {
            topic_tree.find_subscribers(topic, &mut candidate_set).await;
        }
        candidate_set
    }

    pub async fn insert_new_client(&self, ws: WebSocketStream<Stream>) {
        let id = self.topic_tree.next_id().await;

        let (ws_send, ws_recv) = ws.split();
        let ws_client = WebSocketClient {
            inner: Arc::new(WebSocketClientInner {
                id,
                ws_send: Mutex::new(ws_send),
                topic_tree: self.topic_tree.clone(),
                subscribed_topics: Default::default(),
                callback_function: self.client_message_callback.clone(),
            }),
        };

        let ws_client = Arc::new(RwLock::new(ws_client));

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

                    let read_guard = ws_client.read().await;

                    read_guard
                        .callback_function
                        .callback(read_guard.deref(), message)
                        .await;
                })
                .await;

            // TODO ws_client.write().await.remove_client().await;
        });
    }

    pub fn send_message(&self, topics: Vec<TopicSpecifier>, message: Message) {
        let topic_tree = self.topic_tree.clone();
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
pub struct WebSocketClient<F> {
    inner: Arc<WebSocketClientInner<F>>,
}

#[async_trait]
impl<F> Client<Message, Error> for WebSocketClient<F>
where
    F: ClientCallback<Message, Error> + Send + Sync + Clone + 'static,
{
    async fn send_message(&self, message: Message) -> Result<(), Error> {
        self.send_message(message).await
    }
}

impl<F> Deref for WebSocketClient<F> {
    type Target = WebSocketClientInner<F>;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

pub struct WebSocketClientInner<F> {
    id: UniqId,
    ws_send: Mutex<SplitSink<WebSocketStream<Stream>, Message>>,
    subscribed_topics: RwLock<HashSet<TopicSpecifier>>,
    topic_tree: TopicTree<WebSocketClient<F>>,
    callback_function: F,
}

impl<F> WebSocketClient<F>
where
    F: ClientCallback<WebSocketClient<F>, Error> + Send + Sync + Clone + 'static,
{
    pub async fn subscribe_to_topic(&self, topic: &TopicSpecifier) {
        self.topic_tree
            .subscribe_to_topic(self.clone(), self.id, topic)
            .await
    }

    pub async fn unsubscribe_from_topic(&self, topic: &TopicSpecifier) {todo!()}

    pub async fn send_message(&self, message: Message) -> Result<(), Error> {
        self.ws_send.lock().await.send(message).await
    }
}

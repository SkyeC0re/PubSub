use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    marker::PhantomData,
    ops::{AddAssign, Deref},
    sync::{Arc, Mutex as BlockingMutex, Weak},
    time::Duration,
};

use itertools::Itertools;

use async_trait::async_trait;
use futures_util::{
    stream::{futures_unordered, FuturesUnordered},
    Future, StreamExt,
};
use log::{error, warn};
use serde::{Deserialize, Serialize};

use tokio::{
    runtime::Handle,
    sync::{Mutex, RwLock},
    task::JoinError,
    time::{sleep, timeout},
};

pub type UniqId = u128;
type TopicTreeNode<K, V> = Arc<RwLock<TopicNode<K, V>>>;
type WeakTopicTreeNode<K, V> = Weak<RwLock<TopicNode<K, V>>>;

#[async_trait]
pub trait Client<M, E>: Send + Sync {
    async fn send_message(&self, message: &M) -> Result<(), E>;
}

#[async_trait]
pub trait MutableClient<M, E>: Send + Sync {
    async fn send_message(&mut self, message: &M) -> Result<(), E>;
}

#[async_trait]
impl<C, M, E> Client<M, E> for Mutex<C>
where
    C: MutableClient<M, E> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    async fn send_message(&self, message: &M) -> Result<(), E> {
        self.lock().await.send_message(message).await
    }
}

pub struct RegisteredClient<
    K: Hash + Eq + Clone + Send + Sync + 'static,
    C: Client<M, E> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
    E: Send + Sync + 'static,
> {
    id: K,
    client: Arc<C>,
    topic_tree: TopicTree<K, TaggedClient<C, M, E>>,
    subscribed_topics: Arc<RwLock<HashSet<TopicSpecifier>>>,
    tags: Arc<RwLock<HashSet<String>>>,
    runtime_handle: Handle,
}

impl<K, C, M, E> Drop for RegisteredClient<K, C, M, E>
where
    C: Client<M, E> + ?Sized + Send + Sync + 'static,
    K: Hash + Eq + Clone + Send + Sync + 'static,
    M: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn drop(&mut self) {
        let topic_tree = self.topic_tree.clone();
        let subscribed_topics = self.subscribed_topics.clone();
        let id = self.id.clone();
        self.runtime_handle.spawn(async move {
            for topic in subscribed_topics.read().await.deref() {
                topic_tree.unsubscribe_from_topic(&id, topic).await;
            }
        });
    }
}

impl<K, C, M, E> Deref for RegisteredClient<K, C, M, E>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    C: Client<M, E> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    type Target = C;

    fn deref(&self) -> &Self::Target {
        self.client.as_ref()
    }
}

impl<K, C, M, E> RegisteredClient<K, C, M, E>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    C: Client<M, E> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    pub async fn subscribe_to_topic(&self, topic: &TopicSpecifier) {
        if self.subscribed_topics.write().await.insert(topic.clone()) {
            self.topic_tree
                .subscribe_to_topic(self.id.clone(), self.get_tagged_client(), topic)
                .await;
        };
    }

    pub async fn unsubscribe_from_topic(&self, topic: &TopicSpecifier) {
        if self.subscribed_topics.write().await.remove(topic) {
            self.topic_tree
                .unsubscribe_from_topic(&self.id, topic)
                .await;
        };
    }

    pub async fn unsubscribe_from_all(&self) {
        for topic in self.subscribed_topics.write().await.drain() {
            self.topic_tree
                .unsubscribe_from_topic(&self.id, &topic)
                .await;
        }
    }

    pub async fn add_tag(&self, tag: String) {
        self.tags.write().await.insert(tag);
    }

    pub async fn remove_tag(&self, tag: &str) {
        self.tags.write().await.remove(tag);
    }

    fn get_tagged_client(&self) -> TaggedClient<C, M, E> {
        TaggedClient {
            client: self.client.clone(),
            tags: self.tags.clone(),
            _message_type: PhantomData,
            _error_type: PhantomData,
        }
    }
}

pub struct TaggedClient<C, M, E>
where
    C: Client<M, E> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    client: Arc<C>,
    tags: Arc<RwLock<HashSet<String>>>,
    _message_type: PhantomData<&'static M>,
    _error_type: PhantomData<&'static E>,
}

impl<C, M, E> Deref for TaggedClient<C, M, E>
where
    C: Client<M, E> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    type Target = C;

    fn deref(&self) -> &Self::Target {
        self.client.as_ref()
    }
}

impl<C, M, E> Clone for TaggedClient<C, M, E>
where
    C: Client<M, E> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            tags: self.tags.clone(),
            _message_type: PhantomData,
            _error_type: PhantomData,
        }
    }
}

#[derive(PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum TopicSpecifier {
    #[serde(rename = "*")]
    Wildcard,
    #[serde(rename = ".")]
    ThisTopic,
    #[serde(rename = "st")]
    Subtopic {
        #[serde(rename = "t")]
        topic: String,
        #[serde(rename = "spec")]
        specifier: Box<TopicSpecifier>,
    },
}

impl TopicSpecifier {
    pub fn build() -> TopicSpecifierBuilder {
        TopicSpecifierBuilder::new()
    }
}

#[derive(Default)]
pub struct TopicSpecifierBuilder {
    topics: Vec<String>,
}

impl TopicSpecifierBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn subtopic<T: Into<String>>(mut self, subtopic: T) -> Self {
        self.topics.push(subtopic.into());
        self
    }

    pub fn this_topic(self) -> TopicSpecifier {
        self.terminate(TopicSpecifier::ThisTopic)
    }

    pub fn all(self) -> TopicSpecifier {
        self.terminate(TopicSpecifier::Wildcard)
    }

    pub fn terminate(self, terminator: TopicSpecifier) -> TopicSpecifier {
        self.topics
            .into_iter()
            .rev()
            .fold(terminator, |tail, topic| TopicSpecifier::Subtopic {
                topic,
                specifier: Box::new(tail),
            })
    }
}

impl TopicSpecifier {}

#[derive(Default)]
struct TopicNode<K, V> {
    topic_subscribers: HashMap<K, V>,
    wildcard_subtopic_subscribers: HashMap<K, V>,
    subtopics: HashMap<String, TopicTreeNode<K, V>>,
    parent: Option<WeakTopicTreeNode<K, V>>,
    topic: String,
}

impl<K, V> TopicNode<K, V> {
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

impl<K, V> TopicNode<K, V> {
    pub fn is_empty_leaf(&self) -> bool {
        self.topic_subscribers.is_empty()
            && self.wildcard_subtopic_subscribers.is_empty()
            && self.subtopics.is_empty()
    }
}

pub struct TopicTreeConfig {
    prune_delay_ms: u64,
}

impl Default for TopicTreeConfig {
    fn default() -> Self {
        Self {
            prune_delay_ms: 5000,
        }
    }
}

pub struct TopicTree<K, V> {
    tree: TopicTreeNode<K, V>,
    config: Arc<TopicTreeConfig>,
}

impl<K, V> Clone for TopicTree<K, V> {
    fn clone(&self) -> Self {
        Self {
            tree: self.tree.clone(),
            config: self.config.clone(),
        }
    }
}

impl<K: Clone + Send + Sync + 'static + Hash + Eq, V: Clone + Send + Sync + 'static>
    TopicTree<K, V>
{
    pub fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(TopicNode::new())),
            config: Arc::default(),
        }
    }

    pub fn new_with_config(config: TopicTreeConfig) -> Self {
        Self {
            tree: Arc::new(RwLock::new(TopicNode::new())),
            config: Arc::new(config),
        }
    }

    pub async fn find_subscribers(&self, topic: &TopicSpecifier, collector: &mut HashMap<K, V>) {
        let mut current_branch = self.tree.clone();
        let mut current_topic = topic;

        loop {
            match current_topic {
                TopicSpecifier::Wildcard => {
                    Self::collect_all_subscribers(current_branch, collector, usize::MAX).await;
                    break;
                }
                TopicSpecifier::ThisTopic => {
                    Self::collect_all_subscribers(current_branch, collector, 0).await;
                    break;
                }
                TopicSpecifier::Subtopic { topic, specifier } => {
                    let current_branch_read_guard = current_branch.read().await;
                    for (id, client) in &current_branch_read_guard.wildcard_subtopic_subscribers {
                        collector.insert(id.clone(), client.clone());
                    }
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
        branch: TopicTreeNode<K, V>,
        collector: &mut HashMap<K, V>,
        depth: usize,
    ) {
        let mut unvisited_branches = vec![(branch, depth)];
        while let Some((branch, depth)) = unvisited_branches.pop() {
            let branch = branch.read().await;
            for (id, client) in &branch.topic_subscribers {
                collector.insert(id.clone(), client.clone());
            }
            for (id, client) in &branch.wildcard_subtopic_subscribers {
                collector.insert(id.clone(), client.clone());
            }

            if depth > 0 {
                unvisited_branches.extend(
                    branch
                        .subtopics
                        .values()
                        .cloned()
                        .map(|unvisited_branch| (unvisited_branch, depth - 1)),
                );
            }
        }
    }

    async fn subscribe_to_topic(&self, client_id: K, client: V, topic: &TopicSpecifier) {
        let mut current_branch = self.tree.clone();
        let mut current_topic = topic;

        loop {
            match current_topic {
                TopicSpecifier::Wildcard => {
                    current_branch
                        .write()
                        .await
                        .wildcard_subtopic_subscribers
                        .insert(client_id, client);
                    break;
                }
                TopicSpecifier::ThisTopic => {
                    current_branch
                        .write()
                        .await
                        .topic_subscribers
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

    async fn unsubscribe_from_topic(&self, client_id: &K, topic: &TopicSpecifier) {
        let mut current_branch = self.tree.clone();
        let mut current_topic = topic;
        let marked_for_pruning;
        loop {
            match current_topic {
                TopicSpecifier::Wildcard => {
                    let mut current_branch_write_guard = current_branch.write().await;
                    current_branch_write_guard
                        .wildcard_subtopic_subscribers
                        .remove(client_id);
                    marked_for_pruning = current_branch_write_guard.is_empty_leaf();
                    break;
                }
                TopicSpecifier::ThisTopic => {
                    let mut current_branch_write_guard = current_branch.write().await;
                    current_branch_write_guard
                        .topic_subscribers
                        .remove(client_id);
                    marked_for_pruning = current_branch_write_guard.is_empty_leaf();
                    break;
                }
                TopicSpecifier::Subtopic { topic, specifier } => {
                    current_topic = specifier;
                    let subtopic_tree = current_branch.read().await.subtopics.get(topic).cloned();
                    if let Some(subtopic_tree) = subtopic_tree {
                        current_branch = subtopic_tree;
                    } else {
                        // Nothing to do, client was never on that topic
                        return;
                    }
                }
            }
        }

        if marked_for_pruning {
            let delay = self.config.prune_delay_ms;
            tokio::spawn(async move {
                sleep(Duration::from_millis(delay)).await;
                TopicTree::prune_leaf(current_branch).await;
            });
        }
    }

    /// Attempt to prune empty branches of a topic, starting at the furthermost branch and working back to the root.
    async fn prune_leaf(leaf: TopicTreeNode<K, V>) {
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

#[derive(Clone)]
pub struct ManagerConfig {
    pub prune_delay_ms: u64,
    pub send_message_parallel_chunks_size: usize,
    pub client_send_message_timeout_ms: u64,
    pub client_send_message_max_attempts: usize,
}

impl Default for ManagerConfig {
    fn default() -> Self {
        Self {
            prune_delay_ms: 5000,
            send_message_parallel_chunks_size: 100,
            client_send_message_timeout_ms: 5000,
            client_send_message_max_attempts: 3,
        }
    }
}

pub struct Manager<K, C, M = (), E = ()>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    C: Client<M, E> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    topic_tree: TopicTree<K, TaggedClient<C, M, E>>,
    config: Arc<ManagerConfig>,
    runtime_handle: Handle,
    next_id: RwLock<UniqId>,
}

impl<C, M, E> Manager<UniqId, C, M, E>
where
    C: Client<M, E> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    /// Creates a new message manager using a handle to the runtime used for internal actions and with the default config.
    pub fn new(runtime_handle: Handle) -> Self {
        Self::new_with_config(runtime_handle, Default::default())
    }

    /// Creates a new message manager using a handle to the runtime used for internal actions and with the given config.
    pub fn new_with_config(runtime_handle: Handle, config: ManagerConfig) -> Self {
        Self {
            topic_tree: TopicTree::new_with_config(TopicTreeConfig {
                prune_delay_ms: config.prune_delay_ms,
            }),
            config: Arc::new(config),
            runtime_handle,
            next_id: Default::default(),
        }
    }

    /// Find all the subscribed clients present in the given list of topics.
    pub async fn find_subscribed_clients(
        &self,
        topics: impl IntoIterator<Item = &TopicSpecifier>,
    ) -> HashMap<UniqId, TaggedClient<C, M, E>> {
        let mut candidate_set = HashMap::new();
        for topic in topics {
            self.topic_tree
                .find_subscribers(topic, &mut candidate_set)
                .await;
        }
        candidate_set
    }

    /// Sends a message to the all clients in the given list of topics and returns the involved clients.
    pub async fn send_message(
        &self,
        topics: impl IntoIterator<Item = &TopicSpecifier>,
        message: M,
    ) {
        let candidates = self.find_subscribed_clients(topics).await;
        let message = Arc::new(message);
        let config = self.config.clone();

        let send_message_chunk = move |chunk: Vec<TaggedClient<C, M, E>>| async move {
            futures::stream::iter(&chunk)
                .for_each_concurrent(None, |candidate| async {
                    for _ in 0..config.client_send_message_max_attempts {
                        match timeout(
                            Duration::from_millis(config.client_send_message_timeout_ms),
                            candidate.send_message(&message),
                        )
                        .await
                        {
                            Err(_) => warn!("Client timed out during send operation"),
                            Ok(Err(_)) => warn!("Error sending message to client"),
                            Ok(Ok(())) => return,
                        };
                    }
                })
                .await;
        };

        let mut chunk_futures = self
            .apply_to_chunks_in_parallel(
                candidates.into_values(),
                send_message_chunk,
                self.config.send_message_parallel_chunks_size,
            )
            .await;

        while let Some(chunk_future) = chunk_futures.next().await {
            if chunk_future.is_err() {
                error!("Message chunk join failure");
            }
        }
    }

    async fn apply_to_chunks_in_parallel<
        I: Send + 'static,
        O: Send + 'static,
        F: Future<Output = O> + Send + 'static,
    >(
        &self,
        iter: impl IntoIterator<Item = I>,
        chunk_func: impl (FnOnce(Vec<I>) -> F) + Send + Clone + 'static,
        chunk_size: usize,
    ) -> FuturesUnordered<impl Future<Output = Result<O, JoinError>>> {
        let futures = FuturesUnordered::new();
        let mut chunk = Vec::with_capacity(chunk_size);
        let fut = |chunk| async move { tokio::spawn(chunk_func(chunk)).await };
        for elem in iter {
            chunk.push(elem);
            if chunk.len() >= chunk_size {
                futures.push(fut.clone()(chunk));
                chunk = Vec::with_capacity(chunk_size);
            }
        }
        if !chunk.is_empty() {
            futures.push(fut(chunk));
        }
        futures
    }

    pub async fn send_message_and_record_tags(
        &self,
        topics: impl IntoIterator<Item = &TopicSpecifier>,
        message: M,
        tag_filter: Option<HashSet<String>>,
    ) -> HashSet<String> {
        let candidates = self.find_subscribed_clients(topics).await;
        let message = Arc::new(message);
        let config = self.config.clone();
        let tag_filter = Arc::new(tag_filter);
        let mut recorded_tags = HashSet::new();

        let send_message_chunk = move |chunk: Vec<TaggedClient<C, M, E>>| async move {
            let recorded_tags = Mutex::new(HashSet::new());
            futures::stream::iter(&chunk)
                .for_each_concurrent(None, |candidate| async {
                    for _ in 0..config.client_send_message_max_attempts {
                        match timeout(
                            Duration::from_millis(config.client_send_message_timeout_ms),
                            candidate.send_message(&message),
                        )
                        .await
                        {
                            Err(_) => warn!("Client timed out during send operation"),
                            Ok(Err(_)) => warn!("Error sending message to client"),
                            Ok(Ok(())) => {
                                let candidate_tags = candidate.tags.read().await;
                                if let Some(tag_filter) = tag_filter.deref() {
                                    let filtered_tags = candidate_tags.intersection(tag_filter);
                                    recorded_tags
                                        .lock()
                                        .await
                                        .extend(filtered_tags.into_iter().cloned());
                                } else {
                                    recorded_tags
                                        .lock()
                                        .await
                                        .extend(candidate_tags.iter().cloned());
                                };

                                return;
                            }
                        };
                    }
                })
                .await;
            recorded_tags.into_inner()
        };

        let mut chunks_tags = self
            .apply_to_chunks_in_parallel(
                candidates.into_values(),
                send_message_chunk,
                self.config.send_message_parallel_chunks_size,
            )
            .await;

        while let Some(chunk_tags) = chunks_tags.next().await {
            match chunk_tags {
                Ok(tags) => recorded_tags.extend(tags),
                Err(_) => error!("Message chunk join failure"),
            }
        }

        recorded_tags
    }

    /// Registers a client on the manager, granting it a unique id.
    pub async fn register_client(&self, client: Arc<C>) -> RegisteredClient<UniqId, C, M, E> {
        let mut id_write_guard = self.next_id.write().await;
        let id = id_write_guard.clone();
        id_write_guard.add_assign(&1);

        RegisteredClient {
            id,
            client,
            topic_tree: self.topic_tree.clone(),
            subscribed_topics: Default::default(),
            tags: Default::default(),
            runtime_handle: self.runtime_handle.clone(),
        }
    }
}

impl<C, M, E> Manager<UniqId, C, M, E>
where
    C: Client<M, E> + Send + Sync + 'static,
    M: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    pub async fn register_raw_client(&self, client: C) -> RegisteredClient<UniqId, C, M, E> {
        self.register_client(Arc::new(client)).await
    }
}

impl<C, M, E> Manager<UniqId, Mutex<C>, M, E>
where
    C: MutableClient<M, E> + Send + Sync + 'static,
    M: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    pub async fn register_raw_mutable_client(
        &self,
        client: C,
    ) -> RegisteredClient<UniqId, Mutex<C>, M, E> {
        self.register_client(Arc::new(Mutex::new(client))).await
    }
}

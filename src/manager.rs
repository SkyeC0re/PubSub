use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    marker::PhantomData,
    ops::{AddAssign, Deref},
    sync::Arc,
    time::Duration,
};

use futures_util::{stream::FuturesUnordered, Future, StreamExt};
use log::{error, warn};

use tokio::{
    runtime::Handle,
    sync::{Mutex, RwLock},
    task::JoinError,
    time::timeout,
};

use crate::{
    client::{Client, MutableClient},
    topic_specifier::TopicSpecifier,
    topic_tree::{TopicTree, TopicTreeConfig},
};

pub type UniqId = u128;

struct RegistrationInformation<C, M, K>
where
    C: Client<M> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
    K: Hash + Eq + Clone + Send + Sync + 'static,
{
    id: K,
    topic_tree: TopicTree<K, RegisteredClientManagerCopy<C, M, K>>,
    subscribed_topics: RwLock<HashSet<TopicSpecifier>>,
    tags: RwLock<HashSet<String>>,
    runtime_handle: Handle,
}

pub struct RegisteredClient<C, M, K>
where
    C: Client<M> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
    K: Hash + Eq + Clone + Send + Sync + 'static,
{
    client: Arc<C>,
    registration_info: Arc<RegistrationInformation<C, M, K>>,
}

impl<C, M, K> Drop for RegisteredClient<C, M, K>
where
    C: Client<M> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
    K: Hash + Eq + Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        let registration_info = self.registration_info.clone();
        self.registration_info.runtime_handle.spawn(async move {
            let id = &registration_info.id;
            for topic in registration_info.subscribed_topics.read().await.deref() {
                registration_info
                    .topic_tree
                    .unsubscribe_from_topic(id, topic)
                    .await;
            }
        });
    }
}

impl<C, M, K> Deref for RegisteredClient<C, M, K>
where
    C: Client<M> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
    K: Hash + Eq + Clone + Send + Sync + 'static,
{
    type Target = C;

    fn deref(&self) -> &Self::Target {
        self.client.as_ref()
    }
}

impl<C, M, K> RegisteredClient<C, M, K>
where
    C: Client<M> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
    K: Hash + Eq + Clone + Send + Sync + 'static,
{
    pub async fn subscribe_to_topic(&self, topic: &TopicSpecifier) {
        let registration_info = self.registration_info.as_ref();
        if registration_info
            .subscribed_topics
            .write()
            .await
            .insert(topic.clone())
        {
            registration_info
                .topic_tree
                .subscribe_to_topic(
                    registration_info.id.clone(),
                    self.get_tagged_client(),
                    topic,
                )
                .await;
        };
    }

    pub async fn unsubscribe_from_topic(&self, topic: &TopicSpecifier) {
        let registration_info = self.registration_info.as_ref();
        if registration_info
            .subscribed_topics
            .write()
            .await
            .remove(topic)
        {
            registration_info
                .topic_tree
                .unsubscribe_from_topic(&registration_info.id, topic)
                .await;
        };
    }

    pub async fn unsubscribe_from_all(&self) {
        let registration_info = self.registration_info.as_ref();
        for topic in registration_info.subscribed_topics.write().await.drain() {
            registration_info
                .topic_tree
                .unsubscribe_from_topic(&registration_info.id, &topic)
                .await;
        }
    }

    pub async fn add_tag(&self, tag: String) {
        self.registration_info.tags.write().await.insert(tag);
    }

    pub async fn remove_tag(&self, tag: &str) {
        self.registration_info.tags.write().await.remove(tag);
    }

    fn get_tagged_client(&self) -> RegisteredClientManagerCopy<C, M, K> {
        RegisteredClientManagerCopy {
            client: self.client.clone(),
            registration_info: self.registration_info.clone(),
        }
    }
}

pub struct RegisteredClientManagerCopy<C, M, K>
where
    C: Client<M> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
    K: Hash + Eq + Clone + Send + Sync + 'static,
{
    client: Arc<C>,
    registration_info: Arc<RegistrationInformation<C, M, K>>,
}

impl<C, M, K> Clone for RegisteredClientManagerCopy<C, M, K>
where
    C: Client<M> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
    K: Hash + Eq + Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            registration_info: self.registration_info.clone(),
        }
    }
}

impl<C, M, K> Deref for RegisteredClientManagerCopy<C, M, K>
where
    C: Client<M> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
    K: Hash + Eq + Clone + Send + Sync + 'static,
{
    type Target = C;

    fn deref(&self) -> &Self::Target {
        self.client.as_ref()
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

pub struct Manager<C, M = (), K = UniqId>
where
    C: Client<M> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
    K: Hash + Eq + Clone + Send + Sync + 'static,
{
    topic_tree: TopicTree<K, RegisteredClientManagerCopy<C, M, K>>,
    config: Arc<ManagerConfig>,
    runtime_handle: Handle,
    next_id: RwLock<UniqId>,
}

impl<C, M> Manager<C, M, UniqId>
where
    C: Client<M> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
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
    ) -> HashMap<UniqId, RegisteredClientManagerCopy<C, M, UniqId>> {
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

        let send_message_chunk = |chunk: Vec<RegisteredClientManagerCopy<C, M, UniqId>>| async move {
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
        'a,
        I: Send + 'static,
        O: Send + 'static,
        F: Future<Output = O> + Send + 'static,
    >(
        &'a self,
        iter: impl IntoIterator<Item = I>,
        chunk_func: impl (FnOnce(Vec<I>) -> F) + Send + Clone + 'static,
        chunk_size: usize,
    ) -> FuturesUnordered<impl Future<Output = Result<O, JoinError>> + 'a> {
        let futures = FuturesUnordered::new();
        let mut chunk = Vec::with_capacity(chunk_size);
        let fut = |chunk| async {
            self.runtime_handle
                .spawn(async move { chunk_func(chunk).await })
                .await
        };
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
        let send_message_chunk = |chunk: Vec<RegisteredClientManagerCopy<C, M, UniqId>>| async move {
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
                                let candidate_tags = candidate.registration_info.tags.read().await;
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
    pub async fn register_client(&self, client: Arc<C>) -> RegisteredClient<C, M, UniqId> {
        let mut id_write_guard = self.next_id.write().await;
        let id = id_write_guard.clone();
        id_write_guard.add_assign(&1);

        let registration_info = RegistrationInformation {
            id,
            topic_tree: self.topic_tree.clone(),
            subscribed_topics: Default::default(),
            tags: Default::default(),
            runtime_handle: self.runtime_handle.clone(),
        };

        RegisteredClient {
            client,
            registration_info: Arc::new(registration_info),
        }
    }
}

impl<C, M> Manager<C, M, UniqId>
where
    C: Client<M> + Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    pub async fn register_raw_client(&self, client: C) -> RegisteredClient<C, M, UniqId> {
        self.register_client(Arc::new(client)).await
    }
}

impl<C, M> Manager<Mutex<C>, M, UniqId>
where
    C: MutableClient<M> + Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    pub async fn register_raw_mutable_client(
        &self,
        client: C,
    ) -> RegisteredClient<Mutex<C>, M, UniqId> {
        self.register_client(Arc::new(Mutex::new(client))).await
    }
}

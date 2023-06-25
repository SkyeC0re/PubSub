use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::{sync::RwLock, time::sleep};

use crate::topic_specifier::TopicSpecifier;

type TopicTreeNode<K, V> = Arc<RwLock<TopicNode<K, V>>>;
type WeakTopicTreeNode<K, V> = Weak<RwLock<TopicNode<K, V>>>;

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
    pub prune_delay_ms: u64,
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

    pub async fn subscribe_to_topic(&self, client_id: K, client: V, topic: &TopicSpecifier) {
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

    pub async fn unsubscribe_from_topic(&self, client_id: &K, topic: &TopicSpecifier) {
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

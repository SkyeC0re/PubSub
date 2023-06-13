use serde::{Deserialize, Serialize};

use crate::pubsub_manager::TopicSpecifier;

#[derive(Serialize, Deserialize)]
pub struct TopicSpecifiers {
    pub topics: Vec<TopicSpecifier>,
}

use serde::{Deserialize, Serialize};

use crate::websocket_server::TopicSpecifier;

#[derive(Serialize, Deserialize)]
pub struct TopicSpecifiers {
    pub topics: Vec<TopicSpecifier>,
}

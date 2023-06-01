use serde::{Deserialize, Serialize};

use crate::websocket_server::TopicSpecifier;

#[derive(Serialize, Deserialize)]
pub struct AuthMessage {
    pub encrypted_tags: String,
}

#[derive(Serialize, Deserialize)]
pub struct TopicSpecifiers {
    pub topics: Vec<TopicSpecifier>,
}

#[derive(Serialize, Deserialize)]
pub struct JwtValidationMessage {
    pub r#type: String,
    pub success: bool,
    pub token: String,
}

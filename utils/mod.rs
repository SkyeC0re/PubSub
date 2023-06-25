use std::sync::Arc;

use async_trait::async_trait;
use pubsub::client::Client;
use tokio::sync::RwLock;

pub struct TestClient<M: Send + Sync + Clone + 'static> {
    pub messages_received: Arc<RwLock<Vec<M>>>,
}

impl<M: Send + Sync + Clone + 'static> TestClient<M> {
    pub fn new() -> Self {
        TestClient {
            messages_received: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

#[async_trait]
impl<M: Send + Sync + Clone + 'static> Client<M> for TestClient<M> {
    async fn send_message(&self, message: &M) -> Result<(), ()> {
        self.messages_received.write().await.push(message.clone());
        Ok(())
    }
}

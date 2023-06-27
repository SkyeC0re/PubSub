use std::{pin::Pin, sync::Arc};

use async_trait::async_trait;
use futures_util::Future;
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
    async fn produce_handle_message_event<'a>(
        &'a self,
        message: &'a M,
    ) -> Pin<Box<dyn Future<Output = Result<(), ()>> + Send + 'a>> {
        let mut write_guard = self.messages_received.write().await;
        Box::pin(async move {
            write_guard.push(message.clone());
            Ok(())
        })
    }
}

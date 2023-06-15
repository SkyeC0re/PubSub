use std::sync::Arc;

use async_trait::async_trait;
use env_logger::try_init;
use log::error;
use pubsub::pubsub_manager::{Client, Manager, TopicSpecifier};
use tokio::{self, runtime::Handle, sync::RwLock};

struct TestClient<M: Send + Sync + Clone + 'static> {
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
impl<M: Send + Sync + Clone + 'static> Client<M, ()> for TestClient<M> {
    async fn send_message(&self, message: &M) -> Result<(), ()> {
        self.messages_received.write().await.push(message.clone());
        Ok(())
    }
}

#[tokio::test]
pub async fn test_add_client_and_message() {
    let handle = Handle::current();
    let server = Manager::new(handle.clone());
    let registered_client = server.register_client(Arc::new(TestClient::new())).await;

    registered_client
        .subscribe_to_topic(&TopicSpecifier::build().subtopic("A").this_topic())
        .await;

    let message = "TEST".to_string();

    server
        .send_message(
            &vec![TopicSpecifier::build().subtopic("A").all()],
            message.clone(),
        )
        .await;

    let received_messages = registered_client.messages_received.read().await;
    assert_eq!(received_messages.len(), 1);
    assert_eq!(message, received_messages[0]);
}

#[tokio::test]
pub async fn test_wrong_subject() {
    let handle = Handle::current();
    let server = Manager::new(handle.clone());
    let registered_client = server.register_client(Arc::new(TestClient::new())).await;

    registered_client
        .subscribe_to_topic(&TopicSpecifier::build().subtopic("A").this_topic())
        .await;
    registered_client
        .subscribe_to_topic(
            &TopicSpecifier::build()
                .subtopic("A")
                .subtopic("B")
                .this_topic(),
        )
        .await;

    let message = "TEST".to_string();

    server
        .send_message(
            &vec![TopicSpecifier::build().subtopic("C").all()],
            message.clone(),
        )
        .await;

    let received_messages = registered_client.messages_received.read().await;
    assert_eq!(received_messages.len(), 0);
}

#[tokio::test]
pub async fn test_add_remove_subject() {
    let _ = try_init();
    let handle = Handle::current();
    let server = Manager::new(handle.clone());
    let registered_client = server.register_client(Arc::new(TestClient::new())).await;

    let subscribed_topic = TopicSpecifier::build().subtopic("A").subtopic("B").all();
    registered_client
        .subscribe_to_topic(&subscribed_topic)
        .await;

    let message = "TEST".to_string();
    let message_topics = vec![TopicSpecifier::build()
        .subtopic("A")
        .subtopic("B")
        .subtopic("C")
        .this_topic()];
    server.send_message(&message_topics, message.clone()).await;

    let mut received_messages = registered_client.messages_received.write().await;
    assert_eq!(received_messages.len(), 1);
    assert_eq!(message, received_messages[0]);

    received_messages.clear();
    drop(received_messages);

    registered_client
        .unsubscribe_from_topic(&subscribed_topic)
        .await;
    
    server.send_message(&message_topics, message.clone()).await;

    let received_messages = registered_client.messages_received.read().await;
    assert_eq!(received_messages.len(), 0);
}

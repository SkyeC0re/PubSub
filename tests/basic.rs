use std::{ops::Deref, sync::Arc};

use async_trait::async_trait;
use env_logger::try_init;
use log::error;
use pubsub::pubsub_manager::{Client, Manager, TopicSpecifier};
use tokio::{self, runtime::Handle, sync::RwLock};

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

#[tokio::test]
pub async fn test_multiple_clients_multiple_messages() {
    let _ = try_init();
    let handle = Handle::current();
    let server = Manager::new(handle.clone());
    let registered_client1 = server.register_client(Arc::new(TestClient::new())).await;
    let registered_client2 = server.register_client(Arc::new(TestClient::new())).await;
    let registered_client3 = server.register_client(Arc::new(TestClient::new())).await;

    let topic1 = TopicSpecifier::build().all();
    let topic2 = TopicSpecifier::build().subtopic("A").all();
    let topic3 = TopicSpecifier::build().subtopic("B").this_topic();

    registered_client1.subscribe_to_topic(&topic1).await;
    registered_client1.subscribe_to_topic(&topic3).await;
    registered_client2.subscribe_to_topic(&topic2).await;
    registered_client3.subscribe_to_topic(&topic3).await;

    let mut client1_expected_messages = vec![];
    let mut client2_expected_messages = vec![];
    let mut client3_expected_messages = vec![];

    let message = "1";
    let message_topics = vec![TopicSpecifier::build()
        .subtopic("A")
        .subtopic("B")
        .subtopic("C")
        .this_topic()];
    server
        .send_message(&message_topics, message.to_string())
        .await;
    client1_expected_messages.push(message.to_string());
    client2_expected_messages.push(message.to_string());

    let message = "2";
    let message_topics = vec![TopicSpecifier::build()
        .subtopic("C")
        .subtopic("B")
        .this_topic()];
    server
        .send_message(&message_topics, message.to_string())
        .await;
    client1_expected_messages.push(message.to_string());

    let message = "3";
    let message_topics = vec![TopicSpecifier::build().subtopic("B").subtopic("C").all()];
    server
        .send_message(&message_topics, message.to_string())
        .await;
    client1_expected_messages.push(message.to_string());

    let message = "4";
    let message_topics = vec![TopicSpecifier::build().subtopic("B").this_topic()];
    server
        .send_message(&message_topics, message.to_string())
        .await;
    client1_expected_messages.push(message.to_string());
    client3_expected_messages.push(message.to_string());

    assert_eq!(
        &client1_expected_messages,
        registered_client1.messages_received.read().await.deref()
    );
    assert_eq!(
        &client2_expected_messages,
        registered_client2.messages_received.read().await.deref()
    );
    assert_eq!(
        &client3_expected_messages,
        registered_client3.messages_received.read().await.deref()
    );
}

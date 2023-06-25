use std::{ops::Deref, sync::Arc};

use async_trait::async_trait;
use pubsub::{manager::Manager, topic_specifier::TopicSpecifier};
use tokio::{runtime::Handle, sync::RwLock};

#[path = "../utils/mod.rs"]
mod utils;
use utils::*;

#[tokio::test]
pub async fn test_multiple_clients_multiple_messages() {
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

use std::{collections::HashSet, ops::Deref, sync::Arc};

use pubsub::{manager::Manager, topic_specifier::TopicSpecifier};
use tokio::runtime::Handle;

#[path = "../utils/mod.rs"]
mod utils;
use utils::*;

#[tokio::test]
pub async fn test_single_client_with_tags() {
    let handle = Handle::current();
    let server = Manager::new(handle.clone());
    let registered_client = server.register_client(Arc::new(TestClient::new())).await;

    let topic = TopicSpecifier::build().all();
    let tag_a = "A";
    let tag_b = "B";
    registered_client.subscribe_to_topic(&topic).await;
    registered_client.add_tag(tag_a.to_string()).await;
    registered_client.add_tag(tag_b.to_string()).await;
    let mut expected_messages = Vec::new();

    let message = "1";
    let message_topics = vec![TopicSpecifier::build().subtopic("A").this_topic()];
    let retrieved_tags = server
        .send_message_and_record_tags(&message_topics, message.to_string(), None)
        .await;
    assert_eq!(
        retrieved_tags,
        HashSet::from_iter(vec![tag_a.to_string(), tag_b.to_string()])
    );
    expected_messages.push(message.to_string());

    let message = "2";
    let message_topics = vec![TopicSpecifier::build().subtopic("B").this_topic()];
    let retrieved_tags = server
        .send_message_and_record_tags(
            &message_topics,
            message.to_string(),
            Some(HashSet::from_iter(vec![tag_b.to_string()])),
        )
        .await;
    assert_eq!(retrieved_tags, HashSet::from_iter(vec![tag_b.to_string()]));
    expected_messages.push(message.to_string());

    assert_eq!(
        &expected_messages,
        registered_client.messages_received.read().await.deref()
    );
}

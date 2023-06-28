use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Criterion};
use futures_util::{Future, Sink, SinkExt, StreamExt};
use itertools::Itertools;
use log::info;
use pubsub::{client::Client, manager::Manager, topic_specifier::TopicSpecifier};
use serde::{Deserialize, Serialize};
use std::{pin::Pin, sync::Arc};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
};
use tokio_tungstenite::{
    accept_async, connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream,
};

#[derive(Serialize, Deserialize)]
struct TestMessage {
    value: u32,
}

const MAX_TCP_LISTENER_CONNECTIONS: u16 = 25000;

pub async fn generate_tcp_listener(port: u16) -> TcpListener {
    TcpListener::bind(format!("localhost:{}", port))
        .await
        .unwrap()
}
pub async fn generate_client_ws_stream(
    port: u16,
) -> Option<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    connect_async(format!("ws://localhost:{}", port))
        .await
        .ok()
        .map(|(stream, _)| stream)
}

pub fn message_from_serializable<T: Serialize>(v: &T) -> Message {
    Message::Text(serde_json::to_string(v).unwrap())
}

struct BenchWebsocketClient<T: Sink<Message>, I> {
    sink: Mutex<T>,
    i: I,
}

#[async_trait]
impl<T: Sink<Message> + Send + Sync + Unpin, I: Send + Sync> Client<Message>
    for BenchWebsocketClient<T, I>
{
    async fn produce_handle_message_event<'a>(
        &'a self,
        message: &'a Message,
    ) -> Pin<Box<dyn Future<Output = Result<(), ()>> + Send + 'a>> {
        let mut lock = self.sink.lock().await;
        Box::pin(async move { lock.send(message.clone()).await.map_err(|_| ()) })
    }
}

fn bench_5k_clients(c: &mut Criterion) {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .try_init()
        .expect("Could not build logger");
    let listener_port_start_range = 50500;
    let num_clients = 5000;

    let client_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(6)
        .enable_all()
        .build()
        .unwrap();

    let server_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(6)
        .enable_all()
        .build()
        .unwrap();

    let server = Arc::new(Manager::new(server_runtime.handle().clone()));
    let mut registered_clients = Vec::with_capacity(num_clients as usize);
    server_runtime.block_on(async {
        for (port_start_offset, chunk) in (0..num_clients)
            .chunks(MAX_TCP_LISTENER_CONNECTIONS as usize)
            .into_iter()
            .enumerate()
        {
            let port = listener_port_start_range + port_start_offset as u16;
            let listener = generate_tcp_listener(port).await;
            for i in chunk {
                client_runtime.spawn(async move {
                    let client = generate_client_ws_stream(port).await.unwrap();
                    client
                        .for_each(|res| async {
                            let message = if let Ok(message) = res {
                                message
                            } else {
                                return;
                            };
                            match message {
                                Message::Text(serialized_message) => {
                                    let _message: TestMessage =
                                        serde_json::from_str(&serialized_message).unwrap();
                                }
                                _ => unreachable!(),
                            };
                        })
                        .await;
                });
                let server_side_ws_stream = accept_async(listener.accept().await.unwrap().0)
                    .await
                    .unwrap();

                let server_side_client = BenchWebsocketClient {
                    sink: Mutex::new(server_side_ws_stream.split().0),
                    i,
                };

                let registered_client = server.register_raw_client(server_side_client).await;

                registered_clients.push(registered_client);
            }
        }
    });

    info!("Listeners spawned");

    fn get_topics(pos: u32) -> Vec<TopicSpecifier> {
        (0..=pos)
            .into_iter()
            .map(|i| TopicSpecifier::Subtopic {
                topic: format!("topic-{}", i),
                specifier: Box::new(TopicSpecifier::ThisTopic),
            })
            .collect()
    }

    fn get_tags(pos: u32) -> Vec<String> {
        (0..=pos)
            .into_iter()
            .map(|i| format!("tag-{}", i))
            .collect()
    }

    let set_client_topics_and_tags = |num_topics: u32| {
        fn triangle_num(n: u32) -> u32 {
            n * (n + 1) >> 1
        }

        let total_messages = triangle_num(num_topics as u32) * (num_clients / num_topics as u32)
            + triangle_num(num_clients % num_topics as u32);
        info!(
            "Setting topics amount to {}. Expected messages sent per iteration: {}",
            num_topics, total_messages
        );
        server_runtime.block_on(async {
            for client in &registered_clients {
                client.unsubscribe_from_all().await;
                client.remove_all_tags().await;
                let pos = client.i % num_topics;
                for topic in get_topics(pos) {
                    client.subscribe_to_topic(&topic).await;
                }
                for tag in get_tags(pos) {
                    client.add_tag(tag).await;
                }
            }
        });
    };

    let send_message_to_topics = |num_topics: u32| {
        server_runtime.block_on(async {
            futures::stream::iter(0..num_topics)
                .for_each_concurrent(None, |i| {
                    let server = server.clone();
                    async move {
                        let server = server.clone();
                        let join_res = tokio::spawn(async move {
                            server
                                .send_message(
                                    &vec![TopicSpecifier::Subtopic {
                                        topic: format!("topic-{}", i),
                                        specifier: Box::new(TopicSpecifier::ThisTopic),
                                    }],
                                    message_from_serializable(&TestMessage { value: i }),
                                )
                                .await;
                        })
                        .await;

                        assert!(join_res.is_ok());
                    }
                })
                .await;
        })
    };

    let send_message_to_topics_collect_tags = |num_topics: u32| {
        server_runtime.block_on(async {
            futures::stream::iter(0..num_topics)
                .for_each_concurrent(None, |i| {
                    let server = server.clone();
                    async move {
                        let server = server.clone();
                        let join_res = tokio::spawn(async move {
                            server
                                .send_message_and_record_tags(
                                    &vec![TopicSpecifier::Subtopic {
                                        topic: format!("topic-{}", i),
                                        specifier: Box::new(TopicSpecifier::ThisTopic),
                                    }],
                                    message_from_serializable(&TestMessage { value: i }),
                                    Some(get_tags(i).into_iter().collect()),
                                )
                                .await;
                        })
                        .await;

                        assert!(join_res.is_ok());
                    }
                })
                .await;
        })
    };

    let num_topics = 8;
    set_client_topics_and_tags(num_topics);
    c.bench_function("bench_5k_clients_low_density", |b| {
        b.iter(|| send_message_to_topics(num_topics))
    });
    c.bench_function("bench_5k_clients_low_density_collect_tags", |b| {
        b.iter(|| send_message_to_topics_collect_tags(num_topics))
    });

    let num_topics = 15;
    set_client_topics_and_tags(num_topics);
    c.bench_function("bench_5k_clients_med_density", |b| {
        b.iter(|| send_message_to_topics(num_topics))
    });
    c.bench_function("bench_5k_clients_med_density_collect_tags", |b| {
        b.iter(|| send_message_to_topics_collect_tags(num_topics))
    });

    let num_topics = 30;
    set_client_topics_and_tags(num_topics);
    c.bench_function("bench_5k_clients_high_density", |b| {
        b.iter(|| send_message_to_topics(num_topics))
    });
    c.bench_function("bench_5k_clients_med_density_collect_tags", |b| {
        b.iter(|| send_message_to_topics_collect_tags(num_topics))
    });
}

criterion_group!(benches, bench_5k_clients);
criterion_main!(benches);

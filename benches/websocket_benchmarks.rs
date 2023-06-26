use std::{
    collections::HashMap,
    ops::{AddAssign, Deref},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Criterion};
use futures_util::{stream::SplitSink, Sink, SinkExt, StreamExt};
use jwt::{
    Header, PKeyWithDigest, SignWithKey, SigningAlgorithm, Token, VerifyWithKey, VerifyingAlgorithm,
};
use log::error;
use openssl::{hash::MessageDigest, pkey::PKey, rsa::Rsa};
use pubsub::{
    client::Client,
    manager::{Manager, UniqId},
    topic_specifier::{TopicSpecifier, TopicSpecifiers},
};
use serde::{Deserialize, Serialize};
use tokio::{
    net::{TcpListener, TcpStream},
    runtime::Handle,
    sync::Mutex,
    task::block_in_place,
};
use tokio::{
    sync::RwLock,
    time::{sleep, timeout},
};
use tokio_tungstenite::{
    accept_async, connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream,
};
#[derive(Serialize, Deserialize)]
struct TestMessage {
    value: u32,
}

const PORT: u16 = 5000;

pub fn generate_signer_verifier() -> (impl SigningAlgorithm, impl VerifyingAlgorithm) {
    let rsa = Rsa::generate(2048).unwrap();
    let rsa_pub =
        Rsa::public_key_from_pem_pkcs1(rsa.public_key_to_pem_pkcs1().unwrap().as_slice()).unwrap();

    let signer = PKeyWithDigest {
        digest: MessageDigest::sha256(),
        key: PKey::from_rsa(rsa).unwrap(),
    };

    let verifier = PKeyWithDigest {
        digest: MessageDigest::sha256(),
        key: PKey::from_rsa(rsa_pub).unwrap(),
    };
    (signer, verifier)
}

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

pub fn generate_jwt<T: SigningAlgorithm>(signer: &T, topics: TopicSpecifiers) -> String {
    let token = Token::new(
        Header {
            algorithm: signer.algorithm_type(),
            ..Default::default()
        },
        topics,
    )
    .sign_with_key(signer)
    .unwrap();

    token.as_str().into()
}

pub fn message_from_serializable<T: Serialize>(v: &T) -> Message {
    Message::Text(serde_json::to_string(v).unwrap())
}

struct ExclusiveSink<T: Sink<Message>> {
    sink: Mutex<T>,
}

#[async_trait]
impl<T: Sink<Message> + Send + Sync + Unpin> Client<Message> for ExclusiveSink<T> {
    async fn send_message(&self, message: &Message) -> Result<(), ()> {
        if self.sink.lock().await.send(message.clone()).await.is_err() {
            return Err(());
        }
        Ok(())
    }
}

fn generate_client_emitter(
    runtime_handle: Handle,
    server: Arc<
        Manager<ExclusiveSink<SplitSink<WebSocketStream<TcpStream>, Message>>, Message, UniqId>,
    >,
    listener: TcpListener,
    verifier: Arc<impl VerifyingAlgorithm + Send + Sync + 'static>,
) {
    let runtime_handle_clone = runtime_handle.clone();
    runtime_handle_clone.spawn(async move {
        while let Ok(ws) = accept_async(listener.accept().await.unwrap().0).await {
            let verifier = verifier.clone();
            let (send, recv) = ws.split();
            let send = send;
            let client = server
                .register_client(Arc::new(ExclusiveSink {
                    sink: Mutex::new(send),
                }))
                .await;
            runtime_handle.spawn(async move {
                recv.for_each(|message| async {
                    let topic_specifiers: TopicSpecifiers = if let Some(topic_specifiers) = message
                        .ok()
                        .and_then(|message| message.to_text().map(ToString::to_string).ok())
                        .and_then(|text| text.verify_with_key(verifier.as_ref()).ok())
                    {
                        topic_specifiers
                    } else {
                        return;
                    };

                    for topic in topic_specifiers.topics {
                        client.subscribe_to_topic(&topic).await;
                    }

                    let _ = client.send_message(&Message::Ping(Vec::new())).await;
                })
                .await;
            });
        }
    });
}

fn bench_50k_clients(c: &mut Criterion) {
    let _ = env_logger::try_init();
    let (signer, verifier) = generate_signer_verifier();

    let listener_port_start_range = 50500;
    let num_listeners = 4;
    let num_clients = 50000;
    let client_buckets = 30;

    fn triangle_num(n: u32) -> u32 {
        n * (n + 1) >> 1
    }

    let total_messages = triangle_num(client_buckets as u32)
        * (num_clients / client_buckets as u32)
        + triangle_num(num_clients % client_buckets as u32);

    error!("Messages being sent: {}", total_messages);

    let verifier = Arc::new(verifier);

    let listener_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

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

    listener_runtime.block_on(async {
        for i in 0..num_listeners {
            let listener = generate_tcp_listener(listener_port_start_range + i).await;
            generate_client_emitter(
                Handle::current(),
                server.clone(),
                listener,
                verifier.clone(),
            );
        }
    });

    error!("Listeners spawned");

    fn get_topics(pos: u32) -> TopicSpecifiers {
        let topics = (0..=pos)
            .into_iter()
            .map(|i| TopicSpecifier::Subtopic {
                topic: format!("tag-{}", i),
                specifier: Box::new(TopicSpecifier::ThisTopic),
            })
            .collect();

        TopicSpecifiers { topics }
    }

    let join_handles: Vec<_> = (0..num_clients)
        .into_iter()
        .map(|i| {
            let port = listener_port_start_range + (i % num_listeners as u32) as u16;
            let topics = get_topics(i % client_buckets);
            let jwt = generate_jwt(&signer, topics);
            client_runtime.spawn(async move {
                let mut client = if let Some(client) = generate_client_ws_stream(port).await {
                    client
                } else {
                    return Err(format!("Unable to create client"));
                };

                if let Err(e) = client.send(Message::Text(jwt)).await {
                    return Err(format!(
                        "Client {}. Error sending jwt to server, : {:?}",
                        i, e
                    ));
                };
                let message =
                    match timeout(Duration::from_secs(10), client.select_next_some()).await {
                        Ok(Ok(resp)) => resp,
                        Ok(Err(e)) => {
                            return Err(format!(
                                "Client {}. Error during retrieval of validation response: {:?}",
                                i, e
                            ))
                        }
                        Err(e) => {
                            return Err(format!(
                                "Client {}. Retrieval of validation response took too long: {:?}",
                                i, e
                            ))
                        }
                    };

                if !message.is_ping() {
                    return Err(format!("Client {}. Invalid message type response", i));
                }
                Ok((i, client))
            })
        })
        .collect();

    let mut clients = Vec::new();
    server_runtime.block_on(async {
        for join_handle in join_handles {
            if let Ok(Ok(join_handle)) = join_handle.await {
                clients.push(join_handle);
            }
        }
    });

    // start listeners
    clients.into_iter().for_each(|(i, client)| {
        client_runtime.spawn(async move {
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
    });

    c.bench_function("bench_50k_clients_mid_density", |b| {
        b.iter(|| {
            server_runtime.block_on(async {
                futures::stream::iter(0..client_buckets)
                    .for_each_concurrent(None, |i| {
                        let server = server.clone();
                        async move {
                            let server = server.clone();
                            let join_res = tokio::spawn(async move {
                                server
                                    .send_message(
                                        &vec![TopicSpecifier::Subtopic {
                                            topic: format!("tag-{}", i),
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
        })
    });
}

criterion_group!(benches, bench_50k_clients);
criterion_main!(benches);

use std::{
    collections::HashMap,
    ops::{AddAssign, Deref},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use futures_util::{stream::SplitSink, Sink, SinkExt, StreamExt, TryStreamExt};
use jwt::{
    Header, PKeyWithDigest, SignWithKey, SigningAlgorithm, Token, VerifyWithKey, VerifyingAlgorithm,
};
use log::error;
use openssl::{hash::MessageDigest, pkey::PKey, rsa::Rsa};
use pubsub::{
    models::TopicSpecifiers,
    pubsub_manager::{Client, Manager, TopicSpecifier, UniqId},
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
    value: i32,
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
impl<T: Sink<Message> + Send + Sync + Unpin> Client<Message, ()> for ExclusiveSink<T> {
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
        Manager<
            UniqId,
            ExclusiveSink<SplitSink<WebSocketStream<TcpStream>, Message>>,
            //ExclusiveSink<impl Sink<Message> + Send + Sync + Unpin>,
            Message,
            (),
        >,
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

#[tokio::test]
pub async fn test_client_add_and_message() {
    let _ = env_logger::try_init();
    let (signer, verifier) = generate_signer_verifier();

    let handle = Handle::current();

    let server = Arc::new(Manager::new(handle.clone()));

    let server_clone = server.clone();
    let listener = generate_tcp_listener(PORT).await;
    handle.spawn(async move {
        let ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let (send, recv) = ws.split();

        let client = server_clone
            .register_client(Arc::new(ExclusiveSink {
                sink: Mutex::new(send),
            }))
            .await;

        recv.for_each(|message| async {
            let topic_specifiers: TopicSpecifiers = if let Some(topic_specifiers) = message
                .ok()
                .and_then(|message| message.to_text().map(ToString::to_string).ok())
                .and_then(|text| text.verify_with_key(&verifier).ok())
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

    let mut client = generate_client_ws_stream(PORT).await.unwrap();

    let permissions = TopicSpecifiers {
        topics: vec![
            TopicSpecifier::Wildcard,
            TopicSpecifier::Subtopic {
                topic: "orders".to_string(),
                specifier: Box::new(TopicSpecifier::Subtopic {
                    topic: "123".to_string(),
                    specifier: Box::new(TopicSpecifier::Wildcard),
                }),
            },
        ],
    };

    let permission_token = Token::new(
        Header {
            algorithm: signer.algorithm_type(),
            ..Default::default()
        },
        permissions,
    )
    .sign_with_key(&signer)
    .unwrap();

    client
        .send(Message::Text(permission_token.as_str().to_owned()))
        .await
        .unwrap();

    // Jwt response message
    let next_message = timeout(Duration::from_secs(5), client.select_next_some())
        .await
        .unwrap()
        .unwrap();

    assert!(next_message.is_ping());

    let test_message = Message::Text("TEST".to_string());

    server
        .send_message(
            &vec![TopicSpecifier::Subtopic {
                topic: "random".to_string(),
                specifier: Box::new(TopicSpecifier::ThisTopic),
            }],
            test_message.clone(),
        )
        .await;
    let next_message = timeout(Duration::from_secs(5), client.select_next_some())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next_message, test_message);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
pub async fn test_multiple_clients() {
    let _ = env_logger::try_init();
    let (signer, verifier) = generate_signer_verifier();

    let verifier = Arc::new(verifier);

    let server = Arc::new(Manager::new(Handle::current()));
    let listener_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(6)
        .enable_all()
        .build()
        .unwrap();

    let listener_port_start_range = 5050;

    let num_listeners = 10u16;
    let num_clients = 2000;
    let client_buckets = 30;

    for i in 0..num_listeners {
        let listener = generate_tcp_listener(listener_port_start_range + i).await;
        generate_client_emitter(
            listener_runtime.handle().clone(),
            server.clone(),
            listener,
            verifier.clone(),
        );
    }

    error!("Listeners spawned");

    fn get_topics(pos: i32) -> TopicSpecifiers {
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
            let port = listener_port_start_range + (i % num_listeners as i32) as u16;
            let topics = get_topics(i % client_buckets);
            let jwt = generate_jwt(&signer, topics);
            tokio::spawn(async move {
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
    for join_handle in join_handles {
        if let Ok(Ok(join_handle)) = join_handle.await {
            clients.push(join_handle);
        }
    }

    error!(
        "Successfully spawned {}/{} clients",
        clients.len(),
        num_clients
    );

    let total_messages_received = Arc::new(RwLock::new(0));

    // start listeners
    let received_messages = clients
        .into_iter()
        .map(|(i, client)| {
            let received_messages = Arc::new(RwLock::new(HashMap::<i32, i32>::new()));
            let received_messages_clone = received_messages.clone();
            let total_messages_received_clone = total_messages_received.clone();
            tokio::spawn(async move {
                client
                    .for_each(|res| async {
                        let message = if let Ok(message) = res {
                            message
                        } else {
                            return;
                        };
                        match message {
                            Message::Text(serialized_message) => {
                                let message: TestMessage =
                                    serde_json::from_str(&serialized_message).unwrap();
                                *received_messages_clone
                                    .write()
                                    .await
                                    .entry(message.value)
                                    .or_default() += 1;

                                total_messages_received_clone.write().await.add_assign(&1);
                            }
                            _ => unreachable!(),
                        };
                    })
                    .await;
            });
            (i, received_messages)
        })
        .collect::<Vec<_>>();
    for i in 0..client_buckets {
        let server = server.clone();
        tokio::spawn(async move {
            server
                .send_message(
                    &vec![TopicSpecifier::Subtopic {
                        topic: format!("tag-{}", i),
                        specifier: Box::new(TopicSpecifier::ThisTopic),
                    }],
                    message_from_serializable(&TestMessage { value: i }),
                )
                .await;
        });
    }

    // Wait for things to settle
    for _ in 0..25 {
        sleep(Duration::from_secs(1)).await;
        error!(
            "Messages received so far: {}",
            total_messages_received.read().await.deref()
        );
    }

    let mut total = 0;
    let mut correct = 0;
    for (i, received_messages) in received_messages {
        let received_messages = received_messages.read().await;
        // A client `i` should have received all the messages `<= i`
        for i in 0..=i % client_buckets {
            total += 1;
            //assert_eq!(received_messages.get(&i).copied().unwrap_or_default(), 1);
            if received_messages.get(&i).copied().unwrap_or_default() == 1 {
                correct += 1;
            }
        }
    }
    error!("Correct messages {}/{}", correct, total);
    sleep(Duration::from_secs(5)).await;
    block_in_place(|| async {
        listener_runtime.shutdown_background();
    }).await;
}

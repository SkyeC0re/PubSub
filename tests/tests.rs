use std::{
    collections::{HashMap, HashSet},
    ops::{AddAssign, Deref},
    sync::Arc,
    time::Duration, error,
};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use jwt::{
    Header, PKeyWithDigest, SignWithKey, SigningAlgorithm, Token, VerifyWithKey, VerifyingAlgorithm,
};
use log::error;
use openssl::{hash::MessageDigest, pkey::PKey, rsa::Rsa};
use serde::{Deserialize, Serialize};
use tokio::{
    net::{TcpListener, TcpStream},
    runtime::{Handle, Runtime},
    sync::Mutex,
};
use tokio::{
    sync::RwLock,
    time::{sleep, timeout},
};
use tokio_tungstenite::{
    accept_async, connect_async, connect_async_with_config,
    tungstenite::{Error, Message},
    MaybeTlsStream, WebSocketStream,
};
use ws_man::{
    models::{JwtValidationMessage, TopicSpecifiers},
    websocket_server::{Client, ClientCallback, DynamicManager, TopicSpecifier, UniqId},
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

struct Verifier<T: VerifyingAlgorithm>(pub T);

// #[async_trait]
// impl<V: VerifyingAlgorithm + Send + Sync> ClientCallback<WebSocketClient, UniqId, Message, Error>
//     for Verifier<V>
// {
//     async fn callback(&self, ws_client: &WebSocketClient, message: Message) {
//         let text = match message {
//             Message::Text(text) => text,
//             _ => return,
//         };
//         let topic_specifiers: TopicSpecifiers = match text.verify_with_key(&self.0) {
//             Ok(v) => v,
//             Err(e) => {
//                 println!("Invalid JWT request: {:?}", e);
//                 return;
//             }
//         };
//         // Give tag set permissions to client websocket
//         for topic in topic_specifiers.topics {
//             ws_client.subscribe_to_topic(&topic).await;
//         }
//         let response = match serde_json::to_string(&JwtValidationMessage {
//             r#type: "jwt-validation".into(),
//             success: true,
//             token: text,
//         }) {
//             Ok(v) => Message::Text(v),
//             Err(e) => {
//                 error!("Failed to create JWT validation response: {:?}", e);
//                 return;
//             }
//         };
//         if let Err(e) = ws_client.send_message(&response).await {
//             error!("Failed to send validation response to client: {:?}", e);
//         }
//     }
// }

struct WebSocketClient {
    ws_client: RwLock<WebSocketStream<TcpStream>>,
}

#[async_trait]
impl Client<Message, ()> for WebSocketClient {
    async fn send_message(&self, message: &Message) -> Result<(), ()> {
        if self
            .ws_client
            .write()
            .await
            .send(message.clone())
            .await
            .is_err()
        {
            return Err(());
        }
        Ok(())
    }
}

#[tokio::test]
pub async fn test_client_add_and_message() {
    let _ = env_logger::try_init();
    let (signer, verifier) = generate_signer_verifier();

    let handle = Handle::current();

    let server = Arc::new(DynamicManager::new(handle.clone()));

    let server_clone = server.clone();
    let listener = generate_tcp_listener(PORT).await;
    handle.spawn(async move {
        let stream = if let Ok((stream, _)) = listener.accept().await {
            stream
        } else {
            return;
        };

        let ws = if let Ok(ws) = accept_async(stream).await {
            ws
        } else {
            return;
        };

        let client = server_clone
            .register_client(Arc::new(WebSocketClient {
                ws_client: RwLock::new(ws),
            }))
            .await;

        
        fn pass_through<T>(t: T) -> T {
            t
        }

        loop {
            let message = if let Ok(message) = client.ws_client.write().await.select_next_some().await {
                message
            } else {
                break;
            };
            let topic_specifiers: TopicSpecifiers =
                match message.to_text().unwrap().verify_with_key(&verifier) {
                    Ok(v) => v,
                    Err(e) => {
                        println!("Invalid JWT request: {:?}", e);
                        return;
                    }
                };

            for topic in topic_specifiers.topics {
                client.subscribe_to_topic(&topic).await;
            }

            error!("HERE");
            let _ = client.send_message(&Message::Ping(Vec::new())).await;
            error!("HEEERE");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
          
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

    error!("SENDING MESSAGE ACCROSS SERVER");
    server
        .send_message(
            &vec![TopicSpecifier::Subtopic {
                topic: "random".to_string(),
                specifier: Box::new(TopicSpecifier::ThisTopic),
            }],
            test_message.clone(),
        )
        .await;
    error!("FINAL MESSAGE AWAIT");
    let next_message = timeout(Duration::from_secs(5), client.select_next_some())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next_message, test_message);
}

// #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// pub async fn test_multiple_clients() {
//     let _ = env_logger::try_init();
//     let (signer, verifier) = generate_signer_verifier();

//     let server = WebSocketManager::new(Verifier(verifier)).await.unwrap();
//     let listener_port_start_range = 5050;
//     for i in 0..10 {
//         let listener = generate_tcp_listener(listener_port_start_range + i).await;
//         WebSocketManager::add_listener(&server, listener).await;
//     }

//     let max_clients_per_port = 25000;
//     let num_clients = 200000;
//     let client_buckets = 30;

//     fn get_topics(pos: i32) -> TopicSpecifiers {
//         let topics = (0..=pos)
//             .into_iter()
//             .map(|i| TopicSpecifier::Subtopic {
//                 topic: format!("tag-{}", i),
//                 specifier: Box::new(TopicSpecifier::ThisTopic),
//             })
//             .collect();

//         TopicSpecifiers { topics }
//     }

//     let join_handles: Vec<_> = (0..num_clients)
//         .into_iter()
//         .map(|i| {
//             let port = listener_port_start_range + (i / max_clients_per_port) as u16;
//             let topics = get_topics(i % client_buckets);
//             let jwt = generate_jwt(&signer, topics);
//             tokio::spawn(async move {
//                 let mut client = if let Some(client) = generate_client_ws_stream(port).await {
//                     client
//                 } else {
//                     return Err(format!("Unable to create client"));
//                 };

//                 if let Err(e) = client.send(Message::Text(jwt)).await {
//                     return Err(format!(
//                         "Client {}. Error sending jwt to server, : {:?}",
//                         i, e
//                     ));
//                 };
//                 let message =
//                     match timeout(Duration::from_secs(10), client.select_next_some()).await {
//                         Ok(Ok(resp)) => resp,
//                         Ok(Err(e)) => {
//                             return Err(format!(
//                                 "Client {}. Error during retrieval of validation response: {:?}",
//                                 i, e
//                             ))
//                         }
//                         Err(e) => {
//                             return Err(format!(
//                                 "Client {}. Retrieval of validation response took too long: {:?}",
//                                 i, e
//                             ))
//                         }
//                     };
//                 match message {
//                     Message::Text(jwt_resp) => {
//                         let resp: JwtValidationMessage = serde_json::from_str(&jwt_resp).unwrap();
//                         if !resp.success {
//                             return Err(format!(
//                                 "Client {}. Unsuccessful jwt validation response",
//                                 i
//                             ));
//                         }
//                     }
//                     _ => return Err(format!("Client {}. Invalid message type response", i)),
//                 };

//                 Ok((i, client))
//             })
//         })
//         .collect();

//     let mut clients = Vec::new();
//     for join_handle in join_handles {
//         if let Ok(Ok(join_handle)) = join_handle.await {
//             clients.push(join_handle);
//         }
//         // clients.push(join_handle.await.unwrap().unwrap());
//     }

//     error!(
//         "Successfully spawned {}/{} clients",
//         clients.len(),
//         num_clients
//     );

//     let total_messages_received = Arc::new(RwLock::new(0));

//     // start listeners
//     let received_messages = clients
//         .into_iter()
//         .map(|(i, client)| {
//             let received_messages = Arc::new(RwLock::new(HashMap::<i32, i32>::new()));
//             let received_messages_clone = received_messages.clone();
//             let total_messages_received_clone = total_messages_received.clone();
//             tokio::spawn(async move {
//                 client
//                     .for_each(|res| async {
//                         match res.unwrap() {
//                             Message::Text(serialized_message) => {
//                                 let message: TestMessage =
//                                     serde_json::from_str(&serialized_message).unwrap();
//                                 *received_messages_clone
//                                     .write()
//                                     .await
//                                     .entry(message.value)
//                                     .or_default() += 1;

//                                 total_messages_received_clone.write().await.add_assign(&1);
//                             }
//                             _ => unreachable!(),
//                         };
//                     })
//                     .await;
//             });
//             (i, received_messages)
//         })
//         .collect::<Vec<_>>();
//     for i in 0..client_buckets {
//         let server = server.clone();
//         tokio::spawn(async move {
//             server.send_message(
//                 vec![TopicSpecifier::Subtopic {
//                     topic: format!("tag-{}", i),
//                     specifier: Box::new(TopicSpecifier::ThisTopic),
//                 }],
//                 message_from_serializable(&TestMessage { value: i }),
//             );
//         });
//     }

//     // Wait for things to settle
//     for _ in 0..25 {
//         sleep(Duration::from_secs(1)).await;
//         error!(
//             "Messages received so far: {}",
//             total_messages_received.read().await.deref()
//         );
//     }

//     let mut total = 0;
//     let mut correct = 0;
//     for (i, received_messages) in received_messages {
//         let received_messages = received_messages.read().await;
//         // A client `i` should have received all the messages `<= i`
//         for i in 0..=i % client_buckets {
//             total += 1;
//             //assert_eq!(received_messages.get(&i).copied().unwrap_or_default(), 1);
//             if received_messages.get(&i).copied().unwrap_or_default() == 1 {
//                 correct += 1;
//             }
//         }
//     }
//     error!("Correct messages {}/{}", correct, total);
//     sleep(Duration::from_secs(30)).await;
// }

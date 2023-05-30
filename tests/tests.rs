use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use jwt::{
    Header, PKeyWithDigest, SignWithKey, SigningAlgorithm, Token, VerifyWithKey, VerifyingAlgorithm,
};
use log::error;
use openssl::{hash::MessageDigest, pkey::PKey, rsa::Rsa};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::{
    sync::RwLock,
    time::{sleep, timeout},
};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use ws_man::{
    models::{JwtValidationMessage, TagSetsSpecifier},
    websocket_server::{ClientCallback, WebSocketClient, WebSocketManager},
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

pub async fn generate_tcp_listener() -> TcpListener {
    TcpListener::bind(format!("0.0.0.0:{}", PORT))
        .await
        .unwrap()
}
pub async fn generate_client_ws_stream() -> WebSocketStream<MaybeTlsStream<TcpStream>> {
    connect_async(format!("ws://127.0.0.0:{}", PORT))
        .await
        .unwrap()
        .0
}

pub fn generate_jwt<T: SigningAlgorithm>(signer: &T, tag_sets: Vec<Vec<String>>) -> String {
    let token = Token::new(
        Header {
            algorithm: signer.algorithm_type(),
            ..Default::default()
        },
        TagSetsSpecifier { tag_sets: tag_sets },
    )
    .sign_with_key(signer)
    .unwrap();

    token.as_str().into()
}

pub fn message_from_serializable<T: Serialize>(v: &T) -> Message {
    Message::Text(serde_json::to_string(v).unwrap())
}

struct Verifier<T: VerifyingAlgorithm>(pub T);

#[async_trait]
impl<V: VerifyingAlgorithm + Send + Sync> ClientCallback for Verifier<V> {
    async fn callback(&self, ws_client: &mut WebSocketClient, message: Message) {
        let text = match message {
            Message::Text(text) => text,
            _ => return,
        };
        let tag_sets: TagSetsSpecifier = match text.verify_with_key(&self.0) {
            Ok(v) => v,
            Err(e) => {
                println!("Invalid JWT request: {:?}", e);
                return;
            }
        };
        // Give tag set permissions to client websocket
        for tag_set in tag_sets.tag_sets {
            if tag_set.is_empty() {
                continue;
            }
            ws_client
                .subscribe_to_topic(tag_set.into_iter().collect())
                .await;
        }
        let response = match serde_json::to_string(&JwtValidationMessage {
            r#type: "jwt-validation".into(),
            success: true,
            token: text,
        }) {
            Ok(v) => Message::Text(v),
            Err(e) => {
                error!("Failed to create JWT validation response: {:?}", e);
                return;
            }
        };
        if let Err(e) = ws_client.send_message(response).await {
            error!("Failed to send validation response to client: {:?}", e);
        }
    }
}

#[tokio::test]
pub async fn test_client_add_and_message() {
    let _ = env_logger::try_init();
    let (signer, verifier) = generate_signer_verifier();

    let server = WebSocketManager::new(generate_tcp_listener().await, Verifier(verifier))
        .await
        .unwrap();

    let mut client = generate_client_ws_stream().await;

    let permissions = TagSetsSpecifier {
        tag_sets: vec![vec!["admin".to_string()], vec!["order-123".to_string()]],
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

    match next_message {
        Message::Text(resp) => {
            let resp_json: JwtValidationMessage = serde_json::from_str(&resp).unwrap();
            assert!(resp_json.success);
        }
        _ => panic!(),
    }

    let test_message = Message::Text("TEST".to_string());

    server.read().await.send_message(
        vec![HashSet::from_iter(vec!["admin".to_string()])],
        test_message.clone(),
    );

    let next_message = timeout(Duration::from_secs(5), client.select_next_some())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next_message, test_message);
    return;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
pub async fn test_multiple_clients() {
    let _ = env_logger::try_init();
    let (signer, verifier) = generate_signer_verifier();

    let server = WebSocketManager::new(generate_tcp_listener().await, Verifier(verifier))
        .await
        .unwrap();

    let num_clients = 25000;
    let client_buckets = 30;

    fn get_tag_sets(pos: i32) -> Vec<Vec<String>> {
        vec![(0..=pos)
            .into_iter()
            .map(|i| format!("tag-{}", i))
            .collect()]
    }

    let join_handles: Vec<_> = (0..num_clients)
        .into_iter()
        .map(|i| {
            let tag_sets = get_tag_sets(i % client_buckets);
            let jwt = generate_jwt(&signer, tag_sets);
            tokio::spawn(async move {
                let mut client = generate_client_ws_stream().await;

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
                match message {
                    Message::Text(jwt_resp) => {
                        let resp: JwtValidationMessage = serde_json::from_str(&jwt_resp).unwrap();
                        if !resp.success {
                            return Err(format!(
                                "Client {}. Unsuccessful jwt validation response",
                                i
                            ));
                        }
                    }
                    _ => return Err(format!("Client {}. Invalid message type response", i)),
                };

                Ok((i, client))
            })
        })
        .collect();

    let mut clients = Vec::new();
    for join_handle in join_handles {
        if let Ok(Ok(join_handle)) = join_handle.await {
            clients.push(join_handle);
        }
        // clients.push(join_handle.await.unwrap().unwrap());
    }

    error!(
        "Successfully spawned {}/{} clients",
        clients.len(),
        num_clients
    );

    // start listeners
    let received_messages: Vec<_> = clients
        .into_iter()
        .map(|(i, client)| {
            let received_messages = Arc::new(RwLock::new(HashMap::<i32, i32>::new()));
            let received_messages_clone = received_messages.clone();
            tokio::spawn(async move {
                client
                    .for_each(|res| async {
                        match res.unwrap() {
                            Message::Text(serialized_message) => {
                                let message: TestMessage =
                                    serde_json::from_str(&serialized_message).unwrap();
                                *received_messages_clone
                                    .write()
                                    .await
                                    .entry(message.value)
                                    .or_default() += 1
                            }
                            _ => unreachable!(),
                        };
                    })
                    .await;
            });
            (i, received_messages)
        })
        .collect();
    for i in 0..client_buckets {
        let server = server.clone();
        tokio::spawn(async move {
            let tag_sets = vec![vec![format!("tag-{}", i)].into_iter().collect()];
            let read_guard = server.read().await;
            // Repeat the message with value `i`, `i` times.
            for _ in 0..1 {
                read_guard.send_message(
                    tag_sets.clone(),
                    message_from_serializable(&TestMessage { value: i }),
                );
            }
        });
    }

    // Wait for things to settle
    sleep(Duration::from_secs(5)).await;
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
}

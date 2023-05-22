use std::{collections::HashSet, net::SocketAddr, time::Duration};

use async_tungstenite::{
    client_async,
    tokio::{connect_async, ClientStream, TokioAdapter},
    tungstenite::{client, Message},
    WebSocketStream,
};
use futures_util::{stream, SinkExt, StreamExt};
use jwt::{
    AlgorithmType, Header, PKeyWithDigest, SignWithKey, SigningAlgorithm, Token, VerifyingAlgorithm,
};
use log::error;
use openssl::{hash::MessageDigest, pkey::PKey, rsa::Rsa};
use serde::Serialize;
use tokio::time::timeout;
use tokio::{
    net::{TcpListener, TcpStream},
    time::Timeout,
};
use ws_man::{
    models::{JwtValidationMessage, TagSetsSpecifier},
    websocket_server::WebSocketManager,
};

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
pub async fn generate_client_ws_stream() -> WebSocketStream<TokioAdapter<TcpStream>> {
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

#[tokio::test]
pub async fn test_client_add_and_message() {
    let _ = env_logger::try_init();
    let (signer, verifier) = generate_signer_verifier();

    let server = WebSocketManager::new(generate_tcp_listener().await, verifier).await;

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

    server
        .read()
        .await
        .send_message(
            vec![HashSet::from_iter(vec!["admin".to_string()])],
            test_message.clone(),
        )
        .await;

    let next_message = timeout(Duration::from_secs(5), client.select_next_some())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next_message, test_message);
    return;
}

#[tokio::test]
pub async fn test_multiple_clients() {
    let _ = env_logger::try_init();
    let (signer, verifier) = generate_signer_verifier();

    let server = WebSocketManager::new(generate_tcp_listener().await, verifier).await;

    let mut client = generate_client_ws_stream().await;

    fn get_tag_sets(pos: i32) -> Vec<Vec<String>> {
        vec![(1..=pos)
            .into_iter()
            .map(|i| format!("tag-{}", i))
            .collect()]
    }

    let join_handles: Vec<_> = (1..=10)
        .into_iter()
        .map(|i| {
            let tag_sets = get_tag_sets(i);
            let jwt = generate_jwt(&signer, tag_sets);
            tokio::spawn(async move {
                let mut client = generate_client_ws_stream().await;

                if let Err(e) = client.send(Message::Text(jwt)).await {
                    return Err(format!(
                        "Client {}. Error sending jwt to server, : {:?}",
                        i, e
                    ));
                };
                let message = match timeout(Duration::from_secs(2), client.select_next_some()).await
                {
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

                Ok(client)
            })
        })
        .collect();

    let mut clients = Vec::new();
    for join_handle in join_handles {
        clients.push(join_handle.await.unwrap().unwrap());
    }
}

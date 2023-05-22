use std::time::Duration;

use jwt::PKeyWithDigest;
use openssl::hash::MessageDigest;
use openssl::pkey::{PKey, Public};
use openssl::rsa::Rsa;
use openssl::sha::sha256;
use tokio::net::TcpListener;
use tokio::time::sleep;
use ws_man::websocket_server::WebSocketManager;

mod models;

const BIND_ADDR: &str = "0.0.0.0:5000";

#[tokio::main]
async fn main() {
    let mut rng = rand::thread_rng();
    let bits = 2048;
    let rsa = openssl::rsa::Rsa::generate(2048).expect("failed to generate a key");
    let priv_key = PKey::from_rsa(rsa.clone()).unwrap();
    print!(
        "Private key generated:\n{}",
        String::from_utf8_lossy(priv_key.private_key_to_pem_pkcs8().unwrap().as_slice())
    );
    let pub_rsa = Rsa::public_key_from_pem(&rsa.public_key_to_pem().unwrap()).unwrap();
    print!(
        "Public key generated:\n{}",
        String::from_utf8_lossy(rsa.public_key_to_pem().unwrap().as_slice())
    );

    let verifier = PKeyWithDigest {
        digest: MessageDigest::sha256(),
        key: PKey::from_rsa(pub_rsa).unwrap(),
    };

    // Create websocket server
    let listener = TcpListener::bind(BIND_ADDR).await.unwrap();
    WebSocketManager::new(listener, verifier).await;
    println!("Created Manager, listening on address: {}", BIND_ADDR);
    sleep(Duration::from_secs(5000)).await;
}

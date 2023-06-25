use async_trait::async_trait;
use tokio::sync::Mutex;

#[async_trait]
pub trait Client<M, E>: Send + Sync {
    async fn send_message(&self, message: &M) -> Result<(), E>;
}

#[async_trait]
pub trait MutableClient<M, E>: Send + Sync {
    async fn send_message(&mut self, message: &M) -> Result<(), E>;
}

#[async_trait]
impl<C, M, E> Client<M, E> for Mutex<C>
where
    C: MutableClient<M, E> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    async fn send_message(&self, message: &M) -> Result<(), E> {
        self.lock().await.send_message(message).await
    }
}

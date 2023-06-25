use async_trait::async_trait;
use tokio::sync::Mutex;

#[async_trait]
pub trait Client<M>: Send + Sync {
    async fn send_message(&self, message: &M) -> Result<(), ()>;
}

#[async_trait]
pub trait MutableClient<M>: Send + Sync {
    async fn send_message(&mut self, message: &M) -> Result<(), ()>;
}

#[async_trait]
impl<C, M> Client<M> for Mutex<C>
where
    C: MutableClient<M> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    async fn send_message(&self, message: &M) -> Result<(), ()> {
        self.lock().await.send_message(message).await
    }
}

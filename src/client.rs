use std::pin::Pin;

use async_trait::async_trait;
use futures_util::Future;
use tokio::sync::Mutex;

// #[async_trait]
// pub trait Client<M>: Send + Sync {
//     async fn send_message(&self, message: &M) -> Result<(), ()>;
// }

#[async_trait]
pub trait Client<M>: Send + Sync {
    async fn produce_handle_message_event<'a>(
        &'a self,
        message: &'a M,
    ) -> Pin<Box<dyn Future<Output = Result<(), ()>> + Send + 'a>>;
}

// #[async_trait]
// pub trait MutableClient<M>: Send + Sync {
//     async fn send_message(&mut self, message: &M) -> Result<(), ()>;
// }

#[async_trait]
pub trait MutableClient<M>: Send + Sync {
    async fn produce_handle_message_event<'a>(
        &'a mut self,
        message: &'a M,
    ) -> Pin<Box<dyn Future<Output = Result<(), ()>> + Send + 'a>>;
}

#[async_trait]
impl<C, M> Client<M> for Mutex<C>
where
    C: MutableClient<M> + ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    async fn produce_handle_message_event<'a>(
        &'a self,
        message: &'a M,
    ) -> Pin<Box<dyn Future<Output = Result<(), ()>> + Send + 'a>> {
        let mut lock = self.lock().await;
        Box::pin(async move { lock.produce_handle_message_event(message).await.await })
    }
}

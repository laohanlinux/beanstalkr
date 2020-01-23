use async_trait::async_trait;

#[async_trait]
pub trait Backup {
    async fn write() -> Result<(), ::std::io::Error>;
}
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Error};
use tokio::sync::mpsc::UnboundedSender;
use tracing::debug;

#[derive(Debug, Clone)]
pub struct OnceChannel<T> {
    sent: Arc<AtomicBool>,
    sender: UnboundedSender<T>,
}

impl<T> OnceChannel<T> {
    pub fn new(tx: UnboundedSender<T>) -> OnceChannel<T> {
        OnceChannel {
            sent: Arc::new(AtomicBool::new(true)),
            sender: tx,
        }
    }

    pub fn open(&mut self) {
        let b = self
            .sent
            .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed);
        assert!(b.is_ok() && b.unwrap());
    }

    pub async fn send(&mut self, value: T) -> Result<(), Error> {
        let ret = self
            .sent
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed);
        if ret.is_err() || ret.unwrap_or(true) {
            return Err(anyhow!("channel has sent a value"));
        }
        debug!("Get once channel locker");
        self.sender
            .send(value)
            .map_err(|_| anyhow!("reserve reply channel closed"))?;
        Ok(())
    }
}

use async_std::channel::Sender;
use async_std::sync::Arc;
use failure::{self, err_msg, Error};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

#[derive(Debug, Clone)]
pub struct OnceChannel<T> {
    sent: Arc<AtomicBool>,
    sender: Sender<T>,
}

impl<T> OnceChannel<T> {
    pub fn new(tx: Sender<T>) -> OnceChannel<T> {
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
        if ret.is_err() {
            return Err(err_msg("channel has sent a value"));
        }
        if !ret.unwrap() {
            return Err(err_msg("channel has sent a value"));
        }

        debug!("Get once channel locker");
        _ = self.sender.send(value).await;
        return Ok(());
    }
}

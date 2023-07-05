use failure::{self, err_msg, Error, Fail};
use std::sync::Arc;

use crate::channel::Sender;
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
        let b = self.sent.compare_and_swap(true, false, Ordering::Relaxed);
        assert!(b);
    }

    pub async fn send(&mut self, value: T) -> Result<(), Error> {
        if self.sent.compare_and_swap(false, true, Ordering::Relaxed) == false {
            debug!("Get once channel locker");
            self.sender.send(value).await.unwrap();
            return Ok(());
        }
        Err(err_msg("channel has sent a value"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::channel;

    #[tokio::test]
    async fn it_clone() {
        let (tx, mut rx) = channel::<i32>(1);
        let mut ch = OnceChannel::new(tx);
        ch.open();
        ch.send(100).await.expect("TODO: panic message");
        rx.recv().await;
        println!("Hello Word");
    }
}

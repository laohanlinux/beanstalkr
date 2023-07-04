use async_std::channel::{self, Receiver, Sender};
use async_std::sync::Arc;
use failure::{self, err_msg, Error, Fail};
use futures::future::err;

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
            self.sender.send(value).await;
            return Ok(());
        }
        Err(err_msg("channel has sent a value"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;

    #[test]
    fn it_clone() {
        let mut runtime = task::Builder::new();
        let mut join = runtime
            .spawn(async move {
                let (tx, rx) = async_std::channel::bounded::<i32>(1);
                let mut ch = OnceChannel::new(tx);
                ch.send(100).await;
                rx.recv().await;
                println!("Hello Word");
            })
            .unwrap();
        join.task();
    }
}

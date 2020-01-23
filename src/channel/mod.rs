use futures::{channel::mpsc};

pub type Sender<T> = mpsc::UnboundedSender<T>;
pub type Receiver<T> = mpsc::UnboundedReceiver<T>;

pub type SenderReceiver<T> = mpsc::UnboundedSender<mpsc::Receiver<T>>;
pub type ReceiverSender<T> = mpsc::UnboundedSender<mpsc::Sender<T>>;
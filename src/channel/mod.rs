pub type UnBoundedSender<T> = tokio::sync::mpsc::UnboundedSender<T>;
pub type UnBoundedReceiver<T> = tokio::sync::mpsc::UnboundedReceiver<T>;
pub type Sender<T> = tokio::sync::mpsc::Sender<T>;
pub type Receiver<T> = tokio::sync::mpsc::Receiver<T>;

pub type SenderReceiver<T> = tokio::sync::mpsc::UnboundedReceiver<tokio::sync::mpsc::Receiver<T>>;
pub type ReceiverSender<T> = tokio::sync::mpsc::UnboundedReceiver<tokio::sync::mpsc::Sender<T>>;

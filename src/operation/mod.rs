use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::architecture::cmd::{Command, CMD};
use failure::{err_msg, Error, Fail};

use uuid::Uuid;

pub mod dispatch;
pub mod once_channel;

use crate::architecture::error::ProtocolError;
use crate::architecture::job::random_clients;
use crate::architecture::tube::ClientId;
use crate::channel::{Receiver, UnBoundedReceiver, UnBoundedSender};
use crate::operation::once_channel::OnceChannel;
use dispatch::Dispatch;
use dispatch::TubeSender;
use downcast_rs::Downcast;
use failure::_core::iter::once;
use std::borrow::Cow;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::spawn;
use tokio::sync::mpsc::{channel, unbounded_channel};
use tokio::sync::{Mutex, MutexGuard};
use tokio_util::codec::{Framed, LinesCodec};
use futures::SinkExt;

pub struct ClientHandler {
    client_id: ClientId,
    use_tube: String,
    conn: Framed<TcpStream, LinesCodec>,
    dispatch: Arc<Mutex<Dispatch>>,
    tx: Option<UnBoundedSender<Command>>,
    rx: Option<UnBoundedReceiver<Command>>,
    tube_rx: HashMap<String, TubeSender>,
    reserve_tx: OnceChannel<Command>,
    reserve_rx: Receiver<Command>,
    watch_tubes: HashMap<String, ()>,
}

impl ClientHandler {
    pub fn new(conn: Framed<TcpStream, LinesCodec>, dispatch: Arc<Mutex<Dispatch>>) -> Self {
        let (tx, rx) = unbounded_channel();
        let (reserve_tx, reserve_rx) = channel(1);
        let once_channel = OnceChannel::new(reserve_tx);
        let mut watch_tubes = HashMap::new();
        watch_tubes.insert("default".to_string(), ());
        ClientHandler {
            client_id: random_clients(),
            use_tube: "default".to_string(),
            conn,
            dispatch,
            tx: Some(tx),
            rx: Some(rx),
            tube_rx: HashMap::new(),
            reserve_tx: once_channel,
            reserve_rx,
            watch_tubes,
        }
    }

    pub async fn spawn_start(&mut self) -> Result<(), Error> {
        // register
        self.handle_base_command(Command::default()).await.unwrap();
        let ret = self.parse_command().await;
        let mut dispatch: MutexGuard<Dispatch> = self.dispatch.lock().await;
        dispatch.drop_client(&self.use_tube, self.client_id).await;
        info!("Client offline");
        ret
    }

    // Parse client command ASCII protocol
    async fn parse_command(&mut self) -> Result<(), Error> {
        use tokio_stream::StreamExt;
        let mut command: Command = Default::default();
        while let Some(line) = self.conn.next().await {
            if line.is_err() {
                info!("failed to read line command, {}", line.unwrap_err());
                continue;
            }
            let line = line.unwrap();
            debug!("read a new command: {}", line);
            match command.parse(line.as_ref()) {
                Ok(true) => {
                    command = self.handle_base_command(command).await?;
                    self.handle_reply(&mut command).await?;
                    command = Command::default();
                }
                Ok(false) => {
                    debug!("Continue read ...");
                }
                err => {
                    command = Default::default();
                    self.handle_reply_err(err.unwrap_err()).await?;
                }
            }
        }
        Ok(())
    }

    async fn handle_reply(&mut self, command: &mut Command) -> Result<(), Error> {
        loop {
            let (more, reply) = command.reply().await;
            self.conn.send(&reply).await?;
            // writer.write_all((reply + "\r\n").as_bytes()).await?;
            if !more {
                break;
            }
        }
        Ok(())
    }

    async fn handle_reply_err(&mut self, err: Error) -> Result<(), Error> {
        self.conn.send( &err.to_string()).await?;
        Ok(())
    }

    async fn handle_base_command(&mut self, mut command: Command) -> Result<Command, Error> {
        let cmd = CMD::from_str(&command.name).unwrap();
        let tube_name = command.params.get("tube").unwrap();
        match cmd {
            CMD::Use => {
                self.use_tube = tube_name.clone();
                if self.tube_rx.contains_key(tube_name) {
                    return Ok(command);
                }
                let mut dispatch: MutexGuard<Dispatch> = self.dispatch.lock().await;
                let tx = self.tx.as_ref().unwrap();
                let tube_ch = dispatch
                    .spawn_tube(
                        tube_name.clone(),
                        self.client_id.clone(),
                        tx.clone(),
                        self.reserve_tx.clone(),
                    )
                    .await?;
                self.tube_rx.insert(self.use_tube.clone(), tube_ch);
                Ok(command)
            }
            CMD::Watch => {
                let count = self.watch_tubes.len() - 1;
                if self.tube_rx.contains_key(tube_name) {
                    command
                        .params
                        .insert("count".to_owned(), format!("{}", count));
                    return Ok(command);
                }
                let mut dispatch: MutexGuard<Dispatch> = self.dispatch.lock().await;
                let tx = self.tx.as_ref().unwrap();
                let tube_ch = dispatch
                    .spawn_tube(
                        tube_name.clone(),
                        self.client_id.clone(),
                        tx.clone(),
                        self.reserve_tx.clone(),
                    )
                    .await?;
                self.tube_rx.insert(tube_name.clone(), tube_ch);
                self.watch_tubes.insert(tube_name.clone(), ());
                command
                    .params
                    .insert("count".to_owned(), format!("{}", count + 1));
                Ok(command)
            }
            CMD::Ignore => {
                let count = self.watch_tubes.len() - 1;
                if tube_name == "default" {
                    return Ok(command.wrap_result(Err(ProtocolError::NotIgnored)));
                }
                if !self.watch_tubes.contains_key(tube_name) {
                    command
                        .params
                        .insert("count".to_owned(), format!("{}", count));
                    return Ok(command);
                }

                self.watch_tubes.remove(tube_name);
                let tube_tx = self.tube_rx.get_mut(&self.use_tube).unwrap();
                tube_tx
                    .send((self.client_id.clone(), command.clone()))
                    .unwrap();
                let rx = self.rx.as_mut().unwrap();
                let mut command: Command = rx.recv().await.unwrap();
                command
                    .params
                    .insert("count".to_owned(), format!("{}", count));
                Ok(command)
            }
            CMD::Reserve | CMD::ReserveWithTimeout => {
                let client_id = self.client_id.clone();
                self.reserve_tx.open();
                for (tube_name, _) in self.watch_tubes.iter_mut() {
                    debug!("watch {}", tube_name);
                    let tube_ch = self.tube_rx.get_mut(tube_name).unwrap();
                    let mut tube_ch = tube_ch.clone();
                    let command = command.clone();
                    spawn(async move {
                        tube_ch.send((client_id, command)).unwrap();
                    });
                    debug!("send a reserve inner command to {}", tube_name);
                }
                Ok(self.reserve_rx.recv().await.unwrap())
            }
            CMD::ListTubesWatched => {
                let lists: Vec<String> = self.watch_tubes.keys().map(|key| key.clone()).collect();
                let lists = serde_yaml::to_string(&lists).unwrap();
                command.yaml = Some(lists);
                Ok(command)
            }
            CMD::ListTubes => {
                let dispatch = self.dispatch.lock().await;
                let (count, tubes) = dispatch.list_tubes();
                let lists = serde_yaml::to_string(&tubes).unwrap();
                command.yaml = Some(lists);
                command
                    .params
                    .insert("count".to_owned(), format!("{}", count));
                Ok(command)
            }
            CMD::ListTubeUsed => {
                command
                    .params
                    .insert("tube".to_owned(), self.use_tube.clone());
                Ok(command)
            }
            CMD::Quit => Err(err_msg("Client quit")),
            _ => {
                let tube_tx = self.tube_rx.get_mut(&self.use_tube).unwrap();
                tube_tx
                    .send((self.client_id.clone(), command.clone()))
                    .unwrap();
                let rx = self.rx.as_mut().unwrap();
                let command = rx.recv().await.unwrap();
                Ok(command)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use beanstalkc::Beanstalkc;
    use chrono::Local;
    use std::thread::{self, sleep, Thread};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};


    #[test]
    fn it_double_tube() {
        let mut conn = connect();
        let id = conn
            .put(
                b"hello word1",
                1,
                Duration::from_secs(3),
                Duration::from_secs(5),
            )
            .unwrap();
        conn.use_tube("ok").unwrap();
        let id = conn
            .put(
                b"hello word2",
                1,
                Duration::from_secs(3),
                Duration::from_secs(5),
            )
            .unwrap();
        let job = conn.reserve().unwrap();
        println!("{}", job.id());
        let job = conn.reserve().unwrap();
        println!("{}", job.id());
    }

    #[test]
    fn it_reserve() {
        let mut conn = connect();
        let id = conn
            .put(
                b"hello word1",
                1,
                Duration::from_secs(3),
                Duration::from_secs(5),
            )
            .unwrap();
        let job = conn.reserve().unwrap();
        println!("{}", job.id());
    }

    #[test]
    fn it_reserve_with_timeout() {
        let mut conn = connect();
        let id = conn
            .put(
                b"hello word1",
                1,
                Duration::from_secs(3),
                Duration::from_secs(5),
            )
            .unwrap();
        let job = conn.reserve_with_timeout(Duration::from_secs(5)).unwrap();
        let id = job.id();
        println!("{}", job.id());
        let b = conn.reserve_with_timeout(Duration::from_secs(5)).is_ok();
        assert!(b);
    }

    #[test]
    fn it_watch() {
        let mut conn = connect();
        //let id = conn.watch("ok").unwrap();
    }

    #[test]
    fn it_delete() {
        let mut conn = connect();
        let id = conn
            .put(
                b"hello word1",
                1,
                Duration::from_secs(3),
                Duration::from_secs(5),
            )
            .unwrap();
        let job = conn.reserve().unwrap();
        let id = job.id();
        println!("{}", id);
        let b = conn.delete(id).is_ok();
        assert!(b);
    }

    #[test]
    fn it_delete2() {
        let mut conn = connect();
        conn.use_tube("a");
        //        let id = conn.put(b"hello word1", 1, Duration::from_secs(3), Duration::from_secs(5)).unwrap();
        for i in 0..100 {
            let job = conn.reserve().unwrap();
            let id = job.id();
            println!("{}", id);
            let b = conn.delete(id).is_ok();
            //            assert!(b);
        }
    }

    #[test]
    fn it_kick() {
        let mut conn = connect();
        let tube = format!("tube_{}", Local::now().timestamp_nanos());
        conn.use_tube(tube.as_str()).unwrap();
        let id = conn
            .put(b"hello", 1, Duration::from_secs(30), Duration::from_secs(5))
            .unwrap();
        let count = conn.kick(1).unwrap();
        assert_eq!(1, count);
    }

    #[test]
    fn it_pause_job() {
        let mut conn = connect();
        let tube = format!("tube_{}", Local::now().timestamp_nanos());
        conn.use_tube(tube.as_str()).unwrap();

        let tm = Local::now().timestamp();
        let id = conn
            .put(b"hello", 1, Duration::from_secs(5), Duration::from_secs(5))
            .unwrap();
        conn.pause_tube(tube.as_str(), Duration::from_secs(100))
            .unwrap();
        conn.reserve().unwrap();
        println!("{}", Local::now().timestamp() - tm);
    }

    #[test]
    fn it_list_tube_used() {
        let mut conn = connect();
        conn.use_tube("hello".as_ref()).unwrap();
        let tube_name = conn.using().unwrap();
        assert_eq!(tube_name, "hello".to_string());
    }

    #[test]
    fn it_batch_put() {
        for i in 0..100 {
            let mut conn = connect();
            println!(
                "-->{}",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
            );
            for i in 0..100 {
                let id = conn
                    .put(
                        b"hello",
                        1,
                        Duration::from_secs(i) / 13,
                        Duration::from_secs(5),
                    )
                    .unwrap();
                println!("{}", id);
            }
            println!(
                "<--{}",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
            );
        }
        thread::spawn(move || {});
    }

    fn connect() -> Beanstalkc {
        Beanstalkc::new()
            .host("127.0.0.1")
            .port(11300)
            .connection_timeout(Some(Duration::from_secs(3)))
            .connect()
            .expect("connect failed")
    }
}

use async_std::prelude::*;
use async_std::task;
use async_std::stream;
use async_std::io::{self, BufReader};
use async_std::net::{TcpListener, TcpStream};
use async_std::sync::{Arc, Mutex, MutexGuard, Sender, Receiver, channel};

use std::str::FromStr;
use std::collections::HashMap;
use std::time::Duration;
use std::sync::atomic::{AtomicU64, Ordering};

use uuid::Uuid;
use failure::{Error, err_msg, Fail};
use crate::architecture::cmd::{Command, CMD};
use futures::{channel::mpsc::{self, UnboundedSender, UnboundedReceiver}, select, FutureExt, SinkExt};

pub mod dispatch;
pub mod once_channel;

use dispatch::Dispatch;
use dispatch::TubeSender;
use crate::architecture::tube::ClientId;
use crate::architecture::job::random_clients;
use crate::operation::once_channel::OnceChannel;
use failure::_core::iter::once;
use std::borrow::Cow;
use crate::architecture::error::ProtocolError;

pub struct ClientHandler {
    client_id: ClientId,
    use_tube: String,
    conn: Arc<TcpStream>,
    dispatch: Arc<Mutex<Dispatch>>,
    tx: Option<UnboundedSender<Command>>,
    rx: Option<UnboundedReceiver<Command>>,
    tube_rx: HashMap<String, TubeSender>,
    reserve_tx: OnceChannel<Command>,
    reserve_rx: Receiver<Command>,
    watch_tubes: HashMap<String, ()>,
}


impl ClientHandler {
    pub fn new(conn: Arc<TcpStream>, dispatch: Arc<Mutex<Dispatch>>) -> Self {
        let (tx, rx) = mpsc::unbounded();
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

    async fn parse_command(&mut self) -> Result<(), Error> {
        let conn = self.conn.clone();
        let reader = BufReader::new(&*conn);
        let mut lines = reader.lines();
        let mut command: Command = Default::default();
        while let Some(line) = lines.next().await {
            let line = line?;
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
        let stream = self.conn.clone();
        let mut writer = &*stream;
        loop {
            let (more, reply) = command.reply().await;
            writer.write_all((reply + "\r\n").as_bytes()).await?;
            if !more {
                break;
            }
        }
        Ok(())
    }

    async fn handle_reply_err(&mut self, err: Error) -> Result<(), Error> {
        let stream = self.conn.clone();
        let mut writer = &*stream;
        writer.write_all(format!("{}\r\n", err).as_bytes()).await?;
        Ok(())
    }

    async fn handle_base_command(&mut self, mut command: Command) -> Result<Command, Error> {
        let cmd = CMD::from_str(&command.name).unwrap();
        let tube_name = command.params.get("tube").unwrap();
        debug!("client handler handle command: {:?}, tube: {:?}", tube_name, command);
        match cmd {
            CMD::Use => {
                self.use_tube = tube_name.clone();
                if self.tube_rx.contains_key(tube_name) {
                    return Ok(command);
                }
                let mut dispatch: MutexGuard<Dispatch> = self.dispatch.lock().await;
                let tx = self.tx.as_ref().unwrap();
                let tube_ch = dispatch.spawn_tube(tube_name.clone(), self.client_id.clone(), tx.clone(), self.reserve_tx.clone()).await?;
                self.tube_rx.insert(self.use_tube.clone(), tube_ch);
                Ok(command)
            }
            CMD::Watch => {
                let count = self.watch_tubes.len() - 1;
                if self.tube_rx.contains_key(tube_name) {
                    command.params.insert("count".to_owned(), format!("{}", count));
                    return Ok(command);
                }
                let mut dispatch: MutexGuard<Dispatch> = self.dispatch.lock().await;
                let tx = self.tx.as_ref().unwrap();
                let tube_ch = dispatch.spawn_tube(tube_name.clone(), self.client_id.clone(), tx.clone(), self.reserve_tx.clone()).await?;
                self.tube_rx.insert(tube_name.clone(), tube_ch);
                self.watch_tubes.insert(tube_name.clone(), ());
                command.params.insert("count".to_owned(), format!("{}", count + 1));
                Ok(command)
            }
            CMD::Ignore => {
                let count = self.watch_tubes.len() - 1;
                if tube_name == "default" {
                    return Ok(command.wrap_result(Err(ProtocolError::NotIgnored)));
                }
                if !self.watch_tubes.contains_key(tube_name) {
                    command.params.insert("count".to_owned(), format!("{}", count));
                    return Ok(command);
                }

                self.watch_tubes.remove(tube_name);
                let tube_tx = self.tube_rx.get_mut(&self.use_tube).unwrap();
                tube_tx.send((self.client_id.clone(), command.clone())).await.unwrap();
                let rx = self.rx.as_mut().unwrap();
                let mut command: Command = rx.next().await.unwrap();
                command.params.insert("count".to_owned(), format!("{}", count));
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
                    task::spawn(async move {
                        tube_ch.send((client_id, command)).await.unwrap();
                    });
                    debug!("send a reserve inner command to {}", tube_name);
                }
                let conn = self.conn.clone();
                let rx = self.reserve_rx.clone();
                loop {
                    let mut buffer = vec![0; 1];
                    select! {
                        // FIXME: use other way to do that?
                        n = conn.peek(&mut buffer).fuse() => {
                            if n.is_err() || n.unwrap() == 0 {
                                return Err(err_msg("Client Quit"));
                            }
                        }
                        command = rx.recv().fuse() => {
                            return Ok(command.unwrap());
                        }
                    }
                }
            }
            CMD::Peek => {
                Ok(command)
            }
            CMD::Quit => {
                Err(err_msg("Client quit"))
            }
            _ => {
                let tube_tx = self.tube_rx.get_mut(&self.use_tube).unwrap();
                tube_tx.send((self.client_id.clone(), command.clone())).await.unwrap();
                let rx = self.rx.as_mut().unwrap();
                let command = rx.next().await.unwrap();
                Ok(command)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use beanstalkc::Beanstalkc;
    use std::thread::{self, sleep};
    use tokio::prelude::*;
    use tokio::time::delay_for;

    use std::time::{Duration, Instant};


    #[test]
    fn it_double_tube() {
        let mut conn = Beanstalkc::new()
            .host("localhost")
            .port(8080)
            .connection_timeout(Some(Duration::from_secs(3)))
            .connect().expect("connect failed");
        let id = conn.put(b"hello word1", 1, Duration::from_secs(3), Duration::from_secs(5)).unwrap();
        conn.use_tube("ok").unwrap();
        let id = conn.put(b"hello word2", 1, Duration::from_secs(3), Duration::from_secs(5)).unwrap();
        let job = conn.reserve().unwrap();
        println!("{}", job.id());
        let job = conn.reserve().unwrap();
        println!("{}", job.id());
    }

    #[test]
    fn it_reserve() {
        let mut conn = Beanstalkc::new()
            .host("localhost")
            .port(8080)
            .connection_timeout(Some(Duration::from_secs(3)))
            .connect().expect("connect failed");
        let id = conn.put(b"hello word1", 1, Duration::from_secs(3), Duration::from_secs(5)).unwrap();
        let job = conn.reserve().unwrap();
        println!("{}", job.id());
    }

    #[test]
    fn it_reserve_with_timeout() {
        let mut conn = Beanstalkc::new()
            .host("localhost")
            .port(8080)
            .connection_timeout(Some(Duration::from_secs(3)))
            .connect().expect("connect failed");
        let id = conn.put(b"hello word1", 1, Duration::from_secs(3), Duration::from_secs(5)).unwrap();
        let job = conn.reserve_with_timeout(Duration::from_secs(5)).unwrap();
        let id = job.id();
        println!("{}", job.id());
        let b = conn.reserve_with_timeout(Duration::from_secs(5)).is_ok();
        assert!(b);
    }

    #[test]
    fn it_watch() {
        let mut conn = Beanstalkc::new()
            .host("localhost")
            .port(8080)
            .connection_timeout(Some(Duration::from_secs(3)))
            .connect().expect("connect failed");
        let id = conn.watch("ok").unwrap();
    }

    #[test]
    fn it_delete() {
        let mut conn = Beanstalkc::new()
            .host("localhost")
            .port(8080)
            .connection_timeout(Some(Duration::from_secs(3)))
            .connect().expect("connect failed");
        let id = conn.put(b"hello word1", 1, Duration::from_secs(3), Duration::from_secs(5)).unwrap();
        let job = conn.reserve().unwrap();
        let id = job.id();
        println!("{}", id);
        let b = conn.delete(id).is_ok();
        assert!(b);
    }

    #[test]
    fn it_client1() {
        let mut v = vec![];
//        for i in 0..10 {
//            v.push(thread::spawn(move || {
//                let mut conn = Beanstalkc::new()
//                    .host("localhost")
//                    .port(8080)
//                    .connection_timeout(Some(Duration::from_secs(3)))
//                    .connect().expect("connect failed");
//                while let Ok(id) = conn.reserve().map(|job| job.id()) {
//                    println!("{}", id);
//                    conn.delete(id).unwrap();
//                }
//            }));
//        }

        for i in 0..2 {
            v.push(thread::spawn(move || {
                let mut conn = Beanstalkc::new()
                    .host("localhost")
                    .port(8080)
                    .connection_timeout(Some(Duration::from_secs(3)))
                    .connect().expect("connect failed");
                while let Ok(id) = conn.reserve().map(|job| job.id()) {
                    println!("{}", id);
                    conn.delete(id).unwrap();
                }
            }));
        }
        sleep(Duration::from_secs(100));
    }

    #[test]
    fn it_client() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();

//        for i in 0..100 {
//            rt.spawn(async move {

//            });
//        };

        rt.block_on(async move {
            for i in 0..10 {
                tokio::spawn(async move {
                    let mut conn = Beanstalkc::new()
                        .host("localhost")
                        .port(8080)
                        .connection_timeout(Some(Duration::from_secs(3)))
                        .connect().expect("connect failed");

                    for i in 0..100 {
                        let id = conn.put(b"hello word", 1, Duration::from_secs(3), Duration::from_secs(5)).unwrap();
                        println!("job id {}", id);
                    }
                });
            }
            for i in 0..1000 {
                tokio::spawn(async move {
                    let mut conn = Beanstalkc::new()
                        .host("localhost")
                        .port(8080)
                        .connection_timeout(Some(Duration::from_secs(3)))
                        .connect().expect("connect failed");
                    while let Ok(id) = conn.reserve().map(|job| job.id()) {
                        println!("{}", id);
                        conn.delete(id).unwrap();
                    }
                });
            }

            delay_for(Duration::from_secs(300)).await;
        });
    }
}
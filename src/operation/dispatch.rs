use crate::architecture::cmd::{Command, CMD};
use crate::architecture::job::{AwaitingClient, Job};
use crate::architecture::tube::{ClientId, PriorityQueue, Tube};
use crate::backend::min_heap::MinHeap;
use crate::channel::{Receiver, Sender, SenderReceiver};

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::operation::once_channel::OnceChannel;
use async_std::io::{self, BufReader};
use async_std::net::{TcpListener, TcpStream};
use async_std::prelude::*;
use async_std::stream;
use async_std::task;
use failure::{err_msg, Error, Fail};
use futures::{
    channel::mpsc::{self, UnboundedReceiver, UnboundedSender},
    select, FutureExt, SinkExt,
};

pub type CmdReceiver = UnboundedReceiver<Command>;
pub type CmdSender = UnboundedSender<Command>;

pub enum CmdChannel {
    ReserveCmdSender(Option<OnceChannel<Command>>),
    BaseCmdSender(Option<CmdSender>),
}

pub type TubeSender = UnboundedSender<(ClientId, Command)>;
pub type TubeReceiver = UnboundedReceiver<(ClientId, Command)>;

type InnerSender = UnboundedSender<TubeItem>;
type InnerReceiver = UnboundedReceiver<TubeItem>;

#[derive(Clone)]
enum TubeItem {
    Add(
        ClientId,
        CmdSender,
        UnboundedSender<()>,
        OnceChannel<Command>,
    ),
    Delete(ClientId, UnboundedSender<()>),
    Stop,
}

pub struct Dispatch {
    stop: Vec<Sender<()>>,
    tube_ch: HashMap<String, InnerSender>,
    cmd_tx: HashMap<String, UnboundedSender<(ClientId, Command)>>,
}

impl Dispatch {
    pub fn new() -> Dispatch {
        Dispatch {
            stop: Vec::new(),
            tube_ch: HashMap::new(),
            cmd_tx: HashMap::new(),
        }
    }

    pub async fn spawn_tube(
        &mut self,
        name: String,
        client_id: ClientId,
        reply: CmdSender,
        reserve_reply: OnceChannel<Command>,
    ) -> Result<TubeSender, Error> {
        debug!("spawn a tube: {}", name);
        if let Some(cmd_sender) = self.cmd_tx.get(&name) {
            let (mut callback_tx, mut callback_rx) = mpsc::unbounded::<()>();
            self.tube_ch
                .get(&name)
                .unwrap()
                .send(TubeItem::Add(client_id, reply, callback_tx, reserve_reply))
                .await
                .unwrap();
            callback_rx.next().await;
            return Ok(cmd_sender.clone());
        }
        let (mut callback_tx, mut callback_rx) = mpsc::unbounded::<()>();
        let (mut tube_tx, mut tube_rx) = mpsc::unbounded::<TubeItem>();
        self.tube_ch.insert(name.clone(), tube_tx.clone());
        let (mut cmd_tx, mut cmd_rx) = mpsc::unbounded::<(ClientId, Command)>();
        self.cmd_tx.insert(name.clone(), cmd_tx.clone());
        self.task(name, tube_rx, cmd_rx);
        tube_tx
            .send(TubeItem::Add(
                client_id.clone(),
                reply,
                callback_tx,
                reserve_reply,
            ))
            .await
            .unwrap();
        callback_rx.next().await;
        Ok(cmd_tx)
    }

    pub async fn drop_client(&mut self, name: &String, client_id: ClientId) {
        debug!("Drop {} from {}", client_id, name);
        let (mut callback_tx, mut callback_rx) = mpsc::unbounded::<()>();
        if let Some(tube_tx) = self.tube_ch.get_mut(name) {
            tube_tx
                .send(TubeItem::Delete(client_id, callback_tx))
                .await
                .unwrap();
            callback_rx.next().await.unwrap();
        }
    }

    pub fn list_tubes(&self) -> (usize, Vec<String>) {
        (
            self.cmd_tx.len(),
            self.cmd_tx.keys().map(|key| key.clone()).collect(),
        )
    }

    fn task(&mut self, tube_name: String, mut tube_rx: InnerReceiver, mut cmd_rx: TubeReceiver) {
        task::spawn(async move {
            // TODO Optimize
            let mut clients: HashMap<ClientId, (CmdSender, OnceChannel<Command>)> = HashMap::new();
            let mut tube: Tube<MinHeap<Job>, MinHeap<AwaitingClient>> = Tube::new(
                tube_name.clone(),
                MinHeap::new("".to_string()),
                MinHeap::new("".to_string()),
                MinHeap::new("".to_string()),
                MinHeap::new("".to_string()),
                MinHeap::new("".to_string()),
            );
            let mut interval = stream::interval(Duration::from_millis(50));
            let (mut _tx, mut _rx) = mpsc::unbounded::<()>();
            loop {
                select! {
                    _ = interval.next().fuse() => {
                        tube.process().await;
                        tube.process_timed_clients().await;
                    },
                    _ = _rx.next().fuse() => {
                        tube.process().await;
                        tube.process_timed_clients().await;
                    }
                    cmd = tube_rx.next().fuse() => match cmd {
                        Some(cmd) => {
                            match cmd {
                                TubeItem::Add(client_id, ch, mut cb, mut reserve_cb_rx) => {
                                    debug!("Insert a new client:{} into tube:{}", client_id, tube_name);
                                    clients.insert(client_id, (ch, reserve_cb_rx));
                                    cb.send(()).await;
                                },
                                TubeItem::Delete(client_id, mut cb) =>{
                                    debug!("Remove client {}", client_id);
                                    clients.remove(&client_id);
                                    tube.drop_client(&client_id);
                                    cb.send(()).await;
                                },
                                TubeItem::Stop => {
                                    info!("Stop tube: {}", tube_name.clone());
                                    break;
                                },
                            }
                        },
                        _ => unreachable!()
                     },
                    cmd = cmd_rx.next().fuse() => match cmd {
                        Some(command) => {
                             Self::handle_command(&mut clients, &mut tube, command.clone()).await;
                             let cmd = CMD::from_str(&command.1.name).unwrap();
                             if cmd == CMD::ReserveWithTimeout || cmd == CMD::Reserve{
                                _tx.send(()).await;
                             }
                        },
                        None =>{},
                    }
                }
            }
        });
    }

    async fn handle_command<
        J: PriorityQueue<Job> + Send + 'static,
        A: PriorityQueue<AwaitingClient> + Send + 'static,
    >(
        clients: &mut HashMap<ClientId, (CmdSender, OnceChannel<Command>)>,
        tube: &mut Tube<J, A>,
        mut command: (ClientId, Command),
    ) {
        let cmd = CMD::from_str(&command.1.name).unwrap();
        if let Some((ref mut tx, ref reserve_tx)) = clients.get_mut(&command.0) {
            match cmd {
                CMD::Put => {
                    tube.put(command.1.clone()).unwrap();
                    tx.send(command.1).await.unwrap();
                }
                CMD::Reserve => {
                    tube.reserve(command.0.clone(), command.1.clone(), reserve_tx.clone())
                        .unwrap();
                }
                CMD::ReserveWithTimeout => {
                    tube.reserve_with_timeout(
                        command.0.clone(),
                        command.1.clone(),
                        reserve_tx.clone(),
                    )
                    .unwrap();
                }
                CMD::Delete => {
                    // delete buried, reserved
                    command.1.err = tube.delete(&command.1);
                    tx.send(command.1).await.unwrap();
                }
                CMD::Release => {
                    command.1.err = tube.release(&command.1);
                    tx.send(command.1).await.unwrap();
                }
                CMD::Bury => {
                    command.1.err = tube.buried(&command.1);
                    tx.send(command.1).await.unwrap();
                }
                CMD::Kick => {
                    match tube.kick(&command.1) {
                        Ok(count) => {
                            debug!("Count {}", count);
                            command
                                .1
                                .params
                                .insert("count".to_string(), format!("{}", count));
                        }
                        Err(err) => {
                            command.1.err = Err(err);
                        }
                    }
                    tx.send(command.1).await.unwrap();
                }
                CMD::KickJob => {
                    command.1.err = tube.kick_job(&command.1);
                    tx.send(command.1).await.unwrap();
                }
                CMD::PauseTube => {
                    command.1.err = tube.pause_tube(&command.1);
                    tx.send(command.1).await.unwrap();
                }
                CMD::Touch => {
                    command.1.err = tube.touch(&command.1).map(|_| ());
                    tx.send(command.1).await.unwrap();
                }
                CMD::Peek => {
                    match tube.peek(&command.1) {
                        Ok(job) => {
                            command.1.job = job.clone();
                        }
                        Err(err) => {
                            command.1.err = Err(err);
                        }
                    }
                    tx.send(command.1).await.unwrap();
                }
                CMD::PeekReady => {
                    match tube.peek_ready() {
                        Ok(job) => {
                            command.1.job = job.clone();
                        }
                        Err(err) => {
                            command.1.err = Err(err);
                        }
                    }
                    tx.send(command.1).await.unwrap();
                }
                CMD::PeekDelayed => {
                    match tube.peek_delayed() {
                        Ok(job) => {
                            command.1.job = job.clone();
                        }
                        Err(err) => {
                            command.1.err = Err(err);
                        }
                    }
                    tx.send(command.1).await.unwrap();
                }
                CMD::PeekBuried => {
                    match tube.peek_buried() {
                        Ok(job) => {
                            command.1.job = job.clone();
                        }
                        Err(err) => {
                            command.1.err = Err(err);
                        }
                    }
                    tx.send(command.1).await.unwrap();
                }
                CMD::Ignore => {
                    tube.ignore(&command.0);
                    tx.send(command.1).await.unwrap();
                }
                _ => unreachable!(),
            }
        }
    }

    pub async fn stop(&mut self) -> Result<(), Error> {
        for sender in &mut self.stop {
            sender.send(()).await.unwrap();
        }
        Ok(())
    }
}

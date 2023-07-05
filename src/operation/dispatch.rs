use crate::architecture::cmd::{Command, CMD};
use crate::architecture::job::{AwaitingClient, Job};
use crate::architecture::tube::{ClientId, PriorityQueue, Tube};
use crate::backend::min_heap::MinHeap;
use crate::channel::{UnBoundedReceiver, UnBoundedSender, SenderReceiver};

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::operation::once_channel::OnceChannel;
use failure::{err_msg, Error, Fail};
use tokio::spawn;
use tokio::sync::mpsc::unbounded_channel;
use crate::channel;

pub type CmdReceiver = UnBoundedReceiver<Command>;
pub type CmdSender = UnBoundedSender<Command>;

pub enum CmdChannel {
    ReserveCmdSender(Option<OnceChannel<Command>>),
    BaseCmdSender(Option<CmdSender>),
}

pub type TubeSender = UnBoundedSender<(ClientId, Command)>;
pub type TubeReceiver = UnBoundedReceiver<(ClientId, Command)>;

type InnerSender = UnBoundedSender<TubeItem>;
type InnerReceiver = UnBoundedReceiver<TubeItem>;

#[derive(Clone)]
enum TubeItem {
    Add(
        ClientId,
        CmdSender,
        UnBoundedSender<()>,
        OnceChannel<Command>,
    ),
    Delete(ClientId, UnBoundedSender<()>),
    Stop,
}

pub struct Dispatch {
    stop: Vec<UnBoundedSender<()>>,
    tube_ch: HashMap<String, InnerSender>,
    cmd_tx: HashMap<String, UnBoundedSender<(ClientId, Command)>>,
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
            let (mut callback_tx, mut callback_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
            self.tube_ch
                .get(&name)
                .unwrap()
                .send(TubeItem::Add(client_id, reply, callback_tx, reserve_reply))
                .unwrap();
            callback_rx.recv().await;
            return Ok(cmd_sender.clone());
        }
        let (mut callback_tx, mut callback_rx) = unbounded_channel::<()>();
        let (mut tube_tx, mut tube_rx) = unbounded_channel::<TubeItem>();
        self.tube_ch.insert(name.clone(), tube_tx.clone());
        let (mut cmd_tx, mut cmd_rx) = unbounded_channel::<(ClientId, Command)>();
        self.cmd_tx.insert(name.clone(), cmd_tx.clone());
        self.task(name, tube_rx, cmd_rx).await;
        tube_tx
            .send(TubeItem::Add(
                client_id.clone(),
                reply,
                callback_tx,
                reserve_reply,
            ))
            .unwrap();
        callback_rx.recv().await;
        Ok(cmd_tx)
    }

    pub async fn drop_client(&mut self, name: &String, client_id: ClientId) {
        debug!("Drop {} from {}", client_id, name);
        let (mut callback_tx, mut callback_rx) = unbounded_channel::<()>();
        if let Some(tube_tx) = self.tube_ch.get_mut(name) {
            tube_tx
                .send(TubeItem::Delete(client_id, callback_tx))
                .unwrap();
            callback_rx.recv().await.unwrap();
        }
    }

    pub fn list_tubes(&self) -> (usize, Vec<String>) {
        (
            self.cmd_tx.len(),
            self.cmd_tx.keys().map(|key| key.clone()).collect(),
        )
    }

    async fn task(&mut self, tube_name: String, mut tube_rx: InnerReceiver, mut cmd_rx: TubeReceiver) {
        spawn(async move {
            // TODO Optimize
            let mut clients: HashMap<ClientId, (CmdSender, OnceChannel<Command>)> = HashMap::new();
            let mut tube: Tube<MinHeap<Job>, MinHeap<AwaitingClient>> = Tube::new(
                tube_name.clone(),
                MinHeap::new("ready".to_string()),
                MinHeap::new("reserved".to_string()),
                MinHeap::new("delayed".to_string()),
                MinHeap::new("buried".to_string()),
                MinHeap::new("awaiting_clients".to_string()),
            );
            let mut interval = tokio::time::interval(Duration::from_millis(59));
            let (mut _tx, mut _rx) = unbounded_channel::<()>();
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        tube.process().await;
                        tube.process_timed_clients().await;
                    },
                    _ = _rx.recv() => {
                        tube.process().await;
                        tube.process_timed_clients().await;
                    }
                    cmd = tube_rx.recv() => match cmd {
                        Some(cmd) => {
                            match cmd {
                                TubeItem::Add(client_id, ch, mut cb, mut reserve_cb_rx) => {
                                    debug!("Insert a new client:{} into tube:{}", client_id, tube_name);
                                    clients.insert(client_id, (ch, reserve_cb_rx));
                                    cb.send(()).unwrap();
                                },
                                TubeItem::Delete(client_id, mut cb) =>{
                                    debug!("Remove client {}", client_id);
                                    clients.remove(&client_id);
                                    tube.drop_client(&client_id);
                                    cb.send(()).unwrap();
                                },
                                TubeItem::Stop => {
                                    info!("Stop tube: {}", tube_name.clone());
                                    break;
                                },
                            }
                        },
                        _ => unreachable!()
                     },
                    cmd = cmd_rx.recv() => match cmd {
                        Some(command) => {
                             Self::handle_command(&mut clients, &mut tube, command.clone()).await;
                             let cmd = CMD::from_str(&command.1.name).unwrap();
                             if cmd == CMD::ReserveWithTimeout || cmd == CMD::Reserve{
                                _tx.send(()).unwrap();
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
                    tx.send(command.1).unwrap();
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
                    tx.send(command.1).unwrap();
                }
                CMD::Release => {
                    command.1.err = tube.release(&command.1);
                    tx.send(command.1).unwrap();
                }
                CMD::Bury => {
                    command.1.err = tube.buried(&command.1);
                    tx.send(command.1).unwrap();
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
                    tx.send(command.1).unwrap();
                }
                CMD::KickJob => {
                    command.1.err = tube.kick_job(&command.1);
                    tx.send(command.1).unwrap();
                }
                CMD::PauseTube => {
                    command.1.err = tube.pause_tube(&command.1);
                    tx.send(command.1).unwrap();
                }
                CMD::Touch => {
                    command.1.err = tube.touch(&command.1).map(|_| ());
                    tx.send(command.1).unwrap();
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
                    tx.send(command.1).unwrap();
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
                    tx.send(command.1).unwrap();
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
                    tx.send(command.1).unwrap();
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
                    tx.send(command.1).unwrap();
                }
                CMD::Ignore => {
                    tube.ignore(&command.0);
                    tx.send(command.1).unwrap();
                }
                _ => unreachable!(),
            }
        }
    }

    pub async fn stop(&mut self) -> Result<(), Error> {
        for sender in &mut self.stop {
            sender.send(()).unwrap();
        }
        Ok(())
    }
}

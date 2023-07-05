use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use failure::{self, bail, Fail, Error, err_msg};
use downcast_rs::impl_downcast;
use downcast_rs::Downcast;

use crate::architecture::job::{AwaitingClient, Job, State, random_clients, random_factory};
use crate::architecture::cmd::Command;
use crate::backend::fake_queue::FakeHeap;
use crate::backend::min_heap::MinHeap;
use crate::architecture::error::ProtocolError;
use crate::operation::once_channel::OnceChannel;
use chrono::Local;

const QUERY_FREQUENCY: Duration = Duration::from_millis(20);
const MAX_JOB_PER_ITERATION: usize = 20;

pub trait PriorityQueue<Item: PriorityQueueItem> {
    fn enqueue(&mut self, job: Item);
    fn dequeue(&mut self) -> Option<Item>;
    fn peek(&self) -> Option<&Item>;
    fn find(&self, id: &Id) -> Option<&Item>;
    fn remove(&mut self, id: &Id) -> Option<Item>;
    fn len(&self) -> usize;
    fn set_time(&mut self);
}

pub type Id = u64;
pub type ClientId = u64;

pub trait PriorityQueueItem: Downcast {
    fn key(&self, now: Option<i64>) -> i64;
    fn id(&self) -> &Id;
    fn timestamp(&self) -> i64;
    fn enqueue(&mut self);
    fn dequeue(&mut self);
}

impl_downcast!(PriorityQueueItem);

pub struct Tube<J, A> where
    J: PriorityQueue<Job> + Send + 'static,
    A: PriorityQueue<AwaitingClient> + Send + 'static {
    name: String,
    test: Option<J>,
    ready: J,
    reserved: J,
    delayed: J,
    buried: J,
    awaiting_clients: A,
    awaiting_clients_flag: HashMap<ClientId, Id>,
    awaiting_timed_clients: HashMap<Id, AwaitingClient>,
    pause_tube_time: i64,
}

//
impl<J, A> Tube<J, A> where
    J: PriorityQueue<Job> + Send + 'static,
    A: PriorityQueue<AwaitingClient> + Send + 'static {
    pub fn new(name: String, ready: J, reserved: J, delayed: J, buried: J, awaiting_clients: A) -> Self {
        Tube {
            test: None,
            name: name.clone(),
            ready,
            reserved,
            delayed,
            buried,
            awaiting_clients,
            awaiting_clients_flag: HashMap::new(),
            awaiting_timed_clients: HashMap::new(),
            pause_tube_time: Local::now().timestamp(),
        }
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub async fn process(&mut self) {
        self.process_delayed_queue(MAX_JOB_PER_ITERATION);
        self.process_reserved_queue(MAX_JOB_PER_ITERATION).await;
        self.process_ready_queue(MAX_JOB_PER_ITERATION).await;
    }

    pub fn process_delayed_queue(&mut self, mut limit: usize) {
        debug!("{}, delayed queue _size: {}", self.name, self.delayed.len());
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64;
        while let Some(mut delayed_job) = self.delayed.dequeue() {
            if delayed_job.key(Some(timestamp)) <= 0 && limit > 0 {
                //let mut delayed_job = self.delayed.dequeue().unwrap();
                debug!("job:{} from {} to {}", delayed_job.id(), delayed_job.state(), State::Ready);
                delayed_job.set_state(State::Ready).unwrap();
                self.ready.enqueue(delayed_job);
                limit -= 1;
            } else {
                self.delayed.enqueue(delayed_job);
                break;
            }
        }
    }

    pub async fn process_reserved_queue(&mut self, mut limit: usize) {
        let tm = Local::now().timestamp();
        if tm < self.pause_tube_time {
            debug!("tube: {}, wait {} for pause tube", self.name, self.pause_tube_time - tm);
            return;
        }
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64;
        info!("{}, reserve queue _size: {}", self.name, self.reserved.len());
        // 将reserved队列的超时Job，转为ready队列（比如：客户端获取到一个job，但是在规定的时间内，服务端并未收到ack，即处理超时）
        while let Some(job) = self.reserved.peek() {
            if job.key(Some(timestamp)) <= 0 && limit > 0 {
                let mut reserved_job = self.reserved.dequeue().unwrap();
                //debug!("job:{} from {} to {}", reserved_job.id(), reserved_job.state(), State::Ready);
                reserved_job.set_state(State::Ready).unwrap();
                self.ready.enqueue(reserved_job);
                limit -= 1;
            } else {
                break;
            }
        }
    }

    pub async fn process_ready_queue(&mut self, mut limit: usize) {
        info!("{}, ready queue len: {}, client: {},", self.name, self.ready.len(), self.awaiting_clients.len());
        while self.awaiting_clients.peek().is_some() && self.ready.peek().is_some() && limit > 0 {
            let mut awaiting_client_connection = self.awaiting_clients.dequeue().unwrap();
            let mut ready_job = self.ready.dequeue().unwrap();
            awaiting_client_connection.request.job = ready_job.clone();
            let client_id = awaiting_client_connection.id().clone();
            if let Err(err) = awaiting_client_connection.tx.send(awaiting_client_connection.request).await {
                debug!("[{}] Client has closed, enqueue job to ready again", self.name);
                self.ready.enqueue(ready_job);
            } else {
                self.awaiting_clients_flag.remove(&client_id);
                debug!("[{}] process ready queue", self.name);
                ready_job.set_state(State::Reserved).unwrap();
                self.reserved.enqueue(ready_job);
            }
            limit -= 1;
        }
    }

    pub async fn process_timed_clients(&mut self) {
        let mut need_delete_id = vec![];
        for (id, client) in self.awaiting_timed_clients.iter_mut() {
//            debug!("Client await job timeout: {}", id);
            if client.time_left() <= 0 {
                if let Some(job) = self.ready.dequeue() {
                    client.request.job = job;
                    if let Err(err) = client.tx.send(client.request.clone()).await {
                        warn!("client has close");
                        self.ready.enqueue(client.request.job.clone());
                    } else {
                        client.request.job.set_state(State::Reserved).unwrap();
                        self.reserved.enqueue(client.request.job.clone());
                        debug!("ready {}, reserved {}", self.ready.len(), self.reserved.len());
                    }
                } else {
                    // FIXME
                    client.request.err = Err(ProtocolError::NotFound);
                    client.tx.send(client.request.clone()).await;
                }
                need_delete_id.push(id.clone());
            }
        }
        // FIXME
        need_delete_id.iter_mut().for_each(|id| {
            self.awaiting_timed_clients.remove(id);
            self.awaiting_clients.remove(id);
            self.awaiting_clients_flag.remove(id);
        });
    }

    pub fn drop_client(&mut self, client_id: &ClientId) {
        self.awaiting_clients.remove(client_id);
        self.awaiting_timed_clients.remove(client_id);
    }

    pub fn put(&mut self, cmd: Command) -> Result<(), Error> {
        if cmd.job.state() == &State::Ready {
            self.ready.enqueue(cmd.job);
        } else {
            self.delayed.enqueue(cmd.job);
        }
        Ok(())
    }

    pub fn reserve(&mut self, client_id: ClientId, cmd: Command, tx: OnceChannel<Command>) -> Result<(), Error> {
        let id = random_factory();
        let _id = self.awaiting_clients_flag.entry(client_id).or_insert(id);
        if *_id != id {
            return Ok(());
        }
        self.awaiting_clients.enqueue(AwaitingClient::new(client_id, cmd, tx));
        Ok(())
    }

    // TODO:
    pub fn reserve_with_timeout(&mut self, client_id: ClientId, cmd: Command, tx: OnceChannel<Command>) -> Result<(), Error> {
        let id = random_factory();
        let _id = self.awaiting_clients_flag.entry(client_id).or_insert(id);
        if *_id != id {
            return Ok(());
        }
        let mut client = AwaitingClient::new(client_id, cmd, tx);
        self.awaiting_clients.enqueue(client.clone());
        self.awaiting_timed_clients.insert(client.id().clone(), client);
        Ok(())
    }

    pub fn delete(&mut self, cmd: &Command) -> Result<(), ProtocolError> {
        let id = cmd.params.get("id").unwrap().parse::<Id>().map_err(|_| ProtocolError::BadFormat)?;
        debug!("{} would be deleted", id);
        let result = self.buried.remove(&id).map(|_| ()).ok_or(ProtocolError::NotFound);
        if result.is_ok() {
            return result;
        }
        self.reserved.remove(&id).map(|_| ()).ok_or(ProtocolError::NotFound)
    }

    // TODO add BURIED
    pub fn release(&mut self, cmd: &Command) -> Result<(), ProtocolError> {
        let id = cmd.params.get("id").unwrap().parse::<Id>().unwrap();
        let mut job = self.reserved.remove(&id).ok_or(ProtocolError::NotFound)?;
        job.set_state(State::Ready).unwrap();
        self.ready.enqueue(job);
        Ok(())
    }

    // TODO add BURIED
    pub fn buried(&mut self, cmd: &Command) -> Result<(), ProtocolError> {
        let id = cmd.params.get("id").unwrap().parse::<Id>().unwrap();
        let mut job = self.reserved.remove(&id).ok_or(ProtocolError::NotFound)?;
        job.set_state(State::Buried).unwrap();
        self.buried.enqueue(job);
        Ok(())
    }

    pub fn kick(&mut self, cmd: &Command) -> Result<usize, ProtocolError> {
        let mut bound = cmd.params.get("bound").map(|item| item.parse::<usize>().unwrap()).ok_or(ProtocolError::BadFormat)?;
        let _bound = bound.min(self.buried.len());
        for i in 0.._bound {
            let mut job = self.buried.dequeue().unwrap();
            job.set_state(State::Ready).unwrap();
            self.ready.enqueue(job);
        }
        if _bound > 0 {
            return Ok(_bound);
        }

        let _bound = bound.min(self.delayed.len());
        for i in 0.._bound {
            let mut job = self.delayed.dequeue().unwrap();
            job.set_state(State::Ready).unwrap();
            self.ready.enqueue(job);
        }
        Ok(_bound)
    }

    pub fn kick_job(&mut self, cmd: &Command) -> Result<(), ProtocolError> {
        let id = cmd.params.get("id").map(|item| item.parse::<u64>().unwrap()).ok_or(ProtocolError::BadFormat)?;
        if let Some(_) = self.buried.remove(&id) {
            return Ok(());
        }
        self.delayed.remove(&id).map(|_| ()).ok_or(ProtocolError::NotFound)
    }

    pub fn pause_tube(&mut self, cmd: &Command) -> Result<(), ProtocolError> {
        let delay = cmd.params.get("delay").map(|item| item.parse::<i64>().unwrap()).ok_or(ProtocolError::BadFormat)?;
        self.pause_tube_time = Local::now().timestamp() + delay;
        Ok(())
    }

    pub fn touch(&self, cmd: &Command) -> Result<&Id, ProtocolError> {
        self.peek(cmd).map(|job| job.id())
    }

    pub fn peek(&self, cmd: &Command) -> Result<&Job, ProtocolError> {
        let id = cmd.params.get("id").unwrap().parse::<Id>().map_err(|_| ProtocolError::BadFormat)?;
        if let Some(job) = self.delayed.find(&id) {
            return Ok(job);
        }
        if let Some(job) = self.ready.find(&id) {
            return Ok(job);
        }
        if let Some(job) = self.buried.find(&id) {
            return Ok(job);
        }
        Err(ProtocolError::NotFound)
    }

    pub fn peek_ready(&self) -> Result<&Job, ProtocolError> {
        self.ready.peek().ok_or(ProtocolError::NotFound)
    }

    pub fn peek_delayed(&self) -> Result<&Job, ProtocolError> {
        self.delayed.peek().ok_or(ProtocolError::NotFound)
    }

    pub fn peek_buried(&self) -> Result<&Job, ProtocolError> {
        self.buried.peek().ok_or(ProtocolError::NotFound)
    }

    pub fn ignore(&mut self, client_id: &ClientId) -> Result<(), ProtocolError> {
        self.awaiting_clients_flag.remove(client_id);
        self.awaiting_timed_clients.remove(client_id);
        self.awaiting_clients.remove(client_id);
        Ok(())
    }
}

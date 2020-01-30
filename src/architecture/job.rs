use std::time::{Instant, Duration};
use std::sync::{Arc, atomic::{self, AtomicU64}};
use std::cmp::Ordering;
use chrono::Local;
use futures::channel::mpsc::{UnboundedSender as Sender, UnboundedReceiver as Receiver};
use uuid::Uuid;
use std::hash::{Hash, Hasher};
use fasthash::{metro, MetroHasher};
use failure::{self, Fail, bail, Error, err_msg};

use super::cmd::Command;
use super::job_state::is_valid_transitions_to;
use crate::architecture::tube::{PriorityQueueItem, Id, ClientId};
use crate::operation::once_channel::OnceChannel;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Job {
    _id: Id,
    private: i64,
    delay: i64,
    started_delay_at: i64,
    started_ttr_at: i64,
    ttr: i64,
    pub bytes: i64,
    pub(crate) data: String,

    state: State,
    timestamp: i64,
}

impl Ord for Job {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(&other.key())
    }
}

impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum State {
    Ready,
    Delayed,
    Reserved,
    Buried,
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            State::Ready => write!(f, "Ready"),
            State::Delayed => write!(f, "Delayed"),
            State::Reserved => write!(f, "Reserved"),
            State::Buried => write!(f, "Buried"),
        }
    }
}

impl Ord for State {
    fn cmp(&self, other: &State) -> Ordering {
        self.cmp(&other)
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

const NANO: i64 = 1000000000;

impl Default for Job {
    fn default() -> Self {
        Job {
            _id: random_factory(),
            private: 0,
            delay: 0,
            started_delay_at: 0,
            started_ttr_at: 0,
            ttr: 0,
            bytes: 0,
            data: "".to_string(),
            state: State::Ready,
            timestamp: 0,
        }
    }
}

impl Job {
    pub fn new(_id: Id, pri: i64, delay: i64, ttr: i64, bytes: i64, data: String) -> Self {
        let mut job = Job {
            _id,
            private: pri,
            delay,
            started_delay_at: 0,
            started_ttr_at: 0,
            ttr,
            bytes,
            data,
            state: State::Ready,
            timestamp: Local::now().timestamp_nanos(),
        };
        if job.delay > 0 {
            job.state = State::Delayed;
            job.started_delay_at = Local::now().timestamp_nanos();
        }
        job
    }

    // TODO Must
    pub fn set_state(&mut self, state: State) -> Result<(), Error> {
        let ok = is_valid_transitions_to(self.state.clone(), state)?;
        if !ok {
            bail!("Failed to set new state");
        }
        if self.state == State::Delayed {
            self.started_delay_at = Local::now().timestamp_nanos();
        } else if state == State::Reserved {
            self.started_ttr_at = Local::now().timestamp_nanos();
        }
        self.state = state.clone();
        Ok(())
    }

    pub fn state(&self) -> &State {
        &self.state
    }
}

use std::time::SystemTime;

impl PriorityQueueItem for Job {
    fn key(&self) -> i64 {
        let timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as i64;
        match self.state {
            State::Ready => self.private,
            State::Delayed => self.delay * NANO - (timestamp - self.started_delay_at),
            State::Reserved => self.ttr * NANO - (timestamp - self.started_ttr_at),
            _ => 0,
        }
    }

    fn id(&self) -> &Id {
        &self._id
    }

    fn timestamp(&self) -> i64 {
        self.timestamp
    }

    fn enqueue(&mut self) {
        self.timestamp = Local::now().timestamp_nanos();
    }

    fn dequeue(&mut self) {
        self.timestamp = Local::now().timestamp_nanos();
    }
}

pub struct AwaitingClient {
    id: ClientId,
    // nanos
    queued_at: i64,
    // nanos
    timeout: i64,
    pub(crate) tx: OnceChannel<Command>,
    pub(crate) request: Command,
}

impl Clone for AwaitingClient {
    fn clone(&self) -> Self {
        AwaitingClient {
            id: self.id.clone(),
            queued_at: self.queued_at.clone(),
            timeout: self.timeout.clone(),
            tx: self.tx.clone(),
            request: self.request.clone(),
        }
    }
}

impl Ord for AwaitingClient {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl PartialOrd for AwaitingClient {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.id.cmp(&other.id))
    }
}

impl PartialEq for AwaitingClient {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for AwaitingClient {}

impl AwaitingClient {
    pub fn new(_id: ClientId, request: Command, tx: OnceChannel<Command>) -> AwaitingClient {
        let timeout = request.params.get("timeout").map_or_else(|| -1, |tm| tm.parse::<i64>().unwrap() * NANO);
        AwaitingClient {
            id: _id,
            request,
            tx,
            queued_at: Local::now().timestamp_nanos(),
            timeout,
        }
    }

    // unit nanos
    pub fn time_left(&self) -> i64 {
        self.timeout - (Local::now().timestamp_nanos() - self.queued_at)
    }
}

impl PriorityQueueItem for AwaitingClient {
    fn key(&self) -> i64 {
        self.queued_at
    }

    fn id(&self) -> &Id {
        &self.id
    }

    fn timestamp(&self) -> i64 {
        self.queued_at
    }

    fn enqueue(&mut self) {
        self.queued_at = Local::now().timestamp_nanos();
    }

    // not used
    fn dequeue(&mut self) {}
}

lazy_static! {
    static ref id: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
}

pub fn random_factory() -> Id {
//    id.fetch_add(1, atomic::Ordering::SeqCst)
    metro::hash64(Uuid::new_v4().as_bytes())
}

pub fn random_clients() -> Id {
    metro::hash64(Uuid::new_v4().as_bytes())
}

#[cfg(test)]
mod tests {
    use chrono::prelude::*;
    use crate::backend::min_heap::MinHeap;
    use crate::architecture::job::{Job, NANO};
    use crate::architecture::tube::PriorityQueue;

    #[test]
    fn it_enqueue_dequeue() {
        let mut heap = build_min_heap(100);
        let mut min = 0;
        while let Some(job) = heap.dequeue() {
            let delay_at = job.delay * NANO + job.started_delay_at;
            println!("!id: {:2}, delay:{:3}, started_delay_at: {}, delay_at: {}", job._id, job.delay, job.started_delay_at, delay_at);
            assert!(min <= delay_at);
            min = delay_at;
        }
    }

    fn build_min_heap(n: u64) -> MinHeap<Job> {
        let mut heap = MinHeap::new("test".to_owned());
        for i in 0..n {
            heap.enqueue(Job::new(i, (i % 2 + 1) as i64, (i % 10 + 1) as i64, (i + 1) as i64, 10, "awaiting_clients".to_owned()))
        }
        heap
    }
}
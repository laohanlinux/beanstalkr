use std::cmp::Ordering as CmpOrdering;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{bail, Error};
use chrono::Local;

use super::cmd::Command;
use super::job_state::is_valid_transitions_to;
use crate::architecture::tube::{ClientId, Id, PriorityQueueItem};

use crate::operation::once_channel::OnceChannel;

lazy_static::lazy_static! {
    /// 全局 ID 生成器，用于生成唯一的 Job ID 和 Client ID
    static ref ID_GENERATOR: AtomicU64 = AtomicU64::new(0);
}

/// 生成下一个唯一的 Job ID
pub fn next_job_id() -> Id {
    ID_GENERATOR.fetch_add(1, Ordering::SeqCst)
}

/// 生成下一个唯一的 Client ID
pub fn next_client_id() -> ClientId {
    ID_GENERATOR.fetch_add(1, Ordering::SeqCst)
}

/// 表示一个 Beanstalkd 任务
///
/// 任务在其生命周期中会在不同状态间转换：
/// - `Ready`: 等待被 worker 获取
/// - `Reserved`: 已被 worker 获取，正在处理
/// - `Delayed`: 延迟执行
/// - `Buried`: 被埋藏，需要手动 kick
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Job {
    id: Id,
    priority: i64,
    delay: i64,
    started_delay_at: i64,
    started_ttr_at: i64,
    ttr: i64,
    pub bytes: i64,
    pub(crate) data: String,
    state: State,
    timestamp: i64,
    tube: String,
    
    // 统计字段
    reserves: u64,
    timeouts: u64,
    releases: u64,
    buries: u64,
    kicks: u64,
    
    // 预留此 job 的客户端 ID（用于连接断开时重新入队）
    reserver: Option<ClientId>,
}

impl Ord for Job {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.priority_key().cmp(&other.priority_key())
    }
}

impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

/// 任务的当前状态
#[derive(Debug, Eq, PartialEq, Copy, Clone, Hash)]
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
    fn cmp(&self, other: &State) -> CmpOrdering {
        let self_val = *self as i32;
        let other_val = *other as i32;
        self_val.cmp(&other_val)
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

/// 纳秒转换常量
pub(crate) const NANOS_PER_SEC: i64 = 1_000_000_000;

impl Default for Job {
    fn default() -> Self {
        Job {
            id: next_job_id(),
            priority: 0,
            delay: 0,
            started_delay_at: 0,
            started_ttr_at: 0,
            ttr: 0,
            bytes: 0,
            data: String::new(),
            state: State::Ready,
            timestamp: 0,
            tube: "default".to_string(),
            reserves: 0,
            timeouts: 0,
            releases: 0,
            buries: 0,
            kicks: 0,
            reserver: None,
        }
    }
}

impl Job {
    /// 创建一个新任务
    pub fn new(
        id: Id,
        priority: i64,
        delay: i64,
        ttr: i64,
        bytes: i64,
        data: String,
    ) -> Self {
        let timestamp = Local::now().timestamp_nanos_opt().unwrap_or(0);
        
        // 根据 delay 确定初始状态
        let state = if delay > 0 {
            State::Delayed
        } else {
            State::Ready
        };
        
        let started_delay_at = if delay > 0 {
            timestamp
        } else {
            0
        };
        
        let mut job = Job {
            id,
            priority,
            delay,
            started_delay_at,
            started_ttr_at: 0,
            ttr,
            bytes,
            data,
            state,
            timestamp,
            tube: "default".to_string(),
            reserves: 0,
            timeouts: 0,
            releases: 0,
            buries: 0,
            kicks: 0,
            reserver: None,
        };
        job.enqueue();
        job
    }

    /// 获取任务 ID
    pub fn id(&self) -> &Id {
        &self.id
    }

    /// 获取当前状态
    pub fn state(&self) -> &State {
        &self.state
    }

    /// 设置任务状态
    ///
    /// # Errors
    /// 如果状态转换无效，返回错误
    pub fn set_state(&mut self, state: State) -> Result<(), Error> {
        let ok = is_valid_transitions_to(self.state, state)?;
        if !ok {
            bail!("invalid state transition from {} to {}", self.state, state);
        }
        self.state = state;
        Ok(())
    }

    /// 计算任务剩余时间（秒）
    ///
    /// 对于 Reserved 状态，返回 TTR 剩余时间
    pub fn time_left(&self, now: i64) -> i64 {
        match self.state {
            State::Reserved => {
                let elapsed = now - (self.started_ttr_at / NANOS_PER_SEC);
                (self.ttr - elapsed).max(0)
            }
            _ => 0,
        }
    }

    /// 获取任务所在 tube
    pub fn tube(&self) -> &str {
        &self.tube
    }

    /// 设置任务所在 tube
    pub fn set_tube(&mut self, tube: String) {
        self.tube = tube;
    }

    /// 获取 reserve 次数
    pub fn reserves(&self) -> u64 {
        self.reserves
    }

    /// 增加 reserve 次数
    pub fn inc_reserves(&mut self) {
        self.reserves += 1;
    }

    /// 获取 timeout 次数
    pub fn timeouts(&self) -> u64 {
        self.timeouts
    }

    /// 增加 timeout 次数
    pub fn inc_timeouts(&mut self) {
        self.timeouts += 1;
    }

    /// 获取 release 次数
    pub fn releases(&self) -> u64 {
        self.releases
    }

    /// 增加 release 次数
    pub fn inc_releases(&mut self) {
        self.releases += 1;
    }

    /// 获取 bury 次数
    pub fn buries(&self) -> u64 {
        self.buries
    }

    /// 增加 bury 次数
    pub fn inc_buries(&mut self) {
        self.buries += 1;
    }

    /// 获取 kick 次数
    pub fn kicks(&self) -> u64 {
        self.kicks
    }

    /// 增加 kick 次数
    pub fn inc_kicks(&mut self) {
        self.kicks += 1;
    }

    /// 获取 TTR
    pub fn ttr(&self) -> i64 {
        self.ttr
    }

    /// 获取 delay
    pub fn delay(&self) -> i64 {
        self.delay
    }

    /// 获取 priority
    pub fn priority(&self) -> i64 {
        self.priority
    }

    /// 获取创建时间戳
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// 重置 TTR 计时器
    pub fn reset_ttr(&mut self) {
        self.started_ttr_at = Local::now().timestamp_nanos_opt().unwrap_or(0);
    }

    /// 设置优先级
    pub fn set_priority(&mut self, priority: i64) {
        self.priority = priority;
    }

    /// 设置延迟时间
    pub fn set_delay(&mut self, delay: i64) {
        self.delay = delay;
        self.started_delay_at = Local::now().timestamp_nanos_opt().unwrap_or(0);
    }

    /// 获取优先级键值
    fn priority_key(&self) -> i64 {
        self.priority
    }

    /// 获取预留此 job 的客户端 ID
    pub fn reserver(&self) -> Option<ClientId> {
        self.reserver
    }

    /// 设置预留此 job 的客户端 ID
    pub fn set_reserver(&mut self, client_id: ClientId) {
        self.reserver = Some(client_id);
    }

    /// 清除预留此 job 的客户端 ID
    pub fn clear_reserver(&mut self) {
        self.reserver = None;
    }
}

impl PriorityQueueItem for Job {
    fn key(&self, now: Option<i64>) -> i64 {
        let timestamp = now.unwrap_or_else(|| Local::now().timestamp());
        match self.state {
            State::Ready => self.priority,
            State::Delayed => self.delay * NANOS_PER_SEC - (timestamp - self.started_delay_at),
            State::Reserved => self.ttr * NANOS_PER_SEC - (timestamp - self.started_ttr_at),
            _ => 0,
        }
    }

    fn id(&self) -> &Id {
        &self.id
    }

    fn timestamp(&self) -> i64 {
        self.timestamp
    }

    fn enqueue(&mut self) {
        self.timestamp = Local::now().timestamp_nanos_opt().unwrap_or(0);
    }

    fn dequeue(&mut self) {
        self.timestamp = Local::now().timestamp_nanos_opt().unwrap_or(0);
    }
}

/// 表示一个等待任务的客户端
#[derive(Clone)]
pub struct AwaitingClient {
    id: ClientId,
    queued_at: i64,
    timeout: i64,
    pub(crate) tx: OnceChannel<Command>,
    pub(crate) request: Command,
}

impl AwaitingClient {
    pub fn new(client_id: ClientId, cmd: Command, tx: OnceChannel<Command>) -> Self {
        let now = Local::now().timestamp_nanos_opt().unwrap_or(0);
        AwaitingClient {
            id: client_id,
            queued_at: now,
            timeout: 0,
            tx,
            request: cmd,
        }
    }

    /// 创建带超时的客户端
    pub fn new_with_timeout(client_id: ClientId, cmd: Command, tx: OnceChannel<Command>, timeout_secs: i64) -> Self {
        let now = Local::now().timestamp_nanos_opt().unwrap_or(0);
        AwaitingClient {
            id: client_id,
            queued_at: now,
            timeout: timeout_secs * NANOS_PER_SEC,
            tx,
            request: cmd,
        }
    }

    pub fn time_left(&self) -> i64 {
        let now = Local::now().timestamp_nanos_opt().unwrap_or(0);
        (self.timeout - (now - self.queued_at)).max(0)
    }
}

impl std::hash::Hash for AwaitingClient {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.queued_at.hash(state);
        self.timeout.hash(state);
    }
}

impl Ord for AwaitingClient {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.id.cmp(&other.id)
    }
}

impl PartialOrd for AwaitingClient {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for AwaitingClient {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for AwaitingClient {}

impl PriorityQueueItem for AwaitingClient {
    fn key(&self, _now: Option<i64>) -> i64 {
        self.id as i64
    }

    fn id(&self) -> &Id {
        &self.id
    }

    fn timestamp(&self) -> i64 {
        self.queued_at
    }

    fn enqueue(&mut self) {
        self.queued_at = Local::now().timestamp_nanos_opt().unwrap_or(0);
    }

    fn dequeue(&mut self) {
        self.queued_at = Local::now().timestamp_nanos_opt().unwrap_or(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::architecture::tube::PriorityQueue;
    use crate::backend::min_heap::MinHeap;

    #[test]
    fn it_enqueue_dequeue() {
        let mut heap = MinHeap::new("test".to_string());
        for i in 0..100 {
            let job = Job::new(next_job_id(), i as i64, 0, 60, 0, String::new());
            heap.enqueue(job);
        }
        for i in 0..100 {
            let job = heap.dequeue().unwrap();
            assert_eq!(i as i64, job.priority_key());
        }
        assert_eq!(heap.len(), 0);
    }
}

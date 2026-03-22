//! Tube 模块 - Beanstalkd 的核心队列实现
//!
//! Tube 是 Beanstalkd 中的基本单元，每个 tube 包含：
//! - ready 队列：等待被消费的任务
//! - delayed 队列：延迟执行的任务
//! - reserved 队列：已被预留的任务
//! - buried 队列：被埋藏的任务
//! - awaiting_clients：等待任务的客户端

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Error;
use chrono::Local;
use downcast_rs::impl_downcast;
use downcast_rs::Downcast;
use tracing::{debug, info, instrument, warn};

use crate::architecture::cmd::Command;
use crate::architecture::error::ProtocolError;
use crate::architecture::job::{next_job_id, AwaitingClient, Job, State, NANOS_PER_SEC};
use crate::architecture::stats::GLOBAL_STATS;
use crate::operation::once_channel::OnceChannel;

#[allow(dead_code)]
const QUERY_FREQUENCY: Duration = Duration::from_millis(20);
const MAX_JOB_PER_ITERATION: usize = 20;

/// 优先队列 trait，定义队列的基本操作
pub trait PriorityQueue<Item: PriorityQueueItem> {
    fn enqueue(&mut self, job: Item);
    fn dequeue(&mut self) -> Option<Item>;
    fn peek(&self) -> Option<&Item>;
    fn find(&self, id: &Id) -> Option<&Item>;
    fn remove(&mut self, id: &Id) -> Option<Item>;
    fn len(&self) -> usize;
    #[allow(dead_code)]
    fn set_time(&mut self);
    
    /// 获取所有元素的引用（用于 DEADLINE_SOON 检查）
    fn peek_all(&self) -> Vec<&Item>;
}

pub type Id = u64;
pub type ClientId = u64;

pub trait PriorityQueueItem: Downcast {
    fn key(&self, now: Option<i64>) -> i64;
    fn id(&self) -> &Id;
    #[allow(dead_code)]
    fn timestamp(&self) -> i64;
    fn enqueue(&mut self);
    fn dequeue(&mut self);
}

impl_downcast!(PriorityQueueItem);

impl<Item> Debug for dyn PriorityQueue<Item> {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

/// Tube 结构体，表示一个任务队列
///
/// 类型参数：
/// - `J`: Job 队列类型，必须实现 PriorityQueue<Job>
/// - `A`: AwaitingClient 队列类型，必须实现 PriorityQueue<AwaitingClient>
pub struct Tube<J, A>
where
    J: PriorityQueue<Job> + Send + 'static,
    A: PriorityQueue<AwaitingClient> + Send + 'static,
{
    name: String,
    #[allow(dead_code)]
    test: Option<J>,
    ready: J,
    reserved: J,
    delayed: J,
    buried: J,
    awaiting_clients: A,
    awaiting_clients_flag: HashMap<ClientId, Id>,
    awaiting_timed_clients: HashMap<Id, AwaitingClient>,
    pause_tube_time: i64,
    pause_until: i64,
    
    // Tube 级别统计
    total_jobs: u64,
    cmd_delete: u64,
    cmd_pause_tube: u64,
    
    // 客户端统计
    current_using: u64,
    current_watching: u64,
}

impl<J, A> Tube<J, A>
where
    J: PriorityQueue<Job> + Send + 'static,
    A: PriorityQueue<AwaitingClient> + Send + 'static,
{
    /// 创建一个新的 Tube
    ///
    /// # Arguments
    /// * `name` - tube 名称
    /// * `ready` - ready 队列
    /// * `reserved` - reserved 队列
    /// * `delayed` - delayed 队列
    /// * `buried` - buried 队列
    /// * `awaiting_clients` - 等待任务的客户端队列
    pub fn new(
        name: String,
        ready: J,
        reserved: J,
        delayed: J,
        buried: J,
        awaiting_clients: A,
    ) -> Self {
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
            pause_until: 0,
            total_jobs: 0,
            cmd_delete: 0,
            cmd_pause_tube: 0,
            current_using: 0,
            current_watching: 0,
        }
    }

    #[allow(dead_code)]
    pub fn name(&self) -> &String {
        &self.name
    }

    #[instrument(skip(self), fields(tube_name = %self.name))]
    pub async fn process(&mut self) {
        self.process_delayed_queue(MAX_JOB_PER_ITERATION);
        self.process_reserved_queue(MAX_JOB_PER_ITERATION).await;
        self.process_ready_queue(MAX_JOB_PER_ITERATION).await;
    }

    #[instrument(skip(self), fields(tube_name = %self.name))]
    pub fn process_delayed_queue(&mut self, mut limit: usize) {
        debug!("{}, delayed queue _size: {}", self.name, self.delayed.len());
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        while let Some(mut delayed_job) = self.delayed.dequeue() {
            if delayed_job.key(Some(timestamp)) <= 0 && limit > 0 {
                //let mut delayed_job = self.delayed.dequeue().unwrap();
                debug!(
                    "job:{} from {} to {}",
                    delayed_job.id(),
                    delayed_job.state(),
                    State::Ready
                );
                delayed_job.set_state(State::Ready).unwrap();
                self.ready.enqueue(delayed_job);
                limit -= 1;
            } else {
                self.delayed.enqueue(delayed_job);
                break;
            }
        }
    }

    #[instrument(skip(self), fields(tube_name = %self.name))]
    pub async fn process_reserved_queue(&mut self, mut limit: usize) {
        let tm = Local::now().timestamp();
        if tm < self.pause_until {
            debug!(
                "tube: {}, wait {} for pause tube",
                self.name,
                self.pause_until - tm
            );
            return;
        }
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        info!(
            "{}, reserve queue _size: {}",
            self.name,
            self.reserved.len()
        );
        // 将reserved队列的超时Job，转为ready队列（比如：客户端获取到一个job，但是在规定的时间内，服务端并未收到ack，即处理超时）
        let mut timeouts = 0u64;
        while let Some(job) = self.reserved.peek() {
            if job.key(Some(timestamp)) <= 0 && limit > 0 {
                let mut reserved_job = self.reserved.dequeue().unwrap();
                //debug!("job:{} from {} to {}", reserved_job.id(), reserved_job.state(), State::Ready);
                reserved_job.set_state(State::Ready).unwrap();
                reserved_job.inc_timeouts();
                self.ready.enqueue(reserved_job);
                timeouts += 1;
                limit -= 1;
            } else {
                break;
            }
        }
        
        // 更新全局 timeout 计数
        if timeouts > 0 {
            for _ in 0..timeouts {
                GLOBAL_STATS.inc_job_timeout();
            }
            self.update_global_job_stats();
        }
    }

    #[instrument(skip(self), fields(tube_name = %self.name))]
    pub async fn process_ready_queue(&mut self, mut limit: usize) {
        // 检查 tube 是否处于暂停状态
        let now = Local::now().timestamp();
        if self.is_paused(now) {
            debug!(
                "tube: {} is paused, {} seconds remaining",
                self.name,
                self.pause_until - now
            );
            return;
        }
        
        info!(
            "{}, ready queue len: {}, client: {},",
            self.name,
            self.ready.len(),
            self.awaiting_clients.len()
        );
        while self.awaiting_clients.peek().is_some() && self.ready.peek().is_some() && limit > 0 {
            let mut awaiting_client_connection = self.awaiting_clients.dequeue()
                .expect("awaiting_clients should not be empty after peek()");
            
            // Defensive check: equivalent to C's "job not ready" warning
            // In Rust this shouldn't happen due to peek() guard, but we keep it for consistency
            let mut ready_job = match self.ready.dequeue() {
                Some(job) => job,
                None => {
                    warn!("[{}] job not ready - ready queue became empty after peek", self.name);
                    // Put client back and continue
                    self.awaiting_clients.enqueue(awaiting_client_connection);
                    continue;
                }
            };
            
            awaiting_client_connection.request.job = ready_job.clone();
            let client_id = *awaiting_client_connection.id();
            if awaiting_client_connection
                .tx
                .send(awaiting_client_connection.request)
                .await
                .is_err()
            {
                debug!(
                    "[{}] Client has closed, enqueue job to ready again",
                    self.name
                );
                self.ready.enqueue(ready_job);
            } else {
                self.awaiting_clients_flag.remove(&client_id);
                debug!("[{}] process ready queue", self.name);
                ready_job.set_state(State::Reserved).unwrap();
                ready_job.inc_reserves();
                ready_job.set_reserver(client_id); // 设置预留者
                self.reserved.enqueue(ready_job);
                
                // 减少等待计数
                GLOBAL_STATS.dec_waiting();
                
                // 更新全局统计
                self.update_global_job_stats();
            }
            limit -= 1;
        }
    }

    pub async fn process_timed_clients(&mut self) {
        let mut need_delete_id = vec![];
        let mut needs_stats_update = false;
        
        for (id, client) in self.awaiting_timed_clients.iter_mut() {
            //            debug!("Client await job timeout: {}", id);
            if client.time_left() <= 0 {
                if let Some(job) = self.ready.dequeue() {
                    client.request.job = job;
                    if client.tx.send(client.request.clone()).await.is_err() {
                        warn!("[{}] client has closed during timed reserve", self.name);
                        self.ready.enqueue(client.request.job.clone());
                    } else {
                        client.request.job.set_state(State::Reserved).unwrap();
                        client.request.job.inc_reserves();
                        client.request.job.set_reserver(*id); // 设置预留者
                        self.reserved.enqueue(client.request.job.clone());
                        debug!(
                            "[{}] ready {}, reserved {}",
                            self.name,
                            self.ready.len(),
                            self.reserved.len()
                        );
                        needs_stats_update = true;
                    }
                } else {
                    // 超时，返回 TIMED_OUT (beanstalkd protocol behavior)
                    debug!("[{}] reserve-with-timeout expired for client {}", self.name, id);
                    client.request.err = Err(ProtocolError::TimedOut);
                    let _ = client.tx.send(client.request.clone()).await;
                }
                need_delete_id.push(*id);
            }
        }
        
        // 更新全局统计
        if needs_stats_update {
            self.update_global_job_stats();
        }
        
        // 清理已处理的客户端并减少等待计数
        for id in &need_delete_id {
            self.awaiting_timed_clients.remove(id);
            self.awaiting_clients.remove(id);
            self.awaiting_clients_flag.remove(id);
            // 减少等待计数
            GLOBAL_STATS.dec_waiting();
        }
    }

    pub fn drop_client(&mut self, client_id: &ClientId) {
        // 检查客户端是否正在等待，如果是则减少等待计数
        if self.awaiting_clients_flag.contains_key(client_id) {
            GLOBAL_STATS.dec_waiting();
        }
        self.awaiting_clients.remove(client_id);
        self.awaiting_timed_clients.remove(client_id);
        self.awaiting_clients_flag.remove(client_id);
        
        // 将客户端 reserved 的 jobs 重新入队（类似 C 版本的 enqueue_reserved_jobs）
        self.reenqueue_client_reserved_jobs(client_id);
    }
    
    /// 将指定客户端 reserved 的 jobs 重新放回 ready 队列
    /// 类似于 C 版本的 enqueue_reserved_jobs
    fn reenqueue_client_reserved_jobs(&mut self, client_id: &ClientId) {
        let mut jobs_to_reenqueue = Vec::new();
        
        // 收集所有属于该客户端的 reserved jobs
        // 由于 FakeHeap 不支持直接遍历并移除，我们需要先收集
        let all_reserved: Vec<_> = std::iter::from_fn(|| self.reserved.dequeue()).collect();
        
        for mut job in all_reserved {
            if job.reserver() == Some(*client_id) {
                // 清除 reserver
                job.clear_reserver();
                jobs_to_reenqueue.push(job);
            } else {
                // 不属于该客户端，放回 reserved 队列
                self.reserved.enqueue(job);
            }
        }
        
        // 将收集到的 jobs 重新入队
        for mut job in jobs_to_reenqueue {
            // 更新统计
            GLOBAL_STATS.current_jobs_reserved.fetch_sub(1, Ordering::SeqCst);
            self.update_global_job_stats();
            
            // 尝试重新入队，如果失败则 bury
            job.set_state(State::Ready).unwrap();
            let total_size = self.ready.len() + self.delayed.len() + self.reserved.len() + self.buried.len();
            if total_size >= Self::MAX_QUEUE_SIZE {
                // 内存不足，bury job
                job.set_state(State::Buried).unwrap();
                job.inc_buries();
                self.buried.enqueue(job);
            } else {
                self.ready.enqueue(job);
            }
        }
    }

    /// 最大队列大小（用于模拟内存限制）
    const MAX_QUEUE_SIZE: usize = 10000;

    pub fn put(&mut self, cmd: Command) -> Result<(), ProtocolError> {
        // 检查队列大小是否超过限制（模拟内存不足）
        let total_size = self.ready.len() + self.delayed.len() + self.reserved.len() + self.buried.len();
        if total_size >= Self::MAX_QUEUE_SIZE {
            // 内存不足，返回 OUT_OF_MEMORY
            return Err(ProtocolError::OutOfMemory);
        }
        
        let mut job = cmd.job;
        job.set_tube(self.name.clone());
        match job.state() {
            State::Ready => self.ready.enqueue(job),
            State::Delayed => self.delayed.enqueue(job),
            _ => self.ready.enqueue(job), // 其他状态默认放入 ready
        }
        self.total_jobs += 1;
        
        // 更新全局统计
        self.update_global_job_stats();
        Ok(())
    }

    /// 安全边界时间（秒）- 当作业剩余时间小于这个值时返回 DEADLINE_SOON
    const SAFETY_MARGIN: i64 = 1;

    pub fn reserve(
        &mut self,
        client_id: ClientId,
        cmd: Command,
        mut tx: OnceChannel<Command>,
    ) -> Result<(), Error> {
        // 检查客户端是否有即将超时的预留作业
        let now = Local::now().timestamp();
        for job in self.reserved.peek_all() {
            let time_left = job.time_left(now);
            if time_left <= Self::SAFETY_MARGIN && time_left > 0 {
                // 有作业即将超时，返回 DEADLINE_SOON
                let mut deadline_cmd = cmd.clone();
                deadline_cmd.err = Err(ProtocolError::DeadlineSoon);
                drop(tx.send(deadline_cmd));
                return Ok(());
            }
        }

        let id = next_job_id();
        let entry = self.awaiting_clients_flag.entry(client_id).or_insert(id);
        if *entry != id {
            return Ok(());
        }
        
        // 更新等待计数
        GLOBAL_STATS.inc_waiting();
        
        self.awaiting_clients
            .enqueue(AwaitingClient::new(client_id, cmd, tx));
        Ok(())
    }

    pub fn reserve_with_timeout(
        &mut self,
        client_id: ClientId,
        cmd: Command,
        tx: OnceChannel<Command>,
    ) -> Result<(), Error> {
        let id = next_job_id();
        let entry = self.awaiting_clients_flag.entry(client_id).or_insert(id);
        if *entry != id {
            return Ok(());
        }
        
        // 解析超时时间
        let timeout = cmd.params.get("timeout")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        
        // 如果 timeout=0，立即尝试获取任务或返回 TIMED_OUT
        if timeout == 0 {
            // 立即清理 flag，因为这不是一个持久的等待
            self.awaiting_clients_flag.remove(&client_id);
            
            // 尝试立即获取任务
            if let Some(mut job) = self.ready.dequeue() {
                let mut resp_cmd = cmd.clone();
                resp_cmd.job = job.clone();
                
                // 创建异步任务发送响应
                let tx_clone = tx.clone();
                tokio::spawn(async move {
                    let mut tx = tx_clone;
                    if tx.send(resp_cmd).await.is_err() {
                        // 客户端已关闭
                    }
                });
                
                // 更新 job 状态
                job.set_state(State::Reserved).unwrap();
                job.inc_reserves();
                self.reserved.enqueue(job);
                self.update_global_job_stats();
            } else {
                // 没有可用任务，立即返回 TIMED_OUT
                let mut resp_cmd = cmd.clone();
                resp_cmd.err = Err(ProtocolError::TimedOut);
                tokio::spawn(async move {
                    let mut tx = tx;
                    let _ = tx.send(resp_cmd).await;
                });
            }
            return Ok(());
        }
        
        // 更新等待计数
        GLOBAL_STATS.inc_waiting();
        
        let client = AwaitingClient::new_with_timeout(client_id, cmd, tx, timeout);
        self.awaiting_clients.enqueue(client.clone());
        self.awaiting_timed_clients
            .insert(*client.id(), client);
        Ok(())
    }

    pub fn delete(&mut self, cmd: &Command) -> Result<(), ProtocolError> {
        let id = cmd
            .params
            .get("id")
            .unwrap()
            .parse::<Id>()
            .map_err(|_| ProtocolError::BadFormat)?;
        debug!("{} would be deleted", id);
        
        // 尝试从所有队列中删除
        if self.ready.remove(&id).is_some() {
            self.cmd_delete += 1;
            self.update_global_job_stats();
            return Ok(());
        }
        if self.delayed.remove(&id).is_some() {
            self.cmd_delete += 1;
            self.update_global_job_stats();
            return Ok(());
        }
        if self.reserved.remove(&id).is_some() {
            self.cmd_delete += 1;
            self.update_global_job_stats();
            return Ok(());
        }
        if self.buried.remove(&id).is_some() {
            self.cmd_delete += 1;
            self.update_global_job_stats();
            return Ok(());
        }
        
        Err(ProtocolError::NotFound)
    }

    pub fn release(&mut self, cmd: &Command) -> Result<(), ProtocolError> {
        let id = cmd.params.get("id").unwrap().parse::<Id>().unwrap();
        let pri = cmd.params.get("pri").unwrap().parse::<i64>().map_err(|_| ProtocolError::BadFormat)?;
        let delay = cmd.params.get("delay").unwrap().parse::<i64>().map_err(|_| ProtocolError::BadFormat)?;
        
        let mut job = self.reserved.remove(&id).ok_or(ProtocolError::NotFound)?;
        
        // 检查队列大小是否超过限制（模拟内存不足）
        let total_size = self.ready.len() + self.delayed.len() + self.reserved.len() + self.buried.len();
        if total_size >= Self::MAX_QUEUE_SIZE {
            // 内存不足，将作业放入 buried 队列
            job.set_state(State::Buried).unwrap();
            job.inc_buries();
            self.buried.enqueue(job);
            self.update_global_job_stats();
            return Err(ProtocolError::Buried);
        }
        
        // 更新优先级
        job.set_priority(pri);
        job.inc_releases();
        
        if delay > 0 {
            // 有延迟，放入 delayed 队列
            job.set_delay(delay);
            job.set_state(State::Delayed).unwrap();
            self.delayed.enqueue(job);
        } else {
            // 无延迟，放入 ready 队列
            job.set_state(State::Ready).unwrap();
            self.ready.enqueue(job);
        }
        
        self.update_global_job_stats();
        Ok(())
    }

    pub fn buried(&mut self, cmd: &Command) -> Result<(), ProtocolError> {
        let id = cmd.params.get("id").unwrap().parse::<Id>().unwrap();
        let pri = cmd.params.get("pri").unwrap().parse::<i64>().map_err(|_| ProtocolError::BadFormat)?;
        let mut job = self.reserved.remove(&id).ok_or(ProtocolError::NotFound)?;
        job.set_priority(pri);
        job.set_state(State::Buried).unwrap();
        job.inc_buries();
        self.buried.enqueue(job);
        self.update_global_job_stats();
        Ok(())
    }

    pub fn kick(&mut self, cmd: &Command) -> Result<usize, ProtocolError> {
        let bound = cmd
            .params
            .get("bound")
            .map(|item| item.parse::<usize>().unwrap())
            .ok_or(ProtocolError::BadFormat)?;
        let mut kicked = 0usize;
        let _bound = bound.min(self.buried.len());
        for _ in 0.._bound {
            let mut job = self.buried.dequeue().unwrap();
            job.set_state(State::Ready).unwrap();
            job.inc_kicks();
            self.ready.enqueue(job);
            kicked += 1;
        }
        
        if kicked < bound {
            let _bound = (bound - kicked).min(self.delayed.len());
            for _ in 0.._bound {
                let mut job = self.delayed.dequeue().unwrap();
                job.set_state(State::Ready).unwrap();
                job.inc_kicks();
                self.ready.enqueue(job);
                kicked += 1;
            }
        }
        
        if kicked > 0 {
            self.update_global_job_stats();
        }
        Ok(kicked)
    }

    /// 按 ID kick 指定任务（kick-job 命令）- 同步版本
    /// 
    /// 如果 job 在 buried 或 delayed 队列中，将其移动到 ready 队列
    pub fn kick_job_by_id(&mut self, job_id: Id) -> Result<(), ProtocolError> {
        // 尝试从 buried 队列中移除并转移到 ready
        if let Some(mut job) = self.buried.remove(&job_id) {
            job.set_state(State::Ready).map_err(|_| ProtocolError::NotFound)?;
            job.inc_kicks();
            self.ready.enqueue(job);
            self.update_global_job_stats();
            return Ok(());
        }
        
        // 尝试从 delayed 队列中移除并转移到 ready
        if let Some(mut job) = self.delayed.remove(&job_id) {
            job.set_state(State::Ready).map_err(|_| ProtocolError::NotFound)?;
            job.inc_kicks();
            self.ready.enqueue(job);
            self.update_global_job_stats();
            return Ok(());
        }
        
        Err(ProtocolError::NotFound)
    }

    pub fn kick_job(&mut self, cmd: &Command) -> Result<(), ProtocolError> {
        let id: u64 = cmd
            .params
            .get("id")
            .ok_or(ProtocolError::BadFormat)?
            .parse()
            .map_err(|_| ProtocolError::BadFormat)?;
        
        self.kick_job_by_id(id)
    }

    pub fn pause_tube(&mut self, cmd: &Command) -> Result<(), ProtocolError> {
        let delay = cmd
            .params
            .get("delay")
            .map(|item| item.parse::<i64>().unwrap())
            .ok_or(ProtocolError::BadFormat)?;
        self.pause_until = Local::now().timestamp() + delay;
        self.cmd_pause_tube += 1;
        Ok(())
    }

    /// 检查 tube 是否处于暂停状态
    pub fn is_paused(&self, now: i64) -> bool {
        now < self.pause_until
    }

    pub fn touch(&mut self, cmd: &Command) -> Result<(), ProtocolError> {
        let id = cmd
            .params
            .get("id")
            .ok_or(ProtocolError::BadFormat)?
            .parse::<Id>()
            .map_err(|_| ProtocolError::BadFormat)?;
        
        // 尝试在 reserved 队列中找到并更新任务
        if let Some(job) = self.reserved.peek() {
            if *job.id() == id {
                // 找到任务，需要移除并重新入队以重置 TTR
                if let Some(mut job) = self.reserved.remove(&id) {
                    job.reset_ttr();
                    self.reserved.enqueue(job);
                    return Ok(());
                }
            }
        }
        
        Err(ProtocolError::NotFound)
    }

    pub fn peek(&self, cmd: &Command) -> Result<&Job, ProtocolError> {
        let id = cmd
            .params
            .get("id")
            .unwrap()
            .parse::<Id>()
            .map_err(|_| ProtocolError::BadFormat)?;
        // 在所有队列中查找
        if let Some(job) = self.ready.find(&id) {
            return Ok(job);
        }
        if let Some(job) = self.delayed.find(&id) {
            return Ok(job);
        }
        if let Some(job) = self.reserved.find(&id) {
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

    /// 更新全局作业统计
    fn update_global_job_stats(&self) {
        // 计算 urgent 作业数（优先级 < 1024）
        let urgent_count = self.ready.peek_all().iter()
            .filter(|job| job.priority() < 1024)
            .count() as u64;
        
        GLOBAL_STATS.update_job_counts(
            self.ready.len() as u64,
            self.reserved.len() as u64,
            self.delayed.len() as u64,
            self.buried.len() as u64,
        );
        GLOBAL_STATS.set_urgent_count(urgent_count);
    }

    /// 按 ID 预留指定任务（reserve-job 命令）- 同步版本
    /// 
    /// 根据协议，可以从 ready、buried 或 delayed 状态 reserve 一个 job
    /// 返回 reserved 的 job，如果 job 不存在则返回 None
    pub fn reserve_job_by_id(&mut self, job_id: Id) -> Option<Job> {
        // 尝试从 ready 队列中查找并移除指定 job
        if let Some(mut job) = self.ready.remove(&job_id) {
            job.set_state(State::Reserved).unwrap();
            job.inc_reserves();
            self.reserved.enqueue(job.clone());
            self.update_global_job_stats();
            return Some(job);
        }
        
        // 尝试从 buried 队列中查找并移除指定 job
        if let Some(mut job) = self.buried.remove(&job_id) {
            job.set_state(State::Reserved).unwrap();
            job.inc_reserves();
            self.reserved.enqueue(job.clone());
            self.update_global_job_stats();
            return Some(job);
        }
        
        // 尝试从 delayed 队列中查找并移除指定 job
        if let Some(mut job) = self.delayed.remove(&job_id) {
            job.set_state(State::Reserved).unwrap();
            job.inc_reserves();
            self.reserved.enqueue(job.clone());
            self.update_global_job_stats();
            return Some(job);
        }
        
        None
    }

    /// 按 ID 预留指定任务（reserve-job 命令）- 异步版本
    /// 
    /// 根据协议，可以从 ready、buried 或 delayed 状态 reserve 一个 job
    pub async fn reserve_job(
        &mut self,
        _client_id: ClientId,
        cmd: Command,
        mut tx: OnceChannel<Command>,
    ) -> Result<(), Error> {
        let id = cmd
            .params
            .get("id")
            .ok_or_else(|| anyhow::anyhow!("missing id"))?
            .parse::<Id>()?;
        
        // 检查 DEADLINE_SOON 条件
        let now = Local::now().timestamp();
        for job in self.reserved.peek_all() {
            let time_left = job.time_left(now);
            if time_left <= Self::SAFETY_MARGIN && time_left > 0 {
                let mut deadline_cmd = cmd.clone();
                deadline_cmd.err = Err(ProtocolError::DeadlineSoon);
                drop(tx.send(deadline_cmd));
                return Ok(());
            }
        }
        
        // 使用 reserve_job_by_id 方法
        if let Some(job) = self.reserve_job_by_id(id) {
            let mut resp_cmd = cmd.clone();
            resp_cmd.job = job;
            
            // 发送给客户端
            if tx.send(resp_cmd).await.is_err() {
                // 客户端已关闭，需要回滚操作 - 但这比较复杂，
                // 这里简单地记录日志，实际场景可能需要更复杂的处理
                debug!("Client closed during reserve-job, job {} is now reserved", id);
            }
            return Ok(());
        }
        
        // 任务不存在或不在可 reserve 的状态
        let mut not_found_cmd = cmd.clone();
        not_found_cmd.err = Err(ProtocolError::NotFound);
        drop(tx.send(not_found_cmd));
        Ok(())
    }

    pub fn ignore(&mut self, client_id: &ClientId) -> Result<(), ProtocolError> {
        self.awaiting_clients_flag.remove(client_id);
        self.awaiting_timed_clients.remove(client_id);
        self.awaiting_clients.remove(client_id);
        Ok(())
    }

    /// 获取队列统计信息
    pub fn get_stats(&self) -> TubeStats {
        let now = Local::now().timestamp();
        let pause_time_left = (self.pause_until - now).max(0);
        let pause = if self.pause_until > now { self.pause_until - self.pause_tube_time } else { 0 };
        
        TubeStats {
            name: self.name.clone(),
            current_jobs_urgent: self.ready.peek_all().iter().filter(|j| j.priority() < 1024).count(),
            current_jobs_ready: self.ready.len(),
            current_jobs_reserved: self.reserved.len(),
            current_jobs_delayed: self.delayed.len(),
            current_jobs_buried: self.buried.len(),
            total_jobs: self.total_jobs,
            current_using: self.current_using,
            current_waiting: self.awaiting_clients.len(),
            current_watching: self.current_watching,
            pause,
            cmd_delete: self.cmd_delete,
            cmd_pause_tube: self.cmd_pause_tube,
            pause_time_left,
        }
    }
    
    /// 增加 current_using 计数
    pub fn inc_current_using(&mut self) {
        self.current_using += 1;
    }
    
    /// 减少 current_using 计数
    pub fn dec_current_using(&mut self) {
        self.current_using = self.current_using.saturating_sub(1);
    }
    
    /// 增加 current_watching 计数
    pub fn inc_current_watching(&mut self) {
        self.current_watching += 1;
    }
    
    /// 减少 current_watching 计数
    pub fn dec_current_watching(&mut self) {
        self.current_watching = self.current_watching.saturating_sub(1);
    }
    
    /// 获取指定 ID 的任务统计信息
    /// file 参数表示包含此 job 的最早 binlog 文件编号，如果没有使用 binlog 则为 0
    pub fn get_job_stats(&self, id: Id, file: u64) -> Option<JobStats> {
        // 在各个队列中查找任务
        if let Some(job) = self.ready.find(&id) {
            return Some(JobStats::from_job(job, &self.name, file));
        }
        if let Some(job) = self.reserved.find(&id) {
            return Some(JobStats::from_job(job, &self.name, file));
        }
        if let Some(job) = self.delayed.find(&id) {
            return Some(JobStats::from_job(job, &self.name, file));
        }
        if let Some(job) = self.buried.find(&id) {
            return Some(JobStats::from_job(job, &self.name, file));
        }
        None
    }

    /// 检查 tube 是否为空
    pub fn is_empty(&self) -> bool {
        self.ready.len() == 0 && 
        self.reserved.len() == 0 && 
        self.delayed.len() == 0 && 
        self.buried.len() == 0 &&
        self.awaiting_clients.len() == 0
    }
}

/// Tube 统计信息
#[derive(Debug)]
pub struct TubeStats {
    pub name: String,
    pub current_jobs_urgent: usize,
    pub current_jobs_ready: usize,
    pub current_jobs_reserved: usize,
    pub current_jobs_delayed: usize,
    pub current_jobs_buried: usize,
    pub total_jobs: u64,
    pub current_using: u64,
    pub current_waiting: usize,
    pub current_watching: u64,
    pub pause: i64,
    pub cmd_delete: u64,
    pub cmd_pause_tube: u64,
    pub pause_time_left: i64,
}

/// Job 统计信息
#[derive(Debug)]
pub struct JobStats {
    pub id: u64,
    pub tube: String,
    pub state: String,
    pub priority: i64,
    pub age: i64,
    pub delay: i64,
    pub ttr: i64,
    pub time_left: i64,
    pub file: u64,
    pub reserves: u64,
    pub timeouts: u64,
    pub releases: u64,
    pub buries: u64,
    pub kicks: u64,
}

impl JobStats {
    pub fn from_job(job: &Job, tube_name: &str, file: u64) -> Self {
        let now = Local::now().timestamp();
        let age = (now - (job.timestamp() / NANOS_PER_SEC)).max(0);
        let time_left = job.time_left(now);
        
        Self {
            id: *job.id(),
            tube: tube_name.to_string(),
            state: format!("{}", job.state()).to_lowercase(),
            priority: job.priority(),
            age,
            delay: job.delay(),
            ttr: job.ttr(),
            time_left,
            file,
            reserves: job.reserves(),
            timeouts: job.timeouts(),
            releases: job.releases(),
            buries: job.buries(),
            kicks: job.kicks(),
        }
    }
}

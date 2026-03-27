//! Dispatch 模块 - 任务调度中心
//!
//! Dispatch 负责管理所有的 Tube，并将客户端请求路由到对应的 Tube。

use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Error;
use futures::{
    channel::mpsc::{self, UnboundedReceiver, UnboundedSender},
    FutureExt, SinkExt, StreamExt,
};
use tokio::select;
use tokio::task;
use tokio::time::interval;
use tracing::{debug, info, instrument, warn, Instrument};

use crate::architecture::cmd::{Command, CommandKind};
use crate::architecture::error::ProtocolError;
use crate::architecture::job::{AwaitingClient, Job, State};
use crate::architecture::stats::{is_draining, GLOBAL_STATS};
use crate::architecture::tube::{ClientId, PriorityQueue, Tube};
use crate::backup::binlog::{log_put, log_delete, JobState};
use crate::backend::job_store::{global_insert_job, global_remove_job};
use crate::backend::min_heap::MinHeap;
use crate::operation::once_channel::OnceChannel;
use std::sync::atomic::Ordering;

/// 命令发送端类型
pub type CmdSender = UnboundedSender<Command>;

/// Tube 发送端类型，用于向 Tube 发送客户端命令
pub type TubeSender = UnboundedSender<(ClientId, Command)>;
/// Tube 接收端类型
pub type TubeReceiver = UnboundedReceiver<(ClientId, Command)>;

type InnerSender = UnboundedSender<TubeItem>;
type InnerReceiver = UnboundedReceiver<TubeItem>;

/// 内部 Tube 消息类型
#[derive(Clone)]
enum TubeItem {
    /// 添加客户端到 Tube（用于 use 命令）
    AddUsing(
        ClientId,
        UnboundedSender<()>,
    ),
    /// 添加客户端到 Tube（用于 watch 命令）
    AddWatching(
        ClientId,
        CmdSender,
        UnboundedSender<()>,
        OnceChannel<Command>,
    ),
    /// 从 Tube 删除客户端（use）
    DeleteUsing(ClientId, UnboundedSender<()>),
    /// 从 Tube 删除客户端（watch）
    DeleteWatching(ClientId, UnboundedSender<()>),
    /// 停止 Tube
    #[allow(dead_code)]
    Stop,
}

/// Dispatch 结构体，管理所有 Tube 和客户端连接
pub struct Dispatch {
    #[allow(dead_code)]
    stop: Vec<futures::channel::mpsc::Sender<()>>,
    tube_ch: HashMap<String, InnerSender>,
    cmd_tx: HashMap<String, UnboundedSender<(ClientId, Command)>>,
}

impl Dispatch {
    /// 创建一个新的 Dispatch 实例
    pub fn new() -> Dispatch {
        Dispatch {
            stop: Vec::new(),
            tube_ch: HashMap::new(),
            cmd_tx: HashMap::new(),
        }
    }

    /// 创建或获取一个 Tube（用于 use 命令）
    #[instrument(skip(self), fields(tube_name = %name))]
    pub async fn add_tube_using(
        &mut self,
        name: String,
        client_id: ClientId,
    ) -> Result<(), Error> {
        debug!("add using tube: {}", name);
        if let Some(tube_tx) = self.tube_ch.get(&name) {
            let (callback_tx, mut callback_rx) = mpsc::unbounded::<()>();
            tube_tx
                .clone()
                .send(TubeItem::AddUsing(client_id, callback_tx))
                .await
                .unwrap();
            callback_rx.next().await;
            return Ok(());
        }
        // Tube 不存在，不需要创建（use 命令不需要创建 tube，只在 put 时创建）
        Ok(())
    }

    /// 创建或获取一个 Tube（用于 watch 命令）
    #[instrument(skip(self, reply, reserve_reply), fields(tube_name = %name))]
    pub async fn spawn_tube(
        &mut self,
        name: String,
        client_id: ClientId,
        reply: CmdSender,
        reserve_reply: OnceChannel<Command>,
    ) -> Result<TubeSender, Error> {
        debug!("spawn a tube: {}", name);
        if let Some(cmd_sender) = self.cmd_tx.get(&name) {
            let (callback_tx, mut callback_rx) = mpsc::unbounded::<()>();
            self.tube_ch
                .get(&name)
                .unwrap()
                .send(TubeItem::AddWatching(client_id, reply, callback_tx, reserve_reply))
                .await
                .unwrap();
            callback_rx.next().await;
            return Ok(cmd_sender.clone());
        }
        let (callback_tx, mut callback_rx) = mpsc::unbounded::<()>();
        let (mut tube_tx, tube_rx) = mpsc::unbounded::<TubeItem>();
        self.tube_ch.insert(name.clone(), tube_tx.clone());
        let (cmd_tx, cmd_rx) = mpsc::unbounded::<(ClientId, Command)>();
        self.cmd_tx.insert(name.clone(), cmd_tx.clone());
        self.task(name, tube_rx, cmd_rx);
        tube_tx
            .send(TubeItem::AddWatching(client_id, reply, callback_tx, reserve_reply))
            .await
            .unwrap();
        callback_rx.next().await;
        Ok(cmd_tx)
    }

    #[instrument(skip(self), fields(tube_name = %name, client_id))]
    pub async fn drop_client(&mut self, name: &String, client_id: ClientId) {
        debug!("Drop {} from {}", client_id, name);
        let (callback_tx, mut callback_rx) = mpsc::unbounded::<()>();
        if let Some(tube_tx) = self.tube_ch.get_mut(name) {
            tube_tx
                .send(TubeItem::DeleteUsing(client_id, callback_tx))
                .await
                .unwrap();
            callback_rx.next().await.unwrap();
        }
    }

    #[instrument(skip(self), fields(tube_name = %name, client_id))]
    pub async fn drop_watching(&mut self, name: &String, client_id: ClientId) {
        debug!("Drop watching {} from {}", client_id, name);
        let (callback_tx, mut callback_rx) = mpsc::unbounded::<()>();
        if let Some(tube_tx) = self.tube_ch.get_mut(name) {
            tube_tx
                .send(TubeItem::DeleteWatching(client_id, callback_tx))
                .await
                .unwrap();
            callback_rx.next().await.unwrap();
        }
    }

    pub fn list_tubes(&self) -> (usize, Vec<String>) {
        (self.cmd_tx.len(), self.cmd_tx.keys().cloned().collect())
    }

    /// 获取所有 tube 的名称列表
    pub fn get_all_tube_names(&self) -> Vec<String> {
        self.cmd_tx.keys().cloned().collect()
    }

    /// 从 binlog 恢复 jobs 到对应的 tube
    /// jobs 参数: (tube_name, job, state)
    pub async fn recover_jobs(&mut self, jobs: Vec<(String, Job, JobState)>) {
        for (tube_name, job, state) in jobs {
            // 确保 tube 存在
            if !self.cmd_tx.contains_key(&tube_name) {
                // 创建 tube
                let (_callback_tx, _callback_rx) = mpsc::unbounded::<()>();
                let (tube_tx, tube_rx) = mpsc::unbounded::<TubeItem>();
                self.tube_ch.insert(tube_name.clone(), tube_tx.clone());
                let (cmd_tx, cmd_rx) = mpsc::unbounded::<(ClientId, Command)>();
                self.cmd_tx.insert(tube_name.clone(), cmd_tx.clone());
                self.task(tube_name.clone(), tube_rx, cmd_rx);
            }
            
            // 根据状态设置 job 的初始状态
            let job_state = match state {
                JobState::Ready => State::Ready,
                JobState::Reserved => State::Ready, // Reserved jobs 恢复为 Ready
                JobState::Buried => State::Buried,
                JobState::Delayed => State::Delayed,
            };
            
            // 发送 put 命令到 tube
            if let Some(cmd_tx) = self.cmd_tx.get(&tube_name) {
                let mut cmd = Command::default();
                cmd.name = CommandKind::Put.to_string();
                cmd.job = job.clone();
                
                // 设置正确的初始状态
                if job_state == State::Buried {
                    // Buried jobs 需要特殊处理
                    cmd.job.set_state(State::Buried).ok();
                }
                
                // 创建一个虚拟的 client_id (0 表示恢复)
                let mut cmd_tx = cmd_tx.clone();
                let _ = cmd_tx.send((0, cmd)).await;
            }
            
            // 更新统计
            GLOBAL_STATS.inc_total_jobs();
        }
    }

    /// 获取所有 tube 的统计信息
    #[allow(dead_code)]
    pub fn get_all_tube_stats(&self) -> HashMap<String, crate::architecture::tube::TubeStats> {
        let mut stats = HashMap::new();
        for name in self.cmd_tx.keys() {
            // 这里我们需要一个方式来获取 tube 的统计
            // 暂时返回空，后续可以通过 channel 请求统计
            stats.insert(
                name.clone(),
                crate::architecture::tube::TubeStats {
                    name: name.clone(),
                    current_jobs_urgent: 0,
                    current_jobs_ready: 0,
                    current_jobs_reserved: 0,
                    current_jobs_delayed: 0,
                    current_jobs_buried: 0,
                    total_jobs: 0,
                    current_using: 0,
                    current_waiting: 0,
                    current_watching: 0,
                    pause: 0,
                    cmd_delete: 0,
                    cmd_pause_tube: 0,
                    pause_time_left: 0,
                },
            );
        }
        stats
    }

    fn task(&mut self, tube_name: String, mut tube_rx: InnerReceiver, mut cmd_rx: TubeReceiver) {
        let span = tracing::info_span!("tube_task", tube_name = %tube_name);
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
            let mut interval = interval(Duration::from_millis(1));
            let (mut _tx, mut _rx) = mpsc::unbounded::<()>();

            // 自动清理计数器：当 tube 为空时开始计数
            let mut empty_check_counter: u32 = 0;
            const EMPTY_CHECK_THRESHOLD: u32 = 1000; // 约1秒（1ms * 1000）

            loop {
                select! {
                    _ = interval.tick() => {
                        tube.process().await;
                        tube.process_timed_clients().await;

                        // Tube 自动清理检查
                        if tube.is_empty() && clients.is_empty() && tube_name != "default" {
                            empty_check_counter += 1;
                            if empty_check_counter >= EMPTY_CHECK_THRESHOLD {
                                info!("Tube {} is empty, stopping it", tube_name);
                                break;
                            }
                        } else {
                            empty_check_counter = 0;
                        }
                    },
                    _ = _rx.next().fuse() => {
                        tube.process().await;
                        tube.process_timed_clients().await;
                    }
                    cmd = tube_rx.next().fuse() => match cmd {
                        Some(cmd) => {
                            match cmd {
                                TubeItem::AddUsing(client_id, mut cb) => {
                                    debug!("Add using client:{} to tube:{}", client_id, tube_name);
                                    tube.inc_current_using();
                                    empty_check_counter = 0;
                                    _=cb.send(()).await;
                                },
                                TubeItem::AddWatching(client_id, ch, mut cb, reserve_cb_rx) => {
                                    debug!("Add watching client:{} to tube:{}", client_id, tube_name);
                                    clients.insert(client_id, (ch, reserve_cb_rx));
                                    tube.inc_current_watching();
                                    empty_check_counter = 0;
                                    _=cb.send(()).await;
                                },
                                TubeItem::DeleteUsing(client_id, mut cb) =>{
                                    debug!("Remove using client {}", client_id);
                                    tube.dec_current_using();
                                    _=cb.send(()).await;
                                },
                                TubeItem::DeleteWatching(client_id, mut cb) =>{
                                    debug!("Remove watching client {}", client_id);
                                    clients.remove(&client_id);
                                    tube.drop_client(&client_id);
                                    tube.dec_current_watching();
                                    _=cb.send(()).await;
                                },
                                TubeItem::Stop => {
                                    info!("Stop tube: {}", tube_name.clone());
                                    break;
                                },
                            }
                        },
                        _ => break // Tube channel 关闭，退出循环
                     },
                    cmd = cmd_rx.next().fuse() => if let Some(command) = cmd {
                         Self::handle_command(&mut clients, &mut tube, command.clone()).await;
                         let cmd = CommandKind::from_str(&command.1.name).unwrap();
                         // 参照 C 实现 (prot.c): enqueue_job/process_queue - Put 和 Reserve 后同步尝试匹配
                         if cmd == CommandKind::Put
                             || cmd == CommandKind::ReserveWithTimeout
                             || cmd == CommandKind::Reserve
                         {
                             tube.process().await;
                             tube.process_timed_clients().await;
                         }
                         // 重置清理计数器，因为可能有新活动
                         empty_check_counter = 0;
                    }
                }
            }
            info!("Tube task {} ended", tube_name);
        }.instrument(span));
    }

    #[instrument(skip(clients, tube, command), fields(client_id = command.0, cmd = %command.1.name))]
    async fn handle_command<
        J: PriorityQueue<Job> + Send + 'static,
        A: PriorityQueue<AwaitingClient> + Send + 'static,
    >(
        clients: &mut HashMap<ClientId, (CmdSender, OnceChannel<Command>)>,
        tube: &mut Tube<J, A>,
        mut command: (ClientId, Command),
    ) {
        let cmd = CommandKind::from_str(&command.1.name).unwrap();
        if let Some((ref mut tx, ref reserve_tx)) = clients.get_mut(&command.0) {
            match cmd {
                CommandKind::Put => {
                    GLOBAL_STATS.inc_cmd("put");

                    // 检查 drain 模式
                    if is_draining() {
                        let mut cmd = command.1.clone();
                        cmd.err = Err(ProtocolError::Draining);
                        tx.send(cmd).await.unwrap();
                        return;
                    }

                    // 插入全局存储（需先设置 tube，供 Delete 等命令路由）
                    command.1.job.set_tube(tube.name().to_string());
                    global_insert_job(command.1.job.clone());
                    
                    GLOBAL_STATS.inc_total_jobs();
                    match tube.put(command.1.clone()) {
                        Ok(()) => {
                            // 写入 binlog
                            let job = &command.1.job;
                            let tube_name = tube.name();
                            // 根据 delay 确定状态
                            let state = if job.delay() > 0 {
                                crate::architecture::job::State::Delayed
                            } else {
                                crate::architecture::job::State::Ready
                            };
                            if let Err(e) = log_put(job, tube_name, state).await {
                                warn!("Failed to write binlog: {}", e);
                            } else {
                                GLOBAL_STATS.binlog_records_written.fetch_add(1, Ordering::SeqCst);
                            }
                            tx.send(command.1).await.unwrap();
                        }
                        Err(ProtocolError::Buried) => {
                            // 内存不足，返回 BURIED <id>
                            let mut cmd = command.1.clone();
                            cmd.err = Err(ProtocolError::Buried);
                            tx.send(cmd).await.unwrap();
                        }
                        Err(e) => {
                            // 失败时从全局存储移除
                            global_remove_job(command.1.job.id());
                            let mut cmd = command.1.clone();
                            cmd.err = Err(e);
                            tx.send(cmd).await.unwrap();
                        }
                    }
                }
                CommandKind::Reserve => {
                    GLOBAL_STATS.inc_cmd("reserve");
                    tube.reserve(command.0, command.1.clone(), reserve_tx.clone())
                        .unwrap();
                }
                CommandKind::ReserveWithTimeout => {
                    GLOBAL_STATS.inc_cmd("reserve-with-timeout");
                    tube.reserve_with_timeout(command.0, command.1.clone(), reserve_tx.clone())
                        .unwrap();
                }
                CommandKind::Delete => {
                    GLOBAL_STATS.inc_cmd("delete");
                    // 获取 job_id 用于 binlog
                    let job_id = command.1.params.get("id")
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(0);
                    
                    command.1.err = tube.delete(&command.1);
                    
                    // 写入 binlog delete 记录
                    if command.1.err.is_ok() {
                        // 从全局存储移除
                        global_remove_job(&job_id);
                        let tube_name = tube.name();
                        if let Err(e) = log_delete(job_id, tube_name).await {
                            warn!("Failed to write binlog delete: {}", e);
                        }
                    }
                    
                    tx.send(command.1).await.unwrap();
                }
                CommandKind::Release => {
                    GLOBAL_STATS.inc_cmd("release");
                    command.1.err = tube.release(&command.1);
                    tx.send(command.1).await.unwrap();
                }
                CommandKind::Bury => {
                    GLOBAL_STATS.inc_cmd("bury");
                    command.1.err = tube.buried(&command.1);
                    tx.send(command.1).await.unwrap();
                }
                CommandKind::Kick => {
                    GLOBAL_STATS.inc_cmd("kick");
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
                CommandKind::KickJob => {
                    GLOBAL_STATS.inc_cmd("kick-job");
                    command.1.err = tube.kick_job(&command.1);
                    tx.send(command.1).await.unwrap();
                }
                CommandKind::PauseTube => {
                    GLOBAL_STATS.inc_cmd("pause-tube");
                    command.1.err = tube.pause_tube(&command.1);
                    tx.send(command.1).await.unwrap();
                }
                CommandKind::Touch => {
                    GLOBAL_STATS.inc_cmd("touch");
                    command.1.err = tube.touch(&command.1).map(|_| ());
                    tx.send(command.1).await.unwrap();
                }
                CommandKind::Peek => {
                    GLOBAL_STATS.inc_cmd("peek");
                    match tube.peek(&command.1) {
                        Ok(job) => {
                            command.1.job = job;  // peek 已返回复制的 Job
                        }
                        Err(err) => {
                            command.1.err = Err(err);
                        }
                    }
                    tx.send(command.1).await.unwrap();
                }
                CommandKind::PeekReady => {
                    GLOBAL_STATS.inc_cmd("peek-ready");
                    match tube.peek_ready() {
                        Ok(job) => {
                            command.1.job = job;  // peek 已返回复制的 Job
                        }
                        Err(err) => {
                            command.1.err = Err(err);
                        }
                    }
                    tx.send(command.1).await.unwrap();
                }
                CommandKind::PeekDelayed => {
                    GLOBAL_STATS.inc_cmd("peek-delayed");
                    match tube.peek_delayed() {
                        Ok(job) => {
                            command.1.job = job;  // peek 已返回复制的 Job
                        }
                        Err(err) => {
                            command.1.err = Err(err);
                        }
                    }
                    tx.send(command.1).await.unwrap();
                }
                CommandKind::PeekBuried => {
                    GLOBAL_STATS.inc_cmd("peek-buried");
                    match tube.peek_buried() {
                        Ok(job) => {
                            command.1.job = job;  // peek 已返回复制的 Job
                        }
                        Err(err) => {
                            command.1.err = Err(err);
                        }
                    }
                    tx.send(command.1).await.unwrap();
                }
                CommandKind::Ignore => {
                    GLOBAL_STATS.inc_cmd("ignore");
                    _ = tube.ignore(&command.0);
                    tx.send(command.1).await.unwrap();
                }
                CommandKind::ReserveJob => {
                    GLOBAL_STATS.inc_cmd("reserve-job");
                    let tx_clone = reserve_tx.clone();
                    tube.reserve_job(command.0, command.1.clone(), tx_clone)
                        .await
                        .unwrap();
                }
                CommandKind::StatsJob => {
                    GLOBAL_STATS.inc_cmd("stats-job");
                    let id = command.1.params.get("id")
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(0);
                    
                    // 获取 binlog 文件编号，如果没有使用 binlog 则为 0
                    let file = GLOBAL_STATS.binlog_current_index.load(std::sync::atomic::Ordering::SeqCst);
                    
                    if let Some(job_stats) = tube.get_job_stats(id, file) {
                        let stats: std::collections::HashMap<String, serde_yaml::Value> = [
                            ("id".to_string(), serde_yaml::to_value(job_stats.id).unwrap()),
                            ("tube".to_string(), serde_yaml::to_value(job_stats.tube).unwrap()),
                            ("state".to_string(), serde_yaml::to_value(job_stats.state).unwrap()),
                            ("pri".to_string(), serde_yaml::to_value(job_stats.priority).unwrap()),
                            ("age".to_string(), serde_yaml::to_value(job_stats.age).unwrap()),
                            ("delay".to_string(), serde_yaml::to_value(job_stats.delay).unwrap()),
                            ("ttr".to_string(), serde_yaml::to_value(job_stats.ttr).unwrap()),
                            ("time-left".to_string(), serde_yaml::to_value(job_stats.time_left).unwrap()),
                            ("file".to_string(), serde_yaml::to_value(job_stats.file).unwrap()),
                            ("reserves".to_string(), serde_yaml::to_value(job_stats.reserves).unwrap()),
                            ("timeouts".to_string(), serde_yaml::to_value(job_stats.timeouts).unwrap()),
                            ("releases".to_string(), serde_yaml::to_value(job_stats.releases).unwrap()),
                            ("buries".to_string(), serde_yaml::to_value(job_stats.buries).unwrap()),
                            ("kicks".to_string(), serde_yaml::to_value(job_stats.kicks).unwrap()),
                        ].into_iter().collect();
                        command.1.yaml = Some(serde_yaml::to_string(&stats).unwrap());
                    } else {
                        command.1.err = Err(ProtocolError::NotFound);
                    }
                    tx.send(command.1).await.unwrap();
                }
                CommandKind::StatsTube => {
                    GLOBAL_STATS.inc_cmd("stats-tube");
                    let tube_stats = tube.get_stats();
                    let stats: std::collections::HashMap<String, serde_yaml::Value> = [
                        ("name".to_string(), serde_yaml::to_value(tube_stats.name).unwrap()),
                        ("current-jobs-urgent".to_string(), serde_yaml::to_value(tube_stats.current_jobs_urgent).unwrap()),
                        ("current-jobs-ready".to_string(), serde_yaml::to_value(tube_stats.current_jobs_ready).unwrap()),
                        ("current-jobs-reserved".to_string(), serde_yaml::to_value(tube_stats.current_jobs_reserved).unwrap()),
                        ("current-jobs-delayed".to_string(), serde_yaml::to_value(tube_stats.current_jobs_delayed).unwrap()),
                        ("current-jobs-buried".to_string(), serde_yaml::to_value(tube_stats.current_jobs_buried).unwrap()),
                        ("total-jobs".to_string(), serde_yaml::to_value(tube_stats.total_jobs).unwrap()),
                        ("current-using".to_string(), serde_yaml::to_value(tube_stats.current_using).unwrap()),
                        ("current-waiting".to_string(), serde_yaml::to_value(tube_stats.current_waiting).unwrap()),
                        ("current-watching".to_string(), serde_yaml::to_value(tube_stats.current_watching).unwrap()),
                        ("pause".to_string(), serde_yaml::to_value(tube_stats.pause).unwrap()),
                        ("cmd-delete".to_string(), serde_yaml::to_value(tube_stats.cmd_delete).unwrap()),
                        ("cmd-pause-tube".to_string(), serde_yaml::to_value(tube_stats.cmd_pause_tube).unwrap()),
                        ("pause-time-left".to_string(), serde_yaml::to_value(tube_stats.pause_time_left).unwrap()),
                    ].into_iter().collect();
                    command.1.yaml = Some(serde_yaml::to_string(&stats).unwrap());
                    tx.send(command.1).await.unwrap();
                }
                _ => unreachable!(),
            }
        }
    }

    #[allow(dead_code)]
    pub async fn stop(&mut self) -> Result<(), Error> {
        for sender in &mut self.stop {
            sender.send(()).await.unwrap();
        }
        Ok(())
    }
}

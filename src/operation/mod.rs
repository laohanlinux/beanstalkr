use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, Error};
use futures::{
    channel::mpsc::{self as futures_mpsc, UnboundedReceiver as FuturesUnboundedReceiver, UnboundedSender},
    SinkExt, StreamExt,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, MutexGuard};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver as TokioUnboundedReceiver};
use tokio::task;
use tracing::{debug, info, instrument};

use crate::architecture::cmd::{Command, CommandKind};
use crate::architecture::error::ProtocolError;
use crate::architecture::job::next_client_id;
use crate::architecture::stats::GLOBAL_STATS;
use crate::architecture::tube::ClientId;
use crate::util::configure_client_socket;

/// 验证 tube 名称是否符合规范
/// - 不能超过 200 字节
/// - 不能以连字符开头
/// - 只能包含：字母、数字、-、+、/、;、.、$、_、(、)
fn validate_tube_name(name: &str) -> Result<(), ProtocolError> {
    if name.len() > 200 {
        return Err(ProtocolError::BadFormat);
    }
    if name.starts_with('-') {
        return Err(ProtocolError::BadFormat);
    }
    // 验证每个字符是否合法
    for c in name.chars() {
        if !c.is_ascii_alphanumeric() 
            && c != '-'
            && c != '+'
            && c != '/'
            && c != ';'
            && c != '.'
            && c != '$'
            && c != '_'
            && c != '('
            && c != ')' {
            return Err(ProtocolError::BadFormat);
        }
    }
    Ok(())
}
use crate::operation::once_channel::OnceChannel;

pub mod dispatch;
pub mod once_channel;

use dispatch::{Dispatch, TubeSender};

pub struct ClientHandler {
    client_id: ClientId,
    use_tube: String,
    reader: Arc<Mutex<BufReader<OwnedReadHalf>>>,
    writer: Arc<Mutex<OwnedWriteHalf>>,
    dispatch: Arc<Mutex<Dispatch>>,
    tx: Option<UnboundedSender<Command>>,
    rx: Option<FuturesUnboundedReceiver<Command>>,
    tube_rx: HashMap<String, TubeSender>,
    reserve_tx: OnceChannel<Command>,
    reserve_rx: TokioUnboundedReceiver<Command>,
    watch_tubes: HashMap<String, ()>,
    has_put: bool,      // 是否发出过 put 命令
    has_reserved: bool, // 是否发出过 reserve 命令
}

impl ClientHandler {
    /// 创建一个新的客户端处理器
    ///
    /// # Arguments
    /// * `stream` - TCP 连接
    /// * `dispatch` - 调度器引用
    pub fn new(stream: TcpStream, dispatch: Arc<Mutex<Dispatch>>) -> Self {
        // 配置 socket 选项（TCP_NODELAY 等）
        if let Err(e) = configure_client_socket(&stream) {
            tracing::warn!("Failed to configure client socket: {}", e);
        }
        
        let (tx, rx) = futures_mpsc::unbounded();
        // 无界通道：reserve 响应经 OnceChannel 投递，若使用有界 mpsc 在部分调度顺序下
        // 可能出现「sender 等空槽 / ClientHandler 已在 recv 等待」的竞态，导致永久无响应。
        let (reserve_tx, reserve_rx) = unbounded_channel();
        let once_channel = OnceChannel::new(reserve_tx);
        let mut watch_tubes = HashMap::new();
        watch_tubes.insert("default".to_string(), ());
        let (read_half, write_half) = stream.into_split();
        ClientHandler {
            client_id: next_client_id(),
            use_tube: "default".to_string(),
            reader: Arc::new(Mutex::new(BufReader::new(read_half))),
            writer: Arc::new(Mutex::new(write_half)),
            dispatch,
            tx: Some(tx),
            rx: Some(rx),
            tube_rx: HashMap::new(),
            reserve_tx: once_channel,
            reserve_rx,
            watch_tubes,
            has_put: false,
            has_reserved: false,
        }
    }

    /// 启动客户端处理循环
    ///
    /// 处理客户端连接，解析命令并执行相应的操作。
    /// 当客户端断开连接时，清理相关资源。
    #[instrument(skip(self), fields(client_id = self.client_id))]
    pub async fn spawn_start(&mut self) -> Result<(), Error> {
        // 增加连接计数
        GLOBAL_STATS.inc_connection();
        
        // 注册默认 tube
        self.handle_base_command(Command::default()).await?;
        let ret = self.parse_command().await;
        let mut dispatch: MutexGuard<Dispatch> = self.dispatch.lock().await;
        dispatch.drop_client(&self.use_tube, self.client_id).await;
        
        // 减少连接计数
        GLOBAL_STATS.dec_connection();
        
        // 如果此连接是 producer/worker，减少相应计数
        if self.has_put {
            GLOBAL_STATS.dec_producer();
        }
        if self.has_reserved {
            GLOBAL_STATS.dec_worker();
        }
        
        // 减少 watching 计数
        for (tube_name, _) in &self.watch_tubes {
            dispatch.drop_watching(tube_name, self.client_id).await;
        }
        
        info!("Client offline");
        ret
    }

    #[instrument(skip(self))]
    async fn parse_command(&mut self) -> Result<(), Error> {
        let mut command: Command = Default::default();
        loop {
            let mut reader = self.reader.lock().await;
            let mut line = String::new();
            let n = (*reader).read_line(&mut line).await?;
            drop(reader);
            if n == 0 {
                break;
            }
            let line = line.trim_end();
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

    #[instrument(skip(self, command))]
    async fn handle_reply(&mut self, command: &mut Command) -> Result<(), Error> {
        let mut writer = self.writer.lock().await;
        loop {
            let (more, reply) = command.reply().await;
            // 防止 reply() 意外返回空；根据命令名提供默认成功回复
            let reply = if reply.is_empty() && command.err.is_ok() {
                if command.name.contains("delete") {
                    "DELETED".to_string()
                } else if command.name.contains("release") {
                    "RELEASED".to_string()
                } else if command.name.contains("bury") {
                    "BURIED".to_string()
                } else if command.name.contains("touch") {
                    "TOUCHED".to_string()
                } else {
                    reply
                }
            } else {
                reply
            };
            // 成功时的 RESERVED/FOUND 首行与 job body 不加 \r\n（协议：首行后紧跟 <bytes> 字节）。
            // 错误单行（如 TIMED_OUT）必须带 \r\n，否则客户端 read_line 会一直阻塞。
            let to_send = if more {
                reply.clone() + "\r\n"
            } else if command.err.is_ok()
                && matches!(
                    command.name.as_str(),
                    "reserve" | "reserve-with-timeout" | "reserve-job" | "peek" | "peek-ready"
                        | "peek-delayed" | "peek-buried"
                )
            {
                reply.clone()
            } else {
                reply + "\r\n"
            };
            writer.write_all(to_send.as_bytes()).await?;
            if !more {
                break;
            }
        }
        Ok(())
    }

    #[instrument(skip(self))]
    async fn handle_reply_err(&mut self, err: Error) -> Result<(), Error> {
        let mut writer = self.writer.lock().await;
        writer.write_all(format!("{}\r\n", err).as_bytes()).await?;
        Ok(())
    }

    #[instrument(skip(self, command), fields(cmd = %command.name))]
    async fn handle_base_command(&mut self, mut command: Command) -> Result<Command, Error> {
        let cmd = CommandKind::from_str(&command.name)
            .map_err(|_| ProtocolError::UnknownCommand)?;
        match cmd {
            CommandKind::Use => {
                let tube_name = command.params.get("tube")
                    .ok_or_else(|| anyhow!("missing tube parameter"))?;
                GLOBAL_STATS.inc_cmd("use");
                validate_tube_name(tube_name)?;
                
                // 如果切换到新的 tube，更新统计
                if self.use_tube != *tube_name {
                    // 从旧的 tube 移除 using 统计
                    let mut dispatch: MutexGuard<Dispatch> = self.dispatch.lock().await;
                    dispatch.drop_client(&self.use_tube, self.client_id).await;
                    
                    // 添加到新的 tube（如果 tube 存在）
                    dispatch.add_tube_using(tube_name.clone(), self.client_id).await.ok();
                    drop(dispatch);
                    
                    self.use_tube = tube_name.clone();
                }
                
                // 确保 tube 在 tube_rx 中（用于后续命令处理）
                if !self.tube_rx.contains_key(tube_name) {
                    let mut dispatch: MutexGuard<Dispatch> = self.dispatch.lock().await;
                    let tx = self.tx.as_ref()
                        .ok_or_else(|| anyhow!("sender not initialized"))?;
                    let tube_ch = dispatch
                        .spawn_tube(
                            tube_name.clone(),
                            self.client_id,
                            tx.clone(),
                            self.reserve_tx.clone(),
                        )
                        .await?;
                    self.tube_rx.insert(self.use_tube.clone(), tube_ch);
                }
                Ok(command)
            }
            CommandKind::Watch => {
                let tube_name = command.params.get("tube")
                    .ok_or_else(|| anyhow!("missing tube parameter"))?;
                GLOBAL_STATS.inc_cmd("watch");
                validate_tube_name(tube_name)?;
                let count = self.watch_tubes.len() - 1;
                if self.tube_rx.contains_key(tube_name) {
                    command
                        .params
                        .insert("count".to_owned(), format!("{}", count));
                    return Ok(command);
                }
                let mut dispatch: MutexGuard<Dispatch> = self.dispatch.lock().await;
                let tx = self.tx.as_ref()
                    .ok_or_else(|| anyhow!("sender not initialized"))?;
                let tube_ch = dispatch
                    .spawn_tube(
                        tube_name.clone(),
                        self.client_id,
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
            CommandKind::Ignore => {
                let tube_name = command.params.get("tube")
                    .ok_or_else(|| anyhow!("missing tube parameter"))?;
                GLOBAL_STATS.inc_cmd("ignore");
                validate_tube_name(tube_name)?;
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
                let tube_tx = self.tube_rx.get_mut(&self.use_tube)
                    .ok_or_else(|| anyhow!("tube not found: {}", self.use_tube))?;
                tube_tx
                    .send((self.client_id, command.clone()))
                    .await
                    .map_err(|e| anyhow!("send failed: {}", e))?;
                let rx = self.rx.as_mut()
                    .ok_or_else(|| anyhow!("receiver not initialized"))?;
                let mut command: Command = rx.next().await
                    .ok_or_else(|| anyhow!("channel closed"))?;
                command
                    .params
                    .insert("count".to_owned(), format!("{}", count));
                Ok(command)
            }
            CommandKind::Reserve | CommandKind::ReserveWithTimeout => {
                let client_id = self.client_id;
                
                // 标记为 worker（只在第一次 reserve 时）
                if !self.has_reserved {
                    self.has_reserved = true;
                    GLOBAL_STATS.inc_worker();
                }
                
                self.reserve_tx.open();
                for (tube_name, _) in self.watch_tubes.iter() {
                    debug!("watch {}", tube_name);
                    let tube_ch = self.tube_rx.get_mut(tube_name)
                        .ok_or_else(|| anyhow!("tube not found: {}", tube_name))?;
                    tube_ch.send((client_id, command.clone())).await
                        .map_err(|e| anyhow!("send reserve failed: {}", e))?;
                    debug!("sent reserve to {}", tube_name);
                }
                Ok(self
                    .reserve_rx
                    .recv()
                    .await
                    .ok_or_else(|| anyhow!("reserve channel closed"))?)
            }
            CommandKind::ListTubesWatched => {
                GLOBAL_STATS.inc_cmd("list-tubes-watched");
                let lists: Vec<String> = self.watch_tubes.keys().cloned().collect();
                let lists = serde_yaml::to_string(&lists)
                    .map_err(|e| anyhow!("to_value failed: {}", e))?;
                command.yaml = Some(lists);
                Ok(command)
            }
            CommandKind::ListTubes => {
                GLOBAL_STATS.inc_cmd("list-tubes");
                let dispatch = self.dispatch.lock().await;
                let (count, tubes) = dispatch.list_tubes();
                let lists = serde_yaml::to_string(&tubes)
                    .map_err(|e| anyhow!("to_value failed: {}", e))?;
                command.yaml = Some(lists);
                command
                    .params
                    .insert("count".to_owned(), format!("{}", count));
                Ok(command)
            }
            CommandKind::ListTubeUsed => {
                GLOBAL_STATS.inc_cmd("list-tube-used");
                command
                    .params
                    .insert("tube".to_owned(), self.use_tube.clone());
                Ok(command)
            }
            CommandKind::Stats => {
                // 记录 stats 命令
                GLOBAL_STATS.inc_cmd("stats");
                
                // 获取主机名
                let hostname = hostname::get()
                    .ok()
                    .and_then(|h| h.into_string().ok())
                    .unwrap_or_else(|| "localhost".to_string());
                
                // 更新 tube 数量
                let dispatch = self.dispatch.lock().await;
                let (tube_count, _) = dispatch.list_tubes();
                drop(dispatch);
                GLOBAL_STATS.current_tubes.store(tube_count as u64, std::sync::atomic::Ordering::SeqCst);
                
                command.yaml = Some(GLOBAL_STATS.to_yaml(&hostname, env!("CARGO_PKG_VERSION")));
                Ok(command)
            }
            CommandKind::StatsJob | CommandKind::StatsTube => {
                // 这些命令通过 tube 处理以获取真实数据
                let tube_tx = self.tube_rx.get_mut(&self.use_tube)
                    .ok_or_else(|| anyhow!("tube not found: {}", self.use_tube))?;
                tube_tx
                    .send((self.client_id, command.clone()))
                    .await
                    .map_err(|e| anyhow!("send failed: {}", e))?;
                let rx = self.rx.as_mut()
                    .ok_or_else(|| anyhow!("receiver not initialized"))?;
                let command = rx.next().await
                    .ok_or_else(|| anyhow!("channel closed"))?;
                Ok(command)
            }
            CommandKind::Quit => Err(anyhow!("Client quit")),
            CommandKind::Delete | CommandKind::Release | CommandKind::Bury | CommandKind::Touch => {
                // 路由到 job 所在 tube；若 global 中无 job 或 tube_rx 中无该 tube 则用 use_tube
                let tube_name = command.params.get("id")
                    .and_then(|id| id.parse::<u64>().ok())
                    .and_then(|id| {
                        crate::backend::job_store::global_find_job(&id).map(|j| j.tube().to_string())
                    })
                    .filter(|n| self.tube_rx.contains_key(n))
                    .unwrap_or_else(|| self.use_tube.clone());
                let tube_tx = self.tube_rx.get_mut(&tube_name)
                    .ok_or_else(|| anyhow!("tube not found: {}", tube_name))?;
                tube_tx
                    .send((self.client_id, command.clone()))
                    .await
                    .map_err(|e| anyhow!("send failed: {}", e))?;
                let rx = self.rx.as_mut()
                    .ok_or_else(|| anyhow!("receiver not initialized"))?;
                let command = rx.next().await
                    .ok_or_else(|| anyhow!("channel closed"))?;
                Ok(command)
            }
            _ => {
                // 标记 put 命令的生产者状态
                if cmd == CommandKind::Put && !self.has_put {
                    self.has_put = true;
                    GLOBAL_STATS.inc_producer();
                }
                
                // 标记 reserve-job 命令的工作者状态
                if cmd == CommandKind::ReserveJob && !self.has_reserved {
                    self.has_reserved = true;
                    GLOBAL_STATS.inc_worker();
                }
                
                let tube_tx = self.tube_rx.get_mut(&self.use_tube)
                    .ok_or_else(|| anyhow!("tube not found: {}", self.use_tube))?;
                tube_tx
                    .send((self.client_id, command.clone()))
                    .await
                    .map_err(|e| anyhow!("send failed: {}", e))?;
                let rx = self.rx.as_mut()
                    .ok_or_else(|| anyhow!("receiver not initialized"))?;
                let command = rx.next().await
                    .ok_or_else(|| anyhow!("channel closed"))?;
                Ok(command)
            }
        }
    }
}

#[cfg(all(test, feature = "client"))]
#[allow(unused)]
mod test {
    use super::*;
    use crate::client::{BeanstalkError, ClientError, Conn, Tube, TubeSet};
    use chrono::Local;
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::process::{Child, Command};
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    /// Find an available port by binding to 127.0.0.1:0
    fn find_available_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind");
        let port = listener.local_addr().expect("failed to get addr").port();
        drop(listener);
        // Brief delay for OS to release the port before server binds
        thread::sleep(Duration::from_millis(10));
        port
    }

    struct TestServer {
        #[allow(dead_code)]
        process: Child,
        port: u16,
    }

    impl TestServer {
        fn start() -> Self {
            let port = find_available_port();
            let port_str = port.to_string();

            // 若设置了 CARGO_BIN_EXE_beanstalkr（部分测试场景由 Cargo 注入），优先使用以保证与当前构建一致。
            // 单元测试里通常只有 target/debug/beanstalkr；`cargo test` 不保证会重建该默认二进制，请先 `cargo build --bin beanstalkr`。
            let bin_path = std::env::var_os("CARGO_BIN_EXE_beanstalkr")
                .map(PathBuf::from)
                .unwrap_or_else(|| {
                    PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
                        .join("target/debug/beanstalkr")
                });
            assert!(
                bin_path.exists(),
                "server binary missing at {}. Run `cargo build --bin beanstalkr` before this test.",
                bin_path.display()
            );

            let process = Command::new(&bin_path)
                .args(["-l", "127.0.0.1", "-p", &port_str])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::inherit())
                .spawn()
                .expect("failed to spawn server process");

            // 等待服务器启动
            for _ in 0..50 {
                thread::sleep(Duration::from_millis(100));
                if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                    thread::sleep(Duration::from_millis(200)); // 额外等待确保准备好
                    return TestServer { process, port };
                }
            }

            panic!("Server failed to start within 5 seconds");
        }

        async fn connect(&self) -> Conn {
            Conn::connect_timeout(("127.0.0.1", self.port), Duration::from_secs(5))
                .await
                .expect("connect failed")
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            // 先尝试优雅终止
            let _ = self.process.kill();
            // 等待进程退出，避免僵尸进程
            let _ = self.process.wait();
            // 等待端口释放
            thread::sleep(Duration::from_millis(100));
        }
    }

    #[tokio::test]
    async fn it_async() {
        task::spawn(async move {});
        println!("Hello");
    }

    #[tokio::test]
    async fn it_double_tube() {
        // 单 tube 双 job：put 2 个 job，用两个连接分别 reserve 以验证服务器状态
        let _server = TestServer::start();
        let mut producer = _server.connect().await;
        let _id1 = producer
            .put(b"hello word1", 1, Duration::ZERO, Duration::from_secs(5))
            .await
            .unwrap();
        let _id2 = producer
            .put(b"hello word2", 1, Duration::ZERO, Duration::from_secs(5))
            .await
            .unwrap();
        let mut consumer1 = _server.connect().await;
        let mut consumer2 = _server.connect().await;
        let (id1, _) = consumer1
            .reserve(Duration::from_secs(5))
            .await
            .unwrap();
        let (id2, _) = consumer2
            .reserve(Duration::from_secs(5))
            .await
            .unwrap();
        assert_ne!(id1, id2);
    }

    #[tokio::test]
    async fn it_reserve() {
        let _server = TestServer::start();
        let mut conn = _server.connect().await;
        let _id = conn
            .put(
                b"hello word1",
                1,
                Duration::ZERO, // job 立即进入 ready，否则 reserve 会先超时
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        let (id, _) = conn.reserve(Duration::from_secs(2)).await.unwrap();
        println!("{}", id);
    }

    #[tokio::test]
    async fn it_reserve_with_timeout() {
        let _server = TestServer::start();
        let mut conn = _server.connect().await;
        let _id = conn
            .put(
                b"hello word1",
                1,
                Duration::ZERO,
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        let (id, _) = conn.reserve(Duration::from_secs(2)).await.unwrap();
        println!("{}", id);
        // 必须先 delete：同一连接上已 reserved 的任务未释放时，再次 reserve 的行为与「空队列超时」无关
        conn.delete(id).await.unwrap();
        // 队列已空，reserve 应在超时后返回 TIMED_OUT
        let timeout_result = conn.reserve(Duration::from_secs(1)).await;
        assert!(
            matches!(
                timeout_result,
                Err(ClientError::Protocol(BeanstalkError::Timeout))
            ),
            "expected TIMED_OUT, got {:?}",
            timeout_result
        );
    }

    #[tokio::test]
    async fn it_watch() {
        let _server = TestServer::start();
        let mut conn = _server.connect().await;
        let tube_set = conn.tube_set(&["default", "ok"]);
        // watch 了 default+ok，两 tube 均无任务时应 TIMED_OUT
        let r = tube_set.reserve(&mut conn, Duration::from_secs(1)).await;
        assert!(matches!(
            r,
            Err(ClientError::Protocol(BeanstalkError::Timeout))
        ));
    }

    /// 用原始 TCP 验证 put→reserve→delete 流程
    #[test]
    fn it_delete_raw_tcp() {
        let _server = TestServer::start();
        use std::io::{BufRead, BufReader, Read, Write};
        let stream = std::net::TcpStream::connect(("127.0.0.1", _server.port))
            .expect("connect");
        stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
        stream.set_write_timeout(Some(Duration::from_secs(5))).ok();
        let (mut reader, mut writer) = (stream.try_clone().unwrap(), stream.try_clone().unwrap());
        let mut r = BufReader::new(&mut reader);

        fn req(stream: &mut std::net::TcpStream, r: &mut BufReader<&mut std::net::TcpStream>, req: &[u8]) -> String {
            stream.write_all(req).unwrap();
            stream.flush().unwrap();
            let mut line = String::new();
            r.read_line(&mut line).unwrap();
            line
        }

        let use_rep = req(&mut writer, &mut r, b"use default\r\n");
        assert!(use_rep.trim().starts_with("USING"), "use reply: {}", use_rep);

        let watch_rep = req(&mut writer, &mut r, b"watch default\r\n");
        assert!(watch_rep.trim().starts_with("WATCHING"), "watch reply: {}", watch_rep);

        let put_rep = req(&mut writer, &mut r, b"put 1 0 0 5\r\nhello\r\n");
        assert!(put_rep.trim().starts_with("INSERTED"), "put reply: {}", put_rep);
        let job_id: u64 = put_rep.trim().split_whitespace().nth(1).unwrap().parse().unwrap();

        let res_rep = req(&mut writer, &mut r, b"reserve-with-timeout 2\r\n");
        assert!(res_rep.trim().starts_with("RESERVED"), "reserve reply: {}", res_rep);
        let body_len: usize = res_rep.trim().split_whitespace().nth(2).unwrap().parse().unwrap();
        let mut body = vec![0u8; body_len];  // 协议：精确读取 <bytes> 字节，无额外 \r\n
        r.read_exact(&mut body).unwrap();

        let del_rep = req(&mut writer, &mut r, format!("delete {}\r\n", job_id).as_bytes());
        assert!(!del_rep.is_empty(), "delete: empty response");
        assert!(del_rep.trim().starts_with("DELETED"), "delete reply: {:?}", del_rep);
    }

    #[tokio::test]
    async fn it_delete() {
        let _server = TestServer::start();
        let mut conn = _server.connect().await;
        let _id = conn
            .put(
                b"hello word1",
                1,
                Duration::ZERO,
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        let (id, _) = conn.reserve(Duration::from_secs(2)).await.unwrap();
        let b = conn.delete(id).await;
        if let Err(ref e) = b {
            eprintln!("it_delete: delete failed: {:?}", e);
        }
        assert!(b.is_ok());
    }

    #[tokio::test]
    async fn it_delete2() {
        let _server = TestServer::start();
        let mut conn = _server.connect().await;
        for _ in 0..20 {
            let _ = conn
                .put(b"hello word1", 1, Duration::ZERO, Duration::from_secs(5))
                .await
                .unwrap();
        }
        for _ in 0..20 {
            let mut worker = _server.connect().await;
            let (id, _) = worker.reserve(Duration::from_secs(10)).await.unwrap();
            let delete_result = worker.delete(id).await;
            if let Err(ref e) = delete_result {
                eprintln!("it_delete2: delete failed for job {}: {:?}", id, e);
            }
            assert!(delete_result.is_ok());
        }
    }
    #[tokio::test]
    async fn it_kick() {
        let _server = TestServer::start();
        let mut conn = _server.connect().await;
        let tube = format!("tube_{}", Local::now().timestamp_nanos_opt().unwrap_or_default());
        let tube_handle = Tube::named(tube.as_str());
        let _id = tube_handle
            .put(&mut conn, b"hello", 1, Duration::from_secs(30), Duration::from_secs(5))
            .await
            .unwrap();
        let count = tube_handle.kick(&mut conn, 1).await.unwrap();
        assert_eq!(1, count);
    }

    #[tokio::test]
    async fn it_pause_job() {
        let _server = TestServer::start();
        let mut conn = _server.connect().await;
        let tube = format!("tube_{}", Local::now().timestamp_nanos_opt().unwrap_or_default());
        let tube_handle = Tube::named(tube.as_str());

        let _id = tube_handle
            .put(&mut conn, b"hello", 1, Duration::ZERO, Duration::from_secs(5))
            .await
            .unwrap();
        tube_handle
            .pause(&mut conn, Duration::from_secs(100))
            .await
            .unwrap();
        let tube_set = conn.tube_set(&[tube.as_str()]);
        let result = tube_set.reserve(&mut conn, Duration::from_secs(2)).await;
        assert!(result.is_err(), "reserve 在 pause 时应收 TIMED_OUT");
    }

    #[tokio::test]
    async fn it_list_tube_used() {
        let _server = TestServer::start();
        let mut conn = _server.connect().await;
        let tube = Tube::named("hello");
        let _ = tube
            .put(&mut conn, b"x", 1, Duration::ZERO, Duration::from_secs(5))
            .await;
        assert_eq!(conn.using(), "hello");
    }

    #[tokio::test]
    async fn it_batch_put() {
        let _server = TestServer::start();
        for _i in 0..100 {
            let mut conn = _server.connect().await;
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
                    .await
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
    }
}

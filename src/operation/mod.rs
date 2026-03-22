use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, Error};
use futures::{
    channel::mpsc::{self as futures_mpsc, UnboundedReceiver, UnboundedSender},
    SinkExt, StreamExt,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, MutexGuard};
use tokio::sync::mpsc::Receiver;
use tokio::task;
use tracing::{debug, info, instrument};

use crate::architecture::cmd::{Command, CommandKind};
use crate::architecture::error::ProtocolError;
use crate::architecture::job::next_client_id;
use crate::architecture::stats::GLOBAL_STATS;
use crate::architecture::tube::ClientId;

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
    rx: Option<UnboundedReceiver<Command>>,
    tube_rx: HashMap<String, TubeSender>,
    reserve_tx: OnceChannel<Command>,
    reserve_rx: Receiver<Command>,
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
        let (tx, rx) = futures_mpsc::unbounded();
        let (reserve_tx, reserve_rx) = tokio::sync::mpsc::channel(1);
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
            writer.write_all((reply + "\r\n").as_bytes()).await?;
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
        let tube_name = command.params.get("tube")
            .ok_or_else(|| anyhow!("missing tube parameter"))?;
        match cmd {
            CommandKind::Use => {
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
                for (tube_name, _) in self.watch_tubes.iter_mut() {
                    debug!("watch {}", tube_name);
                    let tube_ch = self.tube_rx.get_mut(tube_name)
                    .ok_or_else(|| anyhow!("tube not found: {}", tube_name))?;
                    let mut tube_ch = tube_ch.clone();
                    let command = command.clone();
                    task::spawn(async move {
                        let _ = tube_ch.send((client_id, command)).await;
                    });
                    debug!("send a reserve inner command to {}", tube_name);
                }
                Ok(self.reserve_rx.recv().await
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

#[cfg(test)]
#[allow(unused)]
mod test {
    use super::*;
    use beanstalkc::Beanstalkc;
    use chrono::Local;
    use std::process::{Child, Command};
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    struct TestServer {
        #[allow(dead_code)]
        process: Child,
    }

    impl TestServer {
        fn start() -> Self {
            // 使用 debug 模式编译，更快
            let status = Command::new("cargo")
                .args(["build"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .expect("failed to build");
            
            if !status.success() {
                panic!("cargo build failed");
            }
            
            let mut process = Command::new("cargo")
                .args(["run", "--", "-a", "127.0.0.1:11301"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("failed to start server");
            
            // 等待服务器启动
            for i in 0..50 {
                thread::sleep(Duration::from_millis(100));
                if std::net::TcpStream::connect("127.0.0.1:11301").is_ok() {
                    thread::sleep(Duration::from_millis(200)); // 额外等待确保准备好
                    return TestServer { process };
                }
            }
            
            panic!("Server failed to start within 5 seconds");
        }
        
        fn connect(&self) -> Beanstalkc {
            Beanstalkc::new()
                .host("127.0.0.1")
                .port(11301)
                .connection_timeout(Some(Duration::from_secs(5)))
                .connect()
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

    #[test]
    fn it_double_tube() {
        let _server = TestServer::start();
        let mut conn = _server.connect();
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
        let _server = TestServer::start();
        let mut conn = _server.connect();
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
        let _server = TestServer::start();
        let mut conn = _server.connect();
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
        let _server = TestServer::start();
        let mut conn = _server.connect();
        let id = conn.watch("ok").unwrap();
    }

    #[test]
    fn it_delete() {
        let _server = TestServer::start();
        let mut conn = _server.connect();
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
        let _server = TestServer::start();
        let mut conn = _server.connect();
        let _ = conn.use_tube("a");
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
        let _server = TestServer::start();
        let mut conn = _server.connect();
        let tube = format!("tube_{}", Local::now().timestamp_nanos_opt().unwrap_or_default());
        conn.use_tube(tube.as_str()).unwrap();
        let id = conn
            .put(b"hello", 1, Duration::from_secs(30), Duration::from_secs(5))
            .unwrap();
        let count = conn.kick(1).unwrap();
        assert_eq!(1, count);
    }

    #[test]
    fn it_pause_job() {
        let _server = TestServer::start();
        let mut conn = _server.connect();
        let tube = format!("tube_{}", Local::now().timestamp_nanos_opt().unwrap_or_default());
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
        let _server = TestServer::start();
        let mut conn = _server.connect();
        conn.use_tube("hello".as_ref()).unwrap();
        let tube_name = conn.using().unwrap();
        assert_eq!(tube_name, "hello".to_string());
    }

    #[test]
    fn it_batch_put() {
        let _server = TestServer::start();
        for _i in 0..100 {
            let mut conn = _server.connect();
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
}

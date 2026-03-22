#![recursion_limit = "512"]

#[macro_use]
extern crate lazy_static;
extern crate strum;
#[macro_use]
extern crate strum_macros;

use std::process;
use std::sync::Arc;

use clap::Parser;
use tokio::io;
use tokio::net::TcpListener;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::Mutex;
use tokio::task;
use tracing::{debug, error, info, Instrument};

mod architecture;
mod backend;
mod backup;
mod operation;

use crate::architecture::stats::{set_draining, GLOBAL_STATS};
use crate::backup::binlog::{init_binlog, get_binlog};
use crate::operation::dispatch::Dispatch;
use crate::operation::ClientHandler;
use std::sync::atomic::Ordering;

/// 切换到指定用户（需要 root 权限）
#[cfg(unix)]
fn switch_user(user: &str) -> Result<(), String> {
    use std::ffi::CString;
    
    unsafe {
        // 获取用户信息
        let c_user = CString::new(user).map_err(|e| format!("Invalid username: {}", e))?;
        let pwent = libc::getpwnam(c_user.as_ptr());
        if pwent.is_null() {
            return Err(format!("User '{}' not found", user));
        }
        
        let gid = (*pwent).pw_gid;
        let uid = (*pwent).pw_uid;
        
        // 先设置组 ID
        if libc::setgid(gid) != 0 {
            return Err(format!("Failed to set gid {}: {}", gid, std::io::Error::last_os_error()));
        }
        
        // 再设置用户 ID
        if libc::setuid(uid) != 0 {
            return Err(format!("Failed to set uid {}: {}", uid, std::io::Error::last_os_error()));
        }
        
        info!("Switched to user {} (uid={}, gid={})", user, uid, gid);
        Ok(())
    }
}

#[cfg(not(unix))]
fn switch_user(_user: &str) -> Result<(), String> {
    Err("User switching is only supported on Unix systems".to_string())
}

/// Beanstalkd 协议的 Rust 实现
#[derive(Parser, Debug)]
#[command(name = "beanstalkr", version = env!("CARGO_PKG_VERSION"))]
struct Opt {
    /// 日志详细程度（可多次指定，-v 表示警告，-vv 表示信息，-vvv 表示调试）
    #[arg(short, long, action = clap::ArgAction::Count)]
    #[allow(dead_code)]
    verbose: u8,

    /// 监听地址
    #[arg(short, long, default_value = "0.0.0.0:11300")]
    addr: String,
    
    /// 最大任务大小（字节）
    #[arg(short = 'z', long, default_value = "65535")]
    max_job_size: usize,
    
    /// 启用 binlog 持久化
    #[arg(short = 'b', long, default_value = "false")]
    enable_binlog: bool,
    
    /// Binlog 目录
    #[arg(long, default_value = "./binlog")]
    binlog_dir: String,
    
    /// Binlog 文件最大大小（字节）
    #[arg(long, default_value = "10485760")]  // 10MB
    binlog_size: u64,
    
    /// fsync 间隔（毫秒），0 表示每次写入都 fsync，默认 50ms
    #[arg(short = 'f', long, default_value = "50")]
    fsync_interval: u64,
    
    /// 从不 fsync（覆盖 -f 选项）
    #[arg(short = 'F', long, default_value = "false")]
    no_fsync: bool,
    
    /// 切换到的用户和组（需要 root 权限）
    #[arg(short = 'u', long)]
    user: Option<String>,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt::init();
    
    _ = ctrlc::set_handler(move || {
        info!("beanstalkr exit");
        process::exit(0);
    });

    // 设置 SIGPIPE 信号处理器（忽略 SIGPIPE，与 C 版本一致）
    match signal(SignalKind::pipe()) {
        Ok(mut sigpipe) => {
            task::spawn(async move {
                loop {
                    sigpipe.recv().await;
                    debug!("Received SIGPIPE, ignoring");
                }
            });
        }
        Err(e) => {
            error!("Failed to setup SIGPIPE handler: {}", e);
        }
    }

    // 设置 SIGUSR1 信号处理器（Drain 模式）
    match signal(SignalKind::user_defined1()) {
        Ok(mut sigusr1) => {
            task::spawn(async move {
                loop {
                    sigusr1.recv().await;
                    info!("Received SIGUSR1, entering drain mode");
                    set_draining(true);
                }
            });
        }
        Err(e) => {
            error!("Failed to setup SIGUSR1 handler: {}", e);
        }
    }

    let opt: Opt = Opt::parse();
    
    // 初始化最大任务大小
    GLOBAL_STATS.max_job_size.store(opt.max_job_size as u64, Ordering::SeqCst);
    
    // 初始化 binlog
    let mut recovered_jobs = Vec::new();
    if opt.enable_binlog {
        let fsync_interval = if opt.no_fsync { 0 } else { opt.fsync_interval };
        match init_binlog(&opt.binlog_dir, opt.binlog_size, fsync_interval, opt.no_fsync).await {
            Ok(_) => {
                info!("Binlog enabled: dir={}, max_size={}, fsync_interval={}ms, no_fsync={}", 
                    opt.binlog_dir, opt.binlog_size, fsync_interval, opt.no_fsync);
                
                // 从 binlog 恢复作业
                if let Some(manager) = get_binlog() {
                    let manager = manager.lock().await;
                    match manager.recover().await {
                        Ok(jobs) => {
                            recovered_jobs = jobs;
                            info!("Recovered {} jobs from binlog", recovered_jobs.len());
                        }
                        Err(e) => {
                            error!("Failed to recover from binlog: {}", e);
                        }
                    }
                }
                
                // 更新 stats 中的 binlog 信息
                if let Some(manager) = get_binlog() {
                    let manager = manager.lock().await;
                    GLOBAL_STATS.binlog_current_index.store(manager.current_index(), Ordering::SeqCst);
                    GLOBAL_STATS.binlog_max_size.store(opt.binlog_size, Ordering::SeqCst);
                }
            }
            Err(e) => {
                error!("Failed to initialize binlog: {}", e);
            }
        }
    }
    
    // 切换用户（如果指定了 -u 参数）
    if let Some(ref user) = opt.user {
        if let Err(e) = switch_user(user) {
            error!("Failed to switch user: {}", e);
            std::process::exit(1);
        }
    }
    
    let listener = TcpListener::bind(&opt.addr).await?;
    info!(addr = %opt.addr, max_job_size = opt.max_job_size, "Listening on {}", listener.local_addr()?);
    
    let dispatch: Arc<Mutex<Dispatch>> = Arc::new(Mutex::new(Dispatch::new()));
    
    // 恢复 binlog 中的 jobs
    if !recovered_jobs.is_empty() {
        dispatch.lock().await.recover_jobs(recovered_jobs).await;
    }
    loop {
        let (stream, addr) = listener.accept().await?;
        let dispatch = dispatch.clone();
        let span = tracing::info_span!("client", addr = %addr);
        task::spawn(async move {
            let mut client = ClientHandler::new(stream, dispatch);
            if let Err(err) = client.spawn_start().await {
                let err_str = err.to_string();
                if err_str.contains("Client quit") {
                    debug!("Client disconnected");
                } else {
                    error!(error = %err, "spawn start failed");
                }
            }
        }.instrument(span));
    }
}

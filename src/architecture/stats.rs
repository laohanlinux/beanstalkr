//! 统计信息模块
//!
//! 提供全局统计、tube 统计和 job 统计功能

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::SystemTime;

/// 全局统计信息
#[derive(Debug, Default)]
pub struct GlobalStats {
    // 作业统计
    pub current_jobs_urgent: AtomicU64,
    pub current_jobs_ready: AtomicU64,
    pub current_jobs_reserved: AtomicU64,
    pub current_jobs_delayed: AtomicU64,
    pub current_jobs_buried: AtomicU64,
    pub total_jobs: AtomicU64,
    
    // 连接统计
    pub current_connections: AtomicU64,
    pub current_producers: AtomicU64,
    pub current_workers: AtomicU64,
    pub current_waiting: AtomicU64,
    pub total_connections: AtomicU64,
    
    // Tube 统计
    pub current_tubes: AtomicU64,
    
    // 命令统计
    pub cmd_put: AtomicU64,
    pub cmd_use: AtomicU64,
    pub cmd_reserve: AtomicU64,
    pub cmd_reserve_with_timeout: AtomicU64,
    pub cmd_reserve_job: AtomicU64,
    pub cmd_delete: AtomicU64,
    pub cmd_release: AtomicU64,
    pub cmd_bury: AtomicU64,
    pub cmd_touch: AtomicU64,
    pub cmd_watch: AtomicU64,
    pub cmd_ignore: AtomicU64,
    pub cmd_peek: AtomicU64,
    pub cmd_peek_ready: AtomicU64,
    pub cmd_peek_delayed: AtomicU64,
    pub cmd_peek_buried: AtomicU64,
    pub cmd_kick: AtomicU64,
    pub cmd_kick_job: AtomicU64,
    pub cmd_stats_job: AtomicU64,
    pub cmd_stats_tube: AtomicU64,
    pub cmd_stats: AtomicU64,
    pub cmd_list_tubes: AtomicU64,
    pub cmd_list_tube_used: AtomicU64,
    pub cmd_list_tubes_watched: AtomicU64,
    pub cmd_pause_tube: AtomicU64,
    
    // 其他统计
    pub job_timeouts: AtomicU64,
    
    // 服务器状态
    pub draining: AtomicBool,
    
    // 配置
    pub max_job_size: AtomicU64,
    
    // Binlog 统计
    pub binlog_oldest_index: AtomicU64,
    pub binlog_current_index: AtomicU64,
    pub binlog_max_size: AtomicU64,
    pub binlog_records_written: AtomicU64,
    pub binlog_records_migrated: AtomicU64,
}

impl GlobalStats {
    pub fn new() -> Self {
        Self::default()
    }
    
    /// 增加当前连接数
    pub fn inc_connection(&self) {
        self.current_connections.fetch_add(1, Ordering::SeqCst);
        self.total_connections.fetch_add(1, Ordering::SeqCst);
    }
    
    /// 减少当前连接数
    pub fn dec_connection(&self) {
        self.current_connections.fetch_sub(1, Ordering::SeqCst);
    }
    
    /// 增加生产者数
    pub fn inc_producer(&self) {
        self.current_producers.fetch_add(1, Ordering::SeqCst);
    }
    
    /// 减少生产者数
    pub fn dec_producer(&self) {
        self.current_producers.fetch_sub(1, Ordering::SeqCst);
    }
    
    /// 增加工作者数
    pub fn inc_worker(&self) {
        self.current_workers.fetch_add(1, Ordering::SeqCst);
    }
    
    /// 减少工作者数
    pub fn dec_worker(&self) {
        self.current_workers.fetch_sub(1, Ordering::SeqCst);
    }
    
    /// 增加等待数
    pub fn inc_waiting(&self) {
        self.current_waiting.fetch_add(1, Ordering::SeqCst);
    }
    
    /// 减少等待数
    pub fn dec_waiting(&self) {
        self.current_waiting.fetch_sub(1, Ordering::SeqCst);
    }
    
    /// 增加 total_jobs
    pub fn inc_total_jobs(&self) {
        self.total_jobs.fetch_add(1, Ordering::SeqCst);
    }
    
    /// 更新作业计数
    pub fn update_job_counts(&self, ready: u64, reserved: u64, delayed: u64, buried: u64) {
        self.current_jobs_ready.store(ready, Ordering::SeqCst);
        self.current_jobs_reserved.store(reserved, Ordering::SeqCst);
        self.current_jobs_delayed.store(delayed, Ordering::SeqCst);
        self.current_jobs_buried.store(buried, Ordering::SeqCst);
    }
    
    /// 设置 urgent 作业计数
    pub fn set_urgent_count(&self, count: u64) {
        self.current_jobs_urgent.store(count, Ordering::SeqCst);
    }
    
    /// 增加命令计数
    pub fn inc_cmd(&self, cmd_name: &str) {
        match cmd_name {
            "put" => self.cmd_put.fetch_add(1, Ordering::SeqCst),
            "use" => self.cmd_use.fetch_add(1, Ordering::SeqCst),
            "reserve" => self.cmd_reserve.fetch_add(1, Ordering::SeqCst),
            "reserve-with-timeout" => self.cmd_reserve_with_timeout.fetch_add(1, Ordering::SeqCst),
            "reserve-job" => self.cmd_reserve_job.fetch_add(1, Ordering::SeqCst),
            "delete" => self.cmd_delete.fetch_add(1, Ordering::SeqCst),
            "release" => self.cmd_release.fetch_add(1, Ordering::SeqCst),
            "bury" => self.cmd_bury.fetch_add(1, Ordering::SeqCst),
            "touch" => self.cmd_touch.fetch_add(1, Ordering::SeqCst),
            "watch" => self.cmd_watch.fetch_add(1, Ordering::SeqCst),
            "ignore" => self.cmd_ignore.fetch_add(1, Ordering::SeqCst),
            "peek" => self.cmd_peek.fetch_add(1, Ordering::SeqCst),
            "peek-ready" => self.cmd_peek_ready.fetch_add(1, Ordering::SeqCst),
            "peek-delayed" => self.cmd_peek_delayed.fetch_add(1, Ordering::SeqCst),
            "peek-buried" => self.cmd_peek_buried.fetch_add(1, Ordering::SeqCst),
            "kick" => self.cmd_kick.fetch_add(1, Ordering::SeqCst),
            "kick-job" => self.cmd_kick_job.fetch_add(1, Ordering::SeqCst),
            "stats-job" => self.cmd_stats_job.fetch_add(1, Ordering::SeqCst),
            "stats-tube" => self.cmd_stats_tube.fetch_add(1, Ordering::SeqCst),
            "stats" => self.cmd_stats.fetch_add(1, Ordering::SeqCst),
            "list-tubes" => self.cmd_list_tubes.fetch_add(1, Ordering::SeqCst),
            "list-tube-used" => self.cmd_list_tube_used.fetch_add(1, Ordering::SeqCst),
            "list-tubes-watched" => self.cmd_list_tubes_watched.fetch_add(1, Ordering::SeqCst),
            "pause-tube" => self.cmd_pause_tube.fetch_add(1, Ordering::SeqCst),
            _ => 0,
        };
    }
    
    /// 增加 job timeout 计数
    pub fn inc_job_timeout(&self) {
        self.job_timeouts.fetch_add(1, Ordering::SeqCst);
    }
    
    /// 转换为 YAML 格式
    pub fn to_yaml(&self, hostname: &str, version: &str) -> String {
        let mut map: HashMap<String, serde_yaml::Value> = HashMap::new();
        
        // 作业统计
        map.insert("current-jobs-urgent".to_string(), serde_yaml::to_value(self.current_jobs_urgent.load(Ordering::SeqCst)).unwrap());
        map.insert("current-jobs-ready".to_string(), serde_yaml::to_value(self.current_jobs_ready.load(Ordering::SeqCst)).unwrap());
        map.insert("current-jobs-reserved".to_string(), serde_yaml::to_value(self.current_jobs_reserved.load(Ordering::SeqCst)).unwrap());
        map.insert("current-jobs-delayed".to_string(), serde_yaml::to_value(self.current_jobs_delayed.load(Ordering::SeqCst)).unwrap());
        map.insert("current-jobs-buried".to_string(), serde_yaml::to_value(self.current_jobs_buried.load(Ordering::SeqCst)).unwrap());
        
        // 命令统计
        map.insert("cmd-put".to_string(), serde_yaml::to_value(self.cmd_put.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-use".to_string(), serde_yaml::to_value(self.cmd_use.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-reserve".to_string(), serde_yaml::to_value(self.cmd_reserve.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-reserve-with-timeout".to_string(), serde_yaml::to_value(self.cmd_reserve_with_timeout.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-reserve-job".to_string(), serde_yaml::to_value(self.cmd_reserve_job.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-delete".to_string(), serde_yaml::to_value(self.cmd_delete.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-release".to_string(), serde_yaml::to_value(self.cmd_release.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-bury".to_string(), serde_yaml::to_value(self.cmd_bury.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-touch".to_string(), serde_yaml::to_value(self.cmd_touch.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-watch".to_string(), serde_yaml::to_value(self.cmd_watch.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-ignore".to_string(), serde_yaml::to_value(self.cmd_ignore.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-peek".to_string(), serde_yaml::to_value(self.cmd_peek.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-peek-ready".to_string(), serde_yaml::to_value(self.cmd_peek_ready.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-peek-delayed".to_string(), serde_yaml::to_value(self.cmd_peek_delayed.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-peek-buried".to_string(), serde_yaml::to_value(self.cmd_peek_buried.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-kick".to_string(), serde_yaml::to_value(self.cmd_kick.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-kick-job".to_string(), serde_yaml::to_value(self.cmd_kick_job.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-stats-job".to_string(), serde_yaml::to_value(self.cmd_stats_job.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-stats-tube".to_string(), serde_yaml::to_value(self.cmd_stats_tube.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-stats".to_string(), serde_yaml::to_value(self.cmd_stats.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-list-tubes".to_string(), serde_yaml::to_value(self.cmd_list_tubes.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-list-tube-used".to_string(), serde_yaml::to_value(self.cmd_list_tube_used.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-list-tubes-watched".to_string(), serde_yaml::to_value(self.cmd_list_tubes_watched.load(Ordering::SeqCst)).unwrap());
        map.insert("cmd-pause-tube".to_string(), serde_yaml::to_value(self.cmd_pause_tube.load(Ordering::SeqCst)).unwrap());
        
        // 其他统计
        map.insert("job-timeouts".to_string(), serde_yaml::to_value(self.job_timeouts.load(Ordering::SeqCst)).unwrap());
        map.insert("total-jobs".to_string(), serde_yaml::to_value(self.total_jobs.load(Ordering::SeqCst)).unwrap());
        map.insert("max-job-size".to_string(), serde_yaml::to_value(self.max_job_size.load(Ordering::SeqCst)).unwrap());
        
        // 连接统计
        map.insert("current-tubes".to_string(), serde_yaml::to_value(self.current_tubes.load(Ordering::SeqCst)).unwrap());
        map.insert("current-connections".to_string(), serde_yaml::to_value(self.current_connections.load(Ordering::SeqCst)).unwrap());
        map.insert("current-producers".to_string(), serde_yaml::to_value(self.current_producers.load(Ordering::SeqCst)).unwrap());
        map.insert("current-workers".to_string(), serde_yaml::to_value(self.current_workers.load(Ordering::SeqCst)).unwrap());
        map.insert("current-waiting".to_string(), serde_yaml::to_value(self.current_waiting.load(Ordering::SeqCst)).unwrap());
        map.insert("total-connections".to_string(), serde_yaml::to_value(self.total_connections.load(Ordering::SeqCst)).unwrap());
        
        // 进程信息
        map.insert("pid".to_string(), serde_yaml::to_value(std::process::id()).unwrap());
        map.insert("version".to_string(), serde_yaml::to_value(version.to_string()).unwrap());
        map.insert("hostname".to_string(), serde_yaml::to_value(hostname.to_string()).unwrap());
        map.insert("draining".to_string(), serde_yaml::to_value(self.draining.load(Ordering::SeqCst) as u64).unwrap());
        map.insert("id".to_string(), serde_yaml::to_value(SERVER_ID.clone()).unwrap());
        map.insert("uptime".to_string(), serde_yaml::to_value(get_uptime()).unwrap());
        
        // CPU 时间
        let (utime, stime) = get_cpu_time();
        map.insert("rusage-utime".to_string(), serde_yaml::to_value(utime).unwrap());
        map.insert("rusage-stime".to_string(), serde_yaml::to_value(stime).unwrap());
        
        // 系统信息
        let os_info = get_os_info();
        map.insert("os".to_string(), serde_yaml::to_value(os_info.0).unwrap());
        map.insert("platform".to_string(), serde_yaml::to_value(os_info.1).unwrap());
        
        // Binlog 信息
        map.insert("binlog-oldest-index".to_string(), serde_yaml::to_value(self.binlog_oldest_index.load(Ordering::SeqCst)).unwrap());
        map.insert("binlog-current-index".to_string(), serde_yaml::to_value(self.binlog_current_index.load(Ordering::SeqCst)).unwrap());
        map.insert("binlog-max-size".to_string(), serde_yaml::to_value(self.binlog_max_size.load(Ordering::SeqCst)).unwrap());
        map.insert("binlog-records-written".to_string(), serde_yaml::to_value(self.binlog_records_written.load(Ordering::SeqCst)).unwrap());
        map.insert("binlog-records-migrated".to_string(), serde_yaml::to_value(self.binlog_records_migrated.load(Ordering::SeqCst)).unwrap());
        
        serde_yaml::to_string(&map).unwrap_or_default()
    }
}

lazy_static::lazy_static! {
    /// 全局统计实例
    pub static ref GLOBAL_STATS: GlobalStats = GlobalStats::new();
    
    /// 服务器启动时间
    pub static ref START_TIME: SystemTime = SystemTime::now();
    
    /// 服务器随机ID
    pub static ref SERVER_ID: String = generate_server_id();
}

/// 生成服务器随机ID
/// 优先从 /dev/urandom 读取，失败则回退到时间戳
fn generate_server_id() -> String {
    use std::fs::File;
    use std::io::Read;
    
    // 尝试从 /dev/urandom 读取 16 字节
    let mut buf = [0u8; 16];
    if let Ok(mut file) = File::open("/dev/urandom") {
        if file.read_exact(&mut buf).is_ok() {
            return buf.iter().map(|b| format!("{:02x}", b)).collect();
        }
    }
    
    // 回退到时间戳
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:x}", nanos)
}

/// 获取运行时间（秒）
pub fn get_uptime() -> u64 {
    START_TIME
        .elapsed()
        .unwrap_or_default()
        .as_secs()
}

/// 获取CPU时间（秒）
#[cfg(unix)]
pub fn get_cpu_time() -> (f64, f64) {
    unsafe {
        let mut usage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut usage) == 0 {
            let utime = usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 / 1_000_000.0;
            let stime = usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 / 1_000_000.0;
            (utime, stime)
        } else {
            (0.0, 0.0)
        }
    }
}

#[cfg(not(unix))]
pub fn get_cpu_time() -> (f64, f64) {
    (0.0, 0.0)
}

/// 获取操作系统信息 (os, platform)
pub fn get_os_info() -> (String, String) {
    #[cfg(unix)]
    {
        unsafe {
            let mut uts = std::mem::zeroed();
            if libc::uname(&mut uts) == 0 {
                let sysname = std::ffi::CStr::from_ptr(uts.sysname.as_ptr())
                    .to_string_lossy()
                    .to_string();
                let machine = std::ffi::CStr::from_ptr(uts.machine.as_ptr())
                    .to_string_lossy()
                    .to_string();
                (sysname, machine)
            } else {
                ("unknown".to_string(), "unknown".to_string())
            }
        }
    }
    #[cfg(not(unix))]
    {
        (std::env::consts::OS.to_string(), std::env::consts::ARCH.to_string())
    }
}

/// 设置 drain 模式
pub fn set_draining(value: bool) {
    GLOBAL_STATS.draining.store(value, Ordering::SeqCst);
}

/// 检查是否处于 drain 模式
pub fn is_draining() -> bool {
    GLOBAL_STATS.draining.load(Ordering::SeqCst)
}

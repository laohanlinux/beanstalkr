//! Binlog 模块 - Beanstalkd 的持久化实现
//!
//! Binlog 文件格式:
//! - 每个记录以 32位长度前缀开头（大端序）
//! - 长度包含：类型(1字节) + 数据 + CRC32(4字节)
//! - 类型: 0=put, 1=peek, 2=delete, 3=release, 4=bury, 5=kick
//! - 数据格式取决于类型

use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::fs::File as StdFile;

use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, BytesMut};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::architecture::job::{Job, State};
use crate::architecture::tube::Id;

/// Binlog 记录类型
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum RecordType {
    Put = 0,
    Peek = 1,
    Delete = 2,
    Release = 3,
    Bury = 4,
    Kick = 5,
    Reserve = 6,
    Touch = 7,
}

impl RecordType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(RecordType::Put),
            1 => Some(RecordType::Peek),
            2 => Some(RecordType::Delete),
            3 => Some(RecordType::Release),
            4 => Some(RecordType::Bury),
            5 => Some(RecordType::Kick),
            6 => Some(RecordType::Reserve),
            7 => Some(RecordType::Touch),
            _ => None,
        }
    }
}

/// Job 状态（用于 binlog）
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum JobState {
    Ready = 0,
    Reserved = 1,
    Buried = 2,
    Delayed = 3,
}

impl JobState {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(JobState::Ready),
            1 => Some(JobState::Reserved),
            2 => Some(JobState::Buried),
            3 => Some(JobState::Delayed),
            _ => None,
        }
    }
}

/// Binlog 记录
#[derive(Debug, Clone)]
pub struct BinlogRecord {
    pub record_type: RecordType,
    pub job_id: Id,
    pub tube_name: String,
    pub data: Vec<u8>,
    pub priority: i64,
    pub delay: i64,
    pub ttr: i64,
    pub timestamp: i64,
    pub state: Option<JobState>,  // 用于记录 job 的状态（Put 和 Bury 记录）
}

impl BinlogRecord {
    /// 序列化为字节
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();
        
        // 计算数据长度（不包含长度前缀本身）
        // 格式: 类型(1) + job_id(8) + tube_len(2) + tube + data_len(4) + data + 
        //       pri(8) + delay(8) + ttr(8) + ts(8) + has_state(1) + [state(1)] + crc(4)
        let tube_bytes = self.tube_name.as_bytes();
        let has_state_byte: u8 = if self.state.is_some() { 1 } else { 0 };
        let state_len = 1 + if self.state.is_some() { 1 } else { 0 };
        let data_len = 1 + 8 + 2 + tube_bytes.len() + 4 + self.data.len() + 8 + 8 + 8 + 8 + state_len + 4;
        
        buf.put_u32(data_len as u32);
        buf.put_u8(self.record_type as u8);
        buf.put_u64(self.job_id);
        buf.put_u16(tube_bytes.len() as u16);
        buf.put_slice(tube_bytes);
        buf.put_u32(self.data.len() as u32);
        buf.put_slice(&self.data);
        buf.put_i64(self.priority);
        buf.put_i64(self.delay);
        buf.put_i64(self.ttr);
        buf.put_i64(self.timestamp);
        buf.put_u8(has_state_byte);
        if let Some(state) = self.state {
            buf.put_u8(state as u8);
        }
        
        // 计算 CRC32
        let crc = crc32fast::hash(&buf[4..]); // 从类型开始计算
        buf.put_u32(crc);
        
        buf.to_vec()
    }
    
    /// 从字节解析
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 13 {
            return None;
        }
        
        let mut cursor = Cursor::new(data);
        let total_len = cursor.get_u32() as usize;
        
        if data.len() < total_len + 4 {
            return None;
        }
        
        let record_type = RecordType::from_u8(cursor.get_u8())?;
        let job_id = cursor.get_u64();
        let tube_len = cursor.get_u16() as usize;
        
        let mut tube_bytes = vec![0u8; tube_len];
        std::io::Read::read_exact(&mut cursor, &mut tube_bytes).ok()?;
        let tube_name = String::from_utf8(tube_bytes).ok()?;
        
        let data_len = cursor.get_u32() as usize;
        let mut job_data = vec![0u8; data_len];
        std::io::Read::read_exact(&mut cursor, &mut job_data).ok()?;
        
        let priority = cursor.get_i64();
        let delay = cursor.get_i64();
        let ttr = cursor.get_i64();
        let timestamp = cursor.get_i64();
        
        // 尝试读取状态（新版本格式）
        let state = if cursor.position() < (data.len() - 4) as u64 {
            let has_state = cursor.get_u8();
            if has_state != 0 {
                JobState::from_u8(cursor.get_u8())
            } else {
                None
            }
        } else {
            None
        };
        
        // 验证 CRC32
        let stored_crc = cursor.get_u32();
        let calc_crc = crc32fast::hash(&data[4..data.len()-4]);
        if stored_crc != calc_crc {
            warn!("CRC mismatch in binlog record");
            return None;
        }
        
        Some(BinlogRecord {
            record_type,
            job_id,
            tube_name,
            data: job_data,
            priority,
            delay,
            ttr,
            timestamp,
            state,
        })
    }
    
    /// 从 Job 创建 Put 记录
    pub fn from_job(job: &Job, tube: &str, job_state: State) -> Self {
        let state = match job_state {
            State::Ready => Some(JobState::Ready),
            State::Reserved => Some(JobState::Reserved),
            State::Buried => Some(JobState::Buried),
            State::Delayed => Some(JobState::Delayed),
        };
        
        Self {
            record_type: RecordType::Put,
            job_id: *job.id(),
            tube_name: tube.to_string(),
            data: job.data.as_bytes().to_vec(),
            priority: job.priority(),
            delay: job.delay(),
            ttr: job.ttr(),
            timestamp: chrono::Local::now().timestamp(),
            state,
        }
    }
    
    /// 创建 Bury 记录
    pub fn bury_record(job_id: Id, tube: &str) -> Self {
        Self {
            record_type: RecordType::Bury,
            job_id,
            tube_name: tube.to_string(),
            data: vec![],
            priority: 0,
            delay: 0,
            ttr: 0,
            timestamp: chrono::Local::now().timestamp(),
            state: Some(JobState::Buried),
        }
    }
}

/// Binlog 管理器
pub struct BinlogManager {
    base_dir: PathBuf,
    current_file: Arc<Mutex<File>>,
    current_index: u64,
    max_file_size: u64,
    records_written: u64,
    fsync_interval_ms: u64,  // 0 表示每次写入都 fsync
    no_fsync: bool,          // 如果为 true，从不 fsync
    last_fsync: std::time::Instant,
    #[allow(dead_code)]
    lock_file: StdFile,      // 用于持有目录锁（防止多实例冲突）
}

impl BinlogManager {
    /// 创建新的 binlog 管理器
    pub async fn new(
        base_dir: impl AsRef<Path>, 
        max_file_size: u64,
        fsync_interval_ms: u64,
        no_fsync: bool,
    ) -> Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        
        // 确保目录存在
        tokio::fs::create_dir_all(&base_dir).await?;
        
        // 获取目录锁（防止多实例冲突）
        let lock_file = Self::acquire_dir_lock(&base_dir)?;
        
        // 找到最新的 binlog 文件索引
        let current_index = Self::find_latest_index(&base_dir).await?;
        
        // 打开或创建 binlog 文件
        let file_path = Self::binlog_path(&base_dir, current_index);
        let file = Self::open_binlog_file(&file_path).await?;
        
        // 获取当前文件大小
        let metadata = tokio::fs::metadata(&file_path).await?;
        let current_size = metadata.len();
        
        info!(
            "Binlog initialized: index={}, size={}, fsync_interval={}ms, no_fsync={}", 
            current_index, current_size, fsync_interval_ms, no_fsync
        );
        
        Ok(Self {
            base_dir,
            current_file: Arc::new(Mutex::new(file)),
            current_index,
            max_file_size,
            records_written: 0,
            fsync_interval_ms,
            no_fsync,
            last_fsync: std::time::Instant::now(),
            lock_file,
        })
    }
    
    /// 获取目录锁（防止多个实例同时使用同一个目录）
    fn acquire_dir_lock(base_dir: &Path) -> Result<StdFile> {
        let lock_path = base_dir.join("lock");
        let lock_file = StdFile::create(&lock_path)
            .map_err(|e| anyhow::anyhow!("Failed to create lock file: {}", e))?;
        
        // 尝试获取独占锁
        match fs4::fs_std::FileExt::try_lock_exclusive(&lock_file) {
            Ok(_) => {
                debug!("Acquired binlog directory lock: {:?}", base_dir);
                Ok(lock_file)
            }
            Err(_) => {
                Err(anyhow::anyhow!(
                    "Failed to acquire lock on binlog directory {:?}. \
                     Another beanstalkd instance may be using this directory.", 
                    base_dir
                ))
            }
        }
    }
    
    /// 打开 binlog 文件（追加模式）
    async fn open_binlog_file(path: &Path) -> Result<File> {
        OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)
            .await
            .map_err(|e| anyhow!("Failed to open binlog file: {}", e))
    }
    
    /// 获取 binlog 文件路径
    fn binlog_path(base_dir: &Path, index: u64) -> PathBuf {
        base_dir.join(format!("binlog.{:010}", index))
    }
    
    /// 找到最新的 binlog 索引
    async fn find_latest_index(base_dir: &Path) -> Result<u64> {
        let mut max_index = 1u64;
        
        let mut entries = tokio::fs::read_dir(base_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            
            if name_str.starts_with("binlog.") {
                if let Some(num_str) = name_str.strip_prefix("binlog.") {
                    if let Ok(num) = num_str.parse::<u64>() {
                        max_index = max_index.max(num);
                    }
                }
            }
        }
        
        Ok(max_index)
    }
    
    /// 写入记录
    pub async fn write_record(&mut self, record: &BinlogRecord) -> Result<()> {
        let data = record.serialize();
        
        // 检查是否需要轮转
        if self.needs_rotation(data.len() as u64).await? {
            self.rotate().await?;
        }
        
        let mut file = self.current_file.lock().await;
        file.write_all(&data).await?;
        
        // 处理 fsync 逻辑
        let should_fsync = if self.no_fsync {
            false
        } else if self.fsync_interval_ms == 0 {
            // 每次写入都 fsync
            true
        } else {
            // 检查是否超过 fsync 间隔
            self.last_fsync.elapsed().as_millis() as u64 >= self.fsync_interval_ms
        };
        
        if should_fsync {
            file.sync_all().await?;
            self.last_fsync = std::time::Instant::now();
        }
        
        self.records_written += 1;
        
        debug!("Binlog record written: {:?} job_id={}", record.record_type, record.job_id);
        Ok(())
    }
    
    /// 检查是否需要轮转
    async fn needs_rotation(&self, record_size: u64) -> Result<bool> {
        let file = self.current_file.lock().await;
        let metadata = file.metadata().await?;
        let current_size = metadata.len();
        
        Ok(current_size + record_size > self.max_file_size)
    }
    
    /// 执行 binlog 文件轮转
    async fn rotate(&mut self) -> Result<()> {
        info!("Rotating binlog file: index={}", self.current_index);
        
        // 关闭当前文件
        let mut file = self.current_file.lock().await;
        file.flush().await?;
        drop(file);
        
        // 创建新文件
        self.current_index += 1;
        let new_path = Self::binlog_path(&self.base_dir, self.current_index);
        let new_file = Self::open_binlog_file(&new_path).await?;
        
        self.current_file = Arc::new(Mutex::new(new_file));
        
        info!("Binlog rotated to index={}", self.current_index);
        Ok(())
    }
    
    /// 读取所有 binlog 文件并恢复 jobs
    /// 返回 (tube_name, job, state) 的列表
    pub async fn recover(&self) -> Result<Vec<(String, Job, JobState)>> {
        let mut jobs = Vec::new();
        let mut current_index = 1u64;
        
        loop {
            let path = Self::binlog_path(&self.base_dir, current_index);
            
            if !path.exists() {
                if current_index == 1 {
                    // 没有 binlog 文件
                    break;
                }
                // 已经读完所有文件
                break;
            }
            
            info!("Recovering from binlog: {:?}", path);
            
            let file_jobs = self.recover_from_file(&path).await?;
            jobs.extend(file_jobs);
            
            current_index += 1;
        }
        
        info!("Recovered {} jobs from binlog", jobs.len());
        Ok(jobs)
    }
    
    /// 从单个文件恢复
    /// 返回 (tube_name, job, state) 的列表
    async fn recover_from_file(&self, path: &Path) -> Result<Vec<(String, Job, JobState)>> {
        let mut file = File::open(path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;
        
        let mut jobs = Vec::new();
        let mut offset = 0usize;
        
        // 跟踪已删除的 jobs（用于过滤）
        let mut deleted_jobs: std::collections::HashSet<u64> = std::collections::HashSet::new();
        
        // 第一遍：收集所有删除记录
        let mut scan_offset = 0usize;
        while scan_offset < buffer.len() {
            if scan_offset + 4 > buffer.len() {
                break;
            }
            
            let record_len = u32::from_be_bytes([
                buffer[scan_offset],
                buffer[scan_offset + 1],
                buffer[scan_offset + 2],
                buffer[scan_offset + 3],
            ]) as usize;
            
            if scan_offset + 4 + record_len > buffer.len() {
                break;
            }
            
            if let Some(record) = BinlogRecord::deserialize(&buffer[scan_offset..scan_offset + 4 + record_len]) {
                if record.record_type == RecordType::Delete {
                    deleted_jobs.insert(record.job_id);
                }
            }
            
            scan_offset += 4 + record_len;
        }
        
        // 第二遍：收集所有未被删除的 put 记录
        while offset < buffer.len() {
            // 读取记录长度
            if offset + 4 > buffer.len() {
                break;
            }
            
            let record_len = u32::from_be_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]) as usize;
            
            if offset + 4 + record_len > buffer.len() {
                warn!("Incomplete binlog record at offset {}", offset);
                break;
            }
            
            // 解析记录
            if let Some(record) = BinlogRecord::deserialize(&buffer[offset..offset + 4 + record_len]) {
                if record.record_type == RecordType::Put && !deleted_jobs.contains(&record.job_id) {
                    let job = Job::new(
                        record.job_id,
                        record.priority,
                        record.delay,
                        record.ttr,
                        record.data.len() as i64,
                        String::from_utf8_lossy(&record.data).to_string(),
                    );
                    // 使用记录中的状态，默认为 Ready
                    let state = record.state.unwrap_or(JobState::Ready);
                    jobs.push((record.tube_name, job, state));
                }
            } else {
                warn!("Failed to deserialize binlog record at offset {}", offset);
            }
            
            offset += 4 + record_len;
        }
        
        Ok(jobs)
    }
    
    /// 获取当前 binlog 索引
    pub fn current_index(&self) -> u64 {
        self.current_index
    }
    
    /// 获取写入记录数
    pub fn records_written(&self) -> u64 {
        self.records_written
    }
    
    /// 执行 WAL 垃圾回收
    /// 删除不再包含有效 job 的旧 binlog 文件
    /// 对应 C 版本中的 walgc 函数
    pub async fn gc(&self) -> Result<usize> {
        let mut deleted_count = 0;
        let mut current_check = 1u64;
        
        loop {
            // 检查文件是否存在
            let path = Self::binlog_path(&self.base_dir, current_check);
            if !path.exists() {
                if current_check > self.current_index {
                    break;
                }
                current_check += 1;
                continue;
            }
            
            // 检查文件是否还在被引用（通过读取文件内容检查）
            // 简化实现：删除所有非当前文件的旧文件
            if current_check < self.current_index {
                // 检查文件是否为空或只包含已删除的 job
                match self.should_delete_file(current_check).await {
                    Ok(true) => {
                        match tokio::fs::remove_file(&path).await {
                            Ok(_) => {
                                info!("GC removed binlog file: {:?}", path);
                                deleted_count += 1;
                            }
                            Err(e) => {
                                warn!("Failed to remove binlog file {:?}: {}", path, e);
                            }
                        }
                    }
                    Ok(false) => {
                        debug!("Keeping binlog file: {:?}", path);
                    }
                    Err(e) => {
                        warn!("Error checking binlog file {:?}: {}", path, e);
                    }
                }
            }
            
            current_check += 1;
        }
        
        if deleted_count > 0 {
            info!("WAL GC completed: removed {} files", deleted_count);
        }
        
        Ok(deleted_count)
    }
    
    /// 检查是否应该删除指定的 binlog 文件
    /// 简化实现：如果文件很小（小于1KB），认为可以删除
    /// 实际应该检查所有 job 是否都已被删除
    async fn should_delete_file(&self, index: u64) -> Result<bool> {
        let path = Self::binlog_path(&self.base_dir, index);
        
        // 读取文件内容
        let mut file = File::open(&path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;
        
        // 如果文件为空或很小，可以删除
        if buffer.len() < 100 {
            return Ok(true);
        }
        
        // 解析文件中的所有记录
        let mut offset = 0usize;
        let mut has_active_jobs = false;
        
        while offset < buffer.len() {
            if offset + 4 > buffer.len() {
                break;
            }
            
            let record_len = u32::from_be_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]) as usize;
            
            if offset + 4 + record_len > buffer.len() {
                break;
            }
            
            // 解析记录
            if let Some(record) = BinlogRecord::deserialize(&buffer[offset..offset + 4 + record_len]) {
                // 如果是 put 记录且 job 未被删除，认为文件还有效
                if record.record_type == RecordType::Put {
                    // 检查 job 是否还在全局存储中
                    if crate::backend::job_store::global_contains_job(&record.job_id) {
                        has_active_jobs = true;
                    }
                }
            }
            
            offset += 4 + record_len;
        }
        
        // 如果没有活跃的 job，可以删除
        Ok(!has_active_jobs)
    }
}

/// 全局 binlog 管理器实例
use std::sync::OnceLock;

static BINLOG_MANAGER: OnceLock<Arc<Mutex<BinlogManager>>> = OnceLock::new();

/// 初始化 binlog
pub async fn init_binlog(
    base_dir: impl AsRef<Path>, 
    max_file_size: u64,
    fsync_interval_ms: u64,
    no_fsync: bool,
) -> Result<()> {
    let manager = BinlogManager::new(base_dir, max_file_size, fsync_interval_ms, no_fsync).await?;
    BINLOG_MANAGER.set(Arc::new(Mutex::new(manager)))
        .map_err(|_| anyhow!("Binlog already initialized"))?;
    Ok(())
}

/// 获取 binlog 管理器
pub fn get_binlog() -> Option<Arc<Mutex<BinlogManager>>> {
    BINLOG_MANAGER.get().cloned()
}

/// 写入 job 到 binlog
pub async fn log_put(job: &Job, tube: &str, state: State) -> Result<()> {
    if let Some(manager) = get_binlog() {
        let record = BinlogRecord::from_job(job, tube, state);
        manager.lock().await.write_record(&record).await?;
    }
    Ok(())
}

/// 写入 delete 到 binlog
pub async fn log_delete(job_id: Id, tube: &str) -> Result<()> {
    if let Some(manager) = get_binlog() {
        let record = BinlogRecord {
            record_type: RecordType::Delete,
            job_id,
            tube_name: tube.to_string(),
            data: vec![],
            priority: 0,
            delay: 0,
            ttr: 0,
            timestamp: chrono::Local::now().timestamp(),
            state: None,
        };
        manager.lock().await.write_record(&record).await?;
    }
    Ok(())
}

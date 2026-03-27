//! Job 存储模块 - 使用 HashMap 实现 O(1) 查找
//!
//! 对应 beanstalkd C 源代码中的 job hash table 实现

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::architecture::job::Job;
use crate::architecture::tube::Id;

lazy_static::lazy_static! {
    /// 全局 Job 存储，用于跨 Tube 查找 Job
    /// 对应 C 版本中的 all_jobs 哈希表
    static ref GLOBAL_JOB_STORE: Arc<RwLock<HashMap<Id, Job>>> = Arc::new(RwLock::new(HashMap::new()));
    
    /// 全局 Job ID 生成器
    static ref GLOBAL_NEXT_ID: AtomicU64 = AtomicU64::new(1);
}

/// 获取下一个全局 Job ID
pub fn next_global_job_id() -> Id {
    GLOBAL_NEXT_ID.fetch_add(1, Ordering::SeqCst)
}

/// 将 Job 插入全局存储
pub fn global_insert_job(job: Job) {
    if let Ok(mut store) = GLOBAL_JOB_STORE.write() {
        store.insert(*job.id(), job);
    }
}

/// 从全局存储中查找 Job
pub fn global_find_job(id: &Id) -> Option<Job> {
    GLOBAL_JOB_STORE.read().ok()?.get(id).cloned()
}

/// 从全局存储中移除 Job
pub fn global_remove_job(id: &Id) -> Option<Job> {
    GLOBAL_JOB_STORE.write().ok()?.remove(id)
}

/// 检查全局存储中是否包含指定 Job
pub fn global_contains_job(id: &Id) -> bool {
    GLOBAL_JOB_STORE.read().ok().map_or(false, |store| store.contains_key(id))
}

/// 获取全局存储中的 Job 数量
pub fn global_job_count() -> usize {
    GLOBAL_JOB_STORE.read().ok().map_or(0, |store| store.len())
}

/// 清空全局存储
pub fn global_clear_jobs() {
    if let Ok(mut store) = GLOBAL_JOB_STORE.write() {
        store.clear();
    }
}

/// Job 存储，使用 HashMap 实现 O(1) 查找
/// 
/// 注意：本地 JobStore 已经由 GLOBAL_JOB_STORE 替代
/// 保留此类用于向后兼容和本地存储场景
pub struct JobStore {
    jobs: HashMap<Id, Job>,
    next_id: AtomicU64,
}

impl JobStore {
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
            next_id: AtomicU64::new(1),
        }
    }

    /// 生成下一个 Job ID
    pub fn next_id(&self) -> Id {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    /// 插入 Job
    pub fn insert(&mut self, job: Job) {
        self.jobs.insert(*job.id(), job);
    }

    /// 通过 ID 查找 Job
    pub fn get(&self, id: &Id) -> Option<&Job> {
        self.jobs.get(id)
    }

    /// 通过 ID 查找并获取可变引用
    pub fn get_mut(&mut self, id: &Id) -> Option<&mut Job> {
        self.jobs.get_mut(id)
    }

    /// 通过 ID 移除 Job
    pub fn remove(&mut self, id: &Id) -> Option<Job> {
        self.jobs.remove(id)
    }

    /// 获取 Job 数量
    pub fn len(&self) -> usize {
        self.jobs.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    /// 清空所有 Job
    pub fn clear(&mut self) {
        self.jobs.clear();
    }

    /// 获取所有 Job 的引用
    pub fn values(&self) -> impl Iterator<Item = &Job> {
        self.jobs.values()
    }

    /// 获取所有 Job 的可变引用
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut Job> {
        self.jobs.values_mut()
    }

    /// 检查是否包含指定 ID 的 Job
    pub fn contains(&self, id: &Id) -> bool {
        self.jobs.contains_key(id)
    }
}

impl Default for JobStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::architecture::job::Job;

    #[test]
    fn test_job_store() {
        let mut store = JobStore::new();
        
        // 插入 job
        let job = Job::default();
        let id = *job.id();
        store.insert(job);
        
        // 查找
        assert!(store.contains(&id));
        assert_eq!(store.get(&id).unwrap().id(), &id);
        
        // 移除
        let removed = store.remove(&id);
        assert!(removed.is_some());
        assert!(!store.contains(&id));
    }

    #[test]
    fn test_next_id() {
        let store = JobStore::new();
        let id1 = store.next_id();
        let id2 = store.next_id();
        assert_eq!(id2, id1 + 1);
    }
    
    #[test]
    fn test_global_job_store() {
        // 清空
        global_clear_jobs();
        
        // 创建 job
        let job = Job::default();
        let id = *job.id();
        
        // 插入
        global_insert_job(job);
        
        // 查找
        assert!(global_contains_job(&id));
        let found = global_find_job(&id);
        assert!(found.is_some());
        assert_eq!(found.unwrap().id(), &id);
        
        // 移除
        let removed = global_remove_job(&id);
        assert!(removed.is_some());
        assert!(!global_contains_job(&id));
    }
}

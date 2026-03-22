//! Job 存储模块 - 使用 HashMap 实现 O(1) 查找
//!
//! 对应 beanstalkd C 源代码中的 job hash table 实现

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::architecture::job::Job;
use crate::architecture::tube::Id;

/// Job 存储，使用 HashMap 实现 O(1) 查找
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
}

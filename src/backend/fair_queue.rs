//! 公平队列模块 - 实现 Round-Robin 调度
//!
//! 对应 beanstalkd C 源代码中的 multiset (ms.c) 实现
//! 确保等待的客户端被公平地服务

use std::collections::VecDeque;

/// 公平队列 - Round-Robin 调度
pub struct FairQueue<T> {
    items: VecDeque<T>,
    last_pos: usize, // 上次取出的位置，用于 round-robin
}

impl<T> FairQueue<T> {
    pub fn new() -> Self {
        Self {
            items: VecDeque::new(),
            last_pos: 0,
        }
    }

    /// 添加元素到队列尾部
    pub fn push_back(&mut self, item: T) {
        self.items.push_back(item);
    }

    /// 从队列头部取出元素
    pub fn pop_front(&mut self) -> Option<T> {
        self.items.pop_front()
    }

    /// Round-Robin 取出元素
    /// 如果队列不为空，从上次位置后取出，实现公平调度
    pub fn take_round_robin(&mut self) -> Option<T> {
        if self.items.is_empty() {
            return None;
        }

        // 计算实际位置（循环）
        let pos = self.last_pos % self.items.len();
        
        // 旋转队列，使目标元素在头部
        self.items.rotate_left(pos);
        let item = self.items.pop_front();
        
        // 更新 last_pos，下次从当前位置取（因为 rotate 后顺序变了）
        self.last_pos = pos;

        item
    }

    /// 查看队列头部的元素
    pub fn front(&self) -> Option<&T> {
        self.items.front()
    }

    /// 查看队列头部的可变元素
    pub fn front_mut(&mut self) -> Option<&mut T> {
        self.items.front_mut()
    }

    /// 获取队列长度
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// 获取所有元素的引用
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.items.iter()
    }

    /// 清空队列
    pub fn clear(&mut self) {
        self.items.clear();
        self.last_pos = 0;
    }

    /// 移除指定位置的元素
    pub fn remove(&mut self, pos: usize) -> Option<T> {
        if pos < self.items.len() {
            Some(self.items.remove(pos).unwrap())
        } else {
            None
        }
    }

    /// 根据条件移除元素
    pub fn remove_by<F>(&mut self, predicate: F) -> Option<T>
    where
        F: Fn(&T) -> bool,
    {
        if let Some(pos) = self.items.iter().position(predicate) {
            Some(self.items.remove(pos).unwrap())
        } else {
            None
        }
    }
}

impl<T> Default for FairQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fair_queue_basic() {
        let mut queue = FairQueue::new();
        queue.push_back(1);
        queue.push_back(2);
        queue.push_back(3);

        assert_eq!(queue.len(), 3);
        assert_eq!(queue.pop_front(), Some(1));
        assert_eq!(queue.pop_front(), Some(2));
        assert_eq!(queue.pop_front(), Some(3));
        assert_eq!(queue.pop_front(), None);
    }

    #[test]
    fn test_round_robin() {
        let mut queue = FairQueue::new();
        queue.push_back(1);
        queue.push_back(2);
        queue.push_back(3);

        // 第一次从位置 0 取
        assert_eq!(queue.take_round_robin(), Some(1));
        // 第二次从位置 1 取（原来的 2 现在在位置 0）
        assert_eq!(queue.take_round_robin(), Some(2));
        // 第三次从位置 2 取（原来的 3 现在在位置 0）
        assert_eq!(queue.take_round_robin(), Some(3));
    }

    #[test]
    fn test_remove_by() {
        let mut queue = FairQueue::new();
        queue.push_back(1);
        queue.push_back(2);
        queue.push_back(3);

        let removed = queue.remove_by(|&x| x == 2);
        assert_eq!(removed, Some(2));
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.pop_front(), Some(1));
        assert_eq!(queue.pop_front(), Some(3));
    }
}

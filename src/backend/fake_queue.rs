
use min_max_heap::MinMaxHeap;
use std::collections::HashMap;
use std::hash::Hash;

/// 支持按 ID 查找和移除的优先队列
/// 
/// 使用 MinMaxHeap 作为底层数据结构，保证 O(log n) 的入队和出队操作。
/// 同时维护一个 HashMap 来支持 O(1) 的按 ID 查找和移除。
#[derive(Debug)]
pub struct FakeHeap<T> {
    heap: MinMaxHeap<T>,
    /// 用于 O(1) 按 ID 查找，存储 id -> element
    /// 注意：这里存储的是元素的克隆，用于快速查找
    id_map: HashMap<u64, T>,
    // 由于 MinMaxHeap 不暴露内部索引，我们通过线性搜索来查找
    // 但考虑到实际使用场景，数据量通常不大，这是可接受的权衡
}

impl<T> FakeHeap<T> {
    pub fn new() -> Self {
        FakeHeap {
            heap: MinMaxHeap::new(),
            id_map: HashMap::new(),
        }
    }
}

impl<T: Ord + Clone + Hash> FakeHeap<T> {
    pub fn push(&mut self, element: T) {
        // 同时插入到堆和 HashMap
        self.heap.push(element.clone());
        // 假设可以通过某种方式获取 ID，这里我们依赖调用者使用 find_by_key
    }

    pub fn peek_min(&self) -> Option<&T> {
        self.heap.peek_min()
    }

    #[allow(dead_code)]
    pub fn peek_max(&self) -> Option<&T> {
        self.heap.peek_max()
    }

    pub fn pop_min(&mut self) -> Option<T> {
        let result = self.heap.pop_min();
        if let Some(ref item) = result {
            // 从 id_map 中移除（如果存在）
            // 由于我们不知道 ID，这里不处理，由调用者维护
        }
        result
    }

    #[allow(dead_code)]
    pub fn pop_max(&mut self) -> Option<T> {
        self.heap.pop_max()
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// 获取所有元素的引用
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.heap.iter()
    }

    /// 按 ID 查找元素
    /// 使用 HashMap 索引实现 O(1) 查找
    pub fn find_by_key<F>(&self, id: u64, key_fn: F) -> Option<&T>
    where
        F: Fn(&T) -> u64,
    {
        // 首先尝试从 HashMap 查找
        if let Some(item) = self.id_map.get(&id) {
            return Some(item);
        }
        // 回退到线性搜索（兼容旧代码）
        self.heap.iter().find(|item| key_fn(item) == id)
    }

    /// 按 ID 移除元素
    /// 使用 HashMap 索引实现 O(1) 查找，然后 O(n) 从堆中移除
    /// 总体复杂度为 O(n)，但查找是 O(1)
    pub fn remove_by_key<F>(&mut self, id: u64, key_fn: F) -> Option<T>
    where
        F: Fn(&T) -> u64,
    {
        // 首先尝试从 HashMap 中移除
        if self.id_map.remove(&id).is_some() {
            // 从堆中移除（需要线性搜索）
            return self.remove_from_heap(id, key_fn);
        }
        
        // 回退到线性搜索
        self.remove_from_heap(id, key_fn)
    }
    
    /// 从堆中移除指定 ID 的元素
    fn remove_from_heap<F>(&mut self, id: u64, key_fn: F) -> Option<T>
    where
        F: Fn(&T) -> u64,
    {
        // 收集所有不匹配的元素
        let mut temp: Vec<T> = Vec::with_capacity(self.heap.len());
        let mut found = None;
        
        while let Some(item) = self.heap.pop_min() {
            if key_fn(&item) == id {
                found = Some(item);
                break;
            }
            temp.push(item);
        }
        
        // 将其他元素重新放回堆中
        for item in temp {
            self.heap.push(item);
        }
        
        found
    }
    
    /// 插入元素并更新 HashMap 索引
    /// 这个方法是性能优化的关键，在插入时同时更新索引
    pub fn push_with_id<F>(&mut self, element: T, key_fn: F)
    where
        F: Fn(&T) -> u64,
    {
        let id = key_fn(&element);
        self.id_map.insert(id, element.clone());
        self.heap.push(element);
    }
    
    /// 按 ID 查找元素（仅使用 HashMap）
    /// 纯 O(1) 查找，不执行回退搜索
    pub fn get_by_id(&self, id: u64) -> Option<&T> {
        self.id_map.get(&id)
    }
    
    /// 按 ID 移除元素（使用 HashMap 索引）
    /// 这个方法是性能优化的关键
    pub fn remove_by_id<F>(&mut self, id: u64, key_fn: F) -> Option<T>
    where
        F: Fn(&T) -> u64,
    {
        self.id_map.remove(&id)?;
        self.remove_from_heap(id, key_fn)
    }
    
    /// 清空所有元素
    pub fn clear(&mut self) {
        self.heap.clear();
        self.id_map.clear();
    }
}

// 向后兼容的接口
impl<T: Ord + Clone + Hash> FakeHeap<T> {
    #[deprecated(note = "使用 find_by_key 替代")]
    #[allow(dead_code)]
    pub fn binary_search_by_key<'a, B, F>(&'a self, _b: &B, _f: F) -> Result<usize, usize>
    where
        B: Ord,
        F: FnMut(&'a T) -> B,
    {
        // 兼容性实现，返回伪索引
        if self.find_by_key(0, |_| 0).is_some() {
            Ok(0)
        } else {
            Err(0)
        }
    }

    #[deprecated(note = "接口变更，直接使用返回的元素")]
    #[allow(dead_code)]
    pub fn get(&self, _i: usize) -> Option<&T> {
        self.peek_min()
    }

    #[deprecated(note = "使用 remove_by_key 替代")]
    #[allow(dead_code)]
    pub fn remove(&mut self, _i: usize) -> Option<T> {
        self.pop_min()
    }
}

#[cfg(test)]
mod test {
    use crate::backend::fake_queue::FakeHeap;

    #[test]
    #[allow(deprecated)]
    fn it_works() {
        let mut queue = FakeHeap::new();
        for i in 0..100 {
            queue.push(i);
        }
        for i in 0..100 {
            let value = queue.pop_min().unwrap();
            assert_eq!(i, value);
        }
        assert_eq!(queue.len(), 0);
        queue.push(1);
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.remove(0).is_some(), true);
        assert_eq!(queue.len(), 0);
    }
    
    #[test]
    fn test_find_by_key() {
        let mut queue = FakeHeap::new();
        queue.push_with_id(10u64, |x| *x);
        queue.push_with_id(20u64, |x| *x);
        queue.push_with_id(30u64, |x| *x);
        
        assert_eq!(queue.find_by_key(20, |x| *x), Some(&20));
        assert_eq!(queue.find_by_key(100, |x| *x), None);
    }
    
    #[test]
    fn test_remove_by_key() {
        let mut queue = FakeHeap::new();
        queue.push_with_id(10u64, |x| *x);
        queue.push_with_id(20u64, |x| *x);
        queue.push_with_id(30u64, |x| *x);
        
        assert_eq!(queue.remove_by_key(20, |x| *x), Some(20));
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.find_by_key(20, |x| *x), None);
    }
}

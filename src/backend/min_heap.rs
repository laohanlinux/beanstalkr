
use crate::architecture::tube::{Id, PriorityQueue, PriorityQueueItem};
use crate::backend::fake_queue::FakeHeap;
use std::hash::Hash;
use std::time::{SystemTime, UNIX_EPOCH};

/// 高性能优先队列实现
/// 
/// 使用 FakeHeap 作为底层存储，支持 O(log n) 的入队和出队操作，
/// 以及 O(1) 的按 ID 查找和 O(n) 的按 ID 删除。
pub struct MinHeap<H: PriorityQueueItem + Ord + Clone + Hash + Send> {
    heap: FakeHeap<H>,
    #[allow(dead_code)]
    tube_name: String,
    #[allow(dead_code)]
    timestamp: i64,
}


impl<H> MinHeap<H>
where
    H: PriorityQueueItem + Ord + Clone + Hash + Send,
{
    pub fn new(name: String) -> Self {
        MinHeap {
            heap: FakeHeap::new(),
            tube_name: name,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64,
        }
    }
}

impl<H> PriorityQueue<H> for MinHeap<H>
where
    H: PriorityQueueItem + Ord + Clone + Hash + Send,
{
    fn enqueue(&mut self, mut item: H) {
        item.enqueue();
        // 使用 push_with_id 来维护 HashMap 索引，实现 O(1) 查找
        self.heap.push_with_id(item, |i| *i.id());
    }

    fn dequeue(&mut self) -> Option<H> {
        self.heap.pop_min().map(|mut item| {
            item.dequeue();
            item
        })
    }

    fn peek(&self) -> Option<&H> {
        self.heap.peek_min()
    }

    fn peek_all(&self) -> Vec<&H> {
        self.heap.iter().collect()
    }

    fn find(&self, id: &Id) -> Option<&H> {
        // 使用 HashMap 索引实现 O(1) 查找
        self.heap.get_by_id(*id)
            .or_else(|| self.heap.find_by_key(*id, |item| *item.id()))
    }

    fn remove(&mut self, id: &Id) -> Option<H> {
        // 使用 HashMap 索引实现 O(1) 查找 + O(n) 堆重建
        self.heap.remove_by_id(*id, |item| *item.id())
    }

    fn len(&self) -> usize {
        self.heap.len()
    }

    fn set_time(&mut self) {
        self.timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
    }
}

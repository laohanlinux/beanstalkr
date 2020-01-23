use std::collections::BinaryHeap;

use crate::architecture::tube::{PriorityQueueItem, PriorityQueue, Id};
use crate::backend::fake_queue::FakeHeap;

pub struct MinHeap<H: PriorityQueueItem + Ord> {
    heap: FakeHeap<H>,
    tube_name: String,
}

impl<H> MinHeap<H> where H: PriorityQueueItem + Ord {
    pub fn new(name: String) -> Self {
        MinHeap {
            heap: FakeHeap::new(),
            tube_name: name,
        }
    }
}

impl<H> PriorityQueue<H> for MinHeap<H> where H: PriorityQueueItem + Ord {
    fn enqueue(&mut self, mut item: H) {
        item.enqueue();
        self.heap.push(item);
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

    fn remove(&mut self, id: &Id) -> Option<H> {
        match self.heap.binary_search_by_key(id, |item| item.id().clone()) {
            Ok(idx) => self.heap.remove(idx),
            _ => None
        }
    }

    fn len(&self) -> usize {
        self.heap.len()
    }
}


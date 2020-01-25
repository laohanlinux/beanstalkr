use crate::architecture::tube::PriorityQueueItem;

#[derive(Debug, PartialEq, Eq)]
pub struct FakeHeap<T>(Vec<T>);

impl<T> FakeHeap<T> {
    pub fn new() -> Self {
        FakeHeap(Vec::new())
    }
}

impl<T: Ord> FakeHeap<T> {
    pub fn binary_search_by_key<'a, B, F>(
        &'a self,
        b: &B,
        f: F,
    ) -> Result<usize, usize>
        where
            B: Ord,
            F: FnMut(&'a T) -> B {
        self.0.binary_search_by_key(b, f)
    }

    pub fn get(&self, i: usize) -> Option<&T> {
        self.0.get(i)
    }

    pub fn push(&mut self, element: T) {
        self.0.push(element);
    }

    pub fn peek_min(&self) -> Option<&T> {
        self.find_min().map(|i| &self.0[i])
    }

    pub fn peek_max(&self) -> Option<&T> {
        self.find_max().map(|i| &self.0[i])
    }

    fn find_min(&self) -> Option<usize> {
        self.0.iter().enumerate().min_by_key(|p| p.1).map(|p| p.0)
    }

    fn find_max(&self) -> Option<usize> {
        self.0.iter().enumerate().max_by_key(|p| p.1).map(|p| p.0)
    }

    fn pop_index(&mut self, i: usize) -> T {
        let last = self.0.len() - 1;
        self.0.swap(i, last);
        self.0.pop().unwrap()
    }

    pub fn remove(&mut self, i: usize) -> Option<T> {
        if self.0.len() - 1 < i {
            return None;
        }
        Some(self.pop_index(i))
    }

    pub fn pop_min(&mut self) -> Option<T> {
        self.find_min().map(|i| self.pop_index(i))
    }

    pub fn pop_max(&mut self) -> Option<T> {
        self.find_max().map(|i| self.pop_index(i))
    }

    pub fn push_pop_min(&mut self, element: T) -> T {
        self.push(element);
        self.pop_min().unwrap()
    }

    pub fn push_pop_max(&mut self, element: T) -> T {
        self.push(element);
        self.pop_max().unwrap()
    }

    pub fn replace_min(&mut self, element: T) -> Option<T> {
        let result = self.pop_min();
        self.push(element);
        result
    }

    pub fn replace_max(&mut self, element: T) -> Option<T> {
        let result = self.pop_max();
        self.push(element);
        result
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[cfg(test)]
mod test {
    use crate::backend::fake_queue::FakeHeap;

    #[test]
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
}
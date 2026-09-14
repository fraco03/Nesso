use std::collections::VecDeque;
use super::engine::TaskRef;

const NUM_PRIORITIES: usize = 256;

/// A priority queue designed specifically for discrete 8-bit priorities (`u8`).
///
/// Instead of a generic $O(\log n)$ `BinaryHeap`, this data structure uses 256
/// FIFO buckets (`VecDeque<TaskRef>`) indexed directly by priority, backed by
/// a 256-bit bitmap (`[u64; 4]`) tracking non-empty buckets.
///
/// # Complexity
/// - `push`: $O(1)$ (direct bucket append + bitwise OR).
/// - `pop`: $O(1)$ (at most 4 bitwise checks using hardware `leading_zeros()` / `clz` + `pop_front`).
/// - Strict FIFO: tasks of identical priority are guaranteed to be popped in the exact
///   order they were pushed without extra comparisons.
#[derive(Debug, Clone)]
pub struct PriorityBucketQueue {
    buckets: Vec<VecDeque<TaskRef>>,
    bitmap: [u64; 4],
    len: usize,
}

impl PriorityBucketQueue {
    pub fn new() -> Self {
        let mut buckets = Vec::with_capacity(NUM_PRIORITIES);
        for _ in 0..NUM_PRIORITIES {
            buckets.push(VecDeque::new());
        }
        Self {
            buckets,
            bitmap: [0u64; 4],
            len: 0,
        }
    }

    /// Pushes a new task into the queue in $O(1)$ time.
    pub fn push(&mut self, task: TaskRef) {
        let p = task.priority as usize;
        self.buckets[p].push_back(task);
        self.bitmap[p / 64] |= 1u64 << (p % 64);
        self.len += 1;
    }

    /// Pops the highest-priority task in $O(1)$ time.
    /// If multiple tasks share the same priority, the oldest pushed task (FIFO) is returned.
    pub fn pop(&mut self) -> Option<TaskRef> {
        for word_idx in (0..4).rev() {
            let word = self.bitmap[word_idx];
            if word != 0 {
                let bit_idx = 63 - word.leading_zeros() as usize;
                let p = word_idx * 64 + bit_idx;
                let task = self.buckets[p]
                    .pop_front()
                    .expect("bucket must not be empty when bit is set");
                
                if self.buckets[p].is_empty() {
                    self.bitmap[word_idx] &= !(1u64 << bit_idx);
                }
                self.len -= 1;
                return Some(task);
            }
        }
        None
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn clear(&mut self) {
        for b in &mut self.buckets {
            b.clear();
        }
        self.bitmap = [0u64; 4];
        self.len = 0;
    }
}

impl Default for PriorityBucketQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_queue() {
        let mut q = PriorityBucketQueue::new();
        assert!(q.is_empty());
        assert_eq!(q.len(), 0);
        assert!(q.pop().is_none());
    }

    #[test]
    fn test_fifo_same_priority() {
        let mut q = PriorityBucketQueue::new();
        for id in 1..=100 {
            q.push(TaskRef { id, priority: 5, retries: 0 });
        }
        assert_eq!(q.len(), 100);

        for expected_id in 1..=100 {
            let t = q.pop().expect("expected task");
            assert_eq!(t.id, expected_id);
            assert_eq!(t.priority, 5);
        }
        assert!(q.is_empty());
    }

    #[test]
    fn test_priority_ordering() {
        let mut q = PriorityBucketQueue::new();
        q.push(TaskRef { id: 1, priority: 10, retries: 0 });
        q.push(TaskRef { id: 2, priority: 250, retries: 0 });
        q.push(TaskRef { id: 3, priority: 0, retries: 0 });
        q.push(TaskRef { id: 4, priority: 128, retries: 0 });
        q.push(TaskRef { id: 5, priority: 255, retries: 0 });
        q.push(TaskRef { id: 6, priority: 10, retries: 0 });

        assert_eq!(q.pop().unwrap().id, 5); // priority 255
        assert_eq!(q.pop().unwrap().id, 2); // priority 250
        assert_eq!(q.pop().unwrap().id, 4); // priority 128
        assert_eq!(q.pop().unwrap().id, 1); // priority 10 (first pushed)
        assert_eq!(q.pop().unwrap().id, 6); // priority 10 (second pushed)
        assert_eq!(q.pop().unwrap().id, 3); // priority 0
        assert!(q.pop().is_none());
    }

    #[test]
    fn test_all_word_boundary_priorities() {
        let mut q = PriorityBucketQueue::new();
        let boundaries = [0, 63, 64, 127, 128, 191, 192, 255];
        for &p in &boundaries {
            q.push(TaskRef { id: p as u64, priority: p, retries: 0 });
        }

        let mut expected = boundaries.to_vec();
        expected.sort_by(|a, b| b.cmp(a)); // Descending

        for exp in expected {
            let t = q.pop().unwrap();
            assert_eq!(t.priority, exp);
            assert_eq!(t.id, exp as u64);
        }
        assert!(q.is_empty());
    }
}

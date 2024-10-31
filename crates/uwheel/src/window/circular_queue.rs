use std::vec::Vec;

use crate::Aggregator;

#[repr(align(64))]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Default)]
pub struct CircularQueue<A: Aggregator> {
    pub m_front: i32,
    pub m_rear: i32,
    pub m_size: usize,
    pub m_counter: usize,
    pub m_arr: Vec<A::PartialAggregate>,
}

impl<A: Aggregator> CircularQueue<A> {
    pub fn new(size: usize) -> Self {
        CircularQueue {
            m_front: -1,
            m_rear: -1,
            m_size: size,
            m_counter: 0,
            m_arr: vec![A::PartialAggregate::default(); size],
        }
    }

    // Add an element to the queue
    pub fn enqueue(&mut self, val: A::PartialAggregate) {
        if self.m_counter == self.m_size {
            panic!("Queue is Full");
        } else {
            if self.m_front == -1 {
                self.m_front = 0;
            }
            self.m_rear += 1;
            if self.m_rear == self.m_size as i32 {
                self.m_rear = 0;
            }
            self.m_arr[self.m_rear as usize] = val;
        }
        self.m_counter += 1;
    }

    // Remove and return an element from the front of the queue
    pub fn dequeue(&mut self) -> Option<A::PartialAggregate> {
        if self.m_front == -1 || self.m_counter == 0 {
            println!("Queue is Empty");
            return None;
        }

        let data = self.m_arr[self.m_front as usize].clone();
        self.m_arr[self.m_front as usize] = A::PartialAggregate::default();

        if self.m_front == self.m_rear {
            // Reset the queue when the last element is removed
            self.m_front = -1;
            self.m_rear = -1;
        } else {
            // Move front pointer to the next element
            self.m_front = (self.m_front + 1) % self.m_size as i32;
        }

        self.m_counter -= 1;
        Some(data)
    }

    // Remove multiple elements from the front of the queue
    pub fn dequeue_many(&mut self, num_of_items: usize) {
        if num_of_items > self.m_counter {
            panic!("Cannot dequeue more items than present in the queue");
        }

        self.m_front = (self.m_front + num_of_items as i32) % self.m_size as i32;
        self.m_counter -= num_of_items;

        // If the queue becomes empty, reset the front and rear pointers
        if self.m_front == (self.m_rear + 1) % self.m_size as i32 {
            self.m_front = -1;
            self.m_rear = -1;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::aggregator::sum::U32SumAggregator;

    use super::*;

    type DummyAggregator = U32SumAggregator;

    #[test]
    fn test_enqueue_single_element() {
        let mut queue: CircularQueue<DummyAggregator> = CircularQueue::new(3);
        queue.enqueue(1);
        assert_eq!(queue.dequeue(), Some(1));
    }

    #[test]
    fn test_enqueue_multiple_elements() {
        let mut queue: CircularQueue<DummyAggregator> = CircularQueue::new(3);
        queue.enqueue(1);
        queue.enqueue(2);
        assert_eq!(queue.dequeue(), Some(1));
        assert_eq!(queue.dequeue(), Some(2));
    }

    #[test]
    fn test_enqueue_wrap_around() {
        let mut queue: CircularQueue<DummyAggregator> = CircularQueue::new(3);
        queue.enqueue(1);
        queue.enqueue(2);
        queue.dequeue();
        queue.enqueue(3);
        queue.enqueue(4);

        assert_eq!(queue.dequeue(), Some(2));
        assert_eq!(queue.dequeue(), Some(3));
        assert_eq!(queue.dequeue(), Some(4));
    }

    #[test]
    #[should_panic(expected = "Queue is Full")]
    fn test_enqueue_overflow() {
        let mut queue: CircularQueue<DummyAggregator> = CircularQueue::new(2);
        queue.enqueue(1);
        queue.enqueue(2);
        queue.enqueue(3);
    }

    #[test]
    fn test_dequeue_empty() {
        let mut queue: CircularQueue<DummyAggregator> = CircularQueue::new(2);
        assert_eq!(queue.dequeue(), None);
    }

    #[test]
    #[should_panic(expected = "Cannot dequeue more items than present in the queue")]
    fn test_dequeue_many_overflow() {
        let mut queue: CircularQueue<DummyAggregator> = CircularQueue::new(5);
        for i in 1..=3 {
            queue.enqueue(i);
        }
        queue.dequeue_many(4);
    }
}

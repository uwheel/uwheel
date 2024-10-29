use std::vec::Vec;

#[repr(align(64))]
pub struct CircularQueue<T> {
    pub m_front: i32,
    pub m_rear: i32,
    pub m_size: usize,
    pub m_counter: usize,
    pub m_arr: Vec<T>,
}

impl<T: Default + Clone> CircularQueue<T> {
    pub fn new(size: usize) -> Self {
        CircularQueue {
            m_front: -1,
            m_rear: -1,
            m_size: size,
            m_counter: 0,
            m_arr: vec![T::default(); size],
        }
    }

    // Add an element to the queue
    pub fn enqueue(&mut self, val: T) {
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

    // Add multiple elements to the queue
    pub fn enqueue_many(&mut self, vals: &[T], start: usize, end: usize) {
        let elements_to_add = end - start;
        if self.m_counter + elements_to_add > self.m_size {
            panic!("Queue is Full");
        }

        if self.m_front == -1 {
            self.m_front = 0;
        }

        self.m_rear += 1;
        let diff = if elements_to_add + (self.m_rear as usize) < self.m_size {
            elements_to_add
        } else {
            self.m_size - self.m_rear as usize
        };

        self.m_arr[self.m_rear as usize..self.m_rear as usize + diff]
            .clone_from_slice(&vals[start..start + diff]);

        if diff != elements_to_add {
            let remaining = elements_to_add - diff;
            self.m_arr[0..remaining].clone_from_slice(&vals[start + diff..end]);
        }

        self.m_rear = (self.m_rear + elements_to_add as i32 - 1) % self.m_size as i32;
        self.m_counter += elements_to_add;
    }

    // Remove and return an element from the front of the queue
    pub fn dequeue(&mut self) -> Option<T> {
        if self.m_front == -1 || self.m_counter == 0 {
            println!("Queue is Empty");
            return None;
        }

        let data = self.m_arr[self.m_front as usize].clone();
        self.m_arr[self.m_front as usize] = T::default();

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
        if num_of_items > self.size() {
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

    pub fn reset(&mut self) {
        self.m_front = -1;
        self.m_rear = -1;
        self.m_counter = 0;
    }

    pub fn size(&self) -> usize {
        if self.m_front == -1 {
            return 0;
        }
        if self.m_rear >= self.m_front {
            (self.m_rear - self.m_front + 1) as usize
        } else {
            (self.m_size as i32 - self.m_front + self.m_rear + 1) as usize
        }
    }
}

impl<T: std::fmt::Display> CircularQueue<T> {
    pub fn print_queue(&self) {
        if self.m_front == -1 {
            println!("Queue is Empty");
            return;
        }

        print!("Elements in Circular Queue are: ");
        if self.m_rear >= self.m_front {
            for i in self.m_front..=self.m_rear {
                print!("{} ", self.m_arr[i as usize]);
            }
        } else {
            for i in self.m_front..self.m_size as i32 {
                print!("{} ", self.m_arr[i as usize]);
            }
            for i in 0..=self.m_rear {
                print!("{} ", self.m_arr[i as usize]);
            }
        }
        println!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enqueue_single_element() {
        let mut queue = CircularQueue::new(3);
        queue.enqueue(1);
        assert_eq!(queue.dequeue(), Some(1));
    }

    #[test]
    fn test_enqueue_multiple_elements() {
        let mut queue = CircularQueue::new(3);
        queue.enqueue(1);
        queue.enqueue(2);
        assert_eq!(queue.dequeue(), Some(1));
        assert_eq!(queue.dequeue(), Some(2));
    }

    #[test]
    fn test_enqueue_wrap_around() {
        let mut queue = CircularQueue::new(3);
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
        let mut queue = CircularQueue::new(2);
        queue.enqueue(1);
        queue.enqueue(2);
        queue.enqueue(3);
    }

    #[test]
    fn test_dequeue_empty() {
        let mut queue = CircularQueue::<i32>::new(2);
        assert_eq!(queue.dequeue(), None);
    }

    #[test]
    fn test_enqueue_many_and_dequeue() {
        let mut queue = CircularQueue::new(5);
        queue.enqueue_many(&[1, 2, 3, 4, 5], 0, 5);
        assert_eq!(queue.dequeue(), Some(1));
        assert_eq!(queue.dequeue(), Some(2));
        assert_eq!(queue.dequeue(), Some(3));
    }

    #[test]
    #[should_panic(expected = "Cannot dequeue more items than present in the queue")]
    fn test_dequeue_many_overflow() {
        let mut queue = CircularQueue::new(5);
        queue.enqueue_many(&[1, 2, 3], 0, 3);
        queue.dequeue_many(4);
    }

    #[test]
    fn test_reset() {
        let mut queue = CircularQueue::new(3);
        queue.enqueue(1);
        queue.enqueue(2);
        queue.reset();
        assert_eq!(queue.dequeue(), None);
        queue.enqueue(3);
        assert_eq!(queue.dequeue(), Some(3));
    }
}

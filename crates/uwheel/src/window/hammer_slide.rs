use crate::{window::circular_queue::CircularQueue, Aggregator};

#[cfg(feature = "std")]
use std::vec::Vec;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Default)]
pub struct HammerSlide<A: Aggregator> {
    m_capacity: usize,
    m_istack_size: usize,
    m_istack_ptr: isize,
    m_ostack_size: usize,
    m_ostack_ptr: isize,
    m_istack_val: A::PartialAggregate,

    m_queue: CircularQueue<A>,
    m_ostack_val: Vec<A::PartialAggregate>,
}

impl<A: Aggregator> HammerSlide<A> {
    pub fn with_capacity(capacity: usize) -> Self {
        let queue = CircularQueue::new(capacity);
        let mut stack = Vec::new();

        for _ in 0..capacity {
            stack.push(A::IDENTITY);
        }

        HammerSlide {
            m_capacity: 0,
            m_istack_size: 0,
            m_istack_ptr: -1,
            m_ostack_size: 0,
            m_ostack_ptr: -1,
            m_istack_val: A::IDENTITY,
            m_queue: queue,
            m_ostack_val: stack,
        }
    }

    pub fn push(&mut self, val: A::PartialAggregate) {
        let temp_value = if self.m_istack_size == 0 {
            A::IDENTITY
        } else {
            self.m_istack_val
        };

        self.m_istack_val = A::combine(val, temp_value);

        self.m_queue.enqueue(val);

        self.m_istack_ptr = self.m_queue.m_rear as isize;

        self.m_capacity += 1;
        self.m_istack_size += 1;
    }

    pub fn pop(&mut self) {
        self.m_ostack_ptr += 1;
        self.m_ostack_size -= 1;
        self.m_capacity -= 1;
        self.m_queue.dequeue();
    }

    pub fn query(&mut self) -> A::PartialAggregate {
        // If the output stack is empty, attempt to swap
        if self.m_ostack_size == 0 {
            self.swap();
        }

        // If the swap didn't populate the output stack, return the identity value
        if self.m_ostack_size == 0 {
            return A::IDENTITY;
        }

        let temp1 = self.m_ostack_val[self.m_ostack_size - 1];
        let temp2 = if self.m_istack_size == 0 {
            A::IDENTITY
        } else {
            self.m_istack_val
        };

        A::combine(temp1, temp2)
    }
}

impl<A: Aggregator> HammerSlide<A> {
    fn swap(&mut self) {
        let mut output_index = 0;
        let mut input_index = self.m_istack_ptr as usize;
        let limit = self.m_istack_size;
        let temp_rear = self.m_queue.m_rear;
        let queue_size = self.m_queue.m_size;

        let mut temp_value = A::IDENTITY;

        while output_index < limit {
            let temp_tuple = self.m_queue.m_arr[input_index];

            temp_value = A::combine(temp_tuple, temp_value);
            self.m_ostack_val[output_index] = temp_value;

            input_index = if input_index == 0 {
                queue_size - 1
            } else {
                input_index - 1
            };
            output_index += 1;
        }

        self.m_ostack_size = limit;
        self.m_istack_size = 0;

        self.m_ostack_ptr =
            (temp_rear as isize - self.m_ostack_size as isize + 1).rem_euclid(queue_size as isize);

        self.m_istack_ptr = -1;
    }
}

#[cfg(test)]
mod tests {
    use crate::aggregator::{min::U32MinAggregator, sum::U32SumAggregator};

    use super::*;
    use rand::{distributions::Uniform, Rng};

    #[test]
    fn test_insert_and_query_sum() {
        let mut hammer_slide: HammerSlide<U32SumAggregator> = HammerSlide::with_capacity(10);

        let mut expected = 0;

        for val in 1..11 {
            assert_eq!(hammer_slide.query(), expected);
            hammer_slide.push(val);
            expected += val;
        }

        assert_eq!(hammer_slide.query(), 55);
    }

    #[test]
    fn test_insert_and_query_min() {
        let mut hammer_slide: HammerSlide<U32MinAggregator> = HammerSlide::with_capacity(10);

        let mut expected = u32::MAX;

        for val in 1..11 {
            assert_eq!(hammer_slide.query(), expected);
            hammer_slide.push(val);
            expected = expected.min(val);
        }

        assert_eq!(hammer_slide.query(), 1);
    }

    #[test]
    fn test_evict_and_query_sum() {
        let mut hammer_slide: HammerSlide<U32SumAggregator> = HammerSlide::with_capacity(10);

        let values = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        for val in values {
            hammer_slide.push(val);
        }

        let mut expected = hammer_slide.query();

        for val in 1..11 {
            hammer_slide.pop();
            expected -= val;
            assert_eq!(hammer_slide.query(), expected);
        }
    }

    #[test]
    fn test_evict_and_query_min() {
        let mut hammer_slide: HammerSlide<U32MinAggregator> = HammerSlide::with_capacity(10);

        let values = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        for val in values {
            hammer_slide.push(val);
        }

        assert_eq!(hammer_slide.query(), 1);

        for val in 1..10 {
            hammer_slide.pop();
            assert_eq!(hammer_slide.query(), val + 1);
        }

        hammer_slide.pop();
        assert_eq!(hammer_slide.query(), u32::MAX);
    }

    #[test]
    fn test_hammerslide_random_stream_sum() {
        const WINDOW_SIZE: usize = 1000;
        const WINDOW_SLIDE: usize = 100;
        const INPUT_SIZE: usize = 1_000_000;

        let mut hammer_slide: HammerSlide<U32SumAggregator> =
            HammerSlide::with_capacity(WINDOW_SIZE);

        // Generate random input data
        let rng = rand::thread_rng();
        let input: Vec<u32> = rng
            .sample_iter(Uniform::new(1, INPUT_SIZE as u32 * 2))
            .take(INPUT_SIZE)
            .collect();

        let mut result = 0;

        // Insert the first `WINDOW_SIZE` elements to initialize the sliding window
        let mut idx = 0;
        while idx < WINDOW_SIZE && idx < input.len() {
            hammer_slide.push(input[idx]);
            result += input[idx];
            idx += 1;
        }

        let mut evict_index = 0;

        while idx < input.len() {
            let query_result = hammer_slide.query();
            assert_eq!(query_result, result);

            // Evict `WINDOW_SLIDE` elements to simulate the sliding window
            for _ in 0..WINDOW_SLIDE {
                let evicted_value = input[evict_index];
                hammer_slide.pop();
                result -= evicted_value;
                evict_index += 1;
            }

            // Insert `WINDOW_SLIDE` new elements (if available)
            for _ in 0..WINDOW_SLIDE {
                if idx < input.len() {
                    hammer_slide.push(input[idx]);
                    result += input[idx];
                    idx += 1;
                }
            }
        }

        assert_eq!(hammer_slide.query(), result);
    }

    #[test]
    fn test_hammerslide_stream_min() {
        const WINDOW_SIZE: usize = 1000;
        const WINDOW_SLIDE: usize = 100;
        const INPUT_SIZE: usize = 1_000_000;

        let mut hammer_slide: HammerSlide<U32MinAggregator> =
            HammerSlide::with_capacity(WINDOW_SIZE);

        // Generate input data
        let input: Vec<u32> = (1..=INPUT_SIZE as u32).collect();

        let mut result = u32::MAX;

        // Insert the first `WINDOW_SIZE` elements to initialize the sliding window
        let mut idx = 0;
        while idx < WINDOW_SIZE && idx < input.len() {
            hammer_slide.push(input[idx]);
            result = result.min(input[idx]);
            idx += 1;
        }

        let mut evict_index = 0;

        while idx < input.len() {
            let query_result = hammer_slide.query();
            assert_eq!(query_result, result);

            // Evict `WINDOW_SLIDE` elements to simulate the sliding window
            for _ in 0..WINDOW_SLIDE {
                hammer_slide.pop();
                result = evict_index as u32 + 2;
                evict_index += 1;
            }

            // Insert `WINDOW_SLIDE` new elements (if available)
            for _ in 0..WINDOW_SLIDE {
                if idx < input.len() {
                    hammer_slide.push(input[idx]);
                    idx += 1;
                }
            }
        }

        assert_eq!(hammer_slide.query(), result);
    }
}

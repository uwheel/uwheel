use crate::{window::circular_queue::CircularQueue, Aggregator};
use std::{marker::PhantomData, vec::Vec};

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Default)]
pub struct HammerSlide<A: Aggregator> {
    m_window_slide: usize,

    m_capacity: usize,
    m_istack_size: usize,
    m_istack_ptr: isize,
    m_ostack_size: usize,
    m_ostack_ptr: isize,
    m_istack_val: A::PartialAggregate, // should be PartialAggregate

    m_queue: CircularQueue<A>, // should be any possible input type
    m_ostack_val: Vec<A::PartialAggregate>,

    _phantom: PhantomData<(A::Input, A::Aggregate)>,
}

impl<A: Aggregator> HammerSlide<A> {
    pub fn new(window_size: usize, window_slide: usize) -> Self {
        let queue = CircularQueue::new(window_size); // TODO: fix this
        println!("hahaha");
        HammerSlide {
            m_window_slide: window_slide,
            m_capacity: 0,
            m_istack_size: 0,
            m_istack_ptr: -1,
            m_ostack_size: 0,
            m_ostack_ptr: -1,
            m_istack_val: Default::default(),
            m_queue: queue,
            m_ostack_val: vec![Default::default(); window_size],
            _phantom: PhantomData,
        }
    }
}

impl<A: Aggregator> HammerSlide<A> {
    pub fn insert(&mut self, val: A::PartialAggregate) {
        let temp_value = if self.m_istack_size == 0 {
            A::IDENTITY
        } else {
            self.m_istack_val
        };

        self.m_istack_val = A::combine(val, temp_value);

        self.m_queue.enqueue(val); // TODO: fix this

        self.m_istack_ptr = self.m_queue.m_rear as isize;

        self.m_capacity += 1;
        self.m_istack_size += 1;
    }

    pub fn evict(&mut self, number_of_items: usize) {
        self.m_ostack_ptr += number_of_items as isize;
        self.m_ostack_size -= number_of_items;
        self.m_capacity -= number_of_items;

        self.m_queue.dequeue_many(number_of_items);
    }

    pub fn reset(&mut self) {
        self.m_capacity = 0;
        self.m_istack_size = 0;
        self.m_istack_ptr = -1;
        self.m_ostack_size = 0;
        self.m_ostack_ptr = -1;
        self.m_queue.reset(); // Reset the circular queue
    }

    pub fn insert_simple_range(&mut self, vals: &[A::PartialAggregate], start: usize, end: usize) {
        let num_of_vals = end - start;
        let mut temp_value = if self.m_istack_size == 0 {
            A::IDENTITY
        } else {
            self.m_istack_val
        };

        for i in start..end {
            temp_value = A::combine(vals[i], temp_value);
            self.m_queue.enqueue(vals[i]);
        }

        self.m_istack_ptr = self.m_queue.m_rear as isize;
        self.m_capacity += num_of_vals;
        self.m_istack_size += num_of_vals;
        self.m_istack_val = temp_value;
    }

    pub fn insert_many(&mut self, _vals: &[A::PartialAggregate], _start: usize, _end: usize) {
        unimplemented!()
    }

    fn simd_sum(&mut self, _vals: &[A::PartialAggregate], _start: usize, _n: usize) {
        unimplemented!()
    }

    fn simd_min(&mut self, _vals: &[A::PartialAggregate], _start: usize, _n: usize) {
        unimplemented!()
    }
}

impl<A: Aggregator> HammerSlide<A> {
    pub fn query(&mut self, is_simd: bool) -> A::PartialAggregate {
        // If the output stack is empty, attempt to swap
        if self.m_ostack_size == 0 {
            self.swap(is_simd);
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

    pub fn swap(&mut self, is_simd: bool) {
        let mut output_index = 0;
        let mut input_index = self.m_istack_ptr as usize;
        let limit = self.m_istack_size;
        let temp_rear = self.m_queue.m_rear;
        let queue_size = self.m_queue.m_size;

        let mut temp_value = A::IDENTITY;

        if self.m_window_slide < 16 || !is_simd {
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
        } else {
            self.simd_swap(limit);
        }

        self.m_ostack_size = limit;
        self.m_istack_size = 0;

        self.m_ostack_ptr =
            (temp_rear as isize - self.m_ostack_size as isize + 1).rem_euclid(queue_size as isize);

        self.m_istack_ptr = -1;
    }

    fn simd_swap(&mut self, _limit: usize) {
        unimplemented!()
    }
}

// #[cfg(test)]
// mod tests {
//     use std::i32;

//     use super::*;
//     use rand::{distributions::Uniform, Rng};

//     #[derive(Default, Clone)]
//     pub struct SumAggregation;
//     #[derive(Default, Clone)]
//     pub struct MinAggregation;

//     impl Aggregation for SumAggregation {
//         type In = i32;
//         type Agg = i32;
//         type Out = i32;

//         fn identity(&self) -> Self::Agg {
//             0
//         }

//         fn lift(&self, value: Self::In) -> Self::Agg {
//             value
//         }

//         fn combine(&self, agg1: Self::Agg, agg2: Self::Agg) -> Self::Agg {
//             agg1 + agg2
//         }

//         fn lower(&self, agg: Self::Agg) -> Self::Out {
//             agg
//         }
//     }

//     impl Aggregation for MinAggregation {
//         type In = i32;
//         type Agg = i32;
//         type Out = i32;

//         fn identity(&self) -> Self::Agg {
//             i32::MAX
//         }

//         fn lift(&self, value: Self::In) -> Self::Agg {
//             value
//         }

//         fn combine(&self, agg1: Self::Agg, agg2: Self::Agg) -> Self::Agg {
//             agg1.min(agg2)
//         }

//         fn lower(&self, agg: Self::Agg) -> Self::Out {
//             agg
//         }
//     }

//     #[test]
//     fn test_hammer_slide_sum_insert_and_query() {
//         let mut hammer_slide: HammerSlide<SumAggregation, i32, i32, i32> =
//             HammerSlide::new(10, 1, AggregationType::SUM);

//         let mut expected = 0;

//         for val in 1..11 {
//             assert_eq!(hammer_slide.query(false), expected);
//             hammer_slide.insert(val);
//             expected += val;
//         }

//         assert_eq!(hammer_slide.query(false), 55);
//     }

//     #[test]
//     fn test_hammer_slide_min_insert_and_query() {
//         let mut hammer_slide: HammerSlide<MinAggregation, i32, i32, i32> =
//             HammerSlide::new(10, 1, AggregationType::MIN);

//         let mut expected = i32::MAX;

//         for val in 1..11 {
//             assert_eq!(hammer_slide.query(false), expected);
//             hammer_slide.insert(val);
//             expected = expected.min(val);
//         }

//         assert_eq!(hammer_slide.query(false), 1);
//     }

//     #[test]
//     fn test_insert_many_and_query_sum() {
//         let mut hammer_slide: HammerSlide<SumAggregation, i32, i32, i32> =
//             HammerSlide::new(10, 1, AggregationType::SUM);

//         let values = vec![1, 2, 3, 4, 5, 6, 7, 8];
//         hammer_slide.insert_many(&values, 0, values.len());

//         let result = hammer_slide.query(false);
//         assert_eq!(result, 36);
//     }

//     #[test]
//     fn test_insert_many_and_query_min() {
//         let mut hammer_slide: HammerSlide<MinAggregation, i32, i32, i32> =
//             HammerSlide::new(10, 1, AggregationType::MIN);

//         let values = vec![1, 2, 3, 4, 5, 6, 7, 8];
//         hammer_slide.insert_many(&values, 0, values.len());

//         let result = hammer_slide.query(false);
//         assert_eq!(result, 1);
//     }

//     #[test]
//     fn test_evict_and_query_sum() {
//         let mut hammer_slide: HammerSlide<SumAggregation, i32, i32, i32> =
//             HammerSlide::new(10, 1, AggregationType::SUM);

//         let values = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
//         hammer_slide.insert_many(&values, 0, values.len());

//         let mut expected = hammer_slide.query(false);

//         for val in 1..11 {
//             hammer_slide.evict(1);
//             expected -= val;
//             assert_eq!(hammer_slide.query(false), expected);
//         }
//     }

//     #[test]
//     fn test_evict_and_query_min() {
//         let mut hammer_slide: HammerSlide<MinAggregation, i32, i32, i32> =
//             HammerSlide::new(10, 1, AggregationType::MIN);

//         let values = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
//         hammer_slide.insert_many(&values, 0, values.len());

//         assert_eq!(hammer_slide.query(false), 1);

//         for val in 1..10 {
//             hammer_slide.evict(1);
//             assert_eq!(hammer_slide.query(false), val + 1);
//         }

//         hammer_slide.evict(1);
//         assert_eq!(hammer_slide.query(false), i32::MAX);
//     }

//     #[test]
//     fn test_reset() {
//         let mut hammer_slide: HammerSlide<SumAggregation, i32, i32, i32> =
//             HammerSlide::new(10, 1, AggregationType::SUM);

//         hammer_slide.insert(1);
//         hammer_slide.insert(2);
//         hammer_slide.insert(3);
//         hammer_slide.reset();

//         assert_eq!(hammer_slide.m_istack_size, 0);
//         assert_eq!(hammer_slide.m_capacity, 0);
//         assert_eq!(hammer_slide.query(false), 0);
//     }

//     #[test]
//     fn test_hammerslide_random_stream_sum() {
//         const WINDOW_SIZE: usize = 1000;
//         const WINDOW_SLIDE: usize = 100;
//         const INPUT_SIZE: usize = 1_000_000;

//         let mut hammer_slide: HammerSlide<SumAggregation, i32, i32, i32> =
//             HammerSlide::new(WINDOW_SIZE, WINDOW_SLIDE, AggregationType::SUM);

//         // Generate random input data
//         let rng = rand::thread_rng();
//         let input: Vec<i32> = rng
//             .sample_iter(Uniform::new(1, INPUT_SIZE as i32 * 2))
//             .take(INPUT_SIZE)
//             .collect();

//         let mut result = 0;

//         // Insert the first `WINDOW_SIZE` elements to initialize the sliding window
//         let mut idx = 0;
//         while idx < WINDOW_SIZE && idx < input.len() {
//             hammer_slide.insert(input[idx]);
//             result += input[idx];
//             idx += 1;
//         }

//         let mut evict_index = 0;

//         while idx < input.len() {
//             let query_result = hammer_slide.query(false);
//             assert_eq!(query_result, result);

//             // Evict `WINDOW_SLIDE` elements to simulate the sliding window
//             for _ in 0..WINDOW_SLIDE {
//                 let evicted_value = input[evict_index];
//                 hammer_slide.evict(1);
//                 result -= evicted_value;
//                 evict_index += 1;
//             }

//             // Insert `WINDOW_SLIDE` new elements (if available)
//             for _ in 0..WINDOW_SLIDE {
//                 if idx < input.len() {
//                     hammer_slide.insert(input[idx]);
//                     result += input[idx];
//                     idx += 1;
//                 }
//             }
//         }

//         assert_eq!(hammer_slide.query(false), result);
//     }

//     #[test]
//     fn test_hammerslide_stream_min() {
//         const WINDOW_SIZE: usize = 1000;
//         const WINDOW_SLIDE: usize = 100;
//         const INPUT_SIZE: usize = 1_000_000;

//         let mut hammer_slide: HammerSlide<MinAggregation, i32, i32, i32> =
//             HammerSlide::new(WINDOW_SIZE, WINDOW_SLIDE, AggregationType::MIN);

//         // Generate input data
//         let input: Vec<i32> = (1..=INPUT_SIZE as i32).collect();

//         let mut result = i32::MAX;

//         // Insert the first `WINDOW_SIZE` elements to initialize the sliding window
//         let mut idx = 0;
//         while idx < WINDOW_SIZE && idx < input.len() {
//             hammer_slide.insert(input[idx]);
//             result = result.min(input[idx]);
//             idx += 1;
//         }

//         let mut evict_index = 0;

//         while idx < input.len() {
//             let query_result = hammer_slide.query(false);
//             assert_eq!(query_result, result);

//             // Evict `WINDOW_SLIDE` elements to simulate the sliding window
//             for _ in 0..WINDOW_SLIDE {
//                 hammer_slide.evict(1);
//                 result = evict_index as i32 + 2;
//                 evict_index += 1;
//             }

//             // Insert `WINDOW_SLIDE` new elements (if available)
//             for _ in 0..WINDOW_SLIDE {
//                 if idx < input.len() {
//                     hammer_slide.insert(input[idx]);
//                     idx += 1;
//                 }
//             }
//         }

//         assert_eq!(hammer_slide.query(false), result);
//     }
// }

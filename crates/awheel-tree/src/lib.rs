#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use awheel_core::{
    aggregator::Aggregator,
    time,
    Entry,
    Error,
    ReadWheel,
    RwWheel,
    WriteAheadWheel,
};

use core::{hash::Hash, ops::Deref};
use hashbrown::HashMap;

mod read;

pub use read::ReadTreeWheel;

pub trait Key: PartialEq + Ord + Hash + Eq + Send + Sync + Clone + 'static {}
impl<T> Key for T where T: PartialEq + Ord + Hash + Eq + Send + Sync + Clone + 'static {}

/// A Reader-Writer Tree Wheel
///
/// `RwTreeWheel` maintains a Star Wheel (*) that records updates across all keys and is accesible through `Deref<Target = ReadWheel<A>>`
pub struct RwTreeWheel<K: Key, A: Aggregator + Clone> {
    star_wheel: RwWheel<A>,
    write: HashMap<K, WriteAheadWheel<A>>,
    read: ReadTreeWheel<K, A>,
}
impl<K, A> Deref for RwTreeWheel<K, A>
where
    K: Key,
    A: Aggregator + Clone,
{
    type Target = ReadWheel<A>;

    fn deref(&self) -> &ReadWheel<A> {
        self.star_wheel.read()
    }
}

impl<K: Key, A: Aggregator + Clone + 'static> RwTreeWheel<K, A> {
    pub fn new(watermark: u64) -> Self {
        Self {
            star_wheel: RwWheel::new(watermark),
            write: Default::default(),
            read: ReadTreeWheel::new(),
        }
    }
    #[inline]
    pub fn watermark(&self) -> u64 {
        self.star_wheel.watermark()
    }
    pub fn star_wheel(&self) -> &ReadWheel<A> {
        self.star_wheel.read()
    }
    #[inline]
    pub fn insert(&mut self, key: K, entry: Entry<A::Input>) -> Result<(), Error<A::Input>> {
        self.star_wheel.write().insert(entry)?;

        let wheel = self
            .write
            .entry(key)
            .or_insert(WriteAheadWheel::with_watermark(
                self.star_wheel.read().watermark(),
            ));

        wheel.insert(entry)?;

        Ok(())
    }
    /// Returns a reference to the underlying ReadTreeWheel
    ///
    /// This wheel may be cloned and shared across threads
    pub fn read(&self) -> &ReadTreeWheel<K, A> {
        &self.read
    }
    /// Advance the watermark of the Tree Wheel by the given [time::Duration]
    #[inline]
    pub fn advance(&mut self, duration: time::Duration) {
        self.star_wheel.advance(duration);
        self.read.advance(duration, self.write.iter_mut())
    }

    /// Advances the time of the Tree wheel aligned by the lowest unit (Second)
    #[inline]
    pub fn advance_to(&mut self, watermark: u64) {
        let diff = watermark.saturating_sub(self.watermark());
        self.advance(time::Duration::milliseconds(diff as i64));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use awheel_core::{aggregator::U32SumAggregator, time::*};

    #[test]
    fn rw_tree_test() {
        let mut rw_tree: RwTreeWheel<&'static str, U32SumAggregator> = RwTreeWheel::new(0);
        rw_tree.insert("max 1", Entry::new(10, 1000)).unwrap();
        rw_tree.insert("max 2", Entry::new(5, 1000)).unwrap();
        rw_tree.insert("max 3", Entry::new(3, 1000)).unwrap();

        let res = rw_tree
            .read()
            .interval_range(1.seconds(), "max 1"..="max 3");
        assert_eq!(res, None);

        rw_tree.advance(2.seconds());

        let res = rw_tree.read().interval_range(1.seconds(), "max 1".."max 3");
        assert_eq!(res, Some(15u32));

        let res = rw_tree.read().interval_range(1.seconds(), "max 1"..);
        assert_eq!(res, Some(18u32));

        // verify star wheel result
        assert_eq!(rw_tree.interval(1.seconds()), Some(18u32));
    }
    #[test]
    fn rw_tree_test_across_thread() {
        let mut rw_tree: RwTreeWheel<&'static str, U32SumAggregator> = RwTreeWheel::new(0);
        rw_tree.insert("max 1", Entry::new(10, 1000)).unwrap();
        rw_tree.insert("max 2", Entry::new(5, 1000)).unwrap();
        rw_tree.insert("max 3", Entry::new(3, 1000)).unwrap();

        rw_tree.advance(2.seconds());

        let read_wheel = rw_tree.read().clone();

        let handle = std::thread::spawn(move || {
            assert_eq!(
                read_wheel.get("max 1").unwrap().interval(1.seconds()),
                Some(10u32)
            );
            assert_eq!(
                read_wheel.interval_range(1.seconds(), "max 1".."max 3"),
                Some(15u32)
            );
        });

        handle.join().expect("Failed to join the thread.");
    }
}

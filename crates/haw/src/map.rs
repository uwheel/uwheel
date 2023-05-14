use core::{borrow::Borrow, hash::Hash, ops::Deref};

use hashbrown::HashMap;

use crate::{aggregator::Aggregator, Entry, Error, Wheel};

/// A Map with a `Wheel` per key
///
/// `WheelMap` maintains a Star Wheel (*) that records updates across all keys and is accesible through `Deref<Target = Wheel>`
pub struct WheelMap<K, A>
where
    K: Hash + Eq,
    A: Aggregator,
{
    star_wheel: Wheel<A>,
    map: HashMap<K, Wheel<A>>,
}

impl<K, A> Deref for WheelMap<K, A>
where
    K: Hash + Eq,
    A: Aggregator,
{
    type Target = Wheel<A>;

    fn deref(&self) -> &Wheel<A> {
        &self.star_wheel
    }
}

impl<K, A> WheelMap<K, A>
where
    K: Hash + Eq,
    A: Aggregator,
{
    pub fn new(time: u64) -> Self {
        Self {
            star_wheel: Wheel::new(time),
            map: Default::default(),
        }
    }
    #[inline]
    pub fn insert(&mut self, key: K, entry: Entry<A::Input>) -> Result<(), Error<A::Input>> {
        self.star_wheel.insert(entry)?;

        let wheel = self
            .map
            .entry(key)
            .or_insert(Wheel::new(self.star_wheel.watermark()));

        wheel.insert(entry)?;

        Ok(())
    }
    #[inline]
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&Wheel<A>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.map.get(key)
    }
    pub fn advance_to(&mut self, time: u64) {
        self.star_wheel.advance_to(time);
        for wheel in self.map.values_mut() {
            wheel.advance_to(time);
        }
    }
    pub fn watermark(&self) -> u64 {
        self.star_wheel.watermark()
    }
}

#[cfg(test)]
mod tests {
    use crate::aggregator::U64SumAggregator;

    use super::*;

    #[test]
    fn basic_map_test() {
        let mut map: WheelMap<String, U64SumAggregator> = WheelMap::new(0);

        assert!(map.insert("foo".to_string(), Entry::new(100, 1500)).is_ok());
        assert!(map.insert("bar".to_string(), Entry::new(50, 1600)).is_ok());

        assert!(map.landmark().is_none());
        assert!(map
            .get("foo")
            .unwrap()
            .seconds_unchecked()
            .interval(1)
            .is_none());
        assert!(map
            .get("bar")
            .unwrap()
            .seconds_unchecked()
            .interval(1)
            .is_none());

        map.advance_to(2000);

        assert_eq!(map.landmark(), Some(150));

        assert_eq!(map.get("foo").unwrap().landmark(), Some(100));
        assert_eq!(map.get("bar").unwrap().landmark(), Some(50));

        assert_eq!(
            map.get("foo").unwrap().seconds_unchecked().interval(1),
            Some(100)
        );
        assert_eq!(
            map.get("bar").unwrap().seconds_unchecked().interval(1),
            Some(50)
        );
    }
}

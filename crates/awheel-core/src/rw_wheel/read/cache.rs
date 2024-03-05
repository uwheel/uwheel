use crate::{cfg_not_sync, cfg_sync};
use schnellru::{ByLength, LruMap};

use super::hierarchical::WheelRange;

cfg_not_sync! {
    use core::cell::RefCell;
    #[doc(hidden)]
    pub struct WheelCache<A: Copy> {
        lru: RefCell<LruMap<WheelRange, A>>
    }
    impl<A: Copy> WheelCache<A> {
        pub fn with_capacity(capacity: u32) -> Self {
            Self {
                lru: RefCell::new(LruMap::new(ByLength::new(capacity))),
            }
        }
        pub fn get(&self, range: &WheelRange) -> Option<A> {
            self.lru.borrow_mut().get(range).copied()
        }
    }
}

cfg_sync! {
    use parking_lot::RwLock;
    use std::sync::Arc;
    #[doc(hidden)]
    pub struct WheelCache<A: Copy> {
        lru: Arc<RwLock<LruMap<WheelRange, A>>>
   }
    impl<A: Copy> WheelCache<A> {
        pub fn with_capacity(capacity: u32) -> Self {
            Self {
                lru: Arc::new(RwLock::new(LruMap::new(ByLength::new(capacity)))),
            }
        }

        pub fn get(&self, range: &WheelRange) -> Option<A> {
            self.lru.read().get(range).copied()
        }
    }
}

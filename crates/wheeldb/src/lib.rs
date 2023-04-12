use haw::aggregator::Aggregator;
use parking_lot::RwLock;
use std::sync::Arc;

pub mod db;

pub trait Storage {
    fn insert(&self);
    fn advance(&self, time: u64);
    fn watermark(&self) -> u64;
    fn interval(&self, key: impl AsRef<[u8]>, time: u64);
}

#[derive(Clone)]
pub struct Wheel<A: Aggregator>(pub(crate) Arc<WheelInner<A>>);

impl<A: Aggregator> Wheel<A> {
    pub fn insert(&self, _key: impl AsRef<[u8]>, _entry: haw::Entry<A::Input>) {
        //let s = self.0._star_wheel.read().as_bytes();
        //self.0.star_wheel.write().insert(entry);
    }
}

pub struct WheelInner<A: Aggregator> {
    //id: Vec<u8>,
    _star_wheel: RwLock<haw::Wheel<A>>,
    //aggregator: Box<dyn Aggregator>,
    _aggregator: A,
}
//unsafe impl Send for Wheel {}
//unsafe impl Sync for Wheel {}

// This code is based on the concurrent_map implementation and has been modified

use awheel_core::{delta::DeltaState, Aggregator, ReadWheel};
use stack_map::StackMap;
use std::{
    borrow::Borrow,
    ops::{Bound, Deref, RangeBounds},
    ptr::NonNull,
};

type NodePtr<K, A, const FANOUT: usize> = NonNull<Node<K, A, FANOUT>>;

pub trait Minimum: Ord {
    const MIN: Self;
}
impl Minimum for u64 {
    const MIN: Self = Self::MIN;
}

pub struct Tree<K, A, const FANOUT: usize = 64>
where
    K: Ord + Minimum + Clone,
    A: Aggregator,
{
    // root: NonNull<Node<K, A, FANOUT>>,
    inner: Inner<K, A, FANOUT>,
}

impl<K, A, const FANOUT: usize> Default for Tree<K, A, FANOUT>
where
    K: Ord + Minimum + Clone,
    A: Aggregator,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, A, const FANOUT: usize> Tree<K, A, FANOUT>
where
    K: Ord + Minimum + Clone,
    A: Aggregator,
{
    pub fn new() -> Self {
        assert!(FANOUT > 3, "FANOUT must be greater than 3");

        let mut root_node: Box<Node<K, A, FANOUT>> = Node::new_root();
        let root_lo = K::MIN;
        let leaf_node = Node::<K, A, FANOUT>::new_leaf(root_lo.clone());
        let leaf = NonNull::new(Box::into_raw(leaf_node)).unwrap();

        root_node
            .index_mut()
            .insert(root_lo, (None, NodeView::new(leaf)));
        let root = NonNull::new(Box::into_raw(root_node)).unwrap();
        let inner = Inner {
            root: NodeView::new(root),
        };
        Self { inner }
    }
    #[inline]
    pub fn insert(&mut self, key: K, wheel: ReadWheel<A>) {
        let mut leaf = self.inner.leaf_for_key(LeafSearch::Eq(&key));
        let node_mut_ref: &mut Node<K, A, FANOUT> = unsafe { leaf.get_mut() };
        node_mut_ref.insert(key, wheel);
        assert!(!node_mut_ref.should_split(), "bad leaf: should split");

        if node_mut_ref.should_split() {
            todo!("should split");
        }
    }

    pub fn get<Q>(&self, key: &Q) -> Option<&ReadWheel<A>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        unimplemented!();
    }

    pub fn merge_delta(&mut self, key: K, delta: DeltaState<A::PartialAggregate>) {}
    pub fn range<Q, R>(&self, range: R) -> Iter<'_, K, A, FANOUT, R, Q>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        let kmin = &K::MIN;
        let min = kmin.borrow();
        let start = match range.start_bound() {
            Bound::Unbounded => min,
            Bound::Included(k) | Bound::Excluded(k) => k,
        };

        let end = match range.end_bound() {
            Bound::Unbounded => LeafSearch::Max,
            Bound::Included(k) => LeafSearch::Eq(k),
            Bound::Excluded(k) => {
                assert!(k != K::MIN.borrow());
                LeafSearch::Lt(k)
            }
        };

        let current = self.inner.leaf_for_key(LeafSearch::Eq(start));
        let current_back = self.inner.leaf_for_key(end);

        unimplemented!();
    }
}

#[derive(Debug, Copy, Clone)]
#[repr(u8)]
pub enum Level {
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Year,
}

struct Inner<K, A, const FANOUT: usize>
where
    K: Ord + Minimum + Clone,
    A: Aggregator,
{
    root: NodeView<K, A, FANOUT>,
}

impl<K, A, const FANOUT: usize> Inner<K, A, FANOUT>
where
    K: Ord + Minimum + Clone,
    A: Aggregator,
{
    // locate leaf node for the key
    fn leaf_for_key<'a, Q>(&'a self, search: LeafSearch<&Q>) -> NodeView<K, A, FANOUT>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        // let mut parent_cursor_opt: Option<NodeView<K, V, FANOUT>> = None;
        let mut cursor = &self.root;
        let mut root_cursor = NodeView::new(cursor.ptr);
        loop {
            match search {
                LeafSearch::Eq(k) | LeafSearch::Lt(k) => assert!(k >= cursor.lo.borrow()),
                LeafSearch::Max => {}
            }

            if let Some(hi) = &cursor.hi {
                let go_right = match search {
                    LeafSearch::Eq(k) => k >= hi.borrow(),
                    // Lt looks for a node with lo < K, hi >= K
                    LeafSearch::Lt(k) => k > hi.borrow(),
                    LeafSearch::Max => true,
                };
                if go_right {
                    // go right to the tree sibling
                    // let next = &cursor.next;
                    let rhs = cursor.next.as_ref().unwrap();
                    // if let Some(ref mut parent_cursor) = parent_cursor_opt {
                    // } else {
                    // }
                }
            }
        }
        unimplemented!();
    }
}

enum LeafSearch<K> {
    // For finding a leaf that would contain this key, if present.
    Eq(K),
    // For finding the direct left sibling of a node during reverse
    // iteration. The actual semantic is to find a leaf that has a lo key
    // that is less than K and a hi key that is >= K
    Lt(K),
    Max,
}

#[derive(Debug)]
struct NodeView<K, A, const FANOUT: usize>
where
    K: Clone + Minimum + Ord,
    A: Aggregator,
{
    ptr: NonNull<Node<K, A, FANOUT>>,
}

impl<K, A, const FANOUT: usize> NodeView<K, A, FANOUT>
where
    K: Clone + Minimum + Ord,
    A: Aggregator,
{
    pub fn new(ptr: NonNull<Node<K, A, FANOUT>>) -> Self {
        Self { ptr }
    }
    unsafe fn get_mut(&mut self) -> &mut Node<K, A, FANOUT> {
        self.ptr.as_mut()
    }
}

impl<K, A, const FANOUT: usize> Deref for NodeView<K, A, FANOUT>
where
    K: Clone + Minimum + Ord,
    A: Aggregator,
{
    type Target = Node<K, A, FANOUT>;

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref() }
    }
}

pub struct Iter<'a, K, A, const FANOUT: usize, R = std::ops::RangeFull, Q = K>
where
    K: Clone + Minimum + Ord,
    A: Aggregator,
    R: std::ops::RangeBounds<Q>,
    K: Borrow<Q>,
    Q: ?Sized,
{
    inner: &'a Inner<K, A, FANOUT>,
    range: R,
    current: NodeView<K, A, FANOUT>,
    next_index: usize,
    current_back: NodeView<K, A, FANOUT>,
    next_index_from_back: usize,
    q: std::marker::PhantomData<&'a Q>,
}

impl<'a, K, A, const FANOUT: usize, R, Q> Iterator for Iter<'a, K, A, FANOUT, R, Q>
where
    K: Clone + Minimum + Ord,
    A: Aggregator,
    R: std::ops::RangeBounds<Q>,
    K: Borrow<Q>,
    Q: ?Sized + PartialEq + Ord,
{
    type Item = (K, ReadWheel<A>);

    fn next(&mut self) -> Option<Self::Item> {
        // loop {
        //     if let Some((k, v)) = self.current.leaf().get_index(self.next_index) {
        //         // iterate over current cached b+ tree leaf node
        //         self.next_index += 1;
        //         if !self.range.contains(k.borrow()) {
        //             // we might hit this on the first iteration
        //             continue;
        //         }
        //         return Some((k.clone(), v.clone()));
        //     } else if let Some(next_ptr) = self.current.next {
        //         if !self
        //             .range
        //             .contains(self.current.hi.as_ref().unwrap().borrow())
        //         {
        //             // we have reached the end of our range
        //             return None;
        //         }
        //         if let Some(next_current) = next_ptr.node_view(&mut self.guard) {
        //             // we were able to take the fast path by following the sibling pointer

        //             // it's possible that nodes were merged etc... so we need to make sure
        //             // that we make forward progress
        //             self.next_index = next_current
        //                 .leaf()
        //                 .iter()
        //                 .position(|(k, _v)| k >= self.current.hi.as_ref().unwrap())
        //                 .unwrap_or(0);

        //             self.current = next_current;
        //         } else if let Some(ref hi) = self.current.hi {
        //             // we have to take the slow path by traversing the
        //             // map due to a concurrent merge that deleted the
        //             // right sibling. we are protected from a use after
        //             // free of the ID itself due to holding an ebr Guard
        //             // on the Iter struct, holding a barrier against re-use.
        //             let next_current = self
        //                 .inner
        //                 .leaf_for_key(LeafSearch::Eq(hi.borrow()), &mut self.guard);

        //             // it's possible that nodes were merged etc... so we need to make sure
        //             // that we make forward progress
        //             self.next_index = next_current
        //                 .leaf()
        //                 .iter()
        //                 .position(|(k, _v)| k >= hi)
        //                 .unwrap_or(0);
        //             self.current = next_current;
        //         } else {
        //             panic!("somehow hit a node that has a next but not a hi key");
        //         }
        //     } else {
        //         // end of the collection
        //         return None;
        //     }
        // }
        unimplemented!();
    }
}

pub enum Data<K, A, const FANOUT: usize = 64>
where
    K: Ord + Minimum + Clone,
    A: Aggregator,
{
    /// Leaf Node containing a WheelMap
    Leaf(WheelMap<K, A, FANOUT>),
    Index(IndexMap<K, A, FANOUT>),
}

impl<K, A, const FANOUT: usize> Data<K, A, FANOUT>
where
    K: Ord + Minimum + Clone,
    A: Aggregator,
{
    const fn len(&self) -> usize {
        match self {
            Data::Leaf(ref leaf) => leaf.len(),
            Data::Index(ref index) => index.len(),
        }
    }
}

pub struct Node<K, A, const FANOUT: usize = 64>
where
    K: Ord + Minimum + Clone,
    A: Aggregator,
{
    data: Data<K, A, FANOUT>,
    lo: K,
    hi: Option<K>,
    next: Option<NodeView<K, A, FANOUT>>,
}

impl<K, A, const FANOUT: usize> Node<K, A, FANOUT>
where
    K: Ord + Minimum + Clone,
    A: Aggregator,
{
    fn new_root() -> Box<Self> {
        Box::new(Self {
            lo: K::MIN,
            hi: None,
            data: Data::Index(IndexMap::new()),
            next: None,
        })
    }

    fn new_leaf(lo: K) -> Box<Self> {
        Box::new(Self {
            lo,
            hi: None,
            data: Data::Leaf(WheelMap::new()),
            next: None,
        })
    }
    const fn leaf(&self) -> &WheelMap<K, A, FANOUT> {
        if let Data::Leaf(ref leaf) = self.data {
            leaf
        } else {
            unreachable!()
        }
    }
    const fn leaf_mut(&mut self) -> &mut WheelMap<K, A, FANOUT> {
        if let Data::Leaf(ref mut leaf) = self.data {
            leaf
        } else {
            unreachable!()
        }
    }

    const fn index_mut(&mut self) -> &mut IndexMap<K, A, FANOUT> {
        if let Data::Index(ref mut index) = self.data {
            index
        } else {
            unreachable!()
        }
    }
    const fn should_split(&self) -> bool {
        // if self.merging_child.is_some() || self.is_merging {
        // return false;
        // }
        // self.len() > FANOUT - MERGE_SIZE
        false
    }

    fn insert(&mut self, key: K, value: ReadWheel<A>) {
        // assert!(!self.is_merging);
        // assert!(self.merging_child.is_none());
        // assert!(!self.should_split());
        self.leaf_mut().insert(key, value);
    }

    const fn is_leaf(&self) -> bool {
        matches!(&self.data, Data::Leaf(_))
    }
    const fn is_index(&self) -> bool {
        matches!(&self.data, Data::Index(_))
    }

    fn get<Q>(&self, key: &Q) -> Option<&ReadWheel<A>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        // assert!(!self.is_merging);
        // assert!(self.merging_child.is_none());
        assert!(self.is_leaf());

        self.leaf().get(key)
    }
}

/// Inner Index map with each key pointing to wheel + leaf node
#[derive(Default)]
pub struct IndexMap<K, A, const FANOUT: usize = 64>
where
    K: Ord + Minimum + Clone,
    A: Aggregator,
{
    map: StackMap<K, (Option<ReadWheel<A>>, NodeView<K, A, FANOUT>), FANOUT>,
}

impl<K, A, const FANOUT: usize> IndexMap<K, A, FANOUT>
where
    K: Ord + Minimum + Clone,
    A: Aggregator,
{
    pub fn new() -> Self {
        Self {
            map: StackMap::default(),
        }
    }
    pub fn insert(&mut self, key: K, entry: (Option<ReadWheel<A>>, NodeView<K, A, FANOUT>)) {
        self.map.insert(key, entry);
    }
    #[inline]
    pub const fn len(&self) -> usize {
        self.map.len()
    }
}

/// Fixed-sized map containing Hierarchical Aggregation Wheels per Key
///
/// Can be queried both horizontally (keys) and vertically (time)
#[derive(Default)]
pub struct WheelMap<K, A, const FANOUT: usize = 64>
where
    K: Ord + Minimum + Clone,
    A: Aggregator,
{
    wheels: StackMap<K, ReadWheel<A>, FANOUT>,
}

impl<K, A, const FANOUT: usize> WheelMap<K, A, FANOUT>
where
    K: Ord + Minimum + Clone,
    A: Aggregator,
{
    pub fn new() -> Self {
        Self {
            wheels: StackMap::default(),
        }
    }
    #[inline]
    pub const fn len(&self) -> usize {
        self.wheels.len()
    }
    fn get<Q>(&self, key: &Q) -> Option<&ReadWheel<A>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.wheels.get(key)
    }
    pub fn merge_delta(&mut self, key: K, delta: DeltaState<A::PartialAggregate>) {
        match self.wheels.get(&key) {
            Some(wheel) => wheel.delta_advance(delta.deltas),
            None => {
                let wheel = ReadWheel::from_delta_state(delta);
                self.insert(key, wheel);
            }
        }
    }
    pub fn insert(&mut self, key: K, wheel: ReadWheel<A>) {
        self.wheels.insert(key, wheel);
    }
}

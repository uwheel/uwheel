use core::cmp::Ordering;

/// Defines an ascending order of TopN aggregates
#[derive(Clone, Debug, Copy, Default)]
pub struct Ascending;

/// Defines a descending order of TopN aggregates
#[derive(Clone, Debug, Copy, Default)]
pub struct Descending;

/// A TopN Order type supporting either [Ascending] or [Descending] variants
pub trait Order: private::Sealed {
    #[doc(hidden)]
    fn ordering() -> Ordering;
}

impl Order for Ascending {
    fn ordering() -> Ordering {
        Ordering::Less
    }
}

impl Order for Descending {
    fn ordering() -> Ordering {
        Ordering::Greater
    }
}
/// Sealed traits
mod private {
    use core::fmt::Debug;

    pub trait Sealed: Clone + Copy + Debug + Default + Send + 'static {}
}

impl private::Sealed for Ascending {}
impl private::Sealed for Descending {}

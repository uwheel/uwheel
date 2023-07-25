use core::cmp::Ordering;

#[derive(Clone, Debug, Copy, Default)]
pub struct Ascending;

#[derive(Clone, Debug, Copy, Default)]
pub struct Descending;

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

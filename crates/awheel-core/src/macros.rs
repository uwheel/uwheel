macro_rules! cfg_rkyv {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "rkyv")]
            $item
        )*
    }
}

macro_rules! assert_capacity {
    ($capacity:tt) => {
        assert!($capacity != 0, "Capacity is not allowed to be zero");
        assert!(
            $capacity.is_power_of_two(),
            "Capacity must be a power of two"
        );
    };
}

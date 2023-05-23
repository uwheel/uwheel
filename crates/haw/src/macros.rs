macro_rules! cfg_rkyv {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "rkyv")]
            $item
        )*
    }
}

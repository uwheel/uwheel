macro_rules! cfg_drill_down {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "drill_down")]
            $item
        )*
    }
}

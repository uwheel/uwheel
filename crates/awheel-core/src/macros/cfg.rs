#[macro_export]
macro_rules! cfg_timer {
    ($($item:item)*) => {
        $(
            #[cfg(all(feature = "timer", not(feature = "serde")))]
            $item
        )*
    }
}

#[macro_export]
macro_rules! cfg_sync {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "sync")]
            $item
        )*
    }
}
#[macro_export]
macro_rules! cfg_not_sync {
    ($($item:item)*) => {
        $(
            #[cfg(not(feature = "sync"))]
            $item
        )*
    }
}

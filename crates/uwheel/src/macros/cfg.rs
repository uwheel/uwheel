#[macro_export]
#[doc(hidden)]
macro_rules! cfg_timer {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "timer")]
            $item
        )*
    }
}

#[macro_export]
#[doc(hidden)]
macro_rules! cfg_serde {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "serde")]
            $item
        )*
    }
}

#[macro_export]
#[doc(hidden)]
macro_rules! cfg_sync {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "sync")]
            $item
        )*
    }
}

#[macro_export]
#[doc(hidden)]
macro_rules! cfg_not_sync {
    ($($item:item)*) => {
        $(
            #[cfg(not(feature = "sync"))]
            $item
        )*
    }
}

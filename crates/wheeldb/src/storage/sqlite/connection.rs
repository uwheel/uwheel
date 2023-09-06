use core::{ffi::c_int, marker::PhantomData};

/// A database connection.
#[allow(dead_code)]
pub struct Connection {
    raw: Raw,
    phantom: PhantomData<ffi::sqlite3>,
}
use alloc::string::String;

/// Flags for opening a database connection.
#[derive(Clone, Copy, Debug)]
pub struct OpenFlags(c_int);

struct Raw(*mut ffi::sqlite3);

impl Connection {
    /// Open a read-write connection to a new or existing database.
    pub fn open(path: impl Into<String>) -> Connection {
        Connection::open_with_flags(path, OpenFlags::new().set_create().set_read_write())
    }

    /// Open a connection with specific flags.
    pub fn open_with_flags(path: impl Into<String>, flags: OpenFlags) -> Connection {
        let path = path.into();
        let utf8_bytes = path.into_bytes();

        // Get a pointer to the first element of the Vec<u8> as a *const i8
        use core::ffi::c_char;
        let c_string_ptr = utf8_bytes.as_ptr() as *const c_char;
        // let cstr = core::ffi::CStr::
        let mut raw = core::ptr::null_mut();
        unsafe {
            let _code = ffi::sqlite3_open_v2(c_string_ptr, &mut raw, flags.0, core::ptr::null());
        }
        Connection {
            raw: Raw(raw),
            phantom: PhantomData,
        }
    }
}

impl OpenFlags {
    /// Create flags for opening a database connection.
    #[inline]
    pub fn new() -> Self {
        OpenFlags(0)
    }

    /// Create the database if it does not already exist.
    pub fn set_create(mut self) -> Self {
        self.0 |= ffi::SQLITE_OPEN_CREATE;
        self
    }

    // /// Open the database in the serialized [threading mode][1].
    // ///
    // /// [1]: https://www.sqlite.org/threadsafe.html
    // pub fn set_full_mutex(mut self) -> Self {
    //     self.0 |= ffi::SQLITE_OPEN_FULLMUTEX;
    //     self
    // }

    // /// Opens the database in the multi-thread [threading mode][1].
    // ///
    // /// [1]: https://www.sqlite.org/threadsafe.html
    // pub fn set_no_mutex(mut self) -> Self {
    //     self.0 |= ffi::SQLITE_OPEN_NOMUTEX;
    //     self
    // }

    // /// Open the database for reading only.
    // pub fn set_read_only(mut self) -> Self {
    //     self.0 |= ffi::SQLITE_OPEN_READONLY;
    //     self
    // }

    /// Open the database for reading and writing.
    pub fn set_read_write(mut self) -> Self {
        self.0 |= ffi::SQLITE_OPEN_READWRITE;
        self
    }
}

impl Default for OpenFlags {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

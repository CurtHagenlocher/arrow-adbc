// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Storage providers through the C ABI (`AdbcFileSystemProvider`).
//!
//! PROTOTYPE (file-driver proposal).
//!
//! - A provider library implements [FileSystemProvider] and exports it with
//!   [export_file_system_provider!](crate::export_file_system_provider).
//! - A driver manager loads the library, calls its entrypoint through
//!   [import_provider], and uses the result as a [FileSystemProvider].

use std::ffi::{CStr, CString, c_char, c_int};
use std::os::raw::c_void;
use std::ptr::null;
use std::sync::{Arc, Mutex, RwLock};

use adbc_core::constants::ADBC_STATUS_OK;
use adbc_core::error::{AdbcStatusCode, Error, Result, Status};
use adbc_core::filesystem::{FileSystem, FileSystemProvider};

use super::{FFI_AdbcFileSystem, check, export_file_system, import_file_system, set_error};
use crate::FFI_AdbcError;

/// The provider table version of this crate.
pub const ADBC_FILESYSTEM_PROVIDER_VERSION_1: c_int = 1;

/// `struct AdbcFileSystemProvider`.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_AdbcFileSystemProvider {
    pub struct_size: u32,
    pub version: u32,
    pub private_data: *mut c_void,
    pub set_option: Option<
        unsafe extern "C" fn(
            *mut Self,
            *const c_char,
            *const c_char,
            *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub open: Option<
        unsafe extern "C" fn(
            *mut Self,
            *const c_char,
            *const c_char,
            *mut FFI_AdbcFileSystem,
            *const c_char,
            *mut FFI_AdbcFileSystem,
            *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub release: Option<unsafe extern "C" fn(*mut Self)>,
}

impl Default for FFI_AdbcFileSystemProvider {
    fn default() -> Self {
        Self {
            struct_size: 0,
            version: 0,
            private_data: std::ptr::null_mut(),
            set_option: None,
            open: None,
            release: None,
        }
    }
}

/// `AdbcFileSystemProviderInitFunc`: a provider library's entrypoint.
pub type FFI_AdbcFileSystemProviderInitFunc = unsafe extern "C" fn(
    c_int,
    *mut FFI_AdbcFileSystemProvider,
    *mut FFI_AdbcError,
) -> AdbcStatusCode;

// ---------------------------------------------------------------------------
// Export: a Rust provider as a C table

/// Export a provider into `out`. The body of a provider library's
/// entrypoint; see [export_file_system_provider!](crate::export_file_system_provider).
///
/// # Safety
///
/// `out` must point to writable memory for the table; `error` may be null.
pub unsafe fn init_provider<P: FileSystemProvider + 'static>(
    version: c_int,
    provider: P,
    out: *mut FFI_AdbcFileSystemProvider,
    error: *mut FFI_AdbcError,
) -> AdbcStatusCode {
    unsafe {
        if version != ADBC_FILESYSTEM_PROVIDER_VERSION_1 {
            return set_error(
                error,
                Error::with_message_and_status(
                    format!("unsupported provider table version {version}"),
                    Status::NotImplemented,
                ),
            );
        }
        if out.is_null() {
            return set_error(
                error,
                Error::with_message_and_status("null provider table", Status::InvalidArguments),
            );
        }
        let private: Box<RwLock<Box<dyn FileSystemProvider>>> =
            Box::new(RwLock::new(Box::new(provider)));
        *out = FFI_AdbcFileSystemProvider {
            struct_size: size_of::<FFI_AdbcFileSystemProvider>() as u32,
            version: ADBC_FILESYSTEM_PROVIDER_VERSION_1 as u32,
            private_data: Box::into_raw(private) as *mut c_void,
            set_option: Some(exported_set_option),
            open: Some(exported_open),
            release: Some(exported_release),
        };
        ADBC_STATUS_OK
    }
}

/// Define a provider library's entrypoint, exporting a
/// [FileSystemProvider] made with `Default`.
///
/// ```ignore
/// adbc_ffi::export_file_system_provider!(AdbcFileSystemProviderInit, MyProvider);
/// ```
#[macro_export]
macro_rules! export_file_system_provider {
    ($init:ident, $provider:ty) => {
        /// # Safety
        ///
        /// Called by a driver manager, per `AdbcFileSystemProviderInitFunc`.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $init(
            version: ::std::ffi::c_int,
            out: *mut $crate::filesystem::provider::FFI_AdbcFileSystemProvider,
            error: *mut $crate::FFI_AdbcError,
        ) -> ::adbc_core::error::AdbcStatusCode {
            unsafe {
                $crate::filesystem::provider::init_provider(
                    version,
                    <$provider as ::std::default::Default>::default(),
                    out,
                    error,
                )
            }
        }
    };
}

type ExportedProvider = RwLock<Box<dyn FileSystemProvider>>;

unsafe fn exported<'a>(provider: *mut FFI_AdbcFileSystemProvider) -> &'a ExportedProvider {
    unsafe { &*((*provider).private_data as *const ExportedProvider) }
}

unsafe fn c_str<'a>(s: *const c_char, what: &str) -> Result<&'a str> {
    if s.is_null() {
        return Err(Error::with_message_and_status(
            format!("null {what}"),
            Status::InvalidArguments,
        ));
    }
    unsafe { CStr::from_ptr(s) }.to_str().map_err(|_| {
        Error::with_message_and_status(format!("{what} is not UTF-8"), Status::InvalidArguments)
    })
}

unsafe extern "C" fn exported_set_option(
    provider: *mut FFI_AdbcFileSystemProvider,
    key: *const c_char,
    value: *const c_char,
    error: *mut FFI_AdbcError,
) -> AdbcStatusCode {
    unsafe {
        let result = (|| {
            let key = c_str(key, "option key")?;
            let value = c_str(value, "option value")?;
            exported(provider).write().unwrap().set_option(key, value)
        })();
        match result {
            Ok(()) => ADBC_STATUS_OK,
            Err(e) => set_error(error, e),
        }
    }
}

unsafe extern "C" fn exported_open(
    provider: *mut FFI_AdbcFileSystemProvider,
    scheme: *const c_char,
    authority: *const c_char,
    parent: *mut FFI_AdbcFileSystem,
    parent_name: *const c_char,
    out: *mut FFI_AdbcFileSystem,
    error: *mut FFI_AdbcError,
) -> AdbcStatusCode {
    unsafe {
        let result = (|| {
            // Take the parent first: it is ours even if open fails.
            let parent = if parent.is_null() || (*parent).release.is_none() {
                None
            } else {
                let fs = import_file_system(parent)?;
                Some((fs, c_str(parent_name, "parent name")?.to_string()))
            };
            let scheme = c_str(scheme, "scheme")?;
            let authority = if authority.is_null() {
                ""
            } else {
                c_str(authority, "authority")?
            };
            exported(provider)
                .read()
                .unwrap()
                .open(scheme, authority, parent)
        })();
        match result {
            Ok(fs) => {
                *out = export_file_system(fs);
                ADBC_STATUS_OK
            }
            Err(e) => set_error(error, e),
        }
    }
}

unsafe extern "C" fn exported_release(provider: *mut FFI_AdbcFileSystemProvider) {
    unsafe {
        if provider.is_null() || (*provider).private_data.is_null() {
            return;
        }
        drop(Box::from_raw(
            (*provider).private_data as *mut ExportedProvider,
        ));
        (*provider).private_data = std::ptr::null_mut();
        (*provider).release = None;
    }
}

// ---------------------------------------------------------------------------
// Import: a C provider as a Rust one

/// A provider loaded through the C ABI. The library that defines it must
/// stay loaded for as long as this and every file system it opened live.
pub struct ImportedProvider {
    raw: Mutex<FFI_AdbcFileSystemProvider>,
}

unsafe impl Send for ImportedProvider {}
unsafe impl Sync for ImportedProvider {}

impl Drop for ImportedProvider {
    fn drop(&mut self) {
        let raw = self.raw.get_mut().unwrap();
        if let Some(release) = raw.release {
            unsafe { release(raw) };
        }
    }
}

/// Call a provider library's entrypoint.
///
/// # Safety
///
/// `init` must be a provider entrypoint per `AdbcFileSystemProviderInitFunc`.
pub unsafe fn import_provider(
    init: FFI_AdbcFileSystemProviderInitFunc,
) -> Result<ImportedProvider> {
    let mut raw = FFI_AdbcFileSystemProvider::default();
    let mut error = FFI_AdbcError::default();
    unsafe {
        let status = init(ADBC_FILESYSTEM_PROVIDER_VERSION_1, &mut raw, &mut error);
        check(status, &error)?;
    }
    let provider = ImportedProvider {
        raw: Mutex::new(raw),
    };
    {
        let raw = provider.raw.lock().unwrap();
        if raw.version != ADBC_FILESYSTEM_PROVIDER_VERSION_1 as u32
            || (raw.struct_size as usize) < size_of::<FFI_AdbcFileSystemProvider>()
            || raw.open.is_none()
        {
            return Err(Error::with_message_and_status(
                format!(
                    "unsupported provider table (version {}, struct_size {})",
                    raw.version, raw.struct_size
                ),
                Status::NotImplemented,
            ));
        }
    }
    Ok(provider)
}

fn c_string(s: &str, what: &str) -> Result<CString> {
    CString::new(s).map_err(|_| {
        Error::with_message_and_status(
            format!("{what} contains a NUL byte"),
            Status::InvalidArguments,
        )
    })
}

impl FileSystemProvider for ImportedProvider {
    fn set_option(&mut self, key: &str, value: &str) -> Result<()> {
        let raw = self.raw.get_mut().unwrap();
        let Some(set_option) = raw.set_option else {
            return Err(Error::with_message_and_status(
                format!("the provider takes no options (got '{key}')"),
                Status::NotImplemented,
            ));
        };
        let (key, value) = (c_string(key, "key")?, c_string(value, "value")?);
        let mut error = FFI_AdbcError::default();
        unsafe {
            let status = set_option(raw, key.as_ptr(), value.as_ptr(), &mut error);
            check(status, &error)
        }
    }

    fn open(
        &self,
        scheme: &str,
        authority: &str,
        parent: Option<(Arc<dyn FileSystem>, String)>,
    ) -> Result<Arc<dyn FileSystem>> {
        let scheme = c_string(scheme, "scheme")?;
        let authority = c_string(authority, "authority")?;
        let (mut parent_raw, parent_name) = match parent {
            Some((fs, name)) => (Some(export_file_system(fs)), Some(c_string(&name, "name")?)),
            None => (None, None),
        };
        let mut out = FFI_AdbcFileSystem::default();
        let mut error = FFI_AdbcError::default();
        let mut raw = self.raw.lock().unwrap();
        let open = raw.open.expect("checked at import");
        unsafe {
            let status = open(
                &mut *raw,
                scheme.as_ptr(),
                authority.as_ptr(),
                parent_raw
                    .as_mut()
                    .map_or(std::ptr::null_mut(), |p| p as *mut _),
                parent_name.as_ref().map_or(null(), |n| n.as_ptr()),
                &mut out,
                &mut error,
            );
            // The provider owns the parent now, success or not; but a
            // provider that left it untouched must not leak it.
            if let Some(mut p) = parent_raw.take() {
                if let Some(release) = p.release {
                    release(&mut p);
                }
            }
            check(status, &error)?;
            import_file_system(&mut out)
        }
    }
}

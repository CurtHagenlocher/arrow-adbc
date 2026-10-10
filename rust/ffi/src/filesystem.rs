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

//! The C file-system ABI (`arrow-adbc/adbc_filesystem.h`).
//!
//! PROTOTYPE (file-driver proposal).
//!
//! - [export_file_system] turns a Rust [FileSystem] into a C table (for
//!   providers, and for driver managers handing a file system to a driver).
//! - [import_file_system] turns a C table into a Rust [FileSystem] (for
//!   drivers). The imported file always offers both the blocking and the
//!   async read path, whichever one the provider implements.

#![allow(non_camel_case_types)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::c_char;
use std::os::raw::c_void;
use std::ptr::{NonNull, null, null_mut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use adbc_core::constants::{
    ADBC_STATUS_CANCELLED, ADBC_STATUS_INVALID_ARGUMENT, ADBC_STATUS_NOT_FOUND, ADBC_STATUS_OK,
};
use adbc_core::error::{AdbcStatusCode, Error, Result, Status};
use adbc_core::filesystem::{
    self, AsyncRead, File, FileInfo, FileSystem, ReadCallback, ReadCompletion, ReadRange,
    ReadRequest,
};
use arrow_array::RecordBatchReader;
use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};

use crate::FFI_AdbcError;

pub mod conformance;

/// `ADBC_FILESYSTEM_VERSION_1`.
pub const ADBC_FILESYSTEM_VERSION_1: u32 = 1;
/// `ADBC_FILE_RANGE_SUFFIX`.
pub const ADBC_FILE_RANGE_SUFFIX: u32 = 1;
/// `ADBC_FILE_RANGE_UP_TO`.
pub const ADBC_FILE_RANGE_UP_TO: u32 = 2;
/// `ADBC_FILE_INFO_SIZE_KNOWN`.
pub const ADBC_FILE_INFO_SIZE_KNOWN: u32 = 1;

/// `struct AdbcFileRange`.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FFI_AdbcFileRange {
    pub offset: u64,
    pub length: u64,
    pub buffer: *mut c_void,
    pub flags: u32,
    pub user_data: u64,
}

/// `AdbcFileReadCallback`.
pub type FFI_AdbcFileReadCallback = Option<
    unsafe extern "C" fn(
        context: *mut c_void,
        user_data: u64,
        status: AdbcStatusCode,
        bytes_read: u64,
        error: *const FFI_AdbcError,
    ),
>;

/// `struct AdbcFileInfo`.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_AdbcFileInfo {
    pub struct_size: u32,
    pub flags: u32,
    pub size: u64,
    pub version: *const u8,
    pub version_length: usize,
}

impl Default for FFI_AdbcFileInfo {
    fn default() -> Self {
        Self {
            struct_size: std::mem::size_of::<Self>() as u32,
            flags: 0,
            size: 0,
            version: null(),
            version_length: 0,
        }
    }
}

/// `struct AdbcFileSystem`.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_AdbcFileSystem {
    pub struct_size: u32,
    pub version: u32,
    pub private_data: *mut c_void,
    pub open: Option<
        unsafe extern "C" fn(
            *mut Self,
            *const c_char,
            usize,
            u32,
            *mut *mut FFI_AdbcFile,
            *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub stat: Option<
        unsafe extern "C" fn(
            *mut Self,
            *const c_char,
            usize,
            *mut FFI_AdbcFileInfo,
            *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub list: Option<
        unsafe extern "C" fn(
            *mut Self,
            *const c_char,
            usize,
            *mut FFI_ArrowArrayStream,
            *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub get_extension: Option<
        unsafe extern "C" fn(*mut Self, *const c_char, u32, *mut *const c_void) -> AdbcStatusCode,
    >,
    pub clone:
        Option<unsafe extern "C" fn(*mut Self, *mut Self, *mut FFI_AdbcError) -> AdbcStatusCode>,
    pub release: Option<unsafe extern "C" fn(*mut Self)>,
}

impl Default for FFI_AdbcFileSystem {
    fn default() -> Self {
        Self {
            struct_size: 0,
            version: 0,
            private_data: null_mut(),
            open: None,
            stat: None,
            list: None,
            get_extension: None,
            clone: None,
            release: None,
        }
    }
}

/// `struct AdbcFile`.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_AdbcFile {
    pub struct_size: u32,
    pub version: u32,
    pub private_data: *mut c_void,
    pub info: Option<unsafe extern "C" fn(*mut Self, *mut FFI_AdbcFileInfo) -> AdbcStatusCode>,
    pub read_ranges: Option<
        unsafe extern "C" fn(
            *mut Self,
            *const FFI_AdbcFileRange,
            usize,
            *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub submit_reads: Option<
        unsafe extern "C" fn(
            *mut Self,
            *const FFI_AdbcFileRange,
            usize,
            FFI_AdbcFileReadCallback,
            *mut c_void,
            *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub cancel: Option<unsafe extern "C" fn(*mut Self, u64)>,
    pub close: Option<unsafe extern "C" fn(*mut Self) -> AdbcStatusCode>,
}

fn range_from_ffi(range: &FFI_AdbcFileRange) -> ReadRange {
    ReadRange {
        offset: range.offset,
        length: range.length,
        suffix: range.flags & ADBC_FILE_RANGE_SUFFIX != 0,
        up_to: range.flags & ADBC_FILE_RANGE_UP_TO != 0,
    }
}

fn range_flags(range: &ReadRange) -> u32 {
    let mut flags = 0;
    if range.suffix {
        flags |= ADBC_FILE_RANGE_SUFFIX;
    }
    if range.up_to {
        flags |= ADBC_FILE_RANGE_UP_TO;
    }
    flags
}

/// View a C (pointer, length) pair as a slice, allowing NULL for length 0.
unsafe fn buffer_slice<'a>(buffer: *mut c_void, length: u64) -> &'a mut [u8] {
    if length == 0 {
        return &mut [];
    }
    std::slice::from_raw_parts_mut(buffer as *mut u8, length as usize)
}

unsafe fn name_str<'a>(name: *const c_char, len: usize) -> Result<&'a str> {
    if name.is_null() && len > 0 {
        return Err(Error::with_message_and_status(
            "null name",
            Status::InvalidArguments,
        ));
    }
    let bytes = if len == 0 {
        &[][..]
    } else {
        std::slice::from_raw_parts(name as *const u8, len)
    };
    std::str::from_utf8(bytes).map_err(|e| {
        Error::with_message_and_status(format!("name is not UTF-8: {e}"), Status::InvalidArguments)
    })
}

/// Write `info` to a caller's struct, honoring its struct_size. The version
/// pointer must stay valid as long as the caller may read it.
unsafe fn write_info(out: *mut FFI_AdbcFileInfo, info: &FileInfo, version: Option<&[u8]>) {
    let Some(out) = out.as_mut() else { return };
    let available = out.struct_size as usize;
    let end_of = |offset: usize, size: usize| offset + size <= available;
    use std::mem::{offset_of, size_of};
    if end_of(offset_of!(FFI_AdbcFileInfo, flags), size_of::<u32>()) {
        out.flags = if info.size.is_some() {
            ADBC_FILE_INFO_SIZE_KNOWN
        } else {
            0
        };
    }
    if end_of(offset_of!(FFI_AdbcFileInfo, size), size_of::<u64>()) {
        out.size = info.size.unwrap_or(0);
    }
    if end_of(
        offset_of!(FFI_AdbcFileInfo, version_length),
        size_of::<usize>(),
    ) {
        match version {
            Some(v) => {
                out.version = v.as_ptr();
                out.version_length = v.len();
            }
            None => {
                out.version = null();
                out.version_length = 0;
            }
        }
    }
}

unsafe fn set_error(error: *mut FFI_AdbcError, err: Error) -> AdbcStatusCode {
    let status = err.status.into();
    crate::export_error(error, err);
    status
}

// ---------------------------------------------------------------------------
// Export: Rust -> C

struct ExportedFileSystem {
    file_system: Arc<dyn FileSystem>,
}

/// Export a Rust file system as a C table. The caller owns the result and
/// must call its `release` (or hand it to a driver, which then owns it).
pub fn export_file_system(file_system: Arc<dyn FileSystem>) -> FFI_AdbcFileSystem {
    let private = Box::new(ExportedFileSystem { file_system });
    FFI_AdbcFileSystem {
        struct_size: std::mem::size_of::<FFI_AdbcFileSystem>() as u32,
        version: ADBC_FILESYSTEM_VERSION_1,
        private_data: Box::into_raw(private) as *mut c_void,
        open: Some(exported_fs_open),
        stat: Some(exported_fs_stat),
        list: Some(exported_fs_list),
        get_extension: Some(exported_fs_get_extension),
        clone: Some(exported_fs_clone),
        release: Some(exported_fs_release),
    }
}

unsafe fn exported_fs<'a>(fs: *mut FFI_AdbcFileSystem) -> &'a ExportedFileSystem {
    &*((*fs).private_data as *const ExportedFileSystem)
}

unsafe extern "C" fn exported_fs_open(
    fs: *mut FFI_AdbcFileSystem,
    name: *const c_char,
    name_len: usize,
    hints: u32,
    out: *mut *mut FFI_AdbcFile,
    error: *mut FFI_AdbcError,
) -> AdbcStatusCode {
    if fs.is_null() || out.is_null() {
        return ADBC_STATUS_INVALID_ARGUMENT;
    }
    let result = name_str(name, name_len).and_then(|n| exported_fs(fs).file_system.open(n, hints));
    match result {
        Ok(file) => {
            *out = Box::into_raw(Box::new(export_file(file)));
            ADBC_STATUS_OK
        }
        Err(e) => set_error(error, e),
    }
}

thread_local! {
    /// Backing storage for the version token returned by `stat`: valid until
    /// the next `stat` on the same thread.
    static STAT_VERSION: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

unsafe extern "C" fn exported_fs_stat(
    fs: *mut FFI_AdbcFileSystem,
    name: *const c_char,
    name_len: usize,
    out: *mut FFI_AdbcFileInfo,
    error: *mut FFI_AdbcError,
) -> AdbcStatusCode {
    if fs.is_null() || out.is_null() {
        return ADBC_STATUS_INVALID_ARGUMENT;
    }
    match name_str(name, name_len).and_then(|n| exported_fs(fs).file_system.stat(n)) {
        Ok(info) => {
            STAT_VERSION.with(|v| {
                let mut v = v.borrow_mut();
                v.clear();
                if let Some(version) = &info.version {
                    v.extend_from_slice(version);
                }
                // The thread-local outlives this call; the pointer stays
                // valid until the next stat on this thread.
                let version = info
                    .version
                    .as_ref()
                    .map(|_| &*(v.as_slice() as *const [u8]));
                write_info(out, &info, version);
            });
            ADBC_STATUS_OK
        }
        Err(e) => set_error(error, e),
    }
}

unsafe extern "C" fn exported_fs_list(
    fs: *mut FFI_AdbcFileSystem,
    prefix: *const c_char,
    prefix_len: usize,
    out: *mut FFI_ArrowArrayStream,
    error: *mut FFI_AdbcError,
) -> AdbcStatusCode {
    if fs.is_null() || out.is_null() {
        return ADBC_STATUS_INVALID_ARGUMENT;
    }
    match name_str(prefix, prefix_len).and_then(|p| exported_fs(fs).file_system.list(p)) {
        Ok(reader) => {
            std::ptr::write_unaligned(out, FFI_ArrowArrayStream::new(reader));
            ADBC_STATUS_OK
        }
        Err(e) => set_error(error, e),
    }
}

unsafe extern "C" fn exported_fs_get_extension(
    _fs: *mut FFI_AdbcFileSystem,
    _name: *const c_char,
    _major: u32,
    out: *mut *const c_void,
) -> AdbcStatusCode {
    if !out.is_null() {
        *out = null();
    }
    ADBC_STATUS_NOT_FOUND
}

unsafe extern "C" fn exported_fs_clone(
    fs: *mut FFI_AdbcFileSystem,
    out: *mut FFI_AdbcFileSystem,
    _error: *mut FFI_AdbcError,
) -> AdbcStatusCode {
    if fs.is_null() || out.is_null() {
        return ADBC_STATUS_INVALID_ARGUMENT;
    }
    let clone = export_file_system(exported_fs(fs).file_system.clone());
    std::ptr::write_unaligned(out, clone);
    ADBC_STATUS_OK
}

unsafe extern "C" fn exported_fs_release(fs: *mut FFI_AdbcFileSystem) {
    let Some(fs) = fs.as_mut() else { return };
    if fs.release.is_none() {
        return;
    }
    drop(Box::from_raw(fs.private_data as *mut ExportedFileSystem));
    fs.private_data = null_mut();
    fs.release = None;
}

/// Reads submitted through an exported file that have not completed yet,
/// keyed by an internal id, valued by the caller's user_data.
struct InFlight {
    reads: Mutex<HashMap<u64, u64>>,
    drained: Condvar,
}

struct ExportedFile {
    file: Arc<dyn File>,
    info: OnceLock<Result<FileInfo>>,
    in_flight: Arc<InFlight>,
    next_id: AtomicU64,
}

fn export_file(file: Arc<dyn File>) -> FFI_AdbcFile {
    let private = Box::new(ExportedFile {
        file,
        info: OnceLock::new(),
        in_flight: Arc::new(InFlight {
            reads: Mutex::new(HashMap::new()),
            drained: Condvar::new(),
        }),
        next_id: AtomicU64::new(0),
    });
    FFI_AdbcFile {
        struct_size: std::mem::size_of::<FFI_AdbcFile>() as u32,
        version: ADBC_FILESYSTEM_VERSION_1,
        private_data: Box::into_raw(private) as *mut c_void,
        info: Some(exported_file_info),
        read_ranges: Some(exported_file_read_ranges),
        submit_reads: Some(exported_file_submit_reads),
        cancel: Some(exported_file_cancel),
        close: Some(exported_file_close),
    }
}

unsafe fn exported_file<'a>(file: *mut FFI_AdbcFile) -> &'a ExportedFile {
    &*((*file).private_data as *const ExportedFile)
}

unsafe extern "C" fn exported_file_info(
    file: *mut FFI_AdbcFile,
    out: *mut FFI_AdbcFileInfo,
) -> AdbcStatusCode {
    if file.is_null() || out.is_null() {
        return ADBC_STATUS_INVALID_ARGUMENT;
    }
    let file = exported_file(file);
    // Cache the first answer so the version pointer stays valid for the
    // life of the file.
    match file.info.get_or_init(|| file.file.info()) {
        Ok(info) => {
            write_info(out, info, info.version.as_deref());
            ADBC_STATUS_OK
        }
        Err(e) => e.status.into(),
    }
}

unsafe extern "C" fn exported_file_read_ranges(
    file: *mut FFI_AdbcFile,
    ranges: *const FFI_AdbcFileRange,
    count: usize,
    error: *mut FFI_AdbcError,
) -> AdbcStatusCode {
    if file.is_null() || (ranges.is_null() && count > 0) {
        return ADBC_STATUS_INVALID_ARGUMENT;
    }
    let ranges = if count == 0 {
        &[][..]
    } else {
        std::slice::from_raw_parts(ranges, count)
    };
    let mut requests: Vec<ReadRequest<'_>> = ranges
        .iter()
        .map(|r| ReadRequest {
            range: range_from_ffi(r),
            buffer: buffer_slice(r.buffer, r.length),
            bytes_read: 0,
        })
        .collect();
    match exported_file(file).file.read_ranges(&mut requests) {
        Ok(()) => ADBC_STATUS_OK,
        Err(e) => set_error(error, e),
    }
}

/// A raw pointer that we promise to use only as the C contract allows.
#[derive(Clone, Copy)]
struct SendPtr(*mut c_void);
unsafe impl Send for SendPtr {}
unsafe impl Sync for SendPtr {}

/// Delivers Rust completions to a C callback, copying into the caller's
/// borrowed buffers.
struct CCallback {
    callback: unsafe extern "C" fn(*mut c_void, u64, AdbcStatusCode, u64, *const FFI_AdbcError),
    context: SendPtr,
    /// Indexed by position in the submission: (caller's buffer, id).
    targets: Vec<(SendPtr, u64)>,
    first_id: u64,
    in_flight: Arc<InFlight>,
}

impl ReadCallback for CCallback {
    fn complete(&self, completion: ReadCompletion) {
        let index = (completion.user_data - self.first_id) as usize;
        let (buffer, _) = self.targets[index];
        let id = completion.user_data;
        let user_data = self.in_flight.reads.lock().unwrap().get(&id).copied();
        let Some(user_data) = user_data else {
            return;
        };
        unsafe {
            match completion.result {
                Ok(n) => {
                    if n > 0 {
                        std::ptr::copy_nonoverlapping(
                            completion.buffer.as_ptr(),
                            buffer.0 as *mut u8,
                            n as usize,
                        );
                    }
                    (self.callback)(self.context.0, user_data, ADBC_STATUS_OK, n, null());
                }
                Err(e) => {
                    let status: AdbcStatusCode = e.status.into();
                    let ffi_error = FFI_AdbcError::try_from(e).unwrap_or_else(Into::into);
                    (self.callback)(self.context.0, user_data, status, 0, &ffi_error);
                }
            }
        }
        // Only after the C callback has returned is the read finished.
        let mut reads = self.in_flight.reads.lock().unwrap();
        reads.remove(&id);
        if reads.is_empty() {
            self.in_flight.drained.notify_all();
        }
    }
}

unsafe extern "C" fn exported_file_submit_reads(
    file: *mut FFI_AdbcFile,
    ranges: *const FFI_AdbcFileRange,
    count: usize,
    callback: FFI_AdbcFileReadCallback,
    context: *mut c_void,
    error: *mut FFI_AdbcError,
) -> AdbcStatusCode {
    let Some(callback) = callback else {
        return ADBC_STATUS_INVALID_ARGUMENT;
    };
    if file.is_null() || (ranges.is_null() && count > 0) {
        return ADBC_STATUS_INVALID_ARGUMENT;
    }
    let exported = exported_file(file);
    let ranges = if count == 0 {
        &[][..]
    } else {
        std::slice::from_raw_parts(ranges, count)
    };
    let first_id = exported.next_id.fetch_add(count as u64, Ordering::Relaxed);
    let mut targets = Vec::with_capacity(count);
    let mut reads = Vec::with_capacity(count);
    for (i, r) in ranges.iter().enumerate() {
        let id = first_id + i as u64;
        targets.push((SendPtr(r.buffer), id));
        reads.push(AsyncRead {
            range: range_from_ffi(r),
            buffer: vec![0; r.length as usize],
            user_data: id,
        });
    }
    {
        let mut in_flight = exported.in_flight.reads.lock().unwrap();
        for (i, r) in ranges.iter().enumerate() {
            in_flight.insert(first_id + i as u64, r.user_data);
        }
    }
    let c_callback = Arc::new(CCallback {
        callback,
        context: SendPtr(context),
        targets,
        first_id,
        in_flight: exported.in_flight.clone(),
    });
    match filesystem::submit_reads(&exported.file, reads, c_callback) {
        Ok(()) => ADBC_STATUS_OK,
        Err(e) => {
            // Nothing was admitted: forget the reads again.
            let mut in_flight = exported.in_flight.reads.lock().unwrap();
            for i in 0..count as u64 {
                in_flight.remove(&(first_id + i));
            }
            if in_flight.is_empty() {
                exported.in_flight.drained.notify_all();
            }
            drop(in_flight);
            set_error(error, e)
        }
    }
}

unsafe extern "C" fn exported_file_cancel(file: *mut FFI_AdbcFile, user_data: u64) {
    if file.is_null() {
        return;
    }
    let exported = exported_file(file);
    let ids: Vec<u64> = exported
        .in_flight
        .reads
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, ud)| **ud == user_data)
        .map(|(id, _)| *id)
        .collect();
    for id in ids {
        exported.file.cancel(id);
    }
}

unsafe extern "C" fn exported_file_close(file: *mut FFI_AdbcFile) -> AdbcStatusCode {
    if file.is_null() {
        return ADBC_STATUS_INVALID_ARGUMENT;
    }
    let private = Box::from_raw((*file).private_data as *mut ExportedFile);
    {
        let ids: Vec<u64> = private
            .in_flight
            .reads
            .lock()
            .unwrap()
            .keys()
            .copied()
            .collect();
        for id in ids {
            private.file.cancel(id);
        }
        let mut reads = private.in_flight.reads.lock().unwrap();
        while !reads.is_empty() {
            reads = private.in_flight.drained.wait(reads).unwrap();
        }
    }
    drop(private);
    drop(Box::from_raw(file));
    ADBC_STATUS_OK
}

// ---------------------------------------------------------------------------
// Import: C -> Rust

unsafe fn check(status: AdbcStatusCode, error: &FFI_AdbcError) -> Result<()> {
    if status == ADBC_STATUS_OK {
        return Ok(());
    }
    let status = Status::try_from(status).unwrap_or(Status::Unknown);
    let mut err = Error::try_from(error).unwrap_or_else(|e| e);
    err.status = status;
    if error.message.is_null() {
        err.message = format!("file system call failed: {status:?}");
    }
    Err(err)
}

fn missing(what: &str) -> Error {
    Error::with_message_and_status(
        format!("file system does not implement {what}"),
        Status::NotImplemented,
    )
}

/// A C file system used from Rust. Releases the table on drop.
struct ImportedFileSystem {
    /// The C contract does not promise that one handle may be used from
    /// several threads at once, so calls on it are serialized.
    raw: Mutex<FFI_AdbcFileSystem>,
}

unsafe impl Send for ImportedFileSystem {}
unsafe impl Sync for ImportedFileSystem {}

impl Drop for ImportedFileSystem {
    fn drop(&mut self) {
        let raw = self.raw.get_mut().unwrap();
        if let Some(release) = raw.release {
            unsafe { release(raw) };
        }
    }
}

/// Take ownership of a C file system (zeroing the source) and wrap it as a
/// Rust [FileSystem].
///
/// # Safety
///
/// `raw` must point to a valid, unreleased table.
pub unsafe fn import_file_system(raw: *mut FFI_AdbcFileSystem) -> Result<Arc<dyn FileSystem>> {
    let raw = raw.as_mut().ok_or_else(|| {
        Error::with_message_and_status("null file system", Status::InvalidArguments)
    })?;
    if raw.release.is_none() {
        return Err(Error::with_message_and_status(
            "file system was already released",
            Status::InvalidArguments,
        ));
    }
    let owned = std::mem::take(raw);
    if owned.version != ADBC_FILESYSTEM_VERSION_1
        || (owned.struct_size as usize) < std::mem::size_of::<FFI_AdbcFileSystem>()
    {
        let message = format!(
            "unsupported file system table (version {}, struct_size {})",
            owned.version, owned.struct_size
        );
        drop(ImportedFileSystem {
            raw: Mutex::new(owned),
        });
        return Err(Error::with_message_and_status(
            message,
            Status::NotImplemented,
        ));
    }
    Ok(Arc::new(ImportedFileSystem {
        raw: Mutex::new(owned),
    }))
}

impl FileSystem for ImportedFileSystem {
    fn open(&self, name: &str, hints: u32) -> Result<Arc<dyn File>> {
        let mut raw = self.raw.lock().unwrap();
        let open = raw.open.ok_or_else(|| missing("open"))?;
        let mut file = null_mut();
        let mut error = FFI_AdbcError::default();
        unsafe {
            let status = open(
                &mut *raw,
                name.as_ptr() as *const c_char,
                name.len(),
                hints,
                &mut file,
                &mut error,
            );
            check(status, &error)?;
        }
        let file = NonNull::new(file).ok_or_else(|| {
            Error::with_message_and_status("open returned no file", Status::Internal)
        })?;
        Ok(Arc::new(ImportedFile { raw: file }))
    }

    fn stat(&self, name: &str) -> Result<FileInfo> {
        let mut raw = self.raw.lock().unwrap();
        let stat = raw.stat.ok_or_else(|| missing("stat"))?;
        let mut info = FFI_AdbcFileInfo::default();
        let mut error = FFI_AdbcError::default();
        unsafe {
            let status = stat(
                &mut *raw,
                name.as_ptr() as *const c_char,
                name.len(),
                &mut info,
                &mut error,
            );
            check(status, &error)?;
            Ok(info_from_ffi(&info))
        }
    }

    fn list(&self, prefix: &str) -> Result<Box<dyn RecordBatchReader + Send>> {
        let mut raw = self.raw.lock().unwrap();
        let list = raw.list.ok_or_else(|| missing("list"))?;
        let mut stream = FFI_ArrowArrayStream::empty();
        let mut error = FFI_AdbcError::default();
        unsafe {
            let status = list(
                &mut *raw,
                prefix.as_ptr() as *const c_char,
                prefix.len(),
                &mut stream,
                &mut error,
            );
            check(status, &error)?;
        }
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(Box::new(reader))
    }
}

unsafe fn info_from_ffi(info: &FFI_AdbcFileInfo) -> FileInfo {
    FileInfo {
        size: (info.flags & ADBC_FILE_INFO_SIZE_KNOWN != 0).then_some(info.size),
        version: (!info.version.is_null())
            .then(|| std::slice::from_raw_parts(info.version, info.version_length).to_vec()),
    }
}

/// A C file used from Rust. Closes the file on drop.
struct ImportedFile {
    raw: NonNull<FFI_AdbcFile>,
}

// Reads on one file may be issued concurrently from any thread (C contract).
unsafe impl Send for ImportedFile {}
unsafe impl Sync for ImportedFile {}

impl ImportedFile {
    fn raw(&self) -> *mut FFI_AdbcFile {
        self.raw.as_ptr()
    }

    fn table(&self) -> &FFI_AdbcFile {
        unsafe { self.raw.as_ref() }
    }
}

impl Drop for ImportedFile {
    fn drop(&mut self) {
        if let Some(close) = self.table().close {
            unsafe { close(self.raw()) };
        }
    }
}

/// The state of one submission to a C file: owns the buffers until the
/// last callback.
struct Submission {
    callback: Arc<dyn ReadCallback>,
    reads: Mutex<(Vec<Option<AsyncRead>>, usize)>,
}

unsafe extern "C" fn imported_read_callback(
    context: *mut c_void,
    user_data: u64,
    status: AdbcStatusCode,
    bytes_read: u64,
    error: *const FFI_AdbcError,
) {
    let submission = &*(context as *const Submission);
    let read = {
        let mut reads = submission.reads.lock().unwrap();
        reads.0.get_mut(user_data as usize).and_then(Option::take)
    };
    let delivered = read.is_some();
    if let Some(read) = read {
        let result = if status == ADBC_STATUS_OK {
            Ok(bytes_read)
        } else if let Some(error) = error.as_ref() {
            check(status, error).map(|_| 0)
        } else {
            Err(Error::with_message_and_status(
                if status == ADBC_STATUS_CANCELLED {
                    "read cancelled"
                } else {
                    "read failed"
                },
                Status::try_from(status).unwrap_or(Status::Unknown),
            ))
        };
        submission.callback.complete(ReadCompletion {
            user_data: read.user_data,
            buffer: read.buffer,
            result,
        });
    }
    // Count a read as finished only after its callback has returned, so the
    // last one to finish cannot free the submission while another thread is
    // still inside the callback.
    let last = delivered && {
        let mut reads = submission.reads.lock().unwrap();
        reads.1 -= 1;
        reads.1 == 0
    };
    if last {
        drop(Box::from_raw(context as *mut Submission));
    }
}

impl File for ImportedFile {
    fn info(&self) -> Result<FileInfo> {
        let info_fn = self.table().info.ok_or_else(|| missing("info"))?;
        let mut info = FFI_AdbcFileInfo::default();
        unsafe {
            let status = info_fn(self.raw(), &mut info);
            check(status, &FFI_AdbcError::default())?;
            Ok(info_from_ffi(&info))
        }
    }

    fn read_ranges(&self, requests: &mut [ReadRequest<'_>]) -> Result<()> {
        let Some(read_ranges) = self.table().read_ranges else {
            return filesystem::read_ranges_via_submit(self, requests);
        };
        for r in requests.iter() {
            filesystem::check_buffer(&r.range, r.buffer.len())?;
        }
        let ranges: Vec<FFI_AdbcFileRange> = requests
            .iter_mut()
            .enumerate()
            .map(|(i, r)| FFI_AdbcFileRange {
                offset: r.range.offset,
                length: r.range.length,
                buffer: r.buffer.as_mut_ptr() as *mut c_void,
                flags: range_flags(&r.range),
                user_data: i as u64,
            })
            .collect();
        let mut error = FFI_AdbcError::default();
        unsafe {
            let status = read_ranges(self.raw(), ranges.as_ptr(), ranges.len(), &mut error);
            check(status, &error)?;
        }
        // The blocking call does not report per-range sizes; exact reads
        // are full, and UP_TO reads are resolved against the size.
        let size = if requests.iter().any(|r| r.range.up_to) {
            self.info()?.size
        } else {
            None
        };
        for r in requests.iter_mut() {
            r.bytes_read = match (r.range.up_to, size) {
                (true, Some(size)) => r.range.resolve(size)?.1,
                _ => r.range.length,
            };
        }
        Ok(())
    }

    fn submit_reads(&self, reads: Vec<AsyncRead>, callback: Arc<dyn ReadCallback>) -> Result<()> {
        let submit = self
            .table()
            .submit_reads
            .ok_or_else(|| missing("submit_reads"))?;
        for r in &reads {
            filesystem::check_buffer(&r.range, r.buffer.len())?;
        }
        if reads.is_empty() {
            return Ok(());
        }
        let count = reads.len();
        let mut reads: Vec<Option<AsyncRead>> = reads.into_iter().map(Some).collect();
        let ranges: Vec<FFI_AdbcFileRange> = reads
            .iter_mut()
            .enumerate()
            .map(|(i, r)| {
                let r = r.as_mut().unwrap();
                FFI_AdbcFileRange {
                    offset: r.range.offset,
                    length: r.range.length,
                    buffer: r.buffer.as_mut_ptr() as *mut c_void,
                    flags: range_flags(&r.range),
                    user_data: i as u64,
                }
            })
            .collect();
        // Moving the Vec<Option<AsyncRead>> into the Submission does not
        // move the heap buffers the ranges point at.
        let submission = Box::into_raw(Box::new(Submission {
            callback,
            reads: Mutex::new((reads, count)),
        }));
        let mut error = FFI_AdbcError::default();
        unsafe {
            let status = submit(
                self.raw(),
                ranges.as_ptr(),
                ranges.len(),
                Some(imported_read_callback),
                submission as *mut c_void,
                &mut error,
            );
            if status != ADBC_STATUS_OK {
                // All-or-nothing admission: no callback will fire.
                drop(Box::from_raw(submission));
                check(status, &error)?;
            }
        }
        Ok(())
    }

    fn has_native_submit(&self) -> bool {
        self.table().submit_reads.is_some()
    }

    fn cancel(&self, user_data: u64) {
        // The caller's user_data is not the key we gave the C provider
        // (that is the position within a submission), so cancellation of
        // individual reads is not forwarded in this prototype.
        let _ = user_data;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    struct Bytes(Vec<u8>);

    impl File for Bytes {
        fn info(&self) -> Result<FileInfo> {
            Ok(FileInfo {
                size: Some(self.0.len() as u64),
                version: Some(b"v1".to_vec()),
            })
        }

        fn read_ranges(&self, requests: &mut [ReadRequest<'_>]) -> Result<()> {
            for r in requests {
                let (offset, n) = r.range.resolve(self.0.len() as u64)?;
                r.buffer[..n as usize]
                    .copy_from_slice(&self.0[offset as usize..(offset + n) as usize]);
                r.bytes_read = n;
            }
            Ok(())
        }
    }

    struct OneFile;

    impl FileSystem for OneFile {
        fn open(&self, name: &str, _hints: u32) -> Result<Arc<dyn File>> {
            match name {
                "data" => Ok(Arc::new(Bytes((0..=255).collect()))),
                _ => Err(Error::with_message_and_status(
                    format!("no such file: {name}"),
                    Status::NotFound,
                )),
            }
        }

        fn stat(&self, name: &str) -> Result<FileInfo> {
            self.open(name, 0)?.info()
        }
    }

    /// Export a Rust file system to C and import it back: every call
    /// crosses the C ABI twice.
    fn round_trip() -> Arc<dyn FileSystem> {
        let mut raw = export_file_system(Arc::new(OneFile));
        unsafe { import_file_system(&mut raw).unwrap() }
    }

    #[test]
    fn stat_and_not_found() {
        let fs = round_trip();
        let info = fs.stat("data").unwrap();
        assert_eq!(info.size, Some(256));
        assert_eq!(info.version.as_deref(), Some(&b"v1"[..]));
        let err = fs.open("nope", 0).err().unwrap();
        assert_eq!(err.status, Status::NotFound);
        assert!(err.message.contains("no such file"), "{}", err.message);
    }

    #[test]
    fn blocking_reads_through_c() {
        let fs = round_trip();
        let file = fs.open("data", filesystem::OPEN_RANDOM).unwrap();
        let mut a = [0u8; 3];
        let mut b = [0u8; 4];
        let mut c = [0u8; 10];
        let mut requests = [
            ReadRequest {
                range: ReadRange::exact(5, 3),
                buffer: &mut a,
                bytes_read: 0,
            },
            ReadRequest {
                range: ReadRange::suffix(4),
                buffer: &mut b,
                bytes_read: 0,
            },
            ReadRequest {
                range: ReadRange {
                    up_to: true,
                    ..ReadRange::exact(250, 10)
                },
                buffer: &mut c,
                bytes_read: 0,
            },
        ];
        file.read_ranges(&mut requests).unwrap();
        assert_eq!(requests[2].bytes_read, 6);
        assert_eq!(a, [5, 6, 7]);
        assert_eq!(b, [252, 253, 254, 255]);
        assert_eq!(&c[..6], &[250, 251, 252, 253, 254, 255]);

        let mut d = [0u8; 10];
        let mut past_end = [ReadRequest {
            range: ReadRange::exact(250, 10),
            buffer: &mut d,
            bytes_read: 0,
        }];
        assert!(file.read_ranges(&mut past_end).is_err());
    }

    #[test]
    fn async_reads_through_c() {
        let fs = round_trip();
        let file = fs.open("data", filesystem::OPEN_RANDOM).unwrap();
        // The imported file offers a native submit (the exported table has
        // one), so this goes C -> Rust pool -> C callback -> Rust callback.
        assert!(file.has_native_submit());
        let caller = std::thread::current().id();
        let (tx, rx) = mpsc::channel();
        let tx = Mutex::new(tx);
        let callback = Arc::new(move |c: ReadCompletion| {
            tx.lock()
                .unwrap()
                .send((std::thread::current().id(), c))
                .unwrap();
        });
        let reads = (0..8)
            .map(|i| AsyncRead {
                range: ReadRange::exact(i * 32, 32),
                buffer: vec![0; 32],
                user_data: 100 + i,
            })
            .chain(std::iter::once(AsyncRead {
                range: ReadRange::exact(250, 10),
                buffer: vec![0; 10],
                user_data: 999,
            }))
            .collect();
        filesystem::submit_reads(&file, reads, callback).unwrap();
        let mut ok = 0;
        for _ in 0..9 {
            let (thread, c) = rx.recv().unwrap();
            assert_ne!(thread, caller, "completed inline");
            if c.user_data == 999 {
                assert!(c.result.is_err());
            } else {
                assert_eq!(c.result.unwrap(), 32);
                assert_eq!(c.buffer[0] as u64, (c.user_data - 100) * 32);
                ok += 1;
            }
        }
        assert_eq!(ok, 8);
        assert!(rx.try_recv().is_err(), "a read completed twice");
        drop(file);
    }

    #[test]
    fn clone_shares_and_releases() {
        let mut raw = export_file_system(Arc::new(OneFile));
        let mut clone = FFI_AdbcFileSystem::default();
        unsafe {
            assert_eq!(
                (raw.clone.unwrap())(&mut raw, &mut clone, null_mut()),
                ADBC_STATUS_OK
            );
            let a = import_file_system(&mut raw).unwrap();
            assert!(raw.release.is_none(), "import must take ownership");
            let b = import_file_system(&mut clone).unwrap();
            drop(a);
            assert_eq!(b.stat("data").unwrap().size, Some(256));
        }
    }
}

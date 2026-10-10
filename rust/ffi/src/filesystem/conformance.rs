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

//! Conformance checks for any C file-system provider.
//!
//! PROTOTYPE (file-driver proposal). [check_file_system] drives a provider
//! purely through its C table, so it applies to providers written in any
//! language. It checks the read semantics and the async contract of
//! `adbc_filesystem.h`:
//!
//! - exact reads, suffix reads, `UP_TO` short reads, and past-end failures;
//! - every accepted async read gets exactly one callback;
//! - no callback runs on the submitting thread;
//! - `close` returns only after every callback has returned.

use std::collections::HashMap;
use std::os::raw::c_void;
use std::ptr::null_mut;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::ThreadId;
use std::time::Duration;

use adbc_core::constants::{ADBC_STATUS_CANCELLED, ADBC_STATUS_NOT_FOUND, ADBC_STATUS_OK};
use adbc_core::error::AdbcStatusCode;

use super::{
    ADBC_FILE_INFO_SIZE_KNOWN, ADBC_FILE_RANGE_SUFFIX, ADBC_FILE_RANGE_UP_TO, FFI_AdbcFile,
    FFI_AdbcFileInfo, FFI_AdbcFileRange, FFI_AdbcFileSystem,
};
use crate::FFI_AdbcError;

type Check = std::result::Result<(), String>;

macro_rules! ensure {
    ($cond:expr, $($fmt:tt)*) => {
        if !$cond {
            return Err(format!($($fmt)*));
        }
    };
}

/// Run the conformance checks against a provider.
///
/// `name` must name a file in the provider whose contents are `expected`
/// (at least 64 bytes), and `missing` a name that does not exist. The file
/// system stays owned by the caller.
///
/// # Safety
///
/// `fs` must point to a valid, unreleased table.
pub unsafe fn check_file_system(
    fs: *mut FFI_AdbcFileSystem,
    name: &str,
    missing: &str,
    expected: &[u8],
) -> Check {
    ensure!(
        expected.len() >= 64,
        "need a test file of at least 64 bytes"
    );
    let table = fs.as_ref().ok_or("null file system")?;
    ensure!(table.release.is_some(), "file system is released");
    let open = table.open.ok_or("open is NULL")?;
    let stat = table.stat.ok_or("stat is NULL")?;

    // stat
    let mut info = FFI_AdbcFileInfo::default();
    let mut error = FFI_AdbcError::default();
    let status = stat(fs, name.as_ptr().cast(), name.len(), &mut info, &mut error);
    ensure!(status == ADBC_STATUS_OK, "stat failed with status {status}");
    ensure!(
        info.flags & ADBC_FILE_INFO_SIZE_KNOWN != 0 && info.size == expected.len() as u64,
        "stat reported size {} (flags {}), expected {}",
        info.size,
        info.flags,
        expected.len()
    );

    // open a missing file
    let mut file: *mut FFI_AdbcFile = null_mut();
    let mut error = FFI_AdbcError::default();
    let status = open(
        fs,
        missing.as_ptr().cast(),
        missing.len(),
        0,
        &mut file,
        &mut error,
    );
    if status == ADBC_STATUS_OK {
        // A lazy open may report NOT_FOUND on first use instead.
        let f = &*file;
        let mut info = FFI_AdbcFileInfo::default();
        let status = (f.info.ok_or("info is NULL")?)(file, &mut info);
        (f.close.ok_or("close is NULL")?)(file);
        ensure!(
            status == ADBC_STATUS_NOT_FOUND,
            "opening a missing file succeeded and info returned {status}"
        );
    } else {
        ensure!(
            status == ADBC_STATUS_NOT_FOUND,
            "opening a missing file returned {status}, not NOT_FOUND"
        );
    }

    let file = open_file(fs, name)?;
    let result = check_open_file(file, expected);
    let close = (*file).close.ok_or("close is NULL")?;
    close(file);
    result?;

    check_close_waits(fs, name, expected)
}

unsafe fn open_file(
    fs: *mut FFI_AdbcFileSystem,
    name: &str,
) -> std::result::Result<*mut FFI_AdbcFile, String> {
    let open = (*fs).open.ok_or("open is NULL")?;
    let mut file: *mut FFI_AdbcFile = null_mut();
    let mut error = FFI_AdbcError::default();
    let status = open(
        fs,
        name.as_ptr().cast(),
        name.len(),
        0,
        &mut file,
        &mut error,
    );
    ensure!(
        status == ADBC_STATUS_OK && !file.is_null(),
        "open failed with status {status}"
    );
    Ok(file)
}

unsafe fn check_open_file(file: *mut FFI_AdbcFile, expected: &[u8]) -> Check {
    let f = &*file;
    let size = expected.len() as u64;

    let mut info = FFI_AdbcFileInfo::default();
    let status = (f.info.ok_or("info is NULL")?)(file, &mut info);
    ensure!(status == ADBC_STATUS_OK, "info failed with status {status}");
    ensure!(
        info.size == size,
        "info reported size {}, expected {size}",
        info.size
    );

    // Blocking reads.
    if let Some(read_ranges) = f.read_ranges {
        let mut a = vec![0u8; 16];
        let mut b = vec![0u8; 8];
        let mut empty = [0u8; 0];
        let ranges = [
            range(3, 16, a.as_mut_ptr(), 0, 1),
            range(0, 8, b.as_mut_ptr(), ADBC_FILE_RANGE_SUFFIX, 2),
            range(5, 0, empty.as_mut_ptr(), 0, 3),
        ];
        let mut error = FFI_AdbcError::default();
        let status = read_ranges(file, ranges.as_ptr(), ranges.len(), &mut error);
        ensure!(
            status == ADBC_STATUS_OK,
            "read_ranges failed with status {status}"
        );
        ensure!(a == expected[3..19], "exact read returned wrong bytes");
        ensure!(
            b == expected[expected.len() - 8..],
            "suffix read returned wrong bytes"
        );

        let mut c = vec![0u8; 16];
        let past_end = [range(size - 4, 16, c.as_mut_ptr(), 0, 4)];
        let mut error = FFI_AdbcError::default();
        let status = read_ranges(file, past_end.as_ptr(), 1, &mut error);
        ensure!(
            status != ADBC_STATUS_OK,
            "an exact read past the end of the file succeeded"
        );
    }

    // Async reads: exactly once, never inline, correct bytes and sizes.
    if f.submit_reads.is_some() {
        let count = 16u64;
        let mut buffers: Vec<Vec<u8>> = (0..count).map(|_| vec![0u8; 4]).collect();
        let mut ranges: Vec<FFI_AdbcFileRange> = buffers
            .iter_mut()
            .enumerate()
            .map(|(i, buf)| range(i as u64 * 4, 4, buf.as_mut_ptr(), 0, 100 + i as u64))
            .collect();
        let mut short = vec![0u8; 16];
        ranges.push(range(
            size - 6,
            16,
            short.as_mut_ptr(),
            ADBC_FILE_RANGE_UP_TO,
            900,
        ));
        let mut bad = vec![0u8; 16];
        ranges.push(range(size - 6, 16, bad.as_mut_ptr(), 0, 901));

        let results = submit_and_wait(file, &ranges, Duration::ZERO)?;
        ensure!(
            results.len() == ranges.len(),
            "{} callbacks for {} reads",
            results.len(),
            ranges.len()
        );
        let caller = std::thread::current().id();
        for (user_data, outcome) in &results {
            ensure!(
                outcome.calls == 1,
                "read {user_data} got {} callbacks",
                outcome.calls
            );
            ensure!(
                outcome.thread != caller,
                "read {user_data} completed inline"
            );
        }
        for i in 0..count {
            let outcome = &results[&(100 + i)];
            ensure!(
                outcome.status == ADBC_STATUS_OK,
                "read {i} failed: {}",
                outcome.status
            );
            ensure!(
                outcome.bytes == 4,
                "read {i} reported {} bytes",
                outcome.bytes
            );
            let at = i as usize * 4;
            ensure!(
                buffers[i as usize] == expected[at..at + 4],
                "read {i} returned wrong bytes"
            );
        }
        let up_to = &results[&900];
        ensure!(
            up_to.status == ADBC_STATUS_OK && up_to.bytes == 6,
            "UP_TO read at end of file returned status {} and {} bytes, expected 6",
            up_to.status,
            up_to.bytes
        );
        ensure!(
            short[..6] == expected[expected.len() - 6..],
            "UP_TO read returned wrong bytes"
        );
        ensure!(
            results[&901].status != ADBC_STATUS_OK,
            "an exact async read past the end of the file succeeded"
        );
    }
    ensure!(
        f.read_ranges.is_some() || f.submit_reads.is_some(),
        "neither read_ranges nor submit_reads is implemented"
    );
    Ok(())
}

/// Submit reads whose callback is slow, close the file at once, and check
/// that close did not return before every callback had returned.
unsafe fn check_close_waits(fs: *mut FFI_AdbcFileSystem, name: &str, expected: &[u8]) -> Check {
    let file = open_file(fs, name)?;
    if (*file).submit_reads.is_none() {
        ((*file).close.ok_or("close is NULL")?)(file);
        return Ok(());
    }
    let count = 8u64;
    let mut buffers: Vec<Vec<u8>> = (0..count).map(|_| vec![0u8; 8]).collect();
    let ranges: Vec<FFI_AdbcFileRange> = buffers
        .iter_mut()
        .enumerate()
        .map(|(i, buf)| {
            range(
                i as u64 * 8 % expected.len() as u64,
                8,
                buf.as_mut_ptr(),
                0,
                i as u64,
            )
        })
        .collect();
    let state = Arc::new(State::new(Duration::from_millis(20)));
    let context = Arc::into_raw(state.clone()) as *mut c_void;
    let mut error = FFI_AdbcError::default();
    let submit = (*file).submit_reads.unwrap();
    let status = submit(
        file,
        ranges.as_ptr(),
        ranges.len(),
        Some(on_read),
        context,
        &mut error,
    );
    if status != ADBC_STATUS_OK {
        drop(Arc::from_raw(context as *const State));
        ((*file).close.unwrap())(file);
        return Err(format!("submit_reads failed with status {status}"));
    }
    ((*file).close.ok_or("close is NULL")?)(file);
    let returned = state.inner.lock().unwrap().returned;
    if returned != count {
        // The provider is broken: callbacks may still write to the buffers
        // and touch the context, so leak both rather than free them.
        std::mem::forget(buffers);
        return Err(format!(
            "close returned while {} of {count} callbacks had not returned",
            count - returned
        ));
    }
    // Every callback has returned, so the context can be freed.
    drop(Arc::from_raw(context as *const State));
    for (user_data, outcome) in &state.inner.lock().unwrap().outcomes {
        ensure!(
            outcome.status == ADBC_STATUS_OK || outcome.status == ADBC_STATUS_CANCELLED,
            "read {user_data} failed during close with status {}",
            outcome.status
        );
    }
    Ok(())
}

fn range(
    offset: u64,
    length: u64,
    buffer: *mut u8,
    flags: u32,
    user_data: u64,
) -> FFI_AdbcFileRange {
    FFI_AdbcFileRange {
        offset,
        length,
        buffer: buffer.cast(),
        flags,
        user_data,
    }
}

#[derive(Debug, Clone)]
struct Outcome {
    calls: u32,
    status: AdbcStatusCode,
    bytes: u64,
    thread: ThreadId,
}

#[derive(Default)]
struct Inner {
    outcomes: HashMap<u64, Outcome>,
    returned: u64,
}

struct State {
    inner: Mutex<Inner>,
    changed: Condvar,
    /// How long each callback lingers before returning.
    delay: Duration,
}

impl State {
    fn new(delay: Duration) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            changed: Condvar::new(),
            delay,
        }
    }
}

unsafe extern "C" fn on_read(
    context: *mut c_void,
    user_data: u64,
    status: AdbcStatusCode,
    bytes_read: u64,
    _error: *const FFI_AdbcError,
) {
    let state = &*(context as *const State);
    {
        let mut inner = state.inner.lock().unwrap();
        let outcome = inner.outcomes.entry(user_data).or_insert(Outcome {
            calls: 0,
            status,
            bytes: bytes_read,
            thread: std::thread::current().id(),
        });
        outcome.calls += 1;
    }
    // Lingering is against the "callbacks must be tiny" rule; it is how
    // this check makes a close that does not wait observable.
    if !state.delay.is_zero() {
        std::thread::sleep(state.delay);
    }
    let mut inner = state.inner.lock().unwrap();
    inner.returned += 1;
    state.changed.notify_all();
}

unsafe fn submit_and_wait(
    file: *mut FFI_AdbcFile,
    ranges: &[FFI_AdbcFileRange],
    delay: Duration,
) -> std::result::Result<HashMap<u64, Outcome>, String> {
    let state = Arc::new(State::new(delay));
    let context = Arc::into_raw(state.clone()) as *mut c_void;
    let submit = (*file).submit_reads.ok_or("submit_reads is NULL")?;
    let mut error = FFI_AdbcError::default();
    let status = submit(
        file,
        ranges.as_ptr(),
        ranges.len(),
        Some(on_read),
        context,
        &mut error,
    );
    if status != ADBC_STATUS_OK {
        drop(Arc::from_raw(context as *const State));
        return Err(format!("submit_reads failed with status {status}"));
    }
    // The context is deliberately never reclaimed: the file is still open,
    // and a faulty provider could deliver a late or duplicate callback. A
    // small leak per check is the price of not handing it freed memory.
    let expected = ranges.len() as u64;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut inner = state.inner.lock().unwrap();
    while inner.returned < expected {
        let now = std::time::Instant::now();
        if now >= deadline {
            return Err(format!(
                "only {} of {expected} callbacks within 30 s",
                inner.returned
            ));
        }
        inner = state.changed.wait_timeout(inner, deadline - now).unwrap().0;
    }
    // Give a duplicate callback a moment to show up.
    drop(inner);
    std::thread::sleep(Duration::from_millis(20));
    let outcomes = state.inner.lock().unwrap().outcomes.clone();
    Ok(outcomes)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use adbc_core::error::{Error, Result, Status};
    use adbc_core::filesystem::{File, FileInfo, FileSystem, ReadRequest};

    use super::*;
    use crate::filesystem::export_file_system;

    struct Bytes(Vec<u8>);

    impl File for Bytes {
        fn info(&self) -> Result<FileInfo> {
            Ok(FileInfo {
                size: Some(self.0.len() as u64),
                version: None,
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

    struct OneFile(Vec<u8>);

    impl FileSystem for OneFile {
        fn open(&self, name: &str, _hints: u32) -> Result<Arc<dyn File>> {
            if name == "data" {
                Ok(Arc::new(Bytes(self.0.clone())))
            } else {
                Err(Error::with_message_and_status(
                    "no such file",
                    Status::NotFound,
                ))
            }
        }

        fn stat(&self, name: &str) -> Result<FileInfo> {
            self.open(name, 0)?.info()
        }
    }

    fn data() -> Vec<u8> {
        (0..1000u32).map(|i| (i * 7 % 251) as u8).collect()
    }

    #[test]
    fn rust_provider_conforms() {
        let mut fs = export_file_system(Arc::new(OneFile(data())));
        unsafe {
            check_file_system(&mut fs, "data", "missing", &data()).unwrap();
            (fs.release.unwrap())(&mut fs);
        }
    }

    /// A file whose reads complete on another thread after a delay, and
    /// whose close does NOT wait for them: the bug the kit must catch.
    struct Racy {
        data: Vec<u8>,
    }

    unsafe extern "C" fn racy_submit(
        file: *mut FFI_AdbcFile,
        ranges: *const FFI_AdbcFileRange,
        count: usize,
        callback: super::super::FFI_AdbcFileReadCallback,
        context: *mut c_void,
        _error: *mut FFI_AdbcError,
    ) -> AdbcStatusCode {
        let racy = &*((*file).private_data as *const Racy);
        let data = racy.data.clone();
        // (offset, length, buffer address, user_data): raw pointers are not Send.
        let ranges: Vec<(u64, u64, usize, u64)> = std::slice::from_raw_parts(ranges, count)
            .iter()
            .map(|r| (r.offset, r.length, r.buffer as usize, r.user_data))
            .collect();
        let callback = callback.unwrap() as usize;
        let context = context as usize;
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(5));
            let callback: unsafe extern "C" fn(
                *mut c_void,
                u64,
                AdbcStatusCode,
                u64,
                *const FFI_AdbcError,
            ) = unsafe { std::mem::transmute(callback) };
            for (offset, length, buffer, user_data) in ranges {
                let n = length as usize;
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        data[offset as usize..].as_ptr(),
                        buffer as *mut u8,
                        n,
                    );
                    callback(
                        context as *mut c_void,
                        user_data,
                        ADBC_STATUS_OK,
                        n as u64,
                        std::ptr::null(),
                    );
                }
            }
        });
        ADBC_STATUS_OK
    }

    unsafe extern "C" fn racy_close(_file: *mut FFI_AdbcFile) -> AdbcStatusCode {
        // Leaks the file instead of waiting: callbacks may still run.
        ADBC_STATUS_OK
    }

    #[test]
    fn kit_catches_close_that_does_not_wait() {
        // Start from a conforming file system and swap in a racy file.
        unsafe extern "C" fn racy_open(
            _fs: *mut FFI_AdbcFileSystem,
            _name: *const std::ffi::c_char,
            _len: usize,
            _hints: u32,
            out: *mut *mut FFI_AdbcFile,
            _error: *mut FFI_AdbcError,
        ) -> AdbcStatusCode {
            let racy = Box::into_raw(Box::new(Racy { data: data() }));
            let file = Box::new(FFI_AdbcFile {
                struct_size: std::mem::size_of::<FFI_AdbcFile>() as u32,
                version: 1,
                private_data: racy as *mut c_void,
                info: None,
                read_ranges: None,
                submit_reads: Some(racy_submit),
                cancel: None,
                close: Some(racy_close),
            });
            unsafe { *out = Box::into_raw(file) };
            ADBC_STATUS_OK
        }
        let mut fs = export_file_system(Arc::new(OneFile(data())));
        fs.open = Some(racy_open);
        let err = unsafe { check_close_waits(&mut fs, "data", &data()) }.unwrap_err();
        assert!(err.contains("close returned while"), "{err}");
        // The check leaked the buffers and context, so the stray callbacks
        // are harmless; let them finish before releasing the file system.
        std::thread::sleep(Duration::from_millis(200));
        unsafe { (fs.release.unwrap())(&mut fs) };
    }
}

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

//! Version negotiation and ADBC 1.2.0 table entries of the driver exporter,
//! exercised through the dummy driver's C entry point.

use std::mem::{offset_of, size_of};
use std::os::raw::c_void;
use std::ptr::null_mut;
use std::sync::Arc;

use adbc_core::constants::{
    ADBC_STATUS_NOT_IMPLEMENTED, ADBC_STATUS_OK, ADBC_VERSION_1_0_0, ADBC_VERSION_1_1_0,
    ADBC_VERSION_1_2_0,
};
use adbc_core::error::Result;
use adbc_core::filesystem::{File, FileInfo, FileSystem};
use adbc_ffi::filesystem::export_file_system;
use adbc_ffi::{
    ADBC_DRIVER_1_1_0_SIZE, ADBC_DRIVER_1_2_0_SIZE, FFI_AdbcDatabase, FFI_AdbcDriver, FFI_AdbcError,
};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;

use adbc_dummy::AdbcDummyInit;

const CANARY: u8 = 0xA5;

/// A suitably aligned buffer of `len` bytes, all set to `fill`.
fn table_buffer(len: usize, fill: u8) -> Vec<u64> {
    let words = len.div_ceil(8);
    vec![u64::from_ne_bytes([fill; 8]); words]
}

#[test]
fn init_at_1_1_writes_only_the_1_1_prefix() {
    let total = ADBC_DRIVER_1_2_0_SIZE + 64;
    let mut buffer = table_buffer(total, CANARY);
    let mut error = FFI_AdbcError::default();
    let status = unsafe {
        AdbcDummyInit(
            ADBC_VERSION_1_1_0,
            buffer.as_mut_ptr() as *mut c_void,
            &mut error,
        )
    };
    assert_eq!(status, ADBC_STATUS_OK);

    let bytes = unsafe { std::slice::from_raw_parts(buffer.as_ptr() as *const u8, total) };
    assert!(
        bytes[ADBC_DRIVER_1_1_0_SIZE..].iter().all(|b| *b == CANARY),
        "the exporter wrote past the 1.1.0 table"
    );
    // The 1.1.0 part was written: release and the last 1.1.0 member are set.
    let release = offset_of!(FFI_AdbcDriver, release);
    assert_ne!(&bytes[release..release + 8], &[CANARY; 8]);
    let last = offset_of!(FFI_AdbcDriver, StatementSetOptionInt);
    assert_ne!(&bytes[last..last + 8], &[CANARY; 8]);
}

#[test]
fn init_at_1_2_fills_the_1_2_table() {
    let mut driver = FFI_AdbcDriver::default();
    let mut error = FFI_AdbcError::default();
    let status = unsafe {
        AdbcDummyInit(
            ADBC_VERSION_1_2_0,
            &mut driver as *mut FFI_AdbcDriver as *mut c_void,
            &mut error,
        )
    };
    assert_eq!(status, ADBC_STATUS_OK);
    assert_eq!(size_of::<FFI_AdbcDriver>(), ADBC_DRIVER_1_2_0_SIZE);
    assert!(driver.ErrorGetVendorCode.is_some());
    assert!(driver.DriverGetInfo.is_some());
    assert!(driver.DatabaseSetFileSystem.is_some());
    assert!(driver.ConnectionSetFileSystem.is_some());
    // Not implemented by the exporter yet: left NULL, which is a valid
    // "not supported" signal.
    assert!(driver.StatementExecuteMulti.is_none());

    // The dummy driver does not implement driver-level info.
    let mut stream = FFI_ArrowArrayStream::empty();
    let status =
        unsafe { (driver.DriverGetInfo.unwrap())(std::ptr::null(), 0, &mut stream, &mut error) };
    assert_eq!(status, ADBC_STATUS_NOT_IMPLEMENTED);
}

struct NoFiles;

impl FileSystem for NoFiles {
    fn open(&self, name: &str, _hints: u32) -> Result<Arc<dyn File>> {
        Err(adbc_core::error::Error::with_message_and_status(
            format!("no such file: {name}"),
            adbc_core::error::Status::NotFound,
        ))
    }

    fn stat(&self, name: &str) -> Result<FileInfo> {
        self.open(name, 0).map(|_| FileInfo::default())
    }
}

#[test]
fn non_file_driver_rejects_a_file_system_without_taking_it() {
    let mut driver = FFI_AdbcDriver::default();
    let mut error = FFI_AdbcError::default();
    unsafe {
        assert_eq!(
            AdbcDummyInit(
                ADBC_VERSION_1_2_0,
                &mut driver as *mut FFI_AdbcDriver as *mut c_void,
                &mut error
            ),
            ADBC_STATUS_OK
        );
        let mut database = FFI_AdbcDatabase::default();
        assert_eq!(
            (driver.DatabaseNew.unwrap())(&mut database, &mut error),
            ADBC_STATUS_OK
        );

        let mut fs = export_file_system(Arc::new(NoFiles));
        let status = (driver.DatabaseSetFileSystem.unwrap())(&mut database, &mut fs, &mut error);
        assert_eq!(status, ADBC_STATUS_NOT_IMPLEMENTED);
        // Ownership only moves on success: the caller still owns it.
        assert!(fs.release.is_some());
        (fs.release.unwrap())(&mut fs);

        assert_eq!(
            (driver.DatabaseRelease.unwrap())(&mut database, null_mut()),
            ADBC_STATUS_OK
        );
    }
}

#[test]
fn unsupported_versions_are_refused() {
    for version in [ADBC_VERSION_1_0_0, 999] {
        let mut driver = FFI_AdbcDriver::default();
        let mut error = FFI_AdbcError::default();
        let status = unsafe {
            AdbcDummyInit(
                version,
                &mut driver as *mut FFI_AdbcDriver as *mut c_void,
                &mut error,
            )
        };
        assert_ne!(status, ADBC_STATUS_OK, "version {version} was accepted");
        assert!(
            driver.release.is_none(),
            "table written for version {version}"
        );
    }
    // 1.0.0 is refused with NOT_IMPLEMENTED so a driver manager can fall
    // back; an unknown version is an invalid argument.
    let mut driver = FFI_AdbcDriver::default();
    let status = unsafe {
        AdbcDummyInit(
            ADBC_VERSION_1_0_0,
            &mut driver as *mut FFI_AdbcDriver as *mut c_void,
            null_mut(),
        )
    };
    assert_eq!(status, ADBC_STATUS_NOT_IMPLEMENTED);
}

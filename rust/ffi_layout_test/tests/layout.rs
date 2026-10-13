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

use std::mem::{offset_of, size_of};

use adbc_core::constants;
use adbc_ffi::filesystem::provider::FFI_AdbcFileSystemProvider;
use adbc_ffi::filesystem::{
    self, FFI_AdbcFile, FFI_AdbcFileInfo, FFI_AdbcFileRange, FFI_AdbcFileSystem,
};
use adbc_ffi::{FFI_AdbcDriver, FFI_AdbcError, FFI_AdbcMultiResultSet};

macro_rules! size {
    ($c:literal, $t:ty) => {
        (concat!("sizeof ", $c), size_of::<$t>() as u64)
    };
}

macro_rules! offset {
    ($c:literal, $t:ty, $f:ident) => {
        (concat!($c, ".", stringify!($f)), offset_of!($t, $f) as u64)
    };
}

macro_rules! value {
    ($c:literal, $v:expr) => {
        ($c, $v as u64)
    };
}

fn rust_layout() -> Vec<(&'static str, u64)> {
    vec![
        size!("AdbcError", FFI_AdbcError),
        size!("AdbcDriver", FFI_AdbcDriver),
        size!("AdbcMultiResultSet", FFI_AdbcMultiResultSet),
        value!("ADBC_DRIVER_1_0_0_SIZE", adbc_ffi::ADBC_DRIVER_1_0_0_SIZE),
        value!("ADBC_DRIVER_1_1_0_SIZE", adbc_ffi::ADBC_DRIVER_1_1_0_SIZE),
        value!("ADBC_DRIVER_1_2_0_SIZE", adbc_ffi::ADBC_DRIVER_1_2_0_SIZE),
        offset!("AdbcDriver", FFI_AdbcDriver, StatementSetSubstraitPlan),
        offset!("AdbcDriver", FFI_AdbcDriver, ErrorGetDetailCount),
        offset!("AdbcDriver", FFI_AdbcDriver, StatementSetOptionInt),
        offset!("AdbcDriver", FFI_AdbcDriver, ErrorGetVendorCode),
        offset!("AdbcDriver", FFI_AdbcDriver, DriverGetOptionsList),
        offset!("AdbcDriver", FFI_AdbcDriver, MultiResultSetNext),
        offset!("AdbcDriver", FFI_AdbcDriver, MultiResultSetNextPartitions),
        offset!("AdbcDriver", FFI_AdbcDriver, MultiResultSetRelease),
        offset!("AdbcDriver", FFI_AdbcDriver, ConnectionSetWarningHandler),
        offset!("AdbcDriver", FFI_AdbcDriver, StatementExecuteSchemaMulti),
        offset!("AdbcDriver", FFI_AdbcDriver, StatementExecuteMulti),
        offset!("AdbcDriver", FFI_AdbcDriver, StatementRequestSchema),
        offset!("AdbcDriver", FFI_AdbcDriver, DriverGetInfo),
        offset!("AdbcDriver", FFI_AdbcDriver, DatabaseSetFileSystem),
        offset!("AdbcDriver", FFI_AdbcDriver, ConnectionSetFileSystem),
        value!(
            "ADBC_STATUS_FILESYSTEM_REQUIRED",
            constants::ADBC_STATUS_FILESYSTEM_REQUIRED
        ),
        value!(
            "ADBC_INFO_DRIVER_ACCEPTS_FILESYSTEM",
            constants::ADBC_INFO_DRIVER_ACCEPTS_FILESYSTEM
        ),
        value!(
            "ADBC_INFO_DRIVER_REQUIRES_FILESYSTEM",
            constants::ADBC_INFO_DRIVER_REQUIRES_FILESYSTEM
        ),
        value!(
            "ADBC_INFO_DRIVER_FILE_EXTENSIONS",
            constants::ADBC_INFO_DRIVER_FILE_EXTENSIONS
        ),
        value!(
            "ADBC_INFO_DRIVER_MIME_TYPES",
            constants::ADBC_INFO_DRIVER_MIME_TYPES
        ),
        size!("AdbcFileRange", FFI_AdbcFileRange),
        offset!("AdbcFileRange", FFI_AdbcFileRange, offset),
        offset!("AdbcFileRange", FFI_AdbcFileRange, length),
        offset!("AdbcFileRange", FFI_AdbcFileRange, buffer),
        offset!("AdbcFileRange", FFI_AdbcFileRange, flags),
        offset!("AdbcFileRange", FFI_AdbcFileRange, user_data),
        size!("AdbcFileInfo", FFI_AdbcFileInfo),
        offset!("AdbcFileInfo", FFI_AdbcFileInfo, struct_size),
        offset!("AdbcFileInfo", FFI_AdbcFileInfo, flags),
        offset!("AdbcFileInfo", FFI_AdbcFileInfo, size),
        offset!("AdbcFileInfo", FFI_AdbcFileInfo, version),
        offset!("AdbcFileInfo", FFI_AdbcFileInfo, version_length),
        size!("AdbcFileSystem", FFI_AdbcFileSystem),
        offset!("AdbcFileSystem", FFI_AdbcFileSystem, struct_size),
        offset!("AdbcFileSystem", FFI_AdbcFileSystem, version),
        offset!("AdbcFileSystem", FFI_AdbcFileSystem, private_data),
        offset!("AdbcFileSystem", FFI_AdbcFileSystem, open),
        offset!("AdbcFileSystem", FFI_AdbcFileSystem, stat),
        offset!("AdbcFileSystem", FFI_AdbcFileSystem, list),
        offset!("AdbcFileSystem", FFI_AdbcFileSystem, get_extension),
        offset!("AdbcFileSystem", FFI_AdbcFileSystem, clone),
        offset!("AdbcFileSystem", FFI_AdbcFileSystem, release),
        size!("AdbcFileSystemProvider", FFI_AdbcFileSystemProvider),
        offset!(
            "AdbcFileSystemProvider",
            FFI_AdbcFileSystemProvider,
            struct_size
        ),
        offset!(
            "AdbcFileSystemProvider",
            FFI_AdbcFileSystemProvider,
            version
        ),
        offset!(
            "AdbcFileSystemProvider",
            FFI_AdbcFileSystemProvider,
            private_data
        ),
        offset!(
            "AdbcFileSystemProvider",
            FFI_AdbcFileSystemProvider,
            set_option
        ),
        offset!("AdbcFileSystemProvider", FFI_AdbcFileSystemProvider, open),
        offset!(
            "AdbcFileSystemProvider",
            FFI_AdbcFileSystemProvider,
            release
        ),
        size!("AdbcFile", FFI_AdbcFile),
        offset!("AdbcFile", FFI_AdbcFile, struct_size),
        offset!("AdbcFile", FFI_AdbcFile, version),
        offset!("AdbcFile", FFI_AdbcFile, private_data),
        offset!("AdbcFile", FFI_AdbcFile, info),
        offset!("AdbcFile", FFI_AdbcFile, read_ranges),
        offset!("AdbcFile", FFI_AdbcFile, submit_reads),
        offset!("AdbcFile", FFI_AdbcFile, cancel),
        offset!("AdbcFile", FFI_AdbcFile, close),
        value!(
            "ADBC_FILESYSTEM_VERSION_1",
            filesystem::ADBC_FILESYSTEM_VERSION_1
        ),
        value!(
            "ADBC_FILESYSTEM_PROVIDER_VERSION_1",
            filesystem::provider::ADBC_FILESYSTEM_PROVIDER_VERSION_1
        ),
        value!("ADBC_FILE_RANGE_SUFFIX", filesystem::ADBC_FILE_RANGE_SUFFIX),
        value!("ADBC_FILE_RANGE_UP_TO", filesystem::ADBC_FILE_RANGE_UP_TO),
        value!(
            "ADBC_FILE_INFO_SIZE_KNOWN",
            filesystem::ADBC_FILE_INFO_SIZE_KNOWN
        ),
        value!(
            "ADBC_FILE_OPEN_SEQUENTIAL",
            adbc_core::filesystem::OPEN_SEQUENTIAL
        ),
        value!("ADBC_FILE_OPEN_RANDOM", adbc_core::filesystem::OPEN_RANDOM),
    ]
}

#[test]
fn rust_mirrors_match_c_headers() {
    let c = adbc_ffi_layout_test::c_layout();
    let rust = rust_layout();
    let c_names: Vec<&str> = c.iter().map(|(n, _)| n.as_str()).collect();
    let rust_names: Vec<&str> = rust.iter().map(|(n, _)| *n).collect();
    assert_eq!(
        c_names, rust_names,
        "layout.c and this test list different items"
    );

    let mismatches: Vec<String> = c
        .iter()
        .zip(&rust)
        .filter(|((_, c), (_, r))| c != r)
        .map(|((name, c), (_, r))| format!("{name}: C={c} Rust={r}"))
        .collect();
    assert!(
        mismatches.is_empty(),
        "layout mismatches:\n{}",
        mismatches.join("\n")
    );
}

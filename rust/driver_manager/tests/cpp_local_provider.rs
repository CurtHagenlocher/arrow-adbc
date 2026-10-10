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

//! The Rust file-system conformance kit, run against the C++ driver
//! manager's built-in local provider (PROTOTYPE): a cross-language check of
//! the file-system ABI.
//!
//! Set `ADBC_CPP_DRIVER_MANAGER` to the path of the C++ driver manager
//! library (`adbc_driver_manager.dll` / `libadbc_driver_manager.so`);
//! otherwise the test is skipped.

use adbc_ffi::FFI_AdbcError;
use adbc_ffi::filesystem::{FFI_AdbcFileSystem, conformance::check_file_system};

type LocalFileSystemInit = unsafe extern "C" fn(*mut FFI_AdbcFileSystem, *mut FFI_AdbcError) -> u8;

#[test]
fn cpp_local_provider_conforms() {
    let Ok(library) = std::env::var("ADBC_CPP_DRIVER_MANAGER") else {
        eprintln!("skipping: set ADBC_CPP_DRIVER_MANAGER to the C++ driver manager library");
        return;
    };
    let dir = tempfile::TempDir::new().unwrap();
    let data: Vec<u8> = (0..10_000u32).map(|i| (i * 31 % 251) as u8).collect();
    let path = dir.path().join("data.bin");
    std::fs::write(&path, &data).unwrap();
    let name = path.to_str().unwrap().replace('\\', "/");
    let missing = dir
        .path()
        .join("missing.bin")
        .to_str()
        .unwrap()
        .replace('\\', "/");

    unsafe {
        let library = libloading::Library::new(&library).unwrap();
        let init: libloading::Symbol<LocalFileSystemInit> = library
            .get(b"AdbcDriverManagerLocalFileSystemInit")
            .unwrap();
        let mut fs = FFI_AdbcFileSystem::default();
        assert_eq!(init(&mut fs, std::ptr::null_mut()), 0);
        check_file_system(&mut fs, &name, &missing, &data).unwrap();
        (fs.release.unwrap())(&mut fs);
    }
}

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

//! Layout checks for `adbc_ffi` against the C headers. See `tests/layout.rs`.

use std::ffi::{CStr, c_char};

#[repr(C)]
struct Entry {
    name: *const c_char,
    value: u64,
}

unsafe extern "C" {
    fn AdbcLayoutEntries(count: *mut usize) -> *const Entry;
}

/// The sizes, offsets, and constants that the C compiler reports.
pub fn c_layout() -> Vec<(String, u64)> {
    let mut count = 0;
    unsafe {
        let entries = AdbcLayoutEntries(&mut count);
        std::slice::from_raw_parts(entries, count)
            .iter()
            .map(|e| {
                (
                    CStr::from_ptr(e.name).to_string_lossy().into_owned(),
                    e.value,
                )
            })
            .collect()
    }
}

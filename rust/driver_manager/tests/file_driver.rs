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

//! How the driver manager handles file drivers (PROTOTYPE): version
//! negotiation, DriverGetInfo, and file-system injection.
//!
//! The driver here is written directly against the C driver table. Its
//! connections read the first bytes of the database `uri` through whatever
//! file system they were handed, and report them as the connection option
//! `test.first_bytes`.

// The test driver below is a C shim; like adbc_ffi, it opts out of
// per-operation unsafe blocks.
#![allow(unsafe_op_in_unsafe_fn)]

use std::collections::HashSet;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::ptr::null_mut;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use adbc_core::constants::{
    ADBC_INFO_DRIVER_ACCEPTS_FILESYSTEM, ADBC_INFO_DRIVER_REQUIRES_FILESYSTEM,
    ADBC_STATUS_FILESYSTEM_REQUIRED, ADBC_STATUS_INVALID_STATE, ADBC_STATUS_NOT_IMPLEMENTED,
    ADBC_STATUS_OK, ADBC_VERSION_1_2_0,
};
use adbc_core::error::{AdbcStatusCode, Error, Result, Status};
use adbc_core::filesystem::{File, FileInfo, FileSystem, Location, ReadRange, ReadRequest};
use adbc_core::info::{InfoValue, info_reader, read_info};
use adbc_core::options::{AdbcVersion, InfoCode, OptionConnection, OptionDatabase};
use adbc_core::{Database, Driver, Optionable};
use adbc_driver_manager::ManagedDriver;
use adbc_driver_manager::filesystem::LocalFileSystem;
use adbc_ffi::filesystem::{FFI_AdbcFileSystem, import_file_system};
use adbc_ffi::{FFI_AdbcConnection, FFI_AdbcDatabase, FFI_AdbcDriver, FFI_AdbcError, export_error};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;

// ---------------------------------------------------------------------------
// A minimal file driver, written against the C table.

struct DatabaseState {
    uri: Option<String>,
    file_system: Option<Arc<dyn FileSystem>>,
}

struct ConnectionState {
    file_system: Option<Arc<dyn FileSystem>>,
    first_bytes: Option<String>,
}

unsafe fn fail(error: *mut FFI_AdbcError, status: Status, message: impl Into<String>) -> u8 {
    export_error(error, Error::with_message_and_status(message, status));
    status.into()
}

unsafe extern "C" fn release(driver: *mut FFI_AdbcDriver, _: *mut FFI_AdbcError) -> u8 {
    (*driver).release = None;
    ADBC_STATUS_OK
}

unsafe extern "C" fn database_new(db: *mut FFI_AdbcDatabase, _: *mut FFI_AdbcError) -> u8 {
    let state = Box::new(DatabaseState {
        uri: None,
        file_system: None,
    });
    (*db).private_data = Box::into_raw(state) as *mut c_void;
    ADBC_STATUS_OK
}

unsafe extern "C" fn database_set_option(
    db: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: *const c_char,
    error: *mut FFI_AdbcError,
) -> u8 {
    let state = &mut *((*db).private_data as *mut DatabaseState);
    match CStr::from_ptr(key).to_str() {
        Ok("uri") => {
            state.uri = Some(CStr::from_ptr(value).to_string_lossy().into_owned());
            ADBC_STATUS_OK
        }
        _ => fail(error, Status::NotImplemented, "unknown option"),
    }
}

unsafe extern "C" fn database_init(_: *mut FFI_AdbcDatabase, _: *mut FFI_AdbcError) -> u8 {
    ADBC_STATUS_OK
}

unsafe extern "C" fn database_release(db: *mut FFI_AdbcDatabase, _: *mut FFI_AdbcError) -> u8 {
    drop(Box::from_raw((*db).private_data as *mut DatabaseState));
    (*db).private_data = null_mut();
    ADBC_STATUS_OK
}

unsafe extern "C" fn database_set_file_system(
    db: *mut FFI_AdbcDatabase,
    fs: *mut FFI_AdbcFileSystem,
    error: *mut FFI_AdbcError,
) -> u8 {
    let state = &mut *((*db).private_data as *mut DatabaseState);
    match import_file_system(fs) {
        Ok(fs) => {
            state.file_system = Some(fs);
            ADBC_STATUS_OK
        }
        Err(e) => fail(error, e.status, e.message),
    }
}

unsafe extern "C" fn connection_new(conn: *mut FFI_AdbcConnection, _: *mut FFI_AdbcError) -> u8 {
    let state = Box::new(ConnectionState {
        file_system: None,
        first_bytes: None,
    });
    (*conn).private_data = Box::into_raw(state) as *mut c_void;
    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_set_file_system(
    conn: *mut FFI_AdbcConnection,
    fs: *mut FFI_AdbcFileSystem,
    error: *mut FFI_AdbcError,
) -> u8 {
    let state = &mut *((*conn).private_data as *mut ConnectionState);
    if state.first_bytes.is_some() {
        return fail(
            error,
            Status::InvalidState,
            "connection already initialized",
        );
    }
    match import_file_system(fs) {
        Ok(fs) => {
            state.file_system = Some(fs);
            ADBC_STATUS_OK
        }
        Err(e) => fail(error, e.status, e.message),
    }
}

unsafe extern "C" fn connection_init(
    conn: *mut FFI_AdbcConnection,
    db: *mut FFI_AdbcDatabase,
    error: *mut FFI_AdbcError,
) -> u8 {
    let state = &mut *((*conn).private_data as *mut ConnectionState);
    let db = &*((*db).private_data as *const DatabaseState);
    // The connection's own file system, else the database default.
    let Some(fs) = state.file_system.clone().or_else(|| db.file_system.clone()) else {
        return fail(error, Status::FileSystemRequired, "no file system");
    };
    let Some(uri) = &db.uri else {
        return fail(error, Status::InvalidState, "no uri");
    };
    let read = || -> Result<String> {
        let name = Location::parse(uri)?.path;
        let file = fs.open(&name, 0)?;
        let mut buf = [0u8; 5];
        let mut request = [ReadRequest {
            range: ReadRange::exact(0, 5),
            buffer: &mut buf,
            bytes_read: 0,
        }];
        file.read_ranges(&mut request)?;
        Ok(String::from_utf8_lossy(&buf).into_owned())
    };
    match read() {
        Ok(bytes) => {
            state.first_bytes = Some(bytes);
            ADBC_STATUS_OK
        }
        Err(e) => fail(error, e.status, e.message),
    }
}

unsafe extern "C" fn connection_get_option(
    conn: *mut FFI_AdbcConnection,
    key: *const c_char,
    value: *mut c_char,
    length: *mut usize,
    error: *mut FFI_AdbcError,
) -> u8 {
    let state = &*((*conn).private_data as *const ConnectionState);
    if CStr::from_ptr(key).to_str() != Ok("test.first_bytes") {
        return fail(error, Status::NotFound, "unknown option");
    }
    let Some(bytes) = &state.first_bytes else {
        return ADBC_STATUS_INVALID_STATE;
    };
    let bytes = CString::new(bytes.as_str()).unwrap();
    let bytes = bytes.as_bytes_with_nul();
    if *length >= bytes.len() {
        std::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, value, bytes.len());
    }
    *length = bytes.len();
    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_release(
    conn: *mut FFI_AdbcConnection,
    _: *mut FFI_AdbcError,
) -> u8 {
    drop(Box::from_raw((*conn).private_data as *mut ConnectionState));
    (*conn).private_data = null_mut();
    ADBC_STATUS_OK
}

unsafe extern "C" fn driver_get_info(
    codes: *const u32,
    length: usize,
    out: *mut FFI_ArrowArrayStream,
    error: *mut FFI_AdbcError,
) -> u8 {
    let codes: Option<HashSet<InfoCode>> = (!codes.is_null()).then(|| {
        std::slice::from_raw_parts(codes, length)
            .iter()
            .map(|c| InfoCode::from(*c))
            .collect()
    });
    let rows = [
        (
            InfoCode::DriverName,
            InfoValue::String("test file driver".into()),
        ),
        (
            InfoCode::Other(ADBC_INFO_DRIVER_ACCEPTS_FILESYSTEM),
            InfoValue::Bool(true),
        ),
        (
            InfoCode::Other(ADBC_INFO_DRIVER_REQUIRES_FILESYSTEM),
            InfoValue::Bool(true),
        ),
    ];
    match info_reader(rows, codes.as_ref()) {
        Ok(reader) => {
            std::ptr::write_unaligned(out, FFI_ArrowArrayStream::new(reader));
            ADBC_STATUS_OK
        }
        Err(e) => fail(error, e.status, e.message),
    }
}

static INIT_CALLS: AtomicUsize = AtomicUsize::new(0);

/// Refuses every version below 1.2.0, as a file driver must.
unsafe extern "C" fn file_driver_init(
    version: c_int,
    raw: *mut c_void,
    _: *mut FFI_AdbcError,
) -> AdbcStatusCode {
    INIT_CALLS.fetch_add(1, Ordering::Relaxed);
    if version < ADBC_VERSION_1_2_0 {
        return ADBC_STATUS_NOT_IMPLEMENTED;
    }
    let driver = &mut *(raw as *mut FFI_AdbcDriver);
    driver.release = Some(release);
    driver.DatabaseNew = Some(database_new);
    driver.DatabaseSetOption = Some(database_set_option);
    driver.DatabaseInit = Some(database_init);
    driver.DatabaseRelease = Some(database_release);
    driver.ConnectionNew = Some(connection_new);
    driver.ConnectionInit = Some(connection_init);
    driver.ConnectionGetOption = Some(connection_get_option);
    driver.ConnectionRelease = Some(connection_release);
    driver.DriverGetInfo = Some(driver_get_info);
    driver.DatabaseSetFileSystem = Some(database_set_file_system);
    driver.ConnectionSetFileSystem = Some(connection_set_file_system);
    ADBC_STATUS_OK
}

// ---------------------------------------------------------------------------
// Tests

fn load() -> ManagedDriver {
    let init: adbc_ffi::FFI_AdbcDriverInitFunc = file_driver_init;
    ManagedDriver::load_static(&init, AdbcVersion::V120).unwrap()
}

fn data_dir() -> tempfile::TempDir {
    let dir = tempfile::TempDir::new().unwrap();
    std::fs::write(dir.path().join("data.txt"), b"hello, file drivers").unwrap();
    dir
}

fn first_bytes(connection: &impl Optionable<Option = OptionConnection>) -> String {
    connection
        .get_option_string(OptionConnection::Other("test.first_bytes".into()))
        .unwrap()
}

#[test]
fn negotiation_and_driver_info() {
    let init: adbc_ffi::FFI_AdbcDriverInitFunc = file_driver_init;
    // Asking for 1.1 cannot reach 1.2: the driver refuses 1.1 and 1.0.
    let before = INIT_CALLS.load(Ordering::Relaxed);
    let err = ManagedDriver::load_static(&init, AdbcVersion::V110).unwrap_err();
    assert_eq!(err.status, Status::NotImplemented);
    assert!(
        INIT_CALLS.load(Ordering::Relaxed) - before >= 2,
        "no fallback attempted"
    );

    let driver = load();
    assert_eq!(driver.version(), AdbcVersion::V120);
    assert!(driver.accepts_file_system());

    let info = read_info(driver.get_info(None).unwrap()).unwrap();
    assert!(info.contains(&(
        InfoCode::Other(ADBC_INFO_DRIVER_REQUIRES_FILESYSTEM),
        InfoValue::Bool(true)
    )));
    // A selection of codes.
    let info = read_info(
        driver
            .get_info(Some([InfoCode::DriverName].into()))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        info,
        vec![(
            InfoCode::DriverName,
            InfoValue::String("test file driver".into())
        )]
    );
}

#[test]
fn manager_injects_a_local_file_system_from_the_uri() {
    let dir = data_dir();
    let uri = dir.path().join("data.txt").to_string_lossy().into_owned();
    let mut driver = load();
    let database = driver
        .new_database_with_opts([(OptionDatabase::Uri, uri.as_str().into())])
        .unwrap();
    let connection = database.new_connection().unwrap();
    assert_eq!(first_bytes(&connection), "hello");

    // Also through a file:// URI.
    let uri = format!("file:///{}", uri.trim_start_matches('/').replace('\\', "/"));
    let database = driver
        .new_database_with_opts([(OptionDatabase::Uri, uri.as_str().into())])
        .unwrap();
    assert_eq!(first_bytes(&database.new_connection().unwrap()), "hello");
}

#[test]
fn no_uri_means_file_system_required() {
    let mut driver = load();
    let database = driver.new_database().unwrap();
    let err = database.new_connection().err().unwrap();
    assert_eq!(err.status, Status::FileSystemRequired);
    assert_eq!(u8::from(err.status), ADBC_STATUS_FILESYSTEM_REQUIRED);
}

#[test]
fn unknown_scheme_is_an_error() {
    let mut driver = load();
    let database = driver
        .new_database_with_opts([(OptionDatabase::Uri, "nope://bucket/data.txt".into())])
        .unwrap();
    let err = database.new_connection().err().unwrap();
    assert_eq!(err.status, Status::NotFound);
    assert!(err.message.contains("nope"), "{}", err.message);
}

/// Counts the files opened through it.
struct Counting(LocalFileSystem, Arc<AtomicUsize>);

impl FileSystem for Counting {
    fn open(&self, name: &str, hints: u32) -> Result<Arc<dyn File>> {
        self.1.fetch_add(1, Ordering::Relaxed);
        self.0.open(name, hints)
    }

    fn stat(&self, name: &str) -> Result<FileInfo> {
        self.0.stat(name)
    }
}

#[test]
fn explicit_connection_file_system_wins() {
    let dir = data_dir();
    let opens = Arc::new(AtomicUsize::new(0));
    // A store-relative name that only the rooted provider can resolve.
    let mut driver = load();
    let database = driver
        .new_database_with_opts([(OptionDatabase::Uri, "data.txt".into())])
        .unwrap();
    let fs = Arc::new(Counting(LocalFileSystem::rooted(dir.path()), opens.clone()));
    let connection = database
        .new_connection_with_file_system(std::iter::empty(), fs)
        .unwrap();
    assert_eq!(first_bytes(&connection), "hello");
    assert_eq!(
        opens.load(Ordering::Relaxed),
        1,
        "the driver bypassed the file system"
    );
}

#[test]
fn database_file_system_is_inherited_not_replaced() {
    let dir = data_dir();
    let opens = Arc::new(AtomicUsize::new(0));
    let mut driver = load();
    let fs = Arc::new(Counting(LocalFileSystem::rooted(dir.path()), opens.clone()));
    let database = driver
        .new_database_with_file_system([(OptionDatabase::Uri, "data.txt".into())], fs)
        .unwrap();
    // Two connections share the database's provider; the manager must not
    // inject its own (which could not resolve the relative name anyway).
    for _ in 0..2 {
        assert_eq!(first_bytes(&database.new_connection().unwrap()), "hello");
    }
    assert_eq!(opens.load(Ordering::Relaxed), 2);
}

#[test]
fn missing_file_reports_not_found() {
    let dir = data_dir();
    let uri = dir
        .path()
        .join("missing.txt")
        .to_string_lossy()
        .into_owned();
    let mut driver = load();
    let database = driver
        .new_database_with_opts([(OptionDatabase::Uri, uri.as_str().into())])
        .unwrap();
    let err = database.new_connection().err().unwrap();
    assert_eq!(err.status, Status::NotFound);
}

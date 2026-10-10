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

//! The Excel file driver end to end: the compiled driver library is loaded
//! through the driver manager and reads a generated workbook through the
//! C file-system ABI.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type, TimestampMicrosecondType};
use arrow_array::{Array, RecordBatch, RecordBatchReader};
use arrow_schema::{DataType, TimeUnit};

use adbc_core::constants::{
    ADBC_INFO_DRIVER_FILE_EXTENSIONS, ADBC_INFO_DRIVER_REQUIRES_FILESYSTEM,
};
use adbc_core::error::{Result, Status};
use adbc_core::filesystem::{File, FileInfo, FileSystem, ReadRequest};
use adbc_core::info::{InfoValue, read_info};
use adbc_core::options::{AdbcVersion, InfoCode, ObjectDepth, OptionDatabase, OptionStatement};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_driver_manager::filesystem::LocalFileSystem;
use adbc_driver_manager::{ManagedConnection, ManagedDatabase, ManagedDriver};
use adbc_excel::{OPTION_COLUMNS, OPTION_HEADER, OPTION_LIMIT, OPTION_TARGET};

fn write_workbook(path: &Path) {
    use rust_xlsxwriter::{ExcelDateTime, Format, Workbook};
    let mut workbook = Workbook::new();
    let date = Format::new().set_num_format("yyyy-mm-dd hh:mm");

    let orders = workbook.add_worksheet();
    orders.set_name("Orders").unwrap();
    for (col, name) in ["id", "sku", "qty", "price", "when", "shipped"]
        .iter()
        .enumerate()
    {
        orders.write(0, col as u16, *name).unwrap();
    }
    let rows = [
        (1, "A-1", 3, 9.5, "2026-01-02T10:30:00", true),
        (2, "B-7", 1, 120.0, "2026-02-03T00:00:00", false),
        (3, "A-1", 12, 9.25, "2026-03-04T23:59:00", true),
    ];
    for (i, (id, sku, qty, price, when, shipped)) in rows.iter().enumerate() {
        let r = i as u32 + 1;
        orders.write(r, 0, *id).unwrap();
        orders.write(r, 1, *sku).unwrap();
        orders.write(r, 2, *qty).unwrap();
        orders.write(r, 3, *price).unwrap();
        let when = ExcelDateTime::parse_from_str(when).unwrap();
        orders
            .write_datetime_with_format(r, 4, &when, &date)
            .unwrap();
        orders.write(r, 5, *shipped).unwrap();
    }

    // A named range that does not start at A1.
    let catalog = workbook.add_worksheet();
    catalog.set_name("Catalog").unwrap();
    catalog.write(0, 0, "ignored title").unwrap();
    catalog.write(1, 1, "sku").unwrap();
    catalog.write(1, 2, "unit_price").unwrap();
    for (i, (sku, price)) in [("A-1", 9.0), ("B-7", 115.5), ("C-3", 0.75)]
        .iter()
        .enumerate()
    {
        catalog.write(i as u32 + 2, 1, *sku).unwrap();
        catalog.write(i as u32 + 2, 2, *price).unwrap();
    }
    workbook
        .define_name("Prices", "=Catalog!$B$2:$C$5")
        .unwrap();
    workbook.save(path).unwrap();
}

struct Fixture {
    _dir: tempfile::TempDir,
    dir: PathBuf,
    path: PathBuf,
}

fn fixture() -> Fixture {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("book.xlsx");
    write_workbook(&path);
    Fixture {
        dir: dir.path().to_path_buf(),
        _dir: dir,
        path,
    }
}

fn load() -> ManagedDriver {
    ManagedDriver::load_dynamic_from_name("adbc_excel", Some(b"AdbcExcelInit"), AdbcVersion::V120)
        .unwrap()
}

fn open(uri: &str) -> (ManagedDatabase, ManagedConnection) {
    let database = load()
        .new_database_with_opts([(OptionDatabase::Uri, uri.into())])
        .unwrap();
    let connection = database.new_connection().unwrap();
    (database, connection)
}

fn collect(reader: impl RecordBatchReader) -> RecordBatch {
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    arrow_select_concat(&schema, &batches)
}

fn arrow_select_concat(schema: &arrow_schema::SchemaRef, batches: &[RecordBatch]) -> RecordBatch {
    // Small results: one batch is all the tests produce.
    assert!(batches.len() <= 1, "expected at most one batch");
    batches
        .first()
        .cloned()
        .unwrap_or_else(|| RecordBatch::new_empty(schema.clone()))
}

fn scan(connection: &mut ManagedConnection, options: &[(&str, &str)]) -> RecordBatch {
    let mut statement = connection.new_statement().unwrap();
    for (key, value) in options {
        statement
            .set_option(OptionStatement::Other(key.to_string()), (*value).into())
            .unwrap();
    }
    collect(statement.execute().unwrap())
}

#[test]
fn driver_is_a_1_2_file_driver() {
    let driver = load();
    assert_eq!(driver.version(), AdbcVersion::V120);
    assert!(driver.accepts_file_system());
    let info = read_info(driver.get_info(None).unwrap()).unwrap();
    assert!(info.contains(&(
        InfoCode::Other(ADBC_INFO_DRIVER_REQUIRES_FILESYSTEM),
        InfoValue::Bool(true)
    )));
    let extensions = info
        .iter()
        .find(|(c, _)| *c == InfoCode::Other(ADBC_INFO_DRIVER_FILE_EXTENSIONS))
        .map(|(_, v)| v.clone());
    assert!(
        matches!(extensions, Some(InfoValue::StringList(e)) if e.contains(&"xlsx".to_string()))
    );

    // A driver manager that only speaks 1.1 cannot load it.
    let err = ManagedDriver::load_dynamic_from_name(
        "adbc_excel",
        Some(b"AdbcExcelInit"),
        AdbcVersion::V110,
    )
    .err()
    .unwrap();
    assert_eq!(err.status, Status::NotImplemented);
}

#[test]
fn objects_and_schemas() {
    let f = fixture();
    let (_db, connection) = open(f.path.to_str().unwrap());

    let types = collect(connection.get_table_types().unwrap());
    let types = types.column(0).as_string::<i32>();
    assert_eq!(
        (0..types.len()).map(|i| types.value(i)).collect::<Vec<_>>(),
        ["SHEET", "NAMED_RANGE"]
    );

    let objects = collect(
        connection
            .get_objects(ObjectDepth::All, None, None, None, None, None)
            .unwrap(),
    );
    let schemas = objects.column(1).as_list::<i32>().value(0);
    let tables = schemas.as_struct().column(1).as_list::<i32>().value(0);
    let tables = tables.as_struct();
    let names = tables.column(0).as_string::<i32>();
    let kinds = tables.column(1).as_string::<i32>();
    let listed: Vec<(&str, &str)> = (0..names.len())
        .map(|i| (names.value(i), kinds.value(i)))
        .collect();
    assert_eq!(
        listed,
        [
            ("Orders", "SHEET"),
            ("Catalog", "SHEET"),
            ("Prices", "NAMED_RANGE")
        ]
    );
    let columns = tables.column(2).as_list::<i32>().value(2);
    let columns = columns.as_struct().column(0).as_string::<i32>();
    assert_eq!(columns.value(0), "sku");
    assert_eq!(columns.value(1), "unit_price");

    // Filters: a table-name pattern and a table type.
    let objects = collect(
        connection
            .get_objects(ObjectDepth::Tables, None, None, Some("%ice%"), None, None)
            .unwrap(),
    );
    let schemas = objects.column(1).as_list::<i32>().value(0);
    let tables = schemas.as_struct().column(1).as_list::<i32>().value(0);
    assert_eq!(tables.len(), 1);
    let objects = collect(
        connection
            .get_objects(
                ObjectDepth::Tables,
                None,
                None,
                None,
                Some(vec!["SHEET"]),
                None,
            )
            .unwrap(),
    );
    let schemas = objects.column(1).as_list::<i32>().value(0);
    let tables = schemas.as_struct().column(1).as_list::<i32>().value(0);
    assert_eq!(tables.len(), 2);

    let schema = connection.get_table_schema(None, None, "Orders").unwrap();
    let types: Vec<(&str, &DataType)> = schema
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.data_type()))
        .collect();
    assert_eq!(
        types,
        [
            ("id", &DataType::Int64),
            ("sku", &DataType::Utf8),
            ("qty", &DataType::Int64),
            ("price", &DataType::Float64),
            ("when", &DataType::Timestamp(TimeUnit::Microsecond, None)),
            ("shipped", &DataType::Boolean),
        ]
    );
}

#[test]
fn scans_with_projection_and_limit() {
    let f = fixture();
    let (_db, mut connection) = open(f.path.to_str().unwrap());

    let all = scan(&mut connection, &[(OPTION_TARGET, "Orders")]);
    assert_eq!(all.num_rows(), 3);
    let when = all.column(4).as_primitive::<TimestampMicrosecondType>();
    // 2026-01-02 10:30:00 UTC
    assert_eq!(when.value(0), 1_767_349_800_000_000);
    let price = all.column(3).as_primitive::<Float64Type>();
    assert_eq!(price.value(2), 9.25);

    let some = scan(
        &mut connection,
        &[
            (OPTION_TARGET, "Orders"),
            (OPTION_COLUMNS, r#"["qty", "sku"]"#),
            (OPTION_LIMIT, "2"),
        ],
    );
    assert_eq!(some.num_rows(), 2);
    assert_eq!(some.schema().field(0).name(), "qty");
    assert_eq!(some.column(0).as_primitive::<Int64Type>().value(1), 1);
    assert_eq!(some.column(1).as_string::<i32>().value(1), "B-7");

    // A named range that starts at B2 takes its header from B2.
    let prices = scan(&mut connection, &[(OPTION_TARGET, "Prices")]);
    assert_eq!(prices.schema().field(1).name(), "unit_price");
    assert_eq!(prices.num_rows(), 3);
    assert_eq!(
        prices.column(1).as_primitive::<Float64Type>().value(1),
        115.5
    );

    // A literal range without a header row: columns are named by letter.
    let cells = scan(
        &mut connection,
        &[(OPTION_TARGET, "Orders!B2:C3"), (OPTION_HEADER, "false")],
    );
    assert_eq!(cells.schema().field(0).name(), "B");
    assert_eq!(cells.schema().field(1).name(), "C");
    assert_eq!(cells.column(0).as_string::<i32>().value(1), "B-7");

    // An unknown column is an error, not an empty result.
    let mut statement = connection.new_statement().unwrap();
    statement
        .set_option(
            OptionStatement::Other(OPTION_TARGET.into()),
            "Orders".into(),
        )
        .unwrap();
    statement
        .set_option(
            OptionStatement::Other(OPTION_COLUMNS.into()),
            r#"["nope"]"#.into(),
        )
        .unwrap();
    assert_eq!(
        statement.execute().err().unwrap().status,
        Status::InvalidArguments
    );
}

#[test]
fn partitions_round_trip() {
    let f = fixture();
    let (_db, mut connection) = open(f.path.to_str().unwrap());
    let mut statement = connection.new_statement().unwrap();
    statement
        .set_option(
            OptionStatement::Other(OPTION_TARGET.into()),
            "Orders".into(),
        )
        .unwrap();
    statement
        .set_option(
            OptionStatement::Other(OPTION_COLUMNS.into()),
            r#"["id","price"]"#.into(),
        )
        .unwrap();
    let result = statement.execute_partitions().unwrap();
    assert_eq!(result.partitions.len(), 1);
    assert_eq!(result.schema.fields().len(), 2);
    let read = collect(connection.read_partition(&result.partitions[0]).unwrap());
    assert_eq!(read.num_rows(), 3);
    assert_eq!(read.schema().as_ref(), &result.schema);
}

#[test]
fn statements_are_scans_not_sql() {
    let f = fixture();
    let (_db, mut connection) = open(f.path.to_str().unwrap());
    let mut statement = connection.new_statement().unwrap();
    let err = statement
        .set_sql_query("SELECT * FROM Orders")
        .err()
        .unwrap();
    assert_eq!(err.status, Status::NotImplemented);
    assert!(err.message.contains(OPTION_TARGET), "{}", err.message);
    // No target set.
    assert_eq!(
        statement.execute().err().unwrap().status,
        Status::InvalidState
    );
}

#[test]
fn errors_from_storage() {
    let f = fixture();
    let missing = f.dir.join("missing.xlsx");
    let database = load()
        .new_database_with_opts([(OptionDatabase::Uri, missing.to_str().unwrap().into())])
        .unwrap();
    assert_eq!(
        database.new_connection().err().unwrap().status,
        Status::NotFound
    );

    // Not a workbook.
    std::fs::write(f.dir.join("notes.xlsx"), b"just text").unwrap();
    let database = load()
        .new_database_with_opts([(
            OptionDatabase::Uri,
            f.dir.join("notes.xlsx").to_str().unwrap().into(),
        )])
        .unwrap();
    assert_eq!(
        database.new_connection().err().unwrap().status,
        Status::InvalidData
    );

    // No uri: the manager has nothing to resolve, so the driver gets no file
    // system and says so.
    let database = load().new_database().unwrap();
    assert_eq!(
        database.new_connection().err().unwrap().status,
        Status::FileSystemRequired
    );
}

/// Counts opens and bytes read, to prove every byte comes through the
/// file system the host provided.
struct Counting {
    inner: LocalFileSystem,
    opens: Arc<AtomicUsize>,
    bytes: Arc<AtomicU64>,
}

struct CountingFile {
    inner: Arc<dyn File>,
    bytes: Arc<AtomicU64>,
}

impl File for CountingFile {
    fn info(&self) -> Result<FileInfo> {
        self.inner.info()
    }

    fn read_ranges(&self, requests: &mut [ReadRequest<'_>]) -> Result<()> {
        self.inner.read_ranges(requests)?;
        let n: u64 = requests.iter().map(|r| r.bytes_read).sum();
        self.bytes.fetch_add(n, Ordering::Relaxed);
        Ok(())
    }
}

impl FileSystem for Counting {
    fn open(&self, name: &str, hints: u32) -> Result<Arc<dyn File>> {
        self.opens.fetch_add(1, Ordering::Relaxed);
        Ok(Arc::new(CountingFile {
            inner: self.inner.open(name, hints)?,
            bytes: self.bytes.clone(),
        }))
    }

    fn stat(&self, name: &str) -> Result<FileInfo> {
        self.inner.stat(name)
    }
}

#[test]
fn reads_only_through_the_provided_file_system() {
    let f = fixture();
    let opens = Arc::new(AtomicUsize::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    let fs = Arc::new(Counting {
        inner: LocalFileSystem::rooted(&f.dir),
        opens: opens.clone(),
        bytes: bytes.clone(),
    });
    // A store-relative name: only the rooted provider can resolve it.
    let database = load()
        .new_database_with_opts([(OptionDatabase::Uri, "book.xlsx".into())])
        .unwrap();
    let mut connection = database
        .new_connection_with_file_system(std::iter::empty(), fs)
        .unwrap();
    let orders = scan(&mut connection, &[(OPTION_TARGET, "Orders")]);
    assert_eq!(orders.num_rows(), 3);
    assert_eq!(opens.load(Ordering::Relaxed), 1);
    let size = std::fs::metadata(&f.path).unwrap().len();
    let read = bytes.load(Ordering::Relaxed);
    // Every byte came through the provider, and none twice: the driver
    // opens by extension instead of probing formats.
    assert_eq!(read, size, "read {read} bytes of a {size}-byte workbook");
}

/// The built driver library: next to the test executable (`deps`, where
/// `cargo test` leaves it) or one level up (where `cargo build` copies it).
fn driver_library() -> PathBuf {
    let exe = std::env::current_exe().unwrap();
    let deps = exe.parent().unwrap();
    let name = format!(
        "{}adbc_excel{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    );
    [deps, deps.parent().unwrap()]
        .iter()
        .map(|dir| dir.join(&name))
        .find(|path| path.exists())
        .unwrap_or_else(|| panic!("{name} not found near {}", deps.display()))
}

fn write_manifest(dir: &Path, adbc: &str) -> PathBuf {
    let path = dir.join("adbc_excel.toml");
    let library = driver_library().to_string_lossy().replace('\\', "/");
    std::fs::write(
        &path,
        format!(
            "manifest_version = 1\nname = 'ADBC Excel File Driver (prototype)'\n\n{adbc}\n\n\
             [Driver]\nentrypoint = 'AdbcExcelInit'\nshared = '{library}'\n"
        ),
    )
    .unwrap();
    path
}

#[test]
fn loads_through_a_manifest() {
    let dir = tempfile::TempDir::new().unwrap();
    let load = |manifest: &Path| {
        ManagedDriver::load_from_name(
            manifest,
            None,
            AdbcVersion::V120,
            adbc_core::LOAD_FLAG_DEFAULT,
            None,
        )
    };

    // The manifest from the proposal (§6), with a leading 'v' as real
    // manifests have.
    let manifest = write_manifest(
        dir.path(),
        "[ADBC]\nversion = 'v1.2.0'\nmin_version = '1.2.0'\n\n\
         [ADBC.file_driver]\nrequires_filesystem = true\nextensions = ['xlsx', 'xls']",
    );
    let driver = load(&manifest).unwrap();
    assert_eq!(driver.version(), AdbcVersion::V120);
    assert!(driver.accepts_file_system());

    // A minimum this manager cannot meet fails before the library loads.
    let manifest = write_manifest(dir.path(), "[ADBC]\nmin_version = '1.3.0'");
    let err = load(&manifest).err().unwrap();
    assert_eq!(err.status, Status::NotImplemented);
    assert!(
        err.message.contains("requires ADBC 1.3.0"),
        "{}",
        err.message
    );

    // A manifest that claims only 1.1 caps negotiation at 1.1, which this
    // 1.2-only driver refuses.
    let manifest = write_manifest(dir.path(), "[ADBC]\nversion = '1.1.0'");
    assert_eq!(
        load(&manifest).err().unwrap().status,
        Status::NotImplemented
    );
}

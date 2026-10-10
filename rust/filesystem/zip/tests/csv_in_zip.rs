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

//! The C++ CSV file driver reading several files from one ZIP archive, in
//! parallel, through the Rust driver manager and this crate's ZIP provider
//! (PROTOTYPE). Neither the driver nor the provider knows about the other.
//!
//! Set `ADBC_CSV_DRIVER` to the CSV driver library; otherwise the tests are
//! skipped.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Once};

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{Array, RecordBatch, RecordBatchReader};
use arrow_schema::DataType;
use zip::CompressionMethod;
use zip::write::SimpleFileOptions;

use adbc_core::error::Status;
use adbc_core::options::{AdbcVersion, ObjectDepth, OptionDatabase, OptionStatement};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_driver_manager::{
    ManagedConnection, ManagedDatabase, ManagedDriver, ManagedStatement, filesystem,
};

fn driver() -> Option<ManagedDriver> {
    let path = std::env::var("ADBC_CSV_DRIVER").ok()?;
    static REGISTER: Once = Once::new();
    REGISTER.call_once(|| {
        filesystem::register_scheme(
            "zip",
            Arc::new(adbc_filesystem_zip::factory(filesystem::resolve)),
        );
    });
    Some(
        ManagedDriver::load_dynamic_from_filename(
            path,
            Some(b"AdbcDriverCsvInit"),
            AdbcVersion::V120,
        )
        .unwrap(),
    )
}

/// Monthly order files: `rows` rows each, ids continuing across months.
fn month(first_id: usize, rows: usize, month: usize) -> String {
    let mut out = String::from("id,sku,qty,price,shipped\n");
    for i in 0..rows {
        let id = first_id + i;
        let price = if id % 4 == 0 {
            "".to_string()
        } else {
            format!("{}.5", id % 7)
        };
        out.push_str(&format!(
            "{id},\"S-{m}{sku}\",{qty},{price},{shipped}\n",
            m = month,
            sku = id % 3,
            qty = id % 5 + 1,
            shipped = id % 2 == 0
        ));
    }
    out
}

struct Fixture {
    _dir: tempfile::TempDir,
    dir: PathBuf,
    archive: PathBuf,
}

fn fixture() -> Fixture {
    let dir = tempfile::TempDir::new().unwrap();
    let archive = dir.path().join("orders.zip");
    let mut zip = zip::ZipWriter::new(std::fs::File::create(&archive).unwrap());
    let files = [
        (
            "orders/2024-01.csv",
            month(0, 100, 1),
            CompressionMethod::Stored,
        ),
        (
            "orders/2024-02.csv",
            month(100, 250, 2),
            CompressionMethod::Deflated,
        ),
        (
            "orders/2024-03.csv",
            month(350, 50, 3),
            CompressionMethod::Deflated,
        ),
        (
            "notes/readme.txt",
            "not a csv".to_string(),
            CompressionMethod::Deflated,
        ),
    ];
    for (name, data, method) in &files {
        zip.start_file(
            *name,
            SimpleFileOptions::default().compression_method(*method),
        )
        .unwrap();
        zip.write_all(data.as_bytes()).unwrap();
    }
    zip.finish().unwrap();
    // The same files, unzipped, for the local-directory test.
    for (name, data, _) in &files {
        let path = dir.path().join("unzipped").join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, data).unwrap();
    }
    Fixture {
        dir: dir.path().to_path_buf(),
        _dir: dir,
        archive,
    }
}

fn zip_uri(archive: &Path) -> String {
    format!("zip:{}!/", archive.to_str().unwrap().replace('\\', "/"))
}

fn open(driver: &mut ManagedDriver, uri: &str) -> ManagedDatabase {
    driver
        .new_database_with_opts([(OptionDatabase::Uri, uri.into())])
        .unwrap()
}

fn statement_with(
    connection: &mut ManagedConnection,
    options: &[(&str, &str)],
) -> ManagedStatement {
    let mut statement = connection.new_statement().unwrap();
    for (key, value) in options {
        statement
            .set_option(OptionStatement::Other(key.to_string()), (*value).into())
            .unwrap();
    }
    statement
}

fn collect(reader: impl RecordBatchReader) -> Vec<RecordBatch> {
    reader.map(|b| b.unwrap()).collect()
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[test]
fn partitions_read_in_parallel() {
    let Some(mut driver) = driver() else {
        eprintln!("skipping: set ADBC_CSV_DRIVER");
        return;
    };
    assert!(driver.accepts_file_system());
    let f = fixture();
    let database = open(&mut driver, &zip_uri(&f.archive));

    let mut connection = database.new_connection().unwrap();
    let mut statement = statement_with(&mut connection, &[("adbc.file.target", "orders")]);
    let result = statement.execute_partitions().unwrap();
    assert_eq!(result.partitions.len(), 3, "one partition per file");
    let types: Vec<&DataType> = result
        .schema
        .fields()
        .iter()
        .map(|f| f.data_type())
        .collect();
    assert_eq!(
        types,
        [
            &DataType::Int64,
            &DataType::Utf8,
            &DataType::Int64,
            &DataType::Float64,
            &DataType::Boolean
        ]
    );

    // Each partition on its own connection and thread, as an engine would.
    let partitions = result.partitions.clone();
    let counts: Vec<(usize, i64)> = std::thread::scope(|scope| {
        let handles: Vec<_> = partitions
            .iter()
            .map(|partition| {
                let database = &database;
                scope.spawn(move || {
                    let connection = database.new_connection().unwrap();
                    let batches = collect(connection.read_partition(partition).unwrap());
                    let ids: i64 = batches
                        .iter()
                        .map(|b| {
                            b.column(0)
                                .as_primitive::<Int64Type>()
                                .iter()
                                .map(|v| v.unwrap())
                                .sum::<i64>()
                        })
                        .sum();
                    (rows(&batches), ids)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let total_rows: usize = counts.iter().map(|(n, _)| n).sum();
    let total_ids: i64 = counts.iter().map(|(_, s)| s).sum();
    assert_eq!(total_rows, 400);
    assert_eq!(total_ids, (0..400).sum::<i64>());
    let mut per_file: Vec<usize> = counts.iter().map(|(n, _)| *n).collect();
    per_file.sort();
    assert_eq!(per_file, [50, 100, 250]);
}

#[test]
fn scans_patterns_projection_and_limit() {
    let Some(mut driver) = driver() else {
        return;
    };
    let f = fixture();
    let database = open(&mut driver, &zip_uri(&f.archive));
    let mut connection = database.new_connection().unwrap();

    // One file, stored in the archive.
    let mut one = statement_with(
        &mut connection,
        &[("adbc.file.target", "orders/2024-01.csv")],
    );
    assert_eq!(rows(&collect(one.execute().unwrap())), 100);

    // A pattern; projection and limit across files.
    let mut some = statement_with(
        &mut connection,
        &[
            ("adbc.file.target", "orders/2024-0[23]*.csv"),
            ("adbc.file.columns", r#"["price", "id"]"#),
            ("adbc.file.limit", "5"),
        ],
    );
    // `[` is not a wildcard: no file matches.
    assert_eq!(some.execute().err().unwrap().status, Status::NotFound);
    let mut some = statement_with(
        &mut connection,
        &[
            ("adbc.file.target", "orders/2024-0?.csv"),
            ("adbc.file.columns", r#"["price", "id"]"#),
            ("adbc.file.limit", "5"),
        ],
    );
    let batches = collect(some.execute().unwrap());
    assert_eq!(rows(&batches), 5);
    assert_eq!(batches[0].schema().field(0).name(), "price");
    let price = batches[0].column(0).as_primitive::<Float64Type>();
    assert!(price.is_null(0), "empty cells are null");
    assert_eq!(price.value(1), 1.5);

    // Statements are scans, not SQL.
    let mut sql = connection.new_statement().unwrap();
    assert_eq!(
        sql.set_sql_query("SELECT 1").err().unwrap().status,
        Status::NotImplemented
    );
}

#[test]
fn objects_list_files_and_directories() {
    let Some(mut driver) = driver() else {
        return;
    };
    let f = fixture();
    let database = open(&mut driver, &zip_uri(&f.archive));
    let connection = database.new_connection().unwrap();
    let batches = collect(
        connection
            .get_objects(ObjectDepth::Tables, None, None, None, None, None)
            .unwrap(),
    );
    let schemas = batches[0].column(1).as_list::<i32>().value(0);
    let tables = schemas.as_struct().column(1).as_list::<i32>().value(0);
    let tables = tables.as_struct();
    let names = tables.column(0).as_string::<i32>();
    let kinds = tables.column(1).as_string::<i32>();
    let mut listed: Vec<(String, String)> = (0..names.len())
        .map(|i| (names.value(i).to_string(), kinds.value(i).to_string()))
        .collect();
    listed.sort();
    assert_eq!(
        listed,
        [
            ("orders".to_string(), "DIRECTORY".to_string()),
            ("orders/2024-01.csv".to_string(), "FILE".to_string()),
            ("orders/2024-02.csv".to_string(), "FILE".to_string()),
            ("orders/2024-03.csv".to_string(), "FILE".to_string()),
        ]
    );
}

#[test]
fn same_driver_over_a_local_directory() {
    let Some(mut driver) = driver() else {
        return;
    };
    let f = fixture();
    let root = f.dir.join("unzipped");
    let database = open(&mut driver, root.to_str().unwrap());
    let mut connection = database.new_connection().unwrap();
    let mut statement = statement_with(&mut connection, &[("adbc.file.target", "orders")]);
    assert_eq!(statement.execute_partitions().unwrap().partitions.len(), 3);
    let mut all = statement_with(&mut connection, &[("adbc.file.target", "orders")]);
    assert_eq!(rows(&collect(all.execute().unwrap())), 400);
}

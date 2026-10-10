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

//! An ADBC file driver for Excel and OpenDocument workbooks.
//!
//! PROTOTYPE for the file-driver proposal. It never opens files itself: it
//! reads the workbook through the [FileSystem] the host hands it, and
//! refuses ADBC versions older than 1.2.0.
//!
//! - **Database:** the `uri` option names one workbook (`.xlsx`, `.xlsm`,
//!   `.xlsb`, `.xls`, `.ods`).
//! - **Objects:** the sheets (table type `SHEET`) and the defined names that
//!   refer to a cell range (`NAMED_RANGE`), in one unnamed catalog and
//!   schema.
//! - **Statement:** a scan of one target, not a SQL query. Options:
//!   - `adbc.file.target`: a sheet, a named range, or a range such as
//!     `Sheet1!A1:C3` (required);
//!   - `adbc.file.columns`: the columns to return, as a JSON array of names;
//!   - `adbc.file.limit`: the maximum number of rows;
//!   - `adbc.file.header`: whether the first row holds column names
//!     (default `true`; otherwise columns are named A, B, C, ...).
//!
//! Column types are inferred from all data cells: integers, floats, booleans
//! and date-times map to Int64, Float64, Boolean and Timestamp(µs); anything
//! mixed or textual is Utf8.

mod objects;
mod workbook;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray};
use arrow_schema::Schema;

use adbc_core::constants::{
    ADBC_INFO_DRIVER_ACCEPTS_FILESYSTEM, ADBC_INFO_DRIVER_FILE_EXTENSIONS,
    ADBC_INFO_DRIVER_MIME_TYPES, ADBC_INFO_DRIVER_REQUIRES_FILESYSTEM, ADBC_VERSION_1_2_0,
};
use adbc_core::error::{Error, Result, Status};
use adbc_core::filesystem::FileSystem;
use adbc_core::info::{InfoValue, info_reader};
use adbc_core::options::{
    AdbcVersion, InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement,
    OptionValue,
};
use adbc_core::schemas::GET_TABLE_TYPES_SCHEMA;
use adbc_core::{Connection, Database, Driver, Optionable, PartitionedResult, Statement};

use workbook::{Layout, ScanOptions, Workbook};
pub use workbook::{TABLE_TYPE_NAMED_RANGE, TABLE_TYPE_SHEET};

/// Statement option: the sheet, named range, or cell range to scan.
pub const OPTION_TARGET: &str = "adbc.file.target";
/// Statement option: the columns to return, as a JSON array of names.
pub const OPTION_COLUMNS: &str = "adbc.file.columns";
/// Statement option: the maximum number of rows to return.
pub const OPTION_LIMIT: &str = "adbc.file.limit";
/// Statement option: whether the first row holds column names.
pub const OPTION_HEADER: &str = "adbc.file.header";

const EXTENSIONS: [&str; 5] = ["xlsx", "xlsm", "xlsb", "xls", "ods"];
const MIME_TYPES: [&str; 5] = [
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-excel.sheet.macroEnabled.12",
    "application/vnd.ms-excel.sheet.binary.macroEnabled.12",
    "application/vnd.ms-excel",
    "application/vnd.oasis.opendocument.spreadsheet",
];

fn not_implemented<T>(what: &str) -> Result<T> {
    Err(Error::with_message_and_status(
        format!("{what} is not supported by the Excel file driver"),
        Status::NotImplemented,
    ))
}

fn driver_info_rows() -> Vec<(InfoCode, InfoValue)> {
    vec![
        (
            InfoCode::DriverName,
            InfoValue::String("ADBC Excel File Driver (prototype)".into()),
        ),
        (
            InfoCode::DriverVersion,
            InfoValue::String(env!("CARGO_PKG_VERSION").into()),
        ),
        (
            InfoCode::DriverAdbcVersion,
            InfoValue::Int64(ADBC_VERSION_1_2_0 as i64),
        ),
        (
            InfoCode::Other(ADBC_INFO_DRIVER_ACCEPTS_FILESYSTEM),
            InfoValue::Bool(true),
        ),
        (
            InfoCode::Other(ADBC_INFO_DRIVER_REQUIRES_FILESYSTEM),
            InfoValue::Bool(true),
        ),
        (
            InfoCode::Other(ADBC_INFO_DRIVER_FILE_EXTENSIONS),
            InfoValue::StringList(EXTENSIONS.iter().map(|s| s.to_string()).collect()),
        ),
        (
            InfoCode::Other(ADBC_INFO_DRIVER_MIME_TYPES),
            InfoValue::StringList(MIME_TYPES.iter().map(|s| s.to_string()).collect()),
        ),
    ]
}

// ---------------------------------------------------------------------------
// Driver

#[derive(Default)]
pub struct ExcelDriver;

impl Driver for ExcelDriver {
    type DatabaseType = ExcelDatabase;

    const MIN_ADBC_VERSION: AdbcVersion = AdbcVersion::V120;
    const ACCEPTS_FILE_SYSTEM: bool = true;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        self.new_database_with_opts(std::iter::empty())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut database = ExcelDatabase::default();
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }

    fn new_database_with_file_system(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
        file_system: Arc<dyn FileSystem>,
    ) -> Result<Self::DatabaseType> {
        let mut database = self.new_database_with_opts(opts)?;
        database.file_system = Some(file_system);
        Ok(database)
    }

    fn get_driver_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        info_reader(driver_info_rows(), codes.as_ref())
    }
}

// ---------------------------------------------------------------------------
// Database

#[derive(Default)]
pub struct ExcelDatabase {
    uri: Option<String>,
    options: HashMap<String, OptionValue>,
    /// The default file system for this database's connections.
    file_system: Option<Arc<dyn FileSystem>>,
}

fn option_string(value: &OptionValue, key: &str) -> Result<String> {
    match value {
        OptionValue::String(s) => Ok(s.clone()),
        _ => Err(Error::with_message_and_status(
            format!("option '{key}' must be a string"),
            Status::InvalidArguments,
        )),
    }
}

fn get_string(options: &HashMap<String, OptionValue>, key: &str) -> Result<String> {
    match options.get(key) {
        Some(OptionValue::String(s)) => Ok(s.clone()),
        Some(_) => Err(Error::with_message_and_status(
            format!("option '{key}' is not a string"),
            Status::InvalidState,
        )),
        None => Err(Error::with_message_and_status(
            format!("option '{key}' is not set"),
            Status::NotFound,
        )),
    }
}

impl Optionable for ExcelDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        if let OptionDatabase::Uri = key {
            self.uri = Some(option_string(&value, key.as_ref())?);
        }
        self.options.insert(key.as_ref().to_string(), value);
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        get_string(&self.options, key.as_ref())
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        not_implemented(&format!("getting '{}' as bytes", key.as_ref()))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        not_implemented(&format!("getting '{}' as an integer", key.as_ref()))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        not_implemented(&format!("getting '{}' as a double", key.as_ref()))
    }
}

impl Database for ExcelDatabase {
    type ConnectionType = ExcelConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        self.new_connection_with_opts(std::iter::empty())
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        match &self.file_system {
            Some(fs) => self.new_connection_with_file_system(opts, fs.clone()),
            None => Err(Error::with_message_and_status(
                "The Excel driver reads through a file system provided by the host, \
                 and none was set on the connection or the database",
                Status::FileSystemRequired,
            )),
        }
    }

    fn new_connection_with_file_system(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
        file_system: Arc<dyn FileSystem>,
    ) -> Result<Self::ConnectionType> {
        let uri = self.uri.as_deref().ok_or_else(|| {
            Error::with_message_and_status(
                "The database option 'uri' must name a workbook",
                Status::InvalidState,
            )
        })?;
        let workbook = Workbook::open(&file_system, uri)?;
        let mut connection = ExcelConnection {
            workbook: Arc::new(Mutex::new(workbook)),
            options: HashMap::new(),
        };
        for (key, value) in opts {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}

// ---------------------------------------------------------------------------
// Connection

pub struct ExcelConnection {
    workbook: Arc<Mutex<Workbook>>,
    options: HashMap<String, OptionValue>,
}

/// One scan: the unit of a statement and of a partition.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Scan {
    target: String,
    options: ScanOptions,
}

impl Scan {
    fn run(
        &self,
        workbook: &Mutex<Workbook>,
    ) -> Result<(arrow_schema::SchemaRef, Vec<RecordBatch>)> {
        let range = workbook.lock().unwrap().read(&self.target)?;
        let layout = Layout::infer(&range, self.options.header);
        let selected = layout.select(self.options.columns.as_deref())?;
        layout.batches(&range, &selected, self.options.limit)
    }

    fn schema(&self, workbook: &Mutex<Workbook>) -> Result<Schema> {
        let range = workbook.lock().unwrap().read(&self.target)?;
        let layout = Layout::infer(&range, self.options.header);
        let selected = layout.select(self.options.columns.as_deref())?;
        Ok(layout.projected_schema(&selected).as_ref().clone())
    }

    /// The partition descriptor: fields separated by NUL, after a version.
    fn encode(&self) -> Vec<u8> {
        let columns = self
            .options
            .columns
            .as_ref()
            .map(|c| encode_json_strings(c))
            .unwrap_or_default();
        let limit = self
            .options
            .limit
            .map(|l| l.to_string())
            .unwrap_or_default();
        [
            "xlsx-scan-1",
            &self.target,
            if self.options.header { "1" } else { "0" },
            &columns,
            &limit,
        ]
        .join("\0")
        .into_bytes()
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let invalid = || {
            Error::with_message_and_status(
                "not a partition of the Excel driver",
                Status::InvalidArguments,
            )
        };
        let text = std::str::from_utf8(bytes).map_err(|_| invalid())?;
        let fields: Vec<&str> = text.split('\0').collect();
        let [version, target, header, columns, limit] = fields[..] else {
            return Err(invalid());
        };
        if version != "xlsx-scan-1" {
            return Err(invalid());
        }
        Ok(Scan {
            target: target.to_string(),
            options: ScanOptions {
                header: header == "1",
                columns: (!columns.is_empty())
                    .then(|| parse_json_strings(columns))
                    .transpose()?,
                limit: (!limit.is_empty())
                    .then(|| limit.parse().map_err(|_| invalid()))
                    .transpose()?,
            },
        })
    }
}

fn reader(
    schema: arrow_schema::SchemaRef,
    batches: Vec<RecordBatch>,
) -> Box<dyn RecordBatchReader + Send + 'static> {
    Box::new(RecordBatchIterator::new(
        batches.into_iter().map(Ok),
        schema,
    ))
}

impl Optionable for ExcelConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        self.options.insert(key.as_ref().to_string(), value);
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        get_string(&self.options, key.as_ref())
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        not_implemented(&format!("getting '{}' as bytes", key.as_ref()))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        not_implemented(&format!("getting '{}' as an integer", key.as_ref()))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        not_implemented(&format!("getting '{}' as a double", key.as_ref()))
    }
}

impl Connection for ExcelConnection {
    type StatementType = ExcelStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(ExcelStatement {
            workbook: self.workbook.clone(),
            target: None,
            options: ScanOptions::default(),
        })
    }

    fn get_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let mut rows = driver_info_rows();
        rows.push((
            InfoCode::VendorName,
            InfoValue::String("Spreadsheet file".into()),
        ));
        rows.push((InfoCode::VendorSql, InfoValue::Bool(false)));
        rows.push((InfoCode::VendorSubstrait, InfoValue::Bool(false)));
        info_reader(rows, codes.as_ref())
    }

    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        // The workbook has one unnamed catalog and schema: a filter on
        // either matches nothing unless it matches the empty name.
        let matches_unnamed = |filter: Option<&str>| filter.is_none_or(|f| objects::like(f, ""));
        let mut tables = Vec::new();
        if matches_unnamed(catalog) && matches_unnamed(db_schema) {
            let mut workbook = self.workbook.lock().unwrap();
            for table in workbook.tables() {
                if !table_name.is_none_or(|p| objects::like(p, &table.name))
                    || !table_type
                        .as_ref()
                        .is_none_or(|types| types.contains(&table.table_type))
                {
                    continue;
                }
                let columns = if matches!(depth, ObjectDepth::All | ObjectDepth::Columns) {
                    let range = workbook.read(&table.name)?;
                    let schema = Layout::infer(&range, true).schema();
                    Some(
                        schema
                            .fields()
                            .iter()
                            .map(|f| f.name().clone())
                            .filter(|n| column_name.is_none_or(|p| objects::like(p, n)))
                            .collect(),
                    )
                } else {
                    None
                };
                tables.push(objects::ObjectTable {
                    name: table.name,
                    table_type: table.table_type,
                    columns,
                });
            }
        }
        let batch = objects::get_objects(depth, tables)?;
        Ok(reader(batch.schema(), vec![batch]))
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        let scan = Scan {
            target: table_name.to_string(),
            options: ScanOptions::default(),
        };
        scan.schema(&self.workbook)
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let batch = RecordBatch::try_new(
            GET_TABLE_TYPES_SCHEMA.clone(),
            vec![Arc::new(StringArray::from(vec![
                TABLE_TYPE_SHEET,
                TABLE_TYPE_NAMED_RANGE,
            ]))],
        )?;
        Ok(reader(batch.schema(), vec![batch]))
    }

    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        not_implemented("GetStatisticNames")
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        not_implemented("GetStatistics")
    }

    fn commit(&mut self) -> Result<()> {
        not_implemented("Commit")
    }

    fn rollback(&mut self) -> Result<()> {
        not_implemented("Rollback")
    }

    fn read_partition(
        &self,
        partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let (schema, batches) = Scan::decode(partition.as_ref())?.run(&self.workbook)?;
        Ok(reader(schema, batches))
    }
}

// ---------------------------------------------------------------------------
// Statement

pub struct ExcelStatement {
    workbook: Arc<Mutex<Workbook>>,
    target: Option<String>,
    options: ScanOptions,
}

impl ExcelStatement {
    fn scan(&self) -> Result<Scan> {
        let target = self.target.clone().ok_or_else(|| {
            Error::with_message_and_status(
                format!("Set the statement option '{OPTION_TARGET}' to a sheet or named range"),
                Status::InvalidState,
            )
        })?;
        Ok(Scan {
            target,
            options: self.options.clone(),
        })
    }
}

impl Optionable for ExcelStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        let invalid = |message: &str| {
            Err(Error::with_message_and_status(
                format!("option '{}': {message}", key.as_ref()),
                Status::InvalidArguments,
            ))
        };
        match (key.as_ref(), &value) {
            (OPTION_TARGET, OptionValue::String(s)) => self.target = Some(s.clone()),
            (OPTION_COLUMNS, OptionValue::String(s)) => {
                self.options.columns = Some(parse_json_strings(s)?)
            }
            (OPTION_LIMIT, OptionValue::Int(i)) if *i >= 0 => {
                self.options.limit = Some(*i as usize)
            }
            (OPTION_LIMIT, OptionValue::String(s)) => match s.parse() {
                Ok(l) => self.options.limit = Some(l),
                Err(_) => return invalid("must be a non-negative integer"),
            },
            (OPTION_HEADER, value) => {
                self.options.header = bool::try_from(value)?;
            }
            (OPTION_TARGET | OPTION_COLUMNS | OPTION_LIMIT, _) => {
                return invalid("has the wrong type");
            }
            _ => {
                return Err(Error::with_message_and_status(
                    format!("unknown statement option '{}'", key.as_ref()),
                    Status::NotImplemented,
                ));
            }
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        let unset = || {
            Error::with_message_and_status(
                format!("option '{}' is not set", key.as_ref()),
                Status::NotFound,
            )
        };
        match key.as_ref() {
            OPTION_TARGET => self.target.clone().ok_or_else(unset),
            OPTION_COLUMNS => self
                .options
                .columns
                .as_ref()
                .map(|c| encode_json_strings(c))
                .ok_or_else(unset),
            OPTION_LIMIT => self.options.limit.map(|l| l.to_string()).ok_or_else(unset),
            OPTION_HEADER => Ok(self.options.header.to_string()),
            _ => Err(unset()),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        not_implemented(&format!("getting '{}' as bytes", key.as_ref()))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        match (key.as_ref(), self.options.limit) {
            (OPTION_LIMIT, Some(limit)) => Ok(limit as i64),
            _ => not_implemented(&format!("getting '{}' as an integer", key.as_ref())),
        }
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        not_implemented(&format!("getting '{}' as a double", key.as_ref()))
    }
}

impl Statement for ExcelStatement {
    fn bind(&mut self, _batch: RecordBatch) -> Result<()> {
        not_implemented("Binding parameters")
    }

    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        not_implemented("Binding parameters")
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let (schema, batches) = self.scan()?.run(&self.workbook)?;
        Ok(reader(schema, batches))
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        not_implemented("Updates")
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        self.scan()?.schema(&self.workbook)
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        // One partition per target. A sheet is not split; the point is that
        // the engine-facing path works, ready for multi-file drivers.
        let scan = self.scan()?;
        let schema = scan.schema(&self.workbook)?;
        Ok(PartitionedResult {
            partitions: vec![scan.encode()],
            schema,
            rows_affected: -1,
        })
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        not_implemented("Parameters")
    }

    fn prepare(&mut self) -> Result<()> {
        Ok(())
    }

    fn set_sql_query(&mut self, _query: impl AsRef<str>) -> Result<()> {
        Err(Error::with_message_and_status(
            format!(
                "The Excel file driver does not run SQL; a statement is a scan \
                 configured with the option '{OPTION_TARGET}'"
            ),
            Status::NotImplemented,
        ))
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        not_implemented("Substrait")
    }
}

// ---------------------------------------------------------------------------
// A minimal JSON string-array codec for the columns option.

fn encode_json_strings(items: &[String]) -> String {
    let mut out = String::from("[");
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        for c in item.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\t' => out.push_str("\\t"),
                '\r' => out.push_str("\\r"),
                c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
                c => out.push(c),
            }
        }
        out.push('"');
    }
    out.push(']');
    out
}

fn parse_json_strings(text: &str) -> Result<Vec<String>> {
    let invalid = || {
        Error::with_message_and_status(
            format!("'{OPTION_COLUMNS}' must be a JSON array of strings, got: {text}"),
            Status::InvalidArguments,
        )
    };
    let mut chars = text.trim().chars().peekable();
    let skip_ws = |chars: &mut std::iter::Peekable<std::str::Chars>| {
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
    };
    if chars.next() != Some('[') {
        return Err(invalid());
    }
    let mut items = Vec::new();
    skip_ws(&mut chars);
    if chars.peek() == Some(&']') {
        chars.next();
        return if chars.next().is_none() {
            Ok(items)
        } else {
            Err(invalid())
        };
    }
    loop {
        skip_ws(&mut chars);
        if chars.next() != Some('"') {
            return Err(invalid());
        }
        let mut item = String::new();
        loop {
            match chars.next().ok_or_else(invalid)? {
                '"' => break,
                '\\' => match chars.next().ok_or_else(invalid)? {
                    '"' => item.push('"'),
                    '\\' => item.push('\\'),
                    '/' => item.push('/'),
                    'n' => item.push('\n'),
                    't' => item.push('\t'),
                    'r' => item.push('\r'),
                    'b' => item.push('\u{8}'),
                    'f' => item.push('\u{c}'),
                    'u' => {
                        let hex: String = (0..4).filter_map(|_| chars.next()).collect();
                        let code = u32::from_str_radix(&hex, 16).map_err(|_| invalid())?;
                        item.push(char::from_u32(code).ok_or_else(invalid)?);
                    }
                    _ => return Err(invalid()),
                },
                c => item.push(c),
            }
        }
        items.push(item);
        skip_ws(&mut chars);
        match chars.next() {
            Some(',') => continue,
            Some(']') => break,
            _ => return Err(invalid()),
        }
    }
    skip_ws(&mut chars);
    if chars.next().is_some() {
        return Err(invalid());
    }
    Ok(items)
}

adbc_ffi::export_driver!(AdbcExcelInit, ExcelDriver);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_strings_round_trip() {
        let items = vec![
            "plain".to_string(),
            "with \"quotes\", commas".to_string(),
            "tab\tand\\slash".to_string(),
        ];
        assert_eq!(
            parse_json_strings(&encode_json_strings(&items)).unwrap(),
            items
        );
        assert_eq!(parse_json_strings(" [ ] ").unwrap(), Vec::<String>::new());
        assert_eq!(parse_json_strings(r#"["é"]"#).unwrap(), vec!["é"]);
        assert!(parse_json_strings("a,b").is_err());
        assert!(parse_json_strings(r#"["a"] x"#).is_err());
    }

    #[test]
    fn partitions_round_trip() {
        let scan = Scan {
            target: "Sheet1!A1:C3".into(),
            options: ScanOptions {
                header: false,
                columns: Some(vec!["A".into(), "C".into()]),
                limit: Some(10),
            },
        };
        assert_eq!(Scan::decode(&scan.encode()).unwrap(), scan);
        let scan = Scan {
            target: "Prices".into(),
            options: ScanOptions::default(),
        };
        assert_eq!(Scan::decode(&scan.encode()).unwrap(), scan);
        assert!(Scan::decode(b"garbage").is_err());
    }
}

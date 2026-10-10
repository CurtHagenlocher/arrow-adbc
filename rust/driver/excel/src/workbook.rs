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

//! A workbook read through a host-provided file system, and the mapping of
//! its sheets and named ranges onto Arrow.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use calamine::{
    Data, Ods, Range, Reader, Sheets, Xls, Xlsb, Xlsx, open_workbook_auto_from_rs,
    open_workbook_from_rs,
};

use adbc_core::error::{Error, Result, Status};
use adbc_core::filesystem::{FileReader, FileSystem, Location, OPEN_RANDOM};

/// The table type of a worksheet.
pub const TABLE_TYPE_SHEET: &str = "SHEET";
/// The table type of a workbook-level defined name.
pub const TABLE_TYPE_NAMED_RANGE: &str = "NAMED_RANGE";

/// Rows per output batch.
const BATCH_ROWS: usize = 65_536;

pub struct Workbook {
    sheets: Sheets<FileReader>,
}

/// A tabular object in the workbook.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Table {
    pub name: String,
    pub table_type: &'static str,
}

/// A cell rectangle, 0-based and inclusive: ((row, col), (row, col)).
type Cells = ((u32, u32), (u32, u32));

impl Workbook {
    /// Open the workbook named by `uri` through `file_system`.
    pub fn open(file_system: &Arc<dyn FileSystem>, uri: &str) -> Result<Self> {
        let name = Location::parse(uri)?.path;
        let file = file_system.open(&name, OPEN_RANDOM)?;
        let reader = FileReader::new(file)?;
        let invalid = |e: &dyn std::fmt::Display| {
            Error::with_message_and_status(
                format!("'{uri}' is not a readable workbook: {e}"),
                Status::InvalidData,
            )
        };
        // Open by extension. Auto-detection probes each format in turn, and
        // every probe reads the file again: on remote storage that is a
        // full extra download per wrong guess.
        let extension = name
            .rsplit_once('.')
            .map(|(_, e)| e.to_ascii_lowercase())
            .unwrap_or_default();
        let sheets = match extension.as_str() {
            "xlsx" | "xlsm" => {
                Sheets::Xlsx(open_workbook_from_rs::<Xlsx<_>, _>(reader).map_err(|e| invalid(&e))?)
            }
            "xlsb" => {
                Sheets::Xlsb(open_workbook_from_rs::<Xlsb<_>, _>(reader).map_err(|e| invalid(&e))?)
            }
            "xls" => {
                Sheets::Xls(open_workbook_from_rs::<Xls<_>, _>(reader).map_err(|e| invalid(&e))?)
            }
            "ods" => {
                Sheets::Ods(open_workbook_from_rs::<Ods<_>, _>(reader).map_err(|e| invalid(&e))?)
            }
            _ => open_workbook_auto_from_rs(reader).map_err(|e| invalid(&e))?,
        };
        Ok(Self { sheets })
    }

    /// The sheets, then the defined names that refer to a cell range.
    pub fn tables(&self) -> Vec<Table> {
        let mut tables: Vec<Table> = self
            .sheets
            .sheet_names()
            .into_iter()
            .map(|name| Table {
                name,
                table_type: TABLE_TYPE_SHEET,
            })
            .collect();
        for (name, formula) in self.sheets.defined_names() {
            // Skip names that are formulas or constants, and Excel's own
            // hidden names (print areas and the like).
            if name.starts_with("_xlnm.") || parse_reference(formula).is_none() {
                continue;
            }
            tables.push(Table {
                name: name.clone(),
                table_type: TABLE_TYPE_NAMED_RANGE,
            });
        }
        tables
    }

    /// Read a target: a sheet name, a defined name, or `Sheet!A1:C3`.
    pub fn read(&mut self, target: &str) -> Result<Range<Data>> {
        let sheet_names = self.sheets.sheet_names();
        if sheet_names.iter().any(|s| s == target) {
            return self.sheet(target);
        }
        let defined = self
            .sheets
            .defined_names()
            .iter()
            .find(|(name, _)| name == target)
            .map(|(_, formula)| formula.clone());
        let (sheet, cells) = match defined {
            Some(formula) => parse_reference(&formula).ok_or_else(|| {
                Error::with_message_and_status(
                    format!("named range '{target}' is not a cell range: {formula}"),
                    Status::InvalidArguments,
                )
            })?,
            None => parse_reference(target).ok_or_else(|| {
                Error::with_message_and_status(
                    format!(
                        "'{target}' is not a sheet, a named range, or a range like Sheet1!A1:C3"
                    ),
                    Status::NotFound,
                )
            })?,
        };
        let range = self.sheet(&sheet)?;
        Ok(range.range(cells.0, cells.1))
    }

    fn sheet(&mut self, name: &str) -> Result<Range<Data>> {
        self.sheets.worksheet_range(name).map_err(|e| {
            Error::with_message_and_status(format!("sheet '{name}': {e}"), Status::NotFound)
        })
    }
}

/// Parse `Sheet1!$A$1:$C$3`, `'My Sheet'!A1:C3`, or `=Sheet1!B2`.
fn parse_reference(reference: &str) -> Option<(String, Cells)> {
    let reference = reference.trim().trim_start_matches('=');
    let (sheet, cells) = reference.rsplit_once('!')?;
    let sheet = match sheet.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')) {
        Some(quoted) => quoted.replace("''", "'"),
        None => sheet.to_string(),
    };
    let cells = cells.replace('$', "");
    let (start, end) = match cells.split_once(':') {
        Some((a, b)) => (parse_cell(a)?, parse_cell(b)?),
        None => {
            let cell = parse_cell(&cells)?;
            (cell, cell)
        }
    };
    let cells = (
        (start.0.min(end.0), start.1.min(end.1)),
        (start.0.max(end.0), start.1.max(end.1)),
    );
    Some((sheet, cells))
}

/// Parse `B12` into (row 11, col 1).
fn parse_cell(cell: &str) -> Option<(u32, u32)> {
    let split = cell.find(|c: char| c.is_ascii_digit())?;
    let (letters, digits) = cell.split_at(split);
    if letters.is_empty() || !letters.chars().all(|c| c.is_ascii_alphabetic()) {
        return None;
    }
    let col = letters.chars().try_fold(0u32, |acc, c| {
        acc.checked_mul(26)?
            .checked_add(c.to_ascii_uppercase() as u32 - 'A' as u32 + 1)
    })? - 1;
    let row: u32 = digits.parse().ok()?;
    (row >= 1).then(|| (row - 1, col))
}

/// The Excel name of a 0-based column: 0 -> A, 27 -> AB.
pub fn column_letter(mut col: u32) -> String {
    let mut letters = Vec::new();
    loop {
        letters.push((b'A' + (col % 26) as u8) as char);
        if col < 26 {
            break;
        }
        col = col / 26 - 1;
    }
    letters.iter().rev().collect()
}

/// How to turn a cell range into a table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanOptions {
    /// Whether the first row holds column names.
    pub header: bool,
    /// The columns to return, by name; all if `None`.
    pub columns: Option<Vec<String>>,
    /// The maximum number of rows to return.
    pub limit: Option<usize>,
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            header: true,
            columns: None,
            limit: None,
        }
    }
}

/// A column's Arrow type, inferred from all of its data cells.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Empty,
    Int,
    Float,
    Bool,
    DateTime,
    String,
}

impl Kind {
    fn of(cell: &Data) -> Kind {
        match cell {
            Data::Empty => Kind::Empty,
            Data::Int(_) => Kind::Int,
            // Excel has no integer type: xlsx stores every number as a
            // float. Whole numbers that a double holds exactly count as
            // integers, as pandas does.
            Data::Float(f) if as_exact_int(*f).is_some() => Kind::Int,
            Data::Float(_) => Kind::Float,
            Data::Bool(_) => Kind::Bool,
            Data::DateTime(_) => Kind::DateTime,
            Data::String(_) | Data::DateTimeIso(_) | Data::DurationIso(_) | Data::Error(_) => {
                Kind::String
            }
        }
    }

    fn merge(self, other: Kind) -> Kind {
        match (self, other) {
            (a, b) if a == b => a,
            (Kind::Empty, k) | (k, Kind::Empty) => k,
            (Kind::Int, Kind::Float) | (Kind::Float, Kind::Int) => Kind::Float,
            _ => Kind::String,
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Kind::Int => DataType::Int64,
            Kind::Float => DataType::Float64,
            Kind::Bool => DataType::Boolean,
            Kind::DateTime => DataType::Timestamp(TimeUnit::Microsecond, None),
            Kind::Empty | Kind::String => DataType::Utf8,
        }
    }
}

/// The columns of a cell range: names and kinds.
pub struct Layout {
    names: Vec<String>,
    kinds: Vec<Kind>,
    header: bool,
}

impl Layout {
    pub fn infer(range: &Range<Data>, header: bool) -> Self {
        let (_, width) = range.get_size();
        let first_col = range.start().map_or(0, |(_, c)| c);
        let mut rows = range.rows();
        let header_row = if header { rows.next() } else { None };

        let mut kinds = vec![Kind::Empty; width];
        for row in rows {
            for (kind, cell) in kinds.iter_mut().zip(row) {
                *kind = kind.merge(Kind::of(cell));
            }
        }

        let mut seen = HashSet::new();
        let names = (0..width)
            .map(|i| {
                let letter = column_letter(first_col + i as u32);
                let base = header_row
                    .and_then(|row| row.get(i))
                    .map(|cell| cell.to_string().trim().to_string())
                    .filter(|name| !name.is_empty())
                    .unwrap_or(letter);
                let mut name = base.clone();
                let mut n = 2;
                while !seen.insert(name.clone()) {
                    name = format!("{base}_{n}");
                    n += 1;
                }
                name
            })
            .collect();
        Self {
            names,
            kinds,
            header,
        }
    }

    pub fn schema(&self) -> Schema {
        Schema::new(
            self.names
                .iter()
                .zip(&self.kinds)
                .map(|(name, kind)| Field::new(name, kind.data_type(), true))
                .collect::<Vec<_>>(),
        )
    }

    /// The indices of the requested columns.
    pub fn select(&self, columns: Option<&[String]>) -> Result<Vec<usize>> {
        let Some(columns) = columns else {
            return Ok((0..self.names.len()).collect());
        };
        columns
            .iter()
            .map(|c| {
                self.names.iter().position(|n| n == c).ok_or_else(|| {
                    Error::with_message_and_status(
                        format!("no column '{c}'; the columns are {:?}", self.names),
                        Status::InvalidArguments,
                    )
                })
            })
            .collect()
    }

    pub fn projected_schema(&self, selected: &[usize]) -> SchemaRef {
        let schema = self.schema();
        Arc::new(Schema::new(
            selected
                .iter()
                .map(|i| schema.field(*i).clone())
                .collect::<Vec<_>>(),
        ))
    }

    /// Convert the data rows to batches.
    pub fn batches(
        &self,
        range: &Range<Data>,
        selected: &[usize],
        limit: Option<usize>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        let schema = self.projected_schema(selected);
        let rows: Vec<&[Data]> = range
            .rows()
            .skip(self.header as usize)
            .take(limit.unwrap_or(usize::MAX))
            .collect();
        let mut batches = Vec::new();
        for chunk in rows.chunks(BATCH_ROWS) {
            let columns: Vec<ArrayRef> = selected
                .iter()
                .map(|col| build_column(self.kinds[*col], chunk, *col))
                .collect();
            batches.push(RecordBatch::try_new(schema.clone(), columns)?);
        }
        Ok((schema, batches))
    }
}

/// The integer value of a float that is a whole number within ±2^53.
fn as_exact_int(f: f64) -> Option<i64> {
    const LIMIT: f64 = 9_007_199_254_740_992.0; // 2^53
    (f.fract() == 0.0 && f.abs() <= LIMIT).then_some(f as i64)
}

fn build_column(kind: Kind, rows: &[&[Data]], col: usize) -> ArrayRef {
    let cells = rows.iter().map(|row| row.get(col).unwrap_or(&Data::Empty));
    match kind {
        Kind::Int => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for cell in cells {
                b.append_option(match cell {
                    Data::Int(i) => Some(*i),
                    Data::Float(f) => as_exact_int(*f),
                    _ => None,
                });
            }
            Arc::new(b.finish())
        }
        Kind::Float => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for cell in cells {
                b.append_option(match cell {
                    Data::Int(i) => Some(*i as f64),
                    Data::Float(f) => Some(*f),
                    _ => None,
                });
            }
            Arc::new(b.finish())
        }
        Kind::Bool => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for cell in cells {
                b.append_option(match cell {
                    Data::Bool(v) => Some(*v),
                    _ => None,
                });
            }
            Arc::new(b.finish())
        }
        Kind::DateTime => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
            for cell in cells {
                b.append_option(match cell {
                    Data::DateTime(dt) => dt.as_datetime().map(|d| d.and_utc().timestamp_micros()),
                    _ => None,
                });
            }
            Arc::new(b.finish())
        }
        Kind::Empty | Kind::String => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 8);
            for cell in cells {
                match cell {
                    Data::Empty => b.append_null(),
                    other => b.append_value(other.to_string()),
                }
            }
            Arc::new(b.finish())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn references() {
        assert_eq!(
            parse_reference("Sheet1!$A$1:$C$3"),
            Some(("Sheet1".into(), ((0, 0), (2, 2))))
        );
        assert_eq!(
            parse_reference("='My ''Q1'' Sheet'!B2"),
            Some(("My 'Q1' Sheet".into(), ((1, 1), (1, 1))))
        );
        assert_eq!(
            parse_reference("Data!C10:A2"),
            Some(("Data".into(), ((1, 0), (9, 2))))
        );
        assert_eq!(parse_reference("SUM(A1:A3)"), None);
        assert_eq!(parse_reference("Sheet1!A0"), None);
        assert_eq!(parse_cell("AB12"), Some((11, 27)));
        assert_eq!(column_letter(0), "A");
        assert_eq!(column_letter(27), "AB");
        assert_eq!(column_letter(701), "ZZ");
        assert_eq!(column_letter(702), "AAA");
    }

    #[test]
    fn kinds_merge() {
        assert_eq!(Kind::Int.merge(Kind::Float), Kind::Float);
        assert_eq!(Kind::Empty.merge(Kind::Bool), Kind::Bool);
        assert_eq!(Kind::Int.merge(Kind::String), Kind::String);
        assert_eq!(Kind::DateTime.merge(Kind::Int), Kind::String);
        assert_eq!(Kind::of(&Data::Float(3.0)), Kind::Int);
        assert_eq!(Kind::of(&Data::Float(3.5)), Kind::Float);
        assert_eq!(Kind::of(&Data::Float(1e300)), Kind::Float);
        assert_eq!(as_exact_int(-42.0), Some(-42));
    }
}

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

//! Encoding and decoding of `GetInfo` results
//! ([GET_INFO_SCHEMA][crate::schemas::GET_INFO_SCHEMA]), as returned by
//! [Connection::get_info][crate::Connection::get_info] and
//! [Driver::get_driver_info][crate::Driver::get_driver_info].

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type, UInt32Type};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, RecordBatchIterator,
    RecordBatchReader, StringArray, UInt32Array, UnionArray, new_empty_array,
};
use arrow_buffer::ScalarBuffer;
use arrow_schema::DataType;

use crate::error::{Error, Result, Status};
use crate::options::InfoCode;
use crate::schemas::GET_INFO_SCHEMA;

/// One `info_value`. The `int32_to_int32_list_map` member is not supported.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InfoValue {
    String(String),
    Bool(bool),
    Int64(i64),
    Int32Bitmask(i32),
    StringList(Vec<String>),
}

impl InfoValue {
    fn type_id(&self) -> i8 {
        match self {
            InfoValue::String(_) => 0,
            InfoValue::Bool(_) => 1,
            InfoValue::Int64(_) => 2,
            InfoValue::Int32Bitmask(_) => 3,
            InfoValue::StringList(_) => 4,
        }
    }
}

/// Build a `GetInfo` result from `(code, value)` rows, keeping only the
/// requested codes (all of them if `codes` is `None`).
pub fn info_reader(
    rows: impl IntoIterator<Item = (InfoCode, InfoValue)>,
    codes: Option<&HashSet<InfoCode>>,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let rows: Vec<(InfoCode, InfoValue)> = rows
        .into_iter()
        .filter(|(code, _)| codes.is_none_or(|c| c.contains(code)))
        .collect();
    let batch = info_batch(&rows)?;
    Ok(Box::new(RecordBatchIterator::new(
        vec![Ok(batch)],
        GET_INFO_SCHEMA.clone(),
    )))
}

/// Build one `GetInfo` batch from `(code, value)` rows.
pub fn info_batch(rows: &[(InfoCode, InfoValue)]) -> Result<RecordBatch> {
    let DataType::Union(fields, _) = GET_INFO_SCHEMA.field(1).data_type() else {
        unreachable!("info_value is a union");
    };

    let mut strings = Vec::new();
    let mut bools = Vec::new();
    let mut int64s = Vec::new();
    let mut bitmasks = Vec::new();
    let mut lists = ListBuilder::new(StringBuilder::new());
    let mut list_len = 0;
    let mut offsets = Vec::with_capacity(rows.len());
    for (_, value) in rows {
        let offset = match value {
            InfoValue::String(s) => {
                strings.push(s.clone());
                strings.len() - 1
            }
            InfoValue::Bool(b) => {
                bools.push(*b);
                bools.len() - 1
            }
            InfoValue::Int64(i) => {
                int64s.push(*i);
                int64s.len() - 1
            }
            InfoValue::Int32Bitmask(i) => {
                bitmasks.push(*i);
                bitmasks.len() - 1
            }
            InfoValue::StringList(items) => {
                for item in items {
                    lists.values().append_value(item);
                }
                lists.append(true);
                list_len += 1;
                list_len - 1
            }
        };
        offsets.push(offset as i32);
    }

    let field_type = |i: usize| fields.iter().nth(i).unwrap().1.data_type().clone();
    // The builder's item field must match the schema's exactly.
    let lists: ArrayRef = {
        let list = lists.finish();
        let DataType::List(item) = field_type(4) else {
            unreachable!("string_list is a list");
        };
        Arc::new(arrow_array::ListArray::new(
            item,
            list.offsets().clone(),
            list.values().clone(),
            list.nulls().cloned(),
        ))
    };
    let children: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(strings)),
        Arc::new(BooleanArray::from(bools)),
        Arc::new(Int64Array::from(int64s)),
        Arc::new(Int32Array::from(bitmasks)),
        lists,
        new_empty_array(&field_type(5)),
    ];
    let type_ids: ScalarBuffer<i8> = rows.iter().map(|(_, v)| v.type_id()).collect();
    let values = UnionArray::try_new(fields.clone(), type_ids, Some(offsets.into()), children)?;
    let names = UInt32Array::from(rows.iter().map(|(c, _)| u32::from(c)).collect::<Vec<_>>());
    Ok(RecordBatch::try_new(
        GET_INFO_SCHEMA.clone(),
        vec![Arc::new(names), Arc::new(values)],
    )?)
}

/// Decode a `GetInfo` result. Rows whose value is null or of an unsupported
/// type are skipped.
pub fn read_info(reader: impl RecordBatchReader) -> Result<Vec<(InfoCode, InfoValue)>> {
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        let invalid = |what: &str| {
            Error::with_message_and_status(
                format!("GetInfo result has an invalid {what} column"),
                Status::InvalidData,
            )
        };
        let names = batch
            .column(0)
            .as_primitive_opt::<UInt32Type>()
            .ok_or_else(|| invalid("info_name"))?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<UnionArray>()
            .ok_or_else(|| invalid("info_value"))?;
        for row in 0..batch.num_rows() {
            let code = InfoCode::from(names.value(row));
            let child = values.child(values.type_id(row));
            let offset = values.value_offset(row);
            if child.is_null(offset) {
                continue;
            }
            let value = match values.type_id(row) {
                0 => InfoValue::String(child.as_string::<i32>().value(offset).to_string()),
                1 => InfoValue::Bool(child.as_boolean().value(offset)),
                2 => InfoValue::Int64(child.as_primitive::<Int64Type>().value(offset)),
                3 => InfoValue::Int32Bitmask(child.as_primitive::<Int32Type>().value(offset)),
                4 => {
                    let list = child.as_list::<i32>().value(offset);
                    let list = list.as_string::<i32>();
                    InfoValue::StringList(
                        (0..list.len())
                            .filter(|i| list.is_valid(*i))
                            .map(|i| list.value(i).to_string())
                            .collect(),
                    )
                }
                _ => continue,
            };
            out.push((code, value));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants;

    #[test]
    fn round_trip() {
        let rows = vec![
            (InfoCode::DriverName, InfoValue::String("Excel".into())),
            (
                InfoCode::Other(constants::ADBC_INFO_DRIVER_ACCEPTS_FILESYSTEM),
                InfoValue::Bool(true),
            ),
            (InfoCode::DriverAdbcVersion, InfoValue::Int64(1_002_000)),
            (
                InfoCode::Other(constants::ADBC_INFO_DRIVER_FILE_EXTENSIONS),
                InfoValue::StringList(vec!["xlsx".into(), "xls".into()]),
            ),
            (InfoCode::DriverVersion, InfoValue::String("0.1".into())),
        ];
        let all = read_info(info_reader(rows.clone(), None).unwrap()).unwrap();
        assert_eq!(all, rows);

        let wanted: HashSet<InfoCode> = [InfoCode::DriverName].into();
        let some = read_info(info_reader(rows, Some(&wanted)).unwrap()).unwrap();
        assert_eq!(
            some,
            vec![(InfoCode::DriverName, InfoValue::String("Excel".into()))]
        );
    }
}

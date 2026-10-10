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

//! `GetObjects` for a workbook: one catalog and one schema (both unnamed)
//! holding the sheets and named ranges.
//!
//! The nested result is built bottom-up from plain vectors (columns, then
//! tables, then schemas, then catalogs); every field not provided is null.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, Int32Array, ListArray, RecordBatch, StringArray, StructArray, new_null_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, FieldRef, Fields, Schema};

use adbc_core::error::Result;
use adbc_core::options::ObjectDepth;
use adbc_core::schemas::GET_OBJECTS_SCHEMA;

pub struct ObjectTable {
    pub name: String,
    pub table_type: &'static str,
    /// Column names in order; `None` when columns were not requested.
    pub columns: Option<Vec<String>>,
}

/// The item field and struct fields of a `list<struct<...>>` type.
fn list_of_struct(data_type: &DataType) -> (FieldRef, Fields) {
    let DataType::List(item) = data_type else {
        panic!("expected a list, got {data_type}");
    };
    let DataType::Struct(fields) = item.data_type() else {
        panic!("expected a list of structs, got {data_type}");
    };
    (item.clone(), fields.clone())
}

/// A struct array of `len` rows from the named arrays; other fields null.
fn struct_of(
    fields: &Fields,
    len: usize,
    mut provided: HashMap<&str, ArrayRef>,
) -> Result<StructArray> {
    let arrays = fields
        .iter()
        .map(|f| {
            provided
                .remove(f.name().as_str())
                .unwrap_or_else(|| new_null_array(f.data_type(), len))
        })
        .collect();
    Ok(StructArray::try_new(fields.clone(), arrays, None)?)
}

/// A list array over `values` with one list per entry of `lengths`; `None`
/// entries are null lists.
fn list_of(item: FieldRef, values: StructArray, lengths: &[Option<usize>]) -> Result<ListArray> {
    let offsets = OffsetBuffer::from_lengths(lengths.iter().map(|l| l.unwrap_or(0)));
    let nulls = NullBuffer::from(lengths.iter().map(Option::is_some).collect::<Vec<_>>());
    let nulls = (nulls.null_count() > 0).then_some(nulls);
    Ok(ListArray::try_new(item, offsets, Arc::new(values), nulls)?)
}

/// Build the `GetObjects` result for a workbook's (already filtered) tables.
pub fn get_objects(depth: ObjectDepth, tables: Vec<ObjectTable>) -> Result<RecordBatch> {
    let schema: &Schema = &GET_OBJECTS_SCHEMA;
    let (schema_item, schema_fields) = list_of_struct(schema.field(1).data_type());
    let (table_item, table_fields) = list_of_struct(
        schema_fields
            .find("db_schema_tables")
            .unwrap()
            .1
            .data_type(),
    );
    let (column_item, column_fields) =
        list_of_struct(table_fields.find("table_columns").unwrap().1.data_type());

    // Columns of all tables, flattened.
    let mut column_names = Vec::new();
    let mut ordinals = Vec::new();
    let mut column_lengths = Vec::new();
    for table in &tables {
        match (&table.columns, depth) {
            (Some(columns), ObjectDepth::All | ObjectDepth::Columns) => {
                for (i, name) in columns.iter().enumerate() {
                    column_names.push(name.clone());
                    ordinals.push(i as i32 + 1);
                }
                column_lengths.push(Some(columns.len()));
            }
            _ => column_lengths.push(None),
        }
    }
    let columns = struct_of(
        &column_fields,
        column_names.len(),
        HashMap::from([
            (
                "column_name",
                Arc::new(StringArray::from(column_names)) as ArrayRef,
            ),
            (
                "ordinal_position",
                Arc::new(Int32Array::from(ordinals)) as ArrayRef,
            ),
        ]),
    )?;
    let table_columns = list_of(column_item, columns, &column_lengths)?;

    let table_count = tables.len();
    let tables_struct = struct_of(
        &table_fields,
        table_count,
        HashMap::from([
            (
                "table_name",
                Arc::new(StringArray::from_iter_values(
                    tables.iter().map(|t| &t.name),
                )) as ArrayRef,
            ),
            (
                "table_type",
                Arc::new(StringArray::from_iter_values(
                    tables.iter().map(|t| t.table_type),
                )) as ArrayRef,
            ),
            ("table_columns", Arc::new(table_columns) as ArrayRef),
        ]),
    )?;
    let with_tables = !matches!(depth, ObjectDepth::Catalogs | ObjectDepth::Schemas);
    let db_schema_tables = list_of(
        table_item,
        tables_struct,
        &[with_tables.then_some(table_count)],
    )?;

    let schemas = struct_of(
        &schema_fields,
        1,
        HashMap::from([
            ("db_schema_name", new_null_array(&DataType::Utf8, 1)),
            ("db_schema_tables", Arc::new(db_schema_tables) as ArrayRef),
        ]),
    )?;
    let with_schemas = !matches!(depth, ObjectDepth::Catalogs);
    let catalog_db_schemas = list_of(schema_item, schemas, &[with_schemas.then_some(1)])?;

    Ok(RecordBatch::try_new(
        GET_OBJECTS_SCHEMA.clone(),
        vec![
            new_null_array(&DataType::Utf8, 1),
            Arc::new(catalog_db_schemas),
        ],
    )?)
}

/// SQL `LIKE` matching, with `%` and `_` wildcards (no escapes).
pub fn like(pattern: &str, value: &str) -> bool {
    fn go(p: &[char], v: &[char]) -> bool {
        match p.split_first() {
            None => v.is_empty(),
            Some(('%', rest)) => (0..=v.len()).any(|i| go(rest, &v[i..])),
            Some(('_', rest)) => !v.is_empty() && go(rest, &v[1..]),
            Some((c, rest)) => v.first() == Some(c) && go(rest, &v[1..]),
        }
    }
    let p: Vec<char> = pattern.chars().collect();
    let v: Vec<char> = value.chars().collect();
    go(&p, &v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use arrow_array::cast::AsArray;

    #[test]
    fn like_patterns() {
        assert!(like("Sheet%", "Sheet1"));
        assert!(like("%1", "Sheet1"));
        assert!(like("S_eet1", "Sheet1"));
        assert!(!like("Sheet", "Sheet1"));
        assert!(like("%", ""));
    }

    #[test]
    fn nested_result_is_valid() {
        let tables = vec![
            ObjectTable {
                name: "Orders".into(),
                table_type: "SHEET",
                columns: Some(vec!["id".into(), "amount".into()]),
            },
            ObjectTable {
                name: "Prices".into(),
                table_type: "NAMED_RANGE",
                columns: Some(vec!["sku".into()]),
            },
        ];
        let batch = get_objects(ObjectDepth::All, tables).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let schemas = batch.column(1).as_list::<i32>().value(0);
        let schemas = schemas.as_struct();
        let tables = schemas
            .column_by_name("db_schema_tables")
            .unwrap()
            .as_list::<i32>()
            .value(0);
        let tables = tables.as_struct();
        let names = tables
            .column_by_name("table_name")
            .unwrap()
            .as_string::<i32>();
        assert_eq!(names.value(1), "Prices");
        let columns = tables
            .column_by_name("table_columns")
            .unwrap()
            .as_list::<i32>();
        assert_eq!(columns.value(0).len(), 2);
        assert_eq!(columns.value(1).len(), 1);

        let batch = get_objects(ObjectDepth::Catalogs, vec![]).unwrap();
        assert!(batch.column(1).is_null(0));
    }
}

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""File drivers through the C++ driver manager (PROTOTYPE).

Loads the prototype Excel file driver (Rust) through this package's C++
driver manager, which hands it its built-in local file system. Set
ADBC_EXCEL_DRIVER to the driver library to run these tests.
"""

import datetime
import os
import pathlib

import pytest

import adbc_driver_manager

pyarrow = pytest.importorskip("pyarrow")
openpyxl = pytest.importorskip("openpyxl")

DRIVER = os.environ.get("ADBC_EXCEL_DRIVER")
pytestmark = pytest.mark.skipif(
    not DRIVER, reason="set ADBC_EXCEL_DRIVER to the Excel file driver library"
)

ADBC_INFO_DRIVER_NAME = 100
ADBC_INFO_DRIVER_ACCEPTS_FILESYSTEM = 300
ADBC_INFO_DRIVER_FILE_EXTENSIONS = 302


def _reader(handle):
    return pyarrow.RecordBatchReader._import_from_c(handle.address)


@pytest.fixture
def workbook(tmp_path: pathlib.Path) -> pathlib.Path:
    wb = openpyxl.Workbook()
    orders = wb.active
    orders.title = "Orders"
    orders.append(["id", "sku", "qty", "when"])
    orders.append([1, "A-1", 3, datetime.datetime(2026, 1, 2, 10, 30)])
    orders.append([2, "B-7", 1, datetime.datetime(2026, 2, 3)])
    orders.append([3, "A-1", 12, datetime.datetime(2026, 3, 4, 23, 59)])

    catalog = wb.create_sheet("Catalog")
    catalog["B2"], catalog["C2"] = "sku", "unit_price"
    for row, (sku, price) in enumerate([("A-1", 9.0), ("B-7", 115.5)], start=3):
        catalog[f"B{row}"], catalog[f"C{row}"] = sku, price
    wb.defined_names["Prices"] = openpyxl.workbook.defined_name.DefinedName(
        "Prices", attr_text="Catalog!$B$2:$C$4"
    )

    path = tmp_path / "budget.xlsx"
    wb.save(path)
    return path


@pytest.fixture
def database(workbook):
    with adbc_driver_manager.AdbcDatabase(
        driver=DRIVER, entrypoint="AdbcExcelInit", uri=str(workbook)
    ) as db:
        yield db


def _scan(conn, **options):
    with adbc_driver_manager.AdbcStatement(conn) as stmt:
        stmt.set_options(**options)
        handle, _ = stmt.execute_query()
        return _reader(handle).read_all()


def test_driver_info(database) -> None:
    info = _reader(database.get_driver_info()).read_all().to_pylist()
    values = {row["info_name"]: row["info_value"] for row in info}
    assert values[ADBC_INFO_DRIVER_ACCEPTS_FILESYSTEM] is True
    assert "xlsx" in values[ADBC_INFO_DRIVER_FILE_EXTENSIONS]

    only = _reader(database.get_driver_info([ADBC_INFO_DRIVER_NAME])).read_all()
    assert only.num_rows == 1


def test_scan_sheet_and_named_range(database) -> None:
    # The C++ manager resolves the uri to its local file system and hands it
    # to the (Rust) driver at connection init.
    with adbc_driver_manager.AdbcConnection(database) as conn:
        orders = _scan(conn, **{"adbc.file.target": "Orders"})
        assert orders.num_rows == 3
        assert orders.schema.field("qty").type == pyarrow.int64()
        assert pyarrow.types.is_timestamp(orders.schema.field("when").type)

        some = _scan(
            conn,
            **{
                "adbc.file.target": "Orders",
                "adbc.file.columns": '["sku", "qty"]',
                "adbc.file.limit": "2",
            },
        )
        assert some.column_names == ["sku", "qty"]
        assert some.to_pydict() == {"sku": ["A-1", "B-7"], "qty": [3, 1]}

        prices = _scan(conn, **{"adbc.file.target": "Prices"})
        assert prices.to_pydict() == {"sku": ["A-1", "B-7"], "unit_price": [9.0, 115.5]}


def test_sql_is_not_supported(database) -> None:
    with adbc_driver_manager.AdbcConnection(database) as conn:
        with adbc_driver_manager.AdbcStatement(conn) as stmt:
            with pytest.raises(adbc_driver_manager.NotSupportedError):
                stmt.set_sql_query("SELECT * FROM Orders")


def test_missing_workbook(tmp_path) -> None:
    with adbc_driver_manager.AdbcDatabase(
        driver=DRIVER,
        entrypoint="AdbcExcelInit",
        uri=str(tmp_path / "missing.xlsx"),
    ) as db:
        with pytest.raises(adbc_driver_manager.ProgrammingError):
            adbc_driver_manager.AdbcConnection(db)

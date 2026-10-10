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

#if defined(_WIN32)
#include <windows.h>
#endif

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>  // NOLINT [build/c++17]
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <toml++/toml.hpp>

#include "adbc_driver_manager_internal.h"
#include "arrow-adbc/adbc.h"
#include "arrow-adbc/adbc_driver_manager.h"
#include "current_arch.h"
#include "driver/framework/utility.h"
#include "validation/adbc_validation.h"
#include "validation/adbc_validation_util.h"

// Tests of the SQLite example driver, except using the driver manager

namespace adbc {

using adbc_validation::Handle;
using adbc_validation::IsOkStatus;
using adbc_validation::IsStatus;

namespace {
void SetDriverPath(const char* path) {
#ifdef _WIN32
  int size_needed = MultiByteToWideChar(CP_UTF8, 0, path, -1, nullptr, 0);
  std::wstring wpath(size_needed, 0);
  MultiByteToWideChar(CP_UTF8, 0, path, -1, &wpath[0], size_needed);
  ASSERT_TRUE(SetEnvironmentVariableW(L"ADBC_DRIVER_PATH", wpath.c_str()));
#else
  setenv("ADBC_DRIVER_PATH", path, 1);
#endif
}

void UnsetDriverPath() { SetDriverPath(""); }

void SetProfilePath(const char* path) {
#ifdef _WIN32
  int size_needed = MultiByteToWideChar(CP_UTF8, 0, path, -1, nullptr, 0);
  std::wstring wpath(size_needed, 0);
  MultiByteToWideChar(CP_UTF8, 0, path, -1, &wpath[0], size_needed);
  ASSERT_TRUE(SetEnvironmentVariableW(L"ADBC_PROFILE_PATH", wpath.c_str()));
#else
  setenv("ADBC_PROFILE_PATH", path, 1);
#endif
}

void UnsetProfilePath() { SetProfilePath(""); }
}  // namespace

class DriverManager : public ::testing::Test {
 public:
  void SetUp() override {
    std::memset(&driver, 0, sizeof(driver));
    std::memset(&error, 0, sizeof(error));

    ASSERT_THAT(AdbcLoadDriver("adbc_driver_sqlite", nullptr, ADBC_VERSION_1_1_0, &driver,
                               &error),
                IsOkStatus(&error));
  }

  void TearDown() override {
    if (error.release) {
      error.release(&error);
    }

    if (driver.release) {
      ASSERT_THAT(driver.release(&driver, &error), IsOkStatus(&error));
      ASSERT_EQ(driver.private_data, nullptr);
      ASSERT_EQ(driver.private_manager, nullptr);
    }
  }

 protected:
  struct AdbcDriver driver = {};
  struct AdbcError error = {};
};

TEST_F(DriverManager, DatabaseCustomInitFunc) {
  adbc_validation::Handle<struct AdbcDatabase> database;

  // Explicitly set entrypoint
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(
      AdbcDatabaseSetOption(&database.value, "driver", "adbc_driver_sqlite", &error),
      IsOkStatus(&error));
  ASSERT_THAT(
      AdbcDatabaseSetOption(&database.value, "entrypoint", "AdbcDriverInit", &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));

  // Set invalid entrypoint
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(
      AdbcDatabaseSetOption(&database.value, "driver", "adbc_driver_sqlite", &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "entrypoint",
                                    "ThisSymbolDoesNotExist", &error),
              IsOkStatus(&error));
  ASSERT_EQ(ADBC_STATUS_INTERNAL, AdbcDatabaseInit(&database.value, &error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));
}

TEST_F(DriverManager, ConnectionOptions) {
  adbc_validation::Handle<struct AdbcDatabase> database;
  adbc_validation::Handle<struct AdbcConnection> connection;

  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(
      AdbcDatabaseSetOption(&database.value, "driver", "adbc_driver_sqlite", &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));

  ASSERT_THAT(AdbcConnectionNew(&connection.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionSetOption(&connection.value, "foo", "bar", &error),
              IsOkStatus(&error));
  ASSERT_EQ(ADBC_STATUS_NOT_IMPLEMENTED,
            AdbcConnectionInit(&connection.value, &database.value, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("Unknown connection option foo='bar'"));

  ASSERT_THAT(AdbcConnectionRelease(&connection.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));
}

TEST_F(DriverManager, GetOptionDatabase) {
  adbc_validation::Handle<struct AdbcDatabase> database;

  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(
      AdbcDatabaseSetOption(&database.value, "driver", "adbc_driver_sqlite", &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "stringoption", "1", &error),
              IsOkStatus(&error));
  std::vector<uint8_t> bytes_value;
  bytes_value.push_back(static_cast<uint8_t>(0));
  bytes_value.push_back(static_cast<uint8_t>(1));
  ASSERT_THAT(AdbcDatabaseSetOptionBytes(&database.value, "bytesoption",
                                         bytes_value.data(), bytes_value.size(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOptionDouble(&database.value, "doubleoption", 42.0, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOptionInt(&database.value, "intoption", 42, &error),
              IsOkStatus(&error));

  {
    double value = 0.0;
    ASSERT_THAT(
        AdbcDatabaseGetOptionDouble(&database.value, "doubleoption", &value, &error),
        IsOkStatus(&error));
    ASSERT_EQ(42.0, value);
  }

  {
    int64_t value = 0;
    ASSERT_THAT(AdbcDatabaseGetOptionInt(&database.value, "intoption", &value, &error),
                IsOkStatus(&error));
    ASSERT_EQ(42, value);
  }

  {
    std::vector<uint8_t> value(32, 42);
    size_t length = value.size();
    ASSERT_THAT(
        AdbcDatabaseGetOptionBytes(&database.value, "bytesoption",
                                   const_cast<uint8_t*>(value.data()), &length, &error),
        IsOkStatus(&error));
    ASSERT_EQ(2, length);
    ASSERT_EQ(0, value[0]);
    ASSERT_EQ(1, value[1]);

    ASSERT_THAT(
        AdbcDatabaseGetOptionBytes(&database.value, "nonexistent",
                                   const_cast<uint8_t*>(value.data()), &length, &error),
        IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  }

  {
    std::vector<char> value(32, 'A');
    size_t length = value.size();
    ASSERT_THAT(AdbcDatabaseGetOption(&database.value, "driver",
                                      const_cast<char*>(value.data()), &length, &error),
                IsOkStatus(&error));
    ASSERT_EQ(19, length);
    std::string actual(value.data(), 18);
    std::string expected = "adbc_driver_sqlite";
    ASSERT_EQ(expected, actual);

    ASSERT_THAT(AdbcDatabaseGetOption(&database.value, "stringoption",
                                      const_cast<char*>(value.data()), &length, &error),
                IsOkStatus(&error));
    ASSERT_EQ(2, length);
    actual = std::string(value.data(), 1);
    expected = "1";
    ASSERT_EQ(expected, actual);

    ASSERT_THAT(AdbcDatabaseGetOption(&database.value, "nonexistent",
                                      const_cast<char*>(value.data()), &length, &error),
                IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  }
}

TEST_F(DriverManager, GetOptionConnection) {
  adbc_validation::Handle<struct AdbcDatabase> database;
  adbc_validation::Handle<struct AdbcConnection> connection;

  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(
      AdbcDatabaseSetOption(&database.value, "driver", "adbc_driver_sqlite", &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));

  ASSERT_THAT(AdbcConnectionNew(&connection.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionSetOption(&connection.value, "stringoption", "1", &error),
              IsOkStatus(&error));
  std::vector<uint8_t> bytes_value;
  bytes_value.push_back(static_cast<uint8_t>(0));
  bytes_value.push_back(static_cast<uint8_t>(1));
  ASSERT_THAT(
      AdbcConnectionSetOptionBytes(&connection.value, "bytesoption", bytes_value.data(),
                                   bytes_value.size(), &error),
      IsOkStatus(&error));
  ASSERT_THAT(
      AdbcConnectionSetOptionDouble(&connection.value, "doubleoption", 42.0, &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionSetOptionInt(&connection.value, "intoption", 42, &error),
              IsOkStatus(&error));

  {
    double value = 0.0;
    ASSERT_THAT(
        AdbcConnectionGetOptionDouble(&connection.value, "doubleoption", &value, &error),
        IsOkStatus(&error));
    ASSERT_EQ(42.0, value);
  }

  {
    int64_t value = 0;
    ASSERT_THAT(
        AdbcConnectionGetOptionInt(&connection.value, "intoption", &value, &error),
        IsOkStatus(&error));
    ASSERT_EQ(42, value);
  }

  {
    std::vector<uint8_t> value(32, 42);
    size_t length = value.size();
    ASSERT_THAT(
        AdbcConnectionGetOptionBytes(&connection.value, "bytesoption",
                                     const_cast<uint8_t*>(value.data()), &length, &error),
        IsOkStatus(&error));
    ASSERT_EQ(2, length);
    ASSERT_EQ(0, value[0]);
    ASSERT_EQ(1, value[1]);

    ASSERT_THAT(
        AdbcConnectionGetOptionBytes(&connection.value, "nonexistent",
                                     const_cast<uint8_t*>(value.data()), &length, &error),
        IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  }

  {
    std::vector<char> value(32, 'A');
    size_t length = value.size();
    ASSERT_THAT(AdbcConnectionGetOption(&connection.value, "stringoption",
                                        const_cast<char*>(value.data()), &length, &error),
                IsOkStatus(&error));
    ASSERT_EQ(2, length);
    std::string actual(value.data(), 1);
    std::string expected = "1";
    ASSERT_EQ(expected, actual);

    ASSERT_THAT(AdbcConnectionGetOption(&connection.value, "nonexistent",
                                        const_cast<char*>(value.data()), &length, &error),
                IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  }
}

TEST_F(DriverManager, MultiDriverTest) {
  // Make sure two distinct drivers work in the same process (basic smoke test)
  adbc_validation::Handle<struct AdbcError> error;
  adbc_validation::Handle<struct AdbcDatabase> sqlite_db;
  adbc_validation::Handle<struct AdbcDatabase> postgres_db;
  adbc_validation::Handle<struct AdbcConnection> sqlite_conn;
  adbc_validation::Handle<struct AdbcConnection> postgres_conn;

  ASSERT_THAT(AdbcDatabaseNew(&sqlite_db.value, &error.value), IsOkStatus(&error.value));
  ASSERT_THAT(AdbcDatabaseNew(&postgres_db.value, &error.value),
              IsOkStatus(&error.value));

  ASSERT_THAT(AdbcDatabaseSetOption(&sqlite_db.value, "driver", "adbc_driver_sqlite",
                                    &error.value),
              IsOkStatus(&error.value));
  ASSERT_THAT(AdbcDatabaseSetOption(&postgres_db.value, "driver",
                                    "adbc_driver_postgresql", &error.value),
              IsOkStatus(&error.value));

  ASSERT_THAT(AdbcDatabaseInit(&sqlite_db.value, &error.value), IsOkStatus(&error.value));
  ASSERT_THAT(AdbcDatabaseInit(&postgres_db.value, &error.value),
              IsStatus(ADBC_STATUS_INVALID_STATE, &error.value));
  ASSERT_THAT(error->message,
              ::testing::HasSubstr(
                  "[libpq] Must set database option 'uri' before creating a connection"));
  error->release(&error.value);

  ASSERT_THAT(AdbcDatabaseSetOption(&postgres_db.value, "uri",
                                    "postgresql://localhost:5432", &error.value),
              IsOkStatus(&error.value));
  ASSERT_THAT(AdbcDatabaseSetOption(&sqlite_db.value, "unknown", "foo", &error.value),
              IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error.value));
  ASSERT_THAT(error->message,
              ::testing::HasSubstr("Unknown database option unknown='foo'"));
  error->release(&error.value);

  ASSERT_THAT(AdbcConnectionNew(&sqlite_conn.value, &error.value),
              IsOkStatus(&error.value));
  ASSERT_THAT(AdbcConnectionNew(&postgres_conn.value, &error.value),
              IsOkStatus(&error.value));

  ASSERT_THAT(AdbcConnectionInit(&sqlite_conn.value, &sqlite_db.value, &error.value),
              IsOkStatus(&error.value));
  ASSERT_THAT(AdbcConnectionInit(&postgres_conn.value, &postgres_db.value, &error.value),
              IsStatus(ADBC_STATUS_IO, &error.value));
  ASSERT_THAT(error->message, ::testing::HasSubstr("[libpq] Failed to connect"));
  error->release(&error.value);
}

TEST_F(DriverManager, NoDefaultEntrypoint) {
#if !defined(ADBC_DRIVER_MANAGER_ENTRYPOINT_TEST_LIB)
  GTEST_SKIP() << "ADBC_DRIVER_MANAGER_ENTRYPOINT_TEST_LIB is not defined";
#else
  adbc_validation::Handle<struct AdbcError> error;
  adbc_validation::Handle<struct AdbcDriver> driver;
  // Must fail with expected status
  ASSERT_THAT(AdbcLoadDriver(ADBC_DRIVER_MANAGER_ENTRYPOINT_TEST_LIB, nullptr,
                             ADBC_VERSION_1_1_0, &driver.value, &error.value),
              IsStatus(ADBC_STATUS_IO, &error.value));

#endif  // !defined(ADBC_DRIVER_MANAGER_ENTRYPOINT_TEST_LIB)
}

TEST_F(DriverManager, NoDefaultEntrypointFound) {
#if !defined(ADBC_DRIVER_MANAGER_NO_ENTRYPOINT_TEST_LIB)
  GTEST_SKIP() << "ADBC_DRIVER_MANAGER_NO_ENTRYPOINT_TEST_LIB is not defined";
#else
  adbc_validation::Handle<struct AdbcError> error;
  adbc_validation::Handle<struct AdbcDriver> driver;
  ASSERT_THAT(AdbcLoadDriver(ADBC_DRIVER_MANAGER_NO_ENTRYPOINT_TEST_LIB, nullptr,
                             ADBC_VERSION_1_1_0, &driver.value, &error.value),
              IsStatus(ADBC_STATUS_INTERNAL, &error.value));
  // Both symbols should not be found, should be mentioned in error message
  ASSERT_THAT(error->message,
              ::testing::HasSubstr("(AdbcDriverNoEntrypointInit) failed"));
  ASSERT_THAT(error->message, ::testing::HasSubstr("(AdbcDriverInit) failed"));

#endif  // !defined(ADBC_DRIVER_MANAGER_NO_ENTRYPOINT_TEST_LIB)
}

class SqliteQuirks : public adbc_validation::DriverQuirks {
 public:
  AdbcStatusCode SetupDatabase(struct AdbcDatabase* database,
                               struct AdbcError* error) const override {
    if (AdbcStatusCode res =
            AdbcDatabaseSetOption(database, "driver", "adbc_driver_sqlite", error);
        res != 0) {
      return res;
    }
    return AdbcDatabaseSetOption(
        database, "uri", "file:Sqlite_Transactions?mode=memory&cache=shared", error);
  }

  std::string BindParameter(int index) const override { return "?"; }

  ArrowType IngestSelectRoundTripType(ArrowType ingest_type) const override {
    switch (ingest_type) {
      case NANOARROW_TYPE_BOOL:
      case NANOARROW_TYPE_INT8:
      case NANOARROW_TYPE_INT16:
      case NANOARROW_TYPE_INT32:
      case NANOARROW_TYPE_INT64:
      case NANOARROW_TYPE_UINT8:
      case NANOARROW_TYPE_UINT16:
      case NANOARROW_TYPE_UINT32:
      case NANOARROW_TYPE_UINT64:
        return NANOARROW_TYPE_INT64;
      case NANOARROW_TYPE_HALF_FLOAT:
      case NANOARROW_TYPE_FLOAT:
        return NANOARROW_TYPE_DOUBLE;
      case NANOARROW_TYPE_LARGE_STRING:
      case NANOARROW_TYPE_STRING_VIEW:
        return NANOARROW_TYPE_STRING;
      case NANOARROW_TYPE_LARGE_BINARY:
      case NANOARROW_TYPE_FIXED_SIZE_BINARY:
      case NANOARROW_TYPE_BINARY_VIEW:
        return NANOARROW_TYPE_BINARY;
      case NANOARROW_TYPE_DATE32:
      case NANOARROW_TYPE_TIMESTAMP:
        return NANOARROW_TYPE_STRING;
      default:
        return ingest_type;
    }
  }

  bool supports_bulk_ingest(const char* mode) const override {
    return std::strcmp(mode, ADBC_INGEST_OPTION_MODE_APPEND) == 0 ||
           std::strcmp(mode, ADBC_INGEST_OPTION_MODE_CREATE) == 0;
  }
  bool supports_concurrent_statements() const override { return true; }
  bool supports_get_option() const override { return false; }
  std::optional<adbc_validation::SqlInfoValue> supports_get_sql_info(
      uint32_t info_code) const override {
    switch (info_code) {
      case ADBC_INFO_DRIVER_NAME:
        return "ADBC SQLite Driver";
      case ADBC_INFO_DRIVER_VERSION:
        return "(unknown)";
      case ADBC_INFO_VENDOR_NAME:
        return "SQLite";
      case ADBC_INFO_VENDOR_VERSION:
        return "3.";
      default:
        return std::nullopt;
    }
  }
  bool supports_metadata_current_catalog() const override { return true; }
  std::string catalog() const override { return "main"; }
};

class SqliteDatabaseTest : public ::testing::Test, public adbc_validation::DatabaseTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

 protected:
  SqliteQuirks quirks_;
};
ADBCV_TEST_DATABASE(SqliteDatabaseTest)

TEST_F(SqliteDatabaseTest, NullError) {
  Handle<AdbcConnection> conn;

  ASSERT_THAT(AdbcDatabaseNew(&database, nullptr), IsOkStatus());
  ASSERT_THAT(quirks()->SetupDatabase(&database, nullptr), IsOkStatus());
  ASSERT_THAT(AdbcDatabaseInit(&database, nullptr), IsOkStatus());

  ASSERT_THAT(AdbcConnectionNew(&conn.value, nullptr), IsOkStatus());
  ASSERT_THAT(AdbcConnectionInit(&conn.value, &database, nullptr), IsOkStatus());
  ASSERT_THAT(AdbcConnectionRelease(&conn.value, nullptr), IsOkStatus());

  ASSERT_THAT(AdbcDatabaseRelease(&database, nullptr), IsOkStatus());
}

class SqliteConnectionTest : public ::testing::Test,
                             public adbc_validation::ConnectionTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

 protected:
  SqliteQuirks quirks_;
};
ADBCV_TEST_CONNECTION(SqliteConnectionTest)

class SqliteStatementTest : public ::testing::Test,
                            public adbc_validation::StatementTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

  void TestSqlIngestUInt64() { GTEST_SKIP() << "Cannot ingest UINT64 (out of range)"; }
  void TestSqlIngestTimestamp() {
    GTEST_SKIP() << "Cannot ingest TIMESTAMP (not implemented)";
  }
  void TestSqlIngestTimestampTz() {
    GTEST_SKIP() << "Cannot ingest TIMESTAMP WITH TIMEZONE (not implemented)";
  }
  void TestSqlIngestDuration() {
    GTEST_SKIP() << "Cannot ingest DURATION (not implemented)";
  }
  void TestSqlIngestInterval() {
    GTEST_SKIP() << "Cannot ingest Interval (not implemented)";
  }
  void TestSqlIngestListOfInt32() {
    GTEST_SKIP() << "Cannot ingest list<int32> (not implemented)";
  }
  void TestSqlIngestListOfString() {
    GTEST_SKIP() << "Cannot ingest list<string> (not implemented)";
  }

 protected:
  SqliteQuirks quirks_;
};
ADBCV_TEST_STATEMENT(SqliteStatementTest)

// Exercise AdbcDatabaseGetOption with different value buffer lengths
TEST(AdbcDriverManagerInternal, DatabaseGetOptionValueBufferSize) {
  struct AdbcDatabase database = {};
  struct AdbcError error = {};

  ASSERT_THAT(AdbcDatabaseNew(&database, &error), IsOkStatus(&error));

  // Set an option we'll later fetch with a n oversized buffer
  ASSERT_THAT(AdbcDatabaseSetOption(&database, "driver", "test_driver", &error),
              IsOkStatus(&error));

  // Too small
  char buf_too_small[11];
  std::memset(buf_too_small, '*',
              sizeof(buf_too_small));  // Pre-fill with "*" just for debugging
  size_t len_too_small = sizeof(buf_too_small);
  ASSERT_THAT(
      AdbcDatabaseGetOption(&database, "driver", buf_too_small, &len_too_small, &error),
      IsOkStatus(&error));
  EXPECT_EQ(len_too_small, 12u);
  EXPECT_STRNE(buf_too_small, "test_driver");

  // Just right
  char buf_just_right[12];
  std::memset(buf_just_right, '*',
              sizeof(buf_just_right));  // Pre-fill with "*" just for debugging
  size_t len_just_right = sizeof(buf_just_right);
  ASSERT_THAT(
      AdbcDatabaseGetOption(&database, "driver", buf_just_right, &len_just_right, &error),
      IsOkStatus(&error));
  EXPECT_EQ(len_just_right, 12u);
  EXPECT_STREQ(buf_just_right, "test_driver");

  // Too large
  char buf_too_large[13];
  std::memset(buf_too_large, '*',
              sizeof(buf_too_large));  // Pre-fill with "*" just for debugging
  size_t len_too_large = sizeof(buf_too_large);
  ASSERT_THAT(
      AdbcDatabaseGetOption(&database, "driver", buf_too_large, &len_too_large, &error),
      IsOkStatus(&error));
  EXPECT_EQ(len_too_large, 12u);
  EXPECT_STREQ(buf_too_large, "test_driver");

  ASSERT_THAT(AdbcDatabaseRelease(&database, &error), IsOkStatus(&error));
}

TEST(AdbcDriverManagerInternal, InternalAdbcDriverManagerDefaultEntrypoint) {
  for (const auto& driver : {
           "adbc_driver_sqlite",
           "adbc_driver_sqlite.dll",
           "driver_sqlite",
           "libadbc_driver_sqlite",
           "libadbc_driver_sqlite.so",
           "libadbc_driver_sqlite.so.6.0.0",
           "/usr/lib/libadbc_driver_sqlite.so",
           "/usr/lib/libadbc_driver_sqlite.so.6.0.0",
           "C:\\System32\\adbc_driver_sqlite.dll",
       }) {
    SCOPED_TRACE(driver);
    EXPECT_EQ("AdbcDriverSqliteInit",
              ::InternalAdbcDriverManagerDefaultEntrypoint(driver));
  }

  for (const auto& driver : {
           "adbc_sqlite",
           "sqlite",
           "/usr/lib/sqlite.so",
           "C:\\System32\\sqlite.dll",
       }) {
    SCOPED_TRACE(driver);
    EXPECT_EQ("AdbcSqliteInit", ::InternalAdbcDriverManagerDefaultEntrypoint(driver));
  }

  for (const auto& driver : {
           "proprietary_engine",
           "libproprietary_engine.so.6.0.0",
           "/usr/lib/proprietary_engine.so",
           "C:\\System32\\proprietary_engine.dll",
       }) {
    SCOPED_TRACE(driver);
    EXPECT_EQ("AdbcProprietaryEngineInit",
              ::InternalAdbcDriverManagerDefaultEntrypoint(driver));
  }

  for (const auto& driver : {
           "driver_example",
           "libdriver_example.so",
       }) {
    SCOPED_TRACE(driver);
    EXPECT_EQ("AdbcDriverExampleInit",
              ::InternalAdbcDriverManagerDefaultEntrypoint(driver));
  }
}

TEST(AdbcDriverManagerInternal, InternalAdbcParsePath) {
  // Test parsing a path of directories
#ifdef _WIN32
  static const char* const delimiter = ";";
#else
  static const char* const delimiter = ":";
#endif

  std::vector<std::string> paths = {
      "/usr/lib/adbc/drivers", "/usr/local/lib/adbc/drivers",
      "/opt/adbc/drivers",     "/home/user/.config/adbc/drivers",
#ifdef _WIN32
      "/home/\":foo:\"/bar",
#endif
  };

  std::ostringstream joined;
  std::copy(paths.begin(), paths.end(),
            std::ostream_iterator<std::string>(joined, delimiter));

  auto output = InternalAdbcParsePath(joined.str());
  EXPECT_THAT(output, ::testing::ElementsAreArray(paths));
}

TEST(AdbcDriverManagerInternal, InternalAdbcParseDriverUri) {
  std::vector<std::pair<std::string, std::optional<ParseDriverUriResult>>> uris = {
      {"sqlite", std::nullopt},
      {"sqlite:", {{"sqlite", std::nullopt, std::nullopt}}},
      {"sqlite:file::memory:", {{"sqlite", "file::memory:", std::nullopt}}},
      {"sqlite:file::memory:?cache=shared",
       {{"sqlite", "file::memory:?cache=shared", std::nullopt}}},
      {"postgresql://a:b@localhost:9999/nonexistent",
       {{"postgresql", "postgresql://a:b@localhost:9999/nonexistent", std::nullopt}}},
      {"profile://foo_prof", {{"", std::nullopt, "foo_prof"}}},
      {"profile:///foo/bar/profile.toml", {{"", std::nullopt, "/foo/bar/profile.toml"}}},
  };

#ifdef _WIN32
  auto temp_dir = std::filesystem::temp_directory_path() / "adbc_driver_manager_tests";
  std::filesystem::create_directories(temp_dir);
  std::string temp_driver_path = temp_dir.string() + "\\driver.dll";
  std::ofstream temp_driver_file(temp_driver_path);
  temp_driver_file << "placeholder";
  temp_driver_file.close();

  uris.push_back({temp_driver_path, {{temp_driver_path, std::nullopt}}});
#endif

  auto cmp = [](std::optional<ParseDriverUriResult> a,
                std::optional<ParseDriverUriResult> b) {
    if (!a.has_value()) {
      EXPECT_FALSE(b.has_value());
      return;
    }
    EXPECT_EQ(a->driver, b->driver);
    if (!a->uri) {
      EXPECT_FALSE(b->uri);
    } else {
      EXPECT_EQ(*a->uri, *b->uri);
    }
  };

  for (const auto& [uri, expected] : uris) {
    std::string_view uri_view = uri;
    auto result = InternalAdbcParseDriverUri(uri_view);
    cmp(result, expected);
  }

#ifdef _WIN32
  std::filesystem::remove_all(temp_dir);
#endif
}

class DriverManifest : public ::testing::Test {
 public:
  void SetUp() override {
    std::memset(&driver, 0, sizeof(driver));
    std::memset(&error, 0, sizeof(error));

#ifndef ADBC_DRIVER_MANAGER_TEST_LIB
    GTEST_SKIP() << "ADBC_DRIVER_MANAGER_TEST_LIB is not defined. "
                    "This test requires a driver library to be specified.";
#else
    driver_path = std::filesystem::path(ADBC_DRIVER_MANAGER_TEST_LIB);
    if (!std::filesystem::exists(driver_path)) {
      GTEST_SKIP() << "Driver library does not exist: " << driver_path;
    }

    simple_manifest = toml::table{
        {"name", "SQLite3"},
        {"publisher", "arrow-adbc"},
        {"version", "X.Y.Z"},
        {"ADBC",
         toml::table{
             {"version", "1.1.0"},
         }},
        {"Driver",
         toml::table{
             {"shared",
              toml::table{
                  {InternalAdbcCurrentArch(), driver_path.string()},
              }},
         }},
    };

    temp_dir = std::filesystem::temp_directory_path() / "adbc_driver_manager_test";
    std::filesystem::create_directories(temp_dir);
#endif
  }

  void TearDown() override {
    if (error.release) {
      error.release(&error);
    }

    if (driver.release) {
      ASSERT_THAT(driver.release(&driver, &error), IsOkStatus(&error));
      ASSERT_EQ(driver.private_data, nullptr);
      ASSERT_EQ(driver.private_manager, nullptr);
    }

    driver_path.clear();
    if (std::filesystem::exists(temp_dir)) {
      std::filesystem::remove_all(temp_dir);
    }
  }

 protected:
  struct AdbcDriver driver = {};
  struct AdbcError error = {};

  std::filesystem::path driver_path;
  std::filesystem::path temp_dir;
  toml::table simple_manifest;
};

TEST_F(DriverManifest, LoadDriverEnv) {
  ASSERT_THAT(AdbcFindLoadDriver("test_sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              Not(IsOkStatus(&error)));

  std::ofstream test_manifest_file(temp_dir / "test_sqlite.toml");
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << simple_manifest;
  test_manifest_file.close();

  SetDriverPath(temp_dir.string().c_str());

  ASSERT_THAT(AdbcFindLoadDriver("test_sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsOkStatus(&error));

  ASSERT_TRUE(std::filesystem::remove(temp_dir / "test_sqlite.toml"));

  UnsetDriverPath();
}

TEST_F(DriverManifest, LoadNonAsciiPath) {
  ASSERT_THAT(AdbcFindLoadDriver("test_sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              Not(IsOkStatus(&error)));

#ifdef _WIN32
  std::filesystem::path non_ascii_dir = temp_dir / L"majestik møøse";
#else
  std::filesystem::path non_ascii_dir = temp_dir / "majestik møøse";
#endif

  ASSERT_TRUE(std::filesystem::create_directories(non_ascii_dir));

  std::ofstream test_manifest_file(non_ascii_dir / "test_sqlite.toml");
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << simple_manifest;
  test_manifest_file.close();

  SetDriverPath(non_ascii_dir.string().c_str());

  ASSERT_THAT(AdbcFindLoadDriver("test_sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsOkStatus(&error));

  ASSERT_TRUE(std::filesystem::remove(non_ascii_dir / "test_sqlite.toml"));

  UnsetDriverPath();
}

TEST_F(DriverManifest, DisallowEnvConfig) {
  std::ofstream test_manifest_file(temp_dir / "test_sqlite.toml");
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << simple_manifest;
  test_manifest_file.close();

  SetDriverPath(temp_dir.string().c_str());

  auto load_options = ADBC_LOAD_FLAG_DEFAULT & ~ADBC_LOAD_FLAG_SEARCH_ENV;
  ASSERT_THAT(AdbcFindLoadDriver("test_sqlite", nullptr, ADBC_VERSION_1_1_0, load_options,
                                 nullptr, &driver, &error),
              Not(IsOkStatus(&error)));

  ASSERT_TRUE(std::filesystem::remove(temp_dir / "test_sqlite.toml"));

  UnsetDriverPath();
}

TEST_F(DriverManifest, ConfigEntrypoint) {
  auto manifest_with_bad_entrypoint = simple_manifest;
  // Override the entrypoint in the manifest
  manifest_with_bad_entrypoint.erase("Driver");
  manifest_with_bad_entrypoint.insert(
      "Driver", toml::table{
                    {"entrypoint", "BadEntrypointSymbolName"},
                    {"shared",
                     toml::table{
                         {InternalAdbcCurrentArch(), driver_path.string()},
                     }},
                });

  auto filepath = temp_dir / "sqlite.toml";
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_with_bad_entrypoint;
  test_manifest_file.close();

  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              Not(IsOkStatus(&error)));

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, LoadAbsolutePath) {
  auto filepath = temp_dir / "sqlite.toml";
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << simple_manifest;
  test_manifest_file.close();

  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsOkStatus(&error));

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, LoadAbsolutePathNoExtension) {
  auto filepath = temp_dir / "sqlite.toml";
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << simple_manifest;
  test_manifest_file.close();

  auto noext = filepath;
  noext.replace_extension();  // Remove the .toml extension
  ASSERT_THAT(AdbcFindLoadDriver(noext.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsOkStatus(&error));

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, LoadRelativePath) {
  std::ofstream test_manifest_file("sqlite.toml");
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << simple_manifest;
  test_manifest_file.close();

  ASSERT_THAT(AdbcFindLoadDriver("sqlite.toml", nullptr, ADBC_VERSION_1_1_0, 0, nullptr,
                                 &driver, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));

  ASSERT_THAT(
      AdbcFindLoadDriver("sqlite.toml", nullptr, ADBC_VERSION_1_1_0,
                         ADBC_LOAD_FLAG_ALLOW_RELATIVE_PATHS, nullptr, &driver, &error),
      IsOkStatus(&error));

  ASSERT_TRUE(std::filesystem::remove("sqlite.toml"));
}

TEST_F(DriverManifest, NotFound) {
  ASSERT_THAT(AdbcFindLoadDriver("nosuchdriver", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  ASSERT_THAT(error.message,
              ::testing::HasSubstr("Also searched these paths for manifests:\n\tnot "
                                   "set: ADBC_DRIVER_PATH"));
}

TEST_F(DriverManifest, ManifestDriverMissing) {
  // Create a manifest without the "Driver" section
  auto filepath = temp_dir / "sqlite.toml";
  toml::table manifest_without_driver = simple_manifest;
  manifest_without_driver.erase("Driver");

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_without_driver;
  test_manifest_file.close();

  // Attempt to load the driver
  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));

  ASSERT_THAT(error.message, ::testing::HasSubstr("Driver path not defined in manifest"));
  ASSERT_THAT(error.message,
              ::testing::HasSubstr("`Driver.shared` must be a string or table"));
  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ManifestDriverMissingAdbcDatabase) {
  // Similar test as above but with AdbcDatabaseInit path and using the
  // additional search path.
  // Create a manifest without the "Driver" section
  auto filepath = temp_dir / "sqlite.toml";
  toml::table manifest_without_driver = simple_manifest;
  manifest_without_driver.erase("Driver");

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_without_driver;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "driver", "sqlite", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDriverManagerDatabaseSetLoadFlags(&database.value,
                                                    ADBC_LOAD_FLAG_DEFAULT, &error),
              IsOkStatus(&error));
  std::string search_path = temp_dir.string();
  ASSERT_THAT(AdbcDriverManagerDatabaseSetAdditionalSearchPathList(
                  &database.value, search_path.data(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("Driver path not defined in manifest"));
  ASSERT_THAT(error.message,
              ::testing::HasSubstr("`Driver.shared` must be a string or table"));

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ManifestDriverInvalid) {
  // "Driver" section is not a table
  auto filepath = temp_dir / "sqlite.toml";
  toml::table manifest_without_driver = simple_manifest;
  manifest_without_driver.erase("Driver");
  manifest_without_driver.insert("Driver", toml::table{{"shared", true}});

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_without_driver;
  test_manifest_file.close();

  // Attempt to load the driver
  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));

  ASSERT_THAT(error.message, ::testing::HasSubstr("Driver path not defined in manifest"));
  ASSERT_THAT(error.message,
              ::testing::HasSubstr("`Driver.shared` must be a string or table"));
  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ManifestDriverEmpty) {
  // "Driver" section is not a table
  auto filepath = temp_dir / "sqlite.toml";
  toml::table manifest_without_driver = simple_manifest;
  manifest_without_driver.erase("Driver");
  manifest_without_driver.insert("Driver", toml::table{{"shared", ""}});

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_without_driver;
  test_manifest_file.close();

  // Attempt to load the driver
  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));

  ASSERT_THAT(error.message,
              ::testing::HasSubstr("Driver path is an empty string in manifest"));
  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ManifestWrongArch) {
  auto filepath = temp_dir / "sqlite.toml";
  toml::table manifest_without_driver = simple_manifest;
  manifest_without_driver.erase("Driver");
  manifest_without_driver.insert("Driver",
                                 toml::table{
                                     {"shared",
                                      toml::table{
                                          {"non-existent", "path/to/bad/driver.so"},
                                          {"windows-alpha64", "path/to/bad/driver.so"},
                                      }},
                                 });

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_without_driver;
  test_manifest_file.close();

  // Attempt to load the driver
  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_NOT_FOUND, &error));

  ASSERT_THAT(error.message, ::testing::HasSubstr("Driver path not found in manifest"));
  ASSERT_THAT(error.message,
              ::testing::HasSubstr("Architectures found: non-existent windows-alpha64"));
  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ManifestDriverMissingArchAdbcDatabase) {
  // Similar test as above but with AdbcDatabaseInit path and using the
  // additional search path.
  // Create a manifest without the "Driver" section
  auto filepath = temp_dir / "test_sqlite.toml";
  toml::table manifest_without_driver = simple_manifest;
  manifest_without_driver.erase("Driver");
  manifest_without_driver.insert("Driver",
                                 toml::table{
                                     {"shared",
                                      toml::table{
                                          {"non-existent", "path/to/bad/driver.so"},
                                          {"windows-alpha64", "path/to/bad/driver.so"},
                                      }},
                                 });

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_without_driver;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "driver", "test_sqlite", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDriverManagerDatabaseSetLoadFlags(&database.value,
                                                    ADBC_LOAD_FLAG_DEFAULT, &error),
              IsOkStatus(&error));
  std::string search_path = temp_dir.string();
  ASSERT_THAT(AdbcDriverManagerDatabaseSetAdditionalSearchPathList(
                  &database.value, search_path.data(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("test_sqlite.toml but:"));
  ASSERT_THAT(error.message,
              ::testing::HasSubstr("Architectures found: non-existent windows-alpha64"));

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ManifestDriverPointsNowhere) {
  // Similar test as above but with AdbcDatabaseInit path and using the
  // additional search path.
  // Create a manifest without the "Driver" section
  auto filepath = temp_dir / "test_sqlite.toml";
  toml::table manifest_without_driver = simple_manifest;
  manifest_without_driver.erase("Driver");
  // The idea is that we can find the manifest, but not the driver it points to.
  manifest_without_driver.insert("Driver", toml::table{
                                               {"shared",
                                                toml::table{
                                                    {"linux_arm64", "adbc-goosedb"},
                                                    {"linux_amd64", "adbc-goosedb"},
                                                    {"macos_arm64", "adbc-goosedb"},
                                                    {"macos_amd64", "adbc-goosedb"},
                                                    {"windows_arm64", "adbc-goosedb"},
                                                    {"windows_amd64", "adbc-goosedb"},
                                                }},
                                           });

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_without_driver;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "driver", "test_sqlite", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDriverManagerDatabaseSetLoadFlags(&database.value,
                                                    ADBC_LOAD_FLAG_DEFAULT, &error),
              IsOkStatus(&error));
  std::string search_path = temp_dir.string();
  ASSERT_THAT(AdbcDriverManagerDatabaseSetAdditionalSearchPathList(
                  &database.value, search_path.data(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("test_sqlite.toml but:"));
  // Message is platform-specific but something like "dlopen() failed:
  // adbc-goosedb: cannot open shared object file..."
  ASSERT_THAT(error.message, ::testing::HasSubstr("adbc-goosedb"));

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ManifestArchPathEmpty) {
  auto filepath = temp_dir / "sqlite.toml";
  toml::table manifest_without_driver = simple_manifest;
  manifest_without_driver.erase("Driver");
  manifest_without_driver.insert("Driver", toml::table{
                                               {"shared",
                                                toml::table{
                                                    {"linux_arm64", ""},
                                                    {"linux_amd64", ""},
                                                    {"macos_arm64", ""},
                                                    {"macos_amd64", ""},
                                                    {"windows_arm64", ""},
                                                    {"windows_amd64", ""},
                                                }},
                                           });

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_without_driver;
  test_manifest_file.close();

  // Attempt to load the driver
  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));

  ASSERT_THAT(error.message,
              ::testing::HasSubstr("Driver path is an empty string in manifest"));
  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ManifestArchPathInvalid) {
  auto filepath = temp_dir / "sqlite.toml";
  toml::table manifest_without_driver = simple_manifest;
  manifest_without_driver.erase("Driver");
  manifest_without_driver.insert("Driver", toml::table{
                                               {"shared",
                                                toml::table{
                                                    {"linux_arm64", 42},
                                                    {"linux_amd64", 42},
                                                    {"macos_arm64", 42},
                                                    {"macos_amd64", 42},
                                                    {"windows_arm64", 42},
                                                    {"windows_amd64", 42},
                                                }},
                                           });

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_without_driver;
  test_manifest_file.close();

  // Attempt to load the driver
  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));

  ASSERT_THAT(error.message, ::testing::HasSubstr("Driver path not found in manifest"));
  ASSERT_THAT(error.message, ::testing::HasSubstr("Value was not a string"));
  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ManifestEntrypointInvalid) {
  auto filepath = temp_dir / "sqlite.toml";
  toml::table manifest_without_driver = simple_manifest;
  manifest_without_driver.erase("Driver");
  manifest_without_driver.insert("Driver", toml::table{
                                               {"shared", "foobar"},
                                               {"entrypoint", 42},
                                           });

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_without_driver;
  test_manifest_file.close();

  // Attempt to load the driver
  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));

  ASSERT_THAT(error.message,
              ::testing::HasSubstr("Driver entrypoint not a string in manifest"));
  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ManifestBadVersion) {
  auto filepath = temp_dir / "sqlite.toml";
  toml::table manifest_with_bad_version = simple_manifest;
  manifest_with_bad_version.insert("manifest_version", 2);

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_with_bad_version;
  test_manifest_file.close();

  // Attempt to load the driver
  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

// only build and run test that puts files in the users home directory if
// it's been enabled via the build system setting this compile def
#ifdef ADBC_DRIVER_MANAGER_TEST_MANIFEST_USER_LEVEL
TEST_F(DriverManifest, LoadUserLevelManifest) {
  ASSERT_THAT(AdbcFindLoadDriver("adbc-test-sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              Not(IsOkStatus(&error)));

#if defined(_WIN32) || defined(__APPLE__)
  auto user_config_dir = InternalAdbcUserConfigDir() / "Drivers";
#else
  auto user_config_dir = InternalAdbcUserConfigDir() / "drivers";
#endif
  bool created = false;
  if (!std::filesystem::exists(user_config_dir)) {
    ASSERT_TRUE(std::filesystem::create_directories(user_config_dir));
    created = true;
  }

  std::ofstream test_manifest_file(user_config_dir / "adbc-test-sqlite.toml");
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << simple_manifest;
  test_manifest_file.close();

  // fail to load if flag doesn't have ADBC_LOAD_FLAG_SEARCH_USER
  ASSERT_THAT(AdbcFindLoadDriver("adbc-test-sqlite", nullptr, ADBC_VERSION_1_1_0, 0,
                                 nullptr, &driver, &error),
              Not(IsOkStatus(&error)));

  // succeed with default load options
  ASSERT_THAT(AdbcFindLoadDriver("adbc-test-sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsOkStatus(&error));

  ASSERT_TRUE(std::filesystem::remove(user_config_dir / "adbc-test-sqlite.toml"));
  if (created) {
    std::filesystem::remove_all(user_config_dir);
  }
}
#endif

// only build and run test that creates / adds a file to /etc/adbc/drivers if
// it's been enabled via the build system setting this compile def
#ifdef ADBC_DRIVER_MANAGER_TEST_MANIFEST_SYSTEM_LEVEL
TEST_F(DriverManifest, LoadSystemLevelManifest) {
  ASSERT_THAT(AdbcFindLoadDriver("adbc-test-sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              Not(IsOkStatus(&error)));

  auto system_config_dir = std::filesystem::path("/etc/adbc/drivers");
  bool created = false;
  if (!std::filesystem::exists(system_config_dir)) {
    ASSERT_TRUE(std::filesystem::create_directories(system_config_dir));
    created = true;
  }

  std::ofstream test_manifest_file(system_config_dir / "adbc-test-sqlite.toml");
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << simple_manifest;
  test_manifest_file.close();

  // fail to load if flag doesn't have ADBC_LOAD_FLAG_SEARCH_SYSTEM
  ASSERT_THAT(AdbcFindLoadDriver("adbc-test-sqlite", nullptr, ADBC_VERSION_1_1_0, 0,
                                 nullptr, &driver, &error),
              Not(IsOkStatus(&error)));

  // succeed with default load options
  ASSERT_THAT(AdbcFindLoadDriver("adbc-test-sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsOkStatus(&error));

  ASSERT_TRUE(std::filesystem::remove(system_config_dir / "adbc-test-sqlite.toml"));
  if (created) {
    std::filesystem::remove_all(system_config_dir);
  }
}
#endif

TEST_F(DriverManifest, AllDisabled) {
  // Test that if the user doesn't set load flags, we properly flag this
  ASSERT_THAT(AdbcFindLoadDriver("adbc-test-sqlite", nullptr, ADBC_VERSION_1_1_0, 0,
                                 nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  EXPECT_THAT(error.message,
              ::testing::HasSubstr("not enabled at run time: ADBC_DRIVER_PATH (enable "
                                   "ADBC_LOAD_FLAG_SEARCH_ENV)"));

#ifdef _WIN32
  EXPECT_THAT(error.message,
              ::testing::HasSubstr("not enabled at run time: HKEY_CURRENT_USER"));
  EXPECT_THAT(error.message,
              ::testing::HasSubstr("not enabled at run time: HKEY_LOCAL_MACHINE"));
#else
  EXPECT_THAT(error.message,
              ::testing::HasSubstr("not enabled at run time: user config dir /"));
  EXPECT_THAT(error.message,
              ::testing::HasSubstr("not enabled at run time: system config dir /"));
#endif  // _WIN32
  EXPECT_THAT(error.message,
              ::testing::HasSubstr(" (enable ADBC_LOAD_FLAG_SEARCH_USER)"));
  EXPECT_THAT(error.message,
              ::testing::HasSubstr(" (enable ADBC_LOAD_FLAG_SEARCH_SYSTEM)"));
}

TEST_F(DriverManifest, CondaPrefix) {
#if ADBC_CONDA_BUILD
  constexpr bool is_conda_build = true;
#else
  constexpr bool is_conda_build = false;
#endif  // ADBC_CONDA_BUILD

  std::cerr << "ADBC_CONDA_BUILD: " << (is_conda_build ? "defined" : "not defined")
            << std::endl;

  auto filepath = temp_dir / "etc" / "adbc" / "drivers" / "test_sqlite.toml";
  std::filesystem::create_directories(filepath.parent_path());
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << simple_manifest;
  test_manifest_file.close();

#ifdef _WIN32
  ASSERT_EQ(0, ::_wputenv_s(L"CONDA_PREFIX", temp_dir.native().c_str()));
#else
  ASSERT_EQ(0, ::setenv("CONDA_PREFIX", temp_dir.native().c_str(), 1));
#endif  // _WIN32

  AdbcStatusCode result =
      AdbcFindLoadDriver("test_sqlite", nullptr, ADBC_VERSION_1_1_0,
                         ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error);

  if constexpr (is_conda_build) {
    ASSERT_THAT(result, IsOkStatus(&error));
  } else {
    ASSERT_THAT(result, IsStatus(ADBC_STATUS_NOT_FOUND, &error));
    ASSERT_THAT(error.message,
                ::testing::HasSubstr("not enabled at build time: Conda prefix"));
  }
}

TEST_F(DriverManifest, ImplicitUri) {
  auto filepath = temp_dir / "postgresql.toml";
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << R"([Driver]
shared = "adbc_driver_postgresql")";
  test_manifest_file.close();

  // Should attempt to load the "postgresql" driver by inferring from the URI
  std::string uri = "postgresql://a:b@localhost:9999/nonexistent";
  adbc_validation::Handle<struct AdbcDatabase> database;
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "driver", uri.c_str(), &error),
              IsOkStatus(&error));
  std::string search_path = temp_dir.string();
  ASSERT_THAT(AdbcDriverManagerDatabaseSetAdditionalSearchPathList(
                  &database.value, search_path.data(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_IO, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("Failed to connect"));

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, DriverFromUri) {
  auto filepath = temp_dir / "sqlite.toml";
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << R"([Driver]
shared = "adbc_driver_sqlite")";
  test_manifest_file.close();

  const std::string uri = "sqlite:file::memory:";
  for (const auto& driver_option : {"driver", "uri"}) {
    SCOPED_TRACE(driver_option);
    SCOPED_TRACE(uri);
    adbc_validation::Handle<struct AdbcDatabase> database;
    ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
    ASSERT_THAT(
        AdbcDatabaseSetOption(&database.value, driver_option, uri.c_str(), &error),
        IsOkStatus(&error));
    std::string search_path = temp_dir.string();
    ASSERT_THAT(AdbcDriverManagerDatabaseSetAdditionalSearchPathList(
                    &database.value, search_path.data(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
                IsStatus(ADBC_STATUS_OK, &error));
  }

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, DriverFromDriverAndUri) {
  // Regression test: if we set both driver and URI, then we shouldn't try to
  // extract driver from URI or vice versa.

  auto filepath = temp_dir / "sqlite.toml";
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << R"([Driver]
shared = "adbc_driver_sqlite")";
  test_manifest_file.close();

  const std::string uri = "foo.db";
  std::vector<std::vector<std::pair<std::string, std::string>>> options_cases = {
      {{"driver", "sqlite"}, {"uri", uri}},
      {{"uri", uri}, {"driver", "sqlite"}},
  };
  for (const auto& options : options_cases) {
    adbc_validation::Handle<struct AdbcDatabase> database;
    ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
    for (const auto& option : options) {
      ASSERT_THAT(AdbcDatabaseSetOption(&database.value, option.first.c_str(),
                                        option.second.c_str(), &error),
                  IsOkStatus(&error));
    }

    std::string search_path = temp_dir.string();
    ASSERT_THAT(AdbcDriverManagerDatabaseSetAdditionalSearchPathList(
                    &database.value, search_path.data(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  }

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ControlCodes) {
  const std::string uri = "\026sqlite:file::memory:";
  for (const auto& driver_option : {"driver", "uri"}) {
    SCOPED_TRACE(driver_option);
    SCOPED_TRACE(uri);
    adbc_validation::Handle<struct AdbcDatabase> database;
    ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
    ASSERT_THAT(
        AdbcDatabaseSetOption(&database.value, driver_option, uri.c_str(), &error),
        IsOkStatus(&error));
    std::string search_path = temp_dir.string();
    ASSERT_THAT(AdbcDriverManagerDatabaseSetAdditionalSearchPathList(
                    &database.value, search_path.data(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
                IsStatus(ADBC_STATUS_NOT_FOUND, &error));
    ASSERT_THAT(
        error.message,
        ::testing::HasSubstr(
            "Note: driver name may have non-printable characters: `\\x16sqlite`"));
  }
}

class ConnectionProfiles : public ::testing::Test {
 public:
  void SetUp() override {
    std::memset(&driver, 0, sizeof(driver));
    std::memset(&error, 0, sizeof(error));

    temp_dir =
        std::filesystem::temp_directory_path() / "adbc_driver_manager_profile_test";
    std::filesystem::create_directories(temp_dir);

    simple_profile = toml::table{
        {"profile_version", 1},
        {"driver", "adbc_driver_sqlite"},
        {"Options",
         toml::table{
             {"uri", "file::memory:"},
         }},
    };
  }

  void TearDown() override {
    if (error.release) {
      error.release(&error);
    }

    if (driver.release) {
      ASSERT_THAT(driver.release(&driver, &error), IsOkStatus(&error));
      ASSERT_EQ(driver.private_data, nullptr);
      ASSERT_EQ(driver.private_manager, nullptr);
    }

    driver_path.clear();
    if (std::filesystem::exists(temp_dir)) {
      std::filesystem::remove_all(temp_dir);
    }
  }

 protected:
  struct AdbcDriver driver = {};
  struct AdbcError error = {};

  std::filesystem::path temp_dir;
  std::filesystem::path driver_path;
  toml::table simple_profile;
};

TEST_F(ConnectionProfiles, SetProfileOption) {
  auto filepath = temp_dir / "profile.toml";
  toml::table profile = simple_profile;
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  // absolute path to the profile
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", filepath.string().c_str(),
                                    &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));

  // inherit additional_search_path_list
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  auto search_path = temp_dir.string();
  ASSERT_THAT(
      AdbcDatabaseSetOption(&database.value, "additional_profile_search_path_list",
                            search_path.c_str(), &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));

  // find profile by name using ADBC_PROFILE_PATH
  SetProfilePath(temp_dir.string().c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));
  UnsetProfilePath();
}

TEST_F(ConnectionProfiles, HierarchicalProfile) {
  auto filepath = temp_dir / "dev" / "profile.toml";
  std::filesystem::create_directories(filepath.parent_path());
  toml::table profile = simple_profile;
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  SetProfilePath(temp_dir.string().c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "dev/profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));
  UnsetProfilePath();
}

TEST_F(ConnectionProfiles, UriProfileOption) {
  auto filepath = temp_dir / "profile.toml";
  toml::table profile = simple_profile;
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  // absolute path to the profile
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "uri",
                                    ("profile://" + filepath.string()).c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));

  // find profile by name using ADBC_PROFILE_PATH
  SetProfilePath(temp_dir.string().c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "uri", "profile://profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));
  UnsetProfilePath();
}

TEST_F(ConnectionProfiles, DriverProfileOption) {
  auto filepath = temp_dir / "profile.toml";
  toml::table profile = simple_profile;
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  // absolute path to the profile
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "driver",
                                    ("profile://" + filepath.string()).c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));

  // find profile by name using ADBC_PROFILE_PATH
  SetProfilePath(temp_dir.string().c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(
      AdbcDatabaseSetOption(&database.value, "driver", "profile://profile", &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));
  UnsetProfilePath();
}

TEST_F(ConnectionProfiles, ExtraStringOption) {
  auto filepath = temp_dir / "profile.toml";
  toml::table profile = simple_profile;
  profile["Options"].as_table()->insert("foo", "bar");
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  // find profile by name using ADBC_PROFILE_PATH
  SetProfilePath(temp_dir.string().c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("Unknown database option foo='bar'"));
  UnsetProfilePath();
}

TEST_F(ConnectionProfiles, ExtraIntOption) {
  auto filepath = temp_dir / "profile.toml";
  toml::table profile = simple_profile;
  profile["Options"].as_table()->insert("foo", int64_t(42));
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  // find profile by name using ADBC_PROFILE_PATH
  SetProfilePath(temp_dir.string().c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("Unknown database option foo=42"));
  UnsetProfilePath();
}

TEST_F(ConnectionProfiles, ExtraDoubleOption) {
  auto filepath = temp_dir / "profile.toml";
  toml::table profile = simple_profile;
  profile["Options"].as_table()->insert("foo", 42.0);
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  // find profile by name using ADBC_PROFILE_PATH
  SetProfilePath(temp_dir.string().c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("Unknown database option foo=42"));
  UnsetProfilePath();
}

TEST_F(ConnectionProfiles, DotSeparatedKey) {
  auto filepath = temp_dir / "profile.toml";
  toml::table profile = toml::parse(R"(
    profile_version = 1
    driver = "adbc_driver_sqlite"
    [Options]
    foo.bar.baz = "bar"
  )");

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  // find profile by name using ADBC_PROFILE_PATH
  SetProfilePath(temp_dir.string().c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error));
  ASSERT_THAT(error.message,
              ::testing::HasSubstr("Unknown database option foo.bar.baz='bar'"));
  UnsetProfilePath();
}

TEST_F(ConnectionProfiles, DuplicateQuotedKey) {
  // Regression: a duplicate *quoted* key must report the real key name, not a
  // copy with its first two characters doubled (vendored toml++ bug, fixed
  // upstream in marzer/tomlplusplus#300 / PR #302 / commit f22f035).
  auto filepath = temp_dir / "profile.toml";
  std::ofstream test_profile_file(filepath);
  ASSERT_TRUE(test_profile_file.is_open());
  test_profile_file << "profile_version = 1\n"
                       "driver = \"adbc_driver_sqlite\"\n"
                       "[Options]\n"
                       "\"redshift.db_name\" = \"dev\"\n"
                       "\"redshift.db_name\" = \"sample_datadev\"\n";
  test_profile_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  // find profile by name using ADBC_PROFILE_PATH
  SetProfilePath(temp_dir.string().c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("redshift.db_name"));
  ASSERT_THAT(error.message, ::testing::Not(::testing::HasSubstr("reredshift")));
  UnsetProfilePath();
}

TEST_F(ConnectionProfiles, UseEnvVar) {
  auto filepath = temp_dir / "profile.toml";
  toml::table profile = toml::parse(R"|(
    profile_version = 1
    driver = "adbc_driver_sqlite"
    [Options]
    foo = "{{ env_var(ADBC_PROFILE_PATH) }}"
  )|");

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  // find profile by name using ADBC_PROFILE_PATH
  SetProfilePath(temp_dir.string().c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("Unknown database option foo='" +
                                                  temp_dir.string() + "'"));
  UnsetProfilePath();
}

TEST_F(ConnectionProfiles, UseEnvVarNotExist) {
  auto filepath = temp_dir / "profile.toml";
  toml::table profile = toml::parse(R"|(
    profile_version = 1
    driver = "adbc_driver_sqlite"
    [Options]
    foo = "{{ env_var(FOOBAR_ENV_VAR_THAT_DOES_NOT_EXIST) }}"
  )|");

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  // find profile by name using ADBC_PROFILE_PATH
  SetProfilePath(temp_dir.string().c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("Unknown database option foo=''"));
  UnsetProfilePath();
}

TEST_F(ConnectionProfiles, UseEnvVarNotExistNoBail) {
  auto filepath = temp_dir / "profile.toml";
  toml::table profile = toml::parse(R"|(
    profile_version = 1
    driver = "adbc_driver_sqlite"
    [Options]
    foo = "foo{{ env_var(FOOBAR_ENV_VAR_THAT_DOES_NOT_EXIST) }}bar"
  )|");

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  // find profile by name using ADBC_PROFILE_PATH
  SetProfilePath(temp_dir.string().c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error));
  ASSERT_THAT(error.message,
              ::testing::HasSubstr("Unknown database option foo='foobar'"));
  UnsetProfilePath();
}

TEST_F(ConnectionProfiles, UseEnvVarMalformed) {
  auto filepath = temp_dir / "profile.toml";
  toml::table profile = toml::parse(R"|(
    profile_version = 1
    driver = "adbc_driver_sqlite"
    [Options]
    foo = "{{ env_var(ENV_VAR_WITHOUT_CLOSING_PAREN }}"
  )|");

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  // find profile by name using ADBC_PROFILE_PATH
  SetProfilePath(temp_dir.string().c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));
  ASSERT_THAT(
      error.message,
      ::testing::HasSubstr(
          "In profile: malformed env_var() in key `foo`: missing closing parenthesis"));
  UnsetProfilePath();
}

TEST_F(ConnectionProfiles, UseEnvVarMissingArg) {
  auto filepath = temp_dir / "profile.toml";
  toml::table profile = toml::parse(R"|(
    profile_version = 1
    driver = "adbc_driver_sqlite"
    [Options]
    foo = "{{ env_var() }}"
  )|");

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  // find profile by name using ADBC_PROFILE_PATH
  SetProfilePath(temp_dir.string().c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));
  ASSERT_THAT(error.message,
              ::testing::HasSubstr("In profile: malformed env_var() in key `foo`: "
                                   "missing environment variable name"));
  UnsetProfilePath();
}

TEST_F(ConnectionProfiles, UseEnvVarInterpolation) {
  auto filepath = temp_dir / "profile.toml";
  toml::table profile = toml::parse(R"|(
    profile_version = 1
    driver = "adbc_driver_sqlite"
    [Options]
    foo = "super {{ env_var(ADBC_PROFILE_PATH) }} duper"
  )|");

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  // find profile by name using ADBC_PROFILE_PATH
  SetProfilePath(temp_dir.string().c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("Unknown database option foo='super " +
                                                  temp_dir.string() + " duper'"));
  UnsetProfilePath();
}

TEST_F(ConnectionProfiles, UseEnvVarInterpolationMultiple) {
  auto filepath = temp_dir / "profile.toml";
  toml::table profile = toml::parse(R"|(
    profile_version = 1
    driver = "adbc_driver_sqlite"
    [Options]
    foo = "super {{ env_var(ADBC_PROFILE_PATH) }} duper {{ env_var(ADBC_PROFILE_PATH) }} end"
  )|");

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  // find profile by name using ADBC_PROFILE_PATH
  SetProfilePath(temp_dir.string().c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("Unknown database option foo='super " +
                                                  temp_dir.string() + " duper " +
                                                  temp_dir.string() + " end'"));
  UnsetProfilePath();
}

TEST_F(ConnectionProfiles, ProfileNotFound) {
  adbc_validation::Handle<struct AdbcDatabase> database;

  // absolute path to the profile
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile",
                                    (temp_dir / "profile.toml").string().c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("Profile file does not exist: " +
                                                  (temp_dir / "profile.toml").string()));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));

  // find profile by name using ADBC_PROFILE_PATH
  SetProfilePath(temp_dir.string().c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  ASSERT_THAT(error.message,
              ::testing::HasSubstr(std::string("Profile not found: profile\n") +
                                   "Also searched these paths for profiles:\n\t" +
                                   "ADBC_PROFILE_PATH: " + temp_dir.string() + "\n\t"));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));
  UnsetProfilePath();
}

TEST_F(ConnectionProfiles, CondaProfileTest) {
#if ADBC_CONDA_BUILD
  constexpr bool is_conda_build = true;
#else
  constexpr bool is_conda_build = false;
#endif  // ADBC_CONDA_BUILD

  std::cerr << "ADBC_CONDA_BUILD: " << (is_conda_build ? "defined" : "not defined")
            << std::endl;

  auto filepath = temp_dir / "etc" / "adbc" / "profiles" / "sqlite-test.toml";
  std::filesystem::create_directories(filepath.parent_path());
  std::ofstream test_profile_file(filepath);
  ASSERT_TRUE(test_profile_file.is_open());
  test_profile_file << simple_profile;
  test_profile_file.close();

#ifdef _WIN32
  ASSERT_EQ(0, ::_wputenv_s(L"CONDA_PREFIX", temp_dir.native().c_str()));
#else
  ASSERT_EQ(0, ::setenv("CONDA_PREFIX", temp_dir.native().c_str(), 1));
#endif  // _WIN32

  adbc_validation::Handle<struct AdbcDatabase> database;

  // absolute path to the profile
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "sqlite-test", &error),
              IsOkStatus(&error));
  if constexpr (is_conda_build) {
    ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  } else {
    ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
                IsStatus(ADBC_STATUS_NOT_FOUND, &error));
    ASSERT_THAT(error.message,
                ::testing::HasSubstr("not enabled at build time: Conda prefix"));
  }
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));
}

TEST_F(ConnectionProfiles, CustomProfileProvider) {
  adbc_validation::Handle<struct AdbcDatabase> database;

  AdbcConnectionProfileProvider provider =
      [](const char* profile_name, const char* additional_path_list,
         struct AdbcConnectionProfile* out, struct AdbcError* error) -> AdbcStatusCode {
    EXPECT_EQ(std::string(profile_name), "profile");

    static const std::string expected = "custom profile provider error";
    error->message = new char[expected.size() + 1];
    std::copy(expected.begin(), expected.end(), error->message);
    error->message[expected.size()] = '\0';
    error->release = [](struct AdbcError* error) {
      delete[] error->message;
      error->message = nullptr;
      error->release = nullptr;
    };
    return ADBC_STATUS_INVALID_ARGUMENT;
  };

  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(
      AdbcDriverManagerDatabaseSetProfileProvider(&database.value, provider, &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("custom profile provider error"));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));
}

TEST_F(ConnectionProfiles, ProfileParseErrorIncludesLocation) {
  auto filepath = temp_dir / "badprofile.toml";
  std::ofstream file(filepath);
  ASSERT_TRUE(file.is_open());
  file << "profile_version = 1\n"
       << "driver = unquoted_value\n"
       << "[Options]\n";
  file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", filepath.string().c_str(),
                                    &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("line 2"));
  ASSERT_THAT(error.message, ::testing::HasSubstr("column"));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));
}

TEST_F(DriverManifest, ManifestParseErrorIncludesLocation) {
  std::ofstream file(temp_dir / "baddriver.toml");
  ASSERT_TRUE(file.is_open());
  file << "manifest_version = 1\n"
       << "[Driver]\n"
       << "shared = unquoted_value\n";
  file.close();

  SetDriverPath(temp_dir.string().c_str());

  ASSERT_THAT(AdbcFindLoadDriver("baddriver", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("line 3"));
  ASSERT_THAT(error.message, ::testing::HasSubstr("column"));

  UnsetDriverPath();
}

struct DriverUriProfile {
  std::string name;
  std::string driver;
  std::string uri;
  std::string profile;

  std::string expected_driver = "";
  std::string expected_uri = "";
  AdbcStatusCode expected_error = ADBC_STATUS_OK;
  std::string expected_message = "";

  DriverUriProfile WithExpected(std::string driver, std::string uri) {
    expected_driver = std::move(driver);
    expected_uri = std::move(uri);
    expected_error = ADBC_STATUS_OK;
    expected_message.clear();
    return *this;
  }

  DriverUriProfile WithExpectedError(AdbcStatusCode error_code,
                                     std::string message_substr) {
    expected_driver.clear();
    expected_uri.clear();
    expected_error = error_code;
    expected_message = std::move(message_substr);
    return *this;
  }
};

class DriverUriProfileTest : public ConnectionProfiles,
                             public testing::WithParamInterface<DriverUriProfile> {
 public:
  void SetUp() override {
    ConnectionProfiles::SetUp();

    auto profiles = temp_dir / "profiles";
    std::filesystem::create_directories(profiles);
    SetProfilePath(profiles.string().c_str());

    {
      auto filepath = profiles / "prod-sqlite.toml";
      std::ofstream profile_file(filepath);
      ASSERT_TRUE(profile_file.is_open());
      profile_file << toml::table{
          {"profile_version", 1},
          {"driver", "sqlite"},
          {"Options", toml::table{{"uri", "file://:memory:"}}},
      };
    }
    {
      auto filepath = profiles / "prod-driver-uri.toml";
      std::ofstream profile_file(filepath);
      ASSERT_TRUE(profile_file.is_open());
      profile_file << toml::table{
          {"profile_version", 1},
          {"driver", "adbc_driver_sqlite"},
          {"Options", toml::table{{"uri", "file://:memory:"}}},
      };
    }
    {
      auto filepath = profiles / "prod-driver.toml";
      std::ofstream profile_file(filepath);
      ASSERT_TRUE(profile_file.is_open());
      profile_file << toml::table{
          {"profile_version", 1},
          {"driver", "adbc_driver_sqlite"},
          {"Options", toml::table{}},
      };
    }
    {
      auto filepath = profiles / "prod-uri.toml";
      std::ofstream profile_file(filepath);
      ASSERT_TRUE(profile_file.is_open());
      profile_file << toml::table{
          {"profile_version", 1},
          {"Options", toml::table{{"uri", "file://:memory:"}}},
      };
    }
    {
      auto filepath = profiles / "prod-empty.toml";
      std::ofstream profile_file(filepath);
      ASSERT_TRUE(profile_file.is_open());
      profile_file << toml::table{
          {"profile_version", 1},
          {"Options", toml::table{{"noturi", "file://:memory:"}}},
      };
    }

    auto manifests = temp_dir / "drivers";
    std::filesystem::create_directories(manifests);
    SetDriverPath(manifests.string().c_str());

    auto filepath = manifests / "sqlite.toml";
    std::ofstream manifest_file(filepath);
    ASSERT_TRUE(manifest_file.is_open());
    manifest_file << toml::table{
        {"name", "SQLite3"},
        {"publisher", "arrow-adbc"},
        {"version", "X.Y.Z"},
        {"ADBC",
         toml::table{
             {"version", "1.1.0"},
         }},
        {"Driver",
         toml::table{
             {"shared",
              toml::table{
                  {InternalAdbcCurrentArch(), driver_path.string()},
              }},
         }},
    };
  }

  void TearDown() override {
    ConnectionProfiles::TearDown();
    UnsetProfilePath();
  }
};

TEST_P(DriverUriProfileTest, Load) {
  const auto& param = GetParam();

  TempDatabase db;
  if (!param.driver.empty()) {
    db.driver = param.driver;
  }
  if (!param.uri.empty()) {
    db.options["uri"] = param.uri;
  }
  if (!param.profile.empty()) {
    db.options["profile"] = param.profile;
  }

  auto status = InternalAdbcParseOptions(&db, &error);
  if (param.expected_error != ADBC_STATUS_OK) {
    ASSERT_THAT(status, IsStatus(param.expected_error, &error));
    ASSERT_THAT(error.message, ::testing::HasSubstr(param.expected_message));
  } else {
    ASSERT_THAT(status, IsOkStatus(&error));
    EXPECT_EQ(db.driver, param.expected_driver);
    if (param.expected_uri.empty()) {
      EXPECT_THAT(db.options, testing::Not(testing::Contains(testing::Key("uri"))));
    } else {
      EXPECT_THAT(db.options,
                  testing::Contains(testing::Pair("uri", param.expected_uri)));
    }
  }
}

constexpr char kNoDriverError[] = "Must set 'driver' option before AdbcDatabaseInit";
constexpr char kMultipleProfilesError[] = "Multiple profiles specified";
// TODO(https://github.com/apache/arrow-adbc/issues/3963): designated initializers
static const DriverUriProfile kOptionsValues[] = {
    // nothing is specified => error
    // ------------------------------------------------------------
    (DriverUriProfile{"empty_empty_empty", "", "", ""})
        .WithExpectedError(ADBC_STATUS_INVALID_ARGUMENT, kNoDriverError),

    // no driver + no URI + profile: use profile's values
    // ------------------------------------------------------------
    (DriverUriProfile{"empty_empty_profile1", "", "", "prod-driver-uri"})
        .WithExpected("adbc_driver_sqlite", "file://:memory:"),
    (DriverUriProfile{"empty_empty_profile2", "", "", "prod-driver"})
        .WithExpected("adbc_driver_sqlite", ""),
    // Profile has no driver, but has a URI, so we parse that
    (DriverUriProfile{"empty_empty_profile3", "", "", "prod-uri"})
        .WithExpected("file", "file://:memory:"),
    (DriverUriProfile{"empty_empty_profile4", "", "", "prod-empty"})
        .WithExpectedError(ADBC_STATUS_INVALID_ARGUMENT, kNoDriverError),

    // no driver + URI (where URI is not a profile URI) + no
    // profile: parse URI for driver
    // ------------------------------------------------------------
    (DriverUriProfile{"empty_driveruri_empty", "", "sqlite://:memory:", ""})
        .WithExpected("sqlite", "sqlite://:memory:"),

    // no driver + URI (where URI is not a profile URI) + profile:
    // it depends on what the profile contains. If it has a driver,
    // then we use the profile's driver
    // ------------------------------------------------------------
    (DriverUriProfile{"empty_driveruri_profile1", "", "bar://:memory:", "prod-sqlite"})
        .WithExpected("sqlite", "bar://:memory:"),
    (DriverUriProfile{"empty_driveruri_profile2", "", "bar://:memory:", "prod-driver"})
        .WithExpected("adbc_driver_sqlite", "bar://:memory:"),
    // here the profile's URI is overridden by the supplied one
    (DriverUriProfile{"empty_driveruri_profile3", "", "bar://:memory:", "prod-uri"})
        .WithExpected("bar", "bar://:memory:"),
    (DriverUriProfile{"empty_driveruri_profile4", "", "bar://:memory:", "prod-empty"})
        .WithExpected("bar", "bar://:memory:"),

    // no driver + profile URI + no profile
    (DriverUriProfile{"empty_profile_empty", "", "profile://prod-sqlite", ""})
        .WithExpected("sqlite", "file://:memory:"),

    // no driver + profile URI + profile
    (DriverUriProfile{"empty_profile_profile", "", "profile://prod-sqlite", "profile"})
        .WithExpectedError(ADBC_STATUS_INVALID_ARGUMENT, kMultipleProfilesError),

    // no driver + URI (not profile or driver) + no profile
    // ------------------------------------------------------------
    // there's no driver, so the URI gets treated as the driver (as
    // otherwise you'd blow up anyways)
    (DriverUriProfile{"empty_uri_empty", "", "DSN={yes}", ""})
        .WithExpected("DSN={yes}", "DSN={yes}"),

    // no driver + URI (not profile or driver) + profile
    // ------------------------------------------------------------
    (DriverUriProfile{"empty_uri_profile1", "", "DSN={yes}", "prod-driver"})
        .WithExpected("adbc_driver_sqlite", "DSN={yes}"),
    (DriverUriProfile{"empty_uri_profile2", "", "DSN={yes}", "prod-driver-uri"})
        .WithExpected("adbc_driver_sqlite", "DSN={yes}"),
    (DriverUriProfile{"empty_uri_profile3", "", "DSN={yes}", "prod-uri"})
        .WithExpected("DSN={yes}", "DSN={yes}"),

    // driver + no URI + no profile
    // ------------------------------------------------------------
    (DriverUriProfile{"driver_empty_empty", "sqlite", "", ""}).WithExpected("sqlite", ""),

    // driver + no URI + profile
    // ------------------------------------------------------------
    (DriverUriProfile{"driver_empty_profile1", "sqlite", "", "prod-sqlite"})
        .WithExpected("sqlite", "file://:memory:"),
    (DriverUriProfile{"driver_empty_profile2", "sqlite", "", "prod-driver-uri"})
        .WithExpectedError(
            ADBC_STATUS_INVALID_ARGUMENT,
            "Profile `prod-driver-uri` specifies driver `adbc_driver_sqlite` which does "
            "not match requested driver `sqlite`"),

    // driver + URI + no profile
    // ------------------------------------------------------------
    (DriverUriProfile{"driver_driveruri_empty", "sqlite", "postgres://bar", ""})
        .WithExpected("sqlite", "postgres://bar"),

    // driver + URI + profile
    // ------------------------------------------------------------
    (DriverUriProfile{"driver_driveruri_profile1", "sqlite", "postgres://bar",
                      "prod-sqlite"})
        .WithExpected("sqlite", "postgres://bar"),
    (DriverUriProfile{"driver_driveruri_profile2", "adbc_driver_sqlite", "postgres://bar",
                      "prod-driver-uri"})
        .WithExpected("adbc_driver_sqlite", "postgres://bar"),

    // driver + profile URI + no profile
    // ------------------------------------------------------------
    (DriverUriProfile{"driver_profile_empty1", "sqlite", "profile://prod-sqlite", ""})
        .WithExpected("sqlite", "file://:memory:"),
    (DriverUriProfile{"driver_profile_empty2", "postgres", "profile://prod-sqlite", ""})
        .WithExpectedError(ADBC_STATUS_INVALID_ARGUMENT,
                           "Profile `prod-sqlite` specifies driver `sqlite` which does "
                           "not match requested driver `postgres`"),

    // driver + profile URI + profile
    // ------------------------------------------------------------
    (DriverUriProfile{"driver_profile_profile", "sqlite", "profile://prod-sqlite",
                      "prod-sqlite"})
        .WithExpectedError(ADBC_STATUS_INVALID_ARGUMENT, kMultipleProfilesError),

    // driver + URI (not profile or driver) + no profile
    // ------------------------------------------------------------
    (DriverUriProfile{"driver_uri_empty", "sqlite", "DSN={yes}", ""})
        .WithExpected("sqlite", "DSN={yes}"),

    // driver + URI (not profile or driver) + profile
    // ------------------------------------------------------------
    (DriverUriProfile{"driver_uri_profile1", "sqlite", "DSN={yes}", "prod-sqlite"})
        .WithExpected("sqlite", "DSN={yes}"),
    (DriverUriProfile{"driver_uri_profile2", "sqlite", "DSN={yes}", "prod-driver"})
        .WithExpectedError(ADBC_STATUS_INVALID_ARGUMENT,
                           "Profile `prod-driver` specifies driver `adbc_driver_sqlite` "
                           "which does not match requested driver `sqlite`"),

    // URI in driver + no URI + no profile
    // ------------------------------------------------------------
    (DriverUriProfile{"driveruri_empty_empty", "sqlite://foo", "", ""})
        .WithExpected("sqlite", "sqlite://foo"),

    // URI in driver + no URI + profile
    // ------------------------------------------------------------
    // this fails because we validate the profile's driver against
    // the requested driver immediately, without doing further
    // parsing (they would resolve to the same thing if we did
    // parse the URIs but that's additional edge cases)
    (DriverUriProfile{"driveruri_empty_profile1", "sqlite://foo", "", "prod-sqlite"})
        .WithExpectedError(ADBC_STATUS_INVALID_ARGUMENT,
                           "Profile `prod-sqlite` specifies driver `sqlite` which does "
                           "not match requested driver `sqlite://foo`"),
    (DriverUriProfile{"driveruri_empty_profile2", "sqlite://foo", "", "prod-uri"})
        .WithExpected("sqlite", "sqlite://foo"),
    (DriverUriProfile{"driveruri_empty_profile3", "sqlite://foo", "", "prod-driver-uri"})
        .WithExpectedError(
            ADBC_STATUS_INVALID_ARGUMENT,
            "Profile `prod-driver-uri` specifies driver `adbc_driver_sqlite` which does "
            "not match requested driver `sqlite://foo`"),

    // URI in driver + URI + no profile
    // ------------------------------------------------------------
    // TODO: I think URI should take precedence here
    (DriverUriProfile{"driveruri_driver_empty", "sqlite://foo", "sqlite://bar", ""})
        .WithExpected("sqlite", "sqlite://foo"),

    // URI in driver + URI + profile
    // ------------------------------------------------------------
    (DriverUriProfile{"driveruri_driver_profile1", "sqlite://foo", "sqlite://bar",
                      "prod-sqlite"})
        .WithExpectedError(ADBC_STATUS_INVALID_ARGUMENT,
                           "Profile `prod-sqlite` specifies driver `sqlite` which does "
                           "not match requested driver `sqlite://foo`"),

    // URI in driver + profile URI + no profile
    // ------------------------------------------------------------
    (DriverUriProfile{"driveruri_profile_empty", "sqlite://foo", "profile://prod-sqlite",
                      ""})
        .WithExpectedError(ADBC_STATUS_INVALID_ARGUMENT,
                           "Profile `prod-sqlite` specifies driver `sqlite` which does "
                           "not match requested driver `sqlite://foo`"),

    // URI in driver + profile URI + profile
    // ------------------------------------------------------------
    (DriverUriProfile{"driveruri_profile_profile1", "sqlite://foo",
                      "profile://prod-sqlite", "prod-sqlite"})
        .WithExpectedError(ADBC_STATUS_INVALID_ARGUMENT, kMultipleProfilesError),

    // URI in driver + URI (not profile or driver) + no profile
    // ------------------------------------------------------------
    // TODO: I think URI should take precedence here
    (DriverUriProfile{"driveruri_uri_empty", "sqlite://foo", "DSN={yes}", ""})
        .WithExpected("sqlite", "sqlite://foo"),

    // URI in driver + URI (not profile or driver) + profile
    // ------------------------------------------------------------
    (DriverUriProfile{"driveruri_uri_profile1", "sqlite://foo", "DSN={yes}",
                      "prod-sqlite"})
        .WithExpectedError(ADBC_STATUS_INVALID_ARGUMENT,
                           "Profile `prod-sqlite` specifies driver `sqlite` which does "
                           "not match requested driver `sqlite://foo`"),

    // profile URI in driver + no URI + no profile
    // ------------------------------------------------------------
    (DriverUriProfile{"profile_empty_empty", "profile://prod-sqlite", "", ""})
        .WithExpected("sqlite", "file://:memory:"),

    // profile URI in driver + no URI + profile
    // ------------------------------------------------------------
    (DriverUriProfile{"profile_empty_profile", "profile://prod-sqlite", "",
                      "prod-sqlite"})
        .WithExpectedError(ADBC_STATUS_INVALID_ARGUMENT, kMultipleProfilesError),

    // profile URI in driver + driver URI + no profile
    // ------------------------------------------------------------
    (DriverUriProfile{"profile_driveruri_empty", "profile://prod-sqlite", "sqlite://foo",
                      ""})
        .WithExpected("sqlite", "sqlite://foo"),

    // profile URI in driver + driver URI + profile
    // ------------------------------------------------------------
    (DriverUriProfile{"profile_driveruri_profile", "profile://prod-sqlite",
                      "sqlite://foo", "prod-sqlite"})
        .WithExpectedError(ADBC_STATUS_INVALID_ARGUMENT, kMultipleProfilesError),

    // profile URI in driver + profile URI + no profile
    // ------------------------------------------------------------
    (DriverUriProfile{"profile_profile_empty", "profile://prod-sqlite",
                      "profile://prod-sqlite", ""})
        .WithExpectedError(ADBC_STATUS_INVALID_ARGUMENT, kMultipleProfilesError),

    // profile URI in driver + profile URI + profile
    // ------------------------------------------------------------
    (DriverUriProfile{"profile_profile_profile", "profile://prod-sqlite",
                      "profile://prod-sqlite", "prod-sqlite"})
        .WithExpectedError(ADBC_STATUS_INVALID_ARGUMENT, kMultipleProfilesError),

    // profile URI in driver + URI (not profile or driver) + no profile
    // ------------------------------------------------------------
    (DriverUriProfile{"profile_uri_empty", "profile://prod-sqlite", "DSN={yes}", ""})
        .WithExpected("sqlite", "DSN={yes}"),

    // profile URI in driver + URI (not profile or driver) + profile
    // ------------------------------------------------------------
    (DriverUriProfile{"profile_uri_profile", "profile://prod-sqlite", "DSN={yes}",
                      "prod-sqlite"})
        .WithExpectedError(ADBC_STATUS_INVALID_ARGUMENT, kMultipleProfilesError),
};

INSTANTIATE_TEST_SUITE_P(
    ValidateOptions, DriverUriProfileTest, testing::ValuesIn(kOptionsValues),
    [](const testing::TestParamInfo<DriverUriProfileTest::ParamType>& info) {
      return info.param.name;
    });

// =============================================================================
// File drivers (PROTOTYPE, file-driver proposal)
// =============================================================================

namespace file_driver {

// A minimal ADBC 1.2 file driver written against the C table. Its
// connections read the first five bytes of the database uri through the
// file system they were given, and report them as "test.first_bytes".

struct Database {
  std::string uri;
  struct AdbcFileSystem fs = {};
};

struct Connection {
  struct AdbcFileSystem fs = {};
  std::string first_bytes;
};

void ReleaseFs(struct AdbcFileSystem* fs) {
  if (fs->release) fs->release(fs);
}

void TakeFs(struct AdbcFileSystem* dst, struct AdbcFileSystem* src) {
  ReleaseFs(dst);
  *dst = *src;
  std::memset(src, 0, sizeof(*src));
}

AdbcStatusCode Fail(struct AdbcError* error, AdbcStatusCode status,
                    const std::string& message) {
  if (error) {
    if (error->release) error->release(error);
    error->message = new char[message.size() + 1];
    std::memcpy(error->message, message.c_str(), message.size() + 1);
    error->release = [](struct AdbcError* e) {
      delete[] e->message;
      e->message = nullptr;
      e->release = nullptr;
    };
  }
  return status;
}

AdbcStatusCode DriverRelease(struct AdbcDriver* driver, struct AdbcError*) {
  driver->release = nullptr;
  return ADBC_STATUS_OK;
}

AdbcStatusCode DatabaseNew(struct AdbcDatabase* database, struct AdbcError*) {
  database->private_data = new Database;
  return ADBC_STATUS_OK;
}

AdbcStatusCode DatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                 const char* value, struct AdbcError* error) {
  if (std::string_view(key) != "uri") {
    return Fail(error, ADBC_STATUS_NOT_IMPLEMENTED, "unknown option");
  }
  static_cast<Database*>(database->private_data)->uri = value;
  return ADBC_STATUS_OK;
}

AdbcStatusCode DatabaseInit(struct AdbcDatabase*, struct AdbcError*) {
  return ADBC_STATUS_OK;
}

AdbcStatusCode DatabaseRelease(struct AdbcDatabase* database, struct AdbcError*) {
  auto* db = static_cast<Database*>(database->private_data);
  ReleaseFs(&db->fs);
  delete db;
  database->private_data = nullptr;
  return ADBC_STATUS_OK;
}

AdbcStatusCode DatabaseSetFileSystem(struct AdbcDatabase* database,
                                     struct AdbcFileSystem* fs, struct AdbcError*) {
  TakeFs(&static_cast<Database*>(database->private_data)->fs, fs);
  return ADBC_STATUS_OK;
}

AdbcStatusCode ConnectionNew(struct AdbcConnection* connection, struct AdbcError*) {
  connection->private_data = new Connection;
  return ADBC_STATUS_OK;
}

AdbcStatusCode ConnectionSetFileSystem(struct AdbcConnection* connection,
                                       struct AdbcFileSystem* fs, struct AdbcError*) {
  TakeFs(&static_cast<Connection*>(connection->private_data)->fs, fs);
  return ADBC_STATUS_OK;
}

AdbcStatusCode ConnectionInit(struct AdbcConnection* connection,
                              struct AdbcDatabase* database, struct AdbcError* error) {
  auto* conn = static_cast<Connection*>(connection->private_data);
  auto* db = static_cast<Database*>(database->private_data);
  // The connection's own file system, else a clone of the database's.
  if (!conn->fs.release && db->fs.release) {
    AdbcStatusCode status = db->fs.clone(&db->fs, &conn->fs, error);
    if (status != ADBC_STATUS_OK) return status;
  }
  if (!conn->fs.release) {
    return Fail(error, ADBC_STATUS_FILESYSTEM_REQUIRED, "no file system");
  }
  // A file:/// uri names a local path (as adbc_core's Location parses it).
  std::string name = db->uri;
  if (name.rfind("file:///", 0) == 0) {
    name = name.substr(7);
    if (name.size() > 2 && name[2] == ':') name = name.substr(1);  // /C:/x -> C:/x
  }
  struct AdbcFile* file = nullptr;
  AdbcStatusCode status =
      conn->fs.open(&conn->fs, name.data(), name.size(), 0, &file, error);
  if (status != ADBC_STATUS_OK) return status;
  char buffer[5] = {};
  struct AdbcFileRange range = {0, 5, buffer, 0, 0};
  status = file->read_ranges(file, &range, 1, error);
  file->close(file);
  if (status != ADBC_STATUS_OK) return status;
  conn->first_bytes.assign(buffer, 5);
  return ADBC_STATUS_OK;
}

AdbcStatusCode ConnectionGetOption(struct AdbcConnection* connection, const char* key,
                                   char* value, size_t* length, struct AdbcError* error) {
  if (std::string_view(key) != "test.first_bytes") {
    return Fail(error, ADBC_STATUS_NOT_FOUND, "unknown option");
  }
  const std::string& bytes =
      static_cast<Connection*>(connection->private_data)->first_bytes;
  if (*length >= bytes.size() + 1) std::memcpy(value, bytes.c_str(), bytes.size() + 1);
  *length = bytes.size() + 1;
  return ADBC_STATUS_OK;
}

AdbcStatusCode ConnectionRelease(struct AdbcConnection* connection, struct AdbcError*) {
  auto* conn = static_cast<Connection*>(connection->private_data);
  ReleaseFs(&conn->fs);
  delete conn;
  connection->private_data = nullptr;
  return ADBC_STATUS_OK;
}

AdbcStatusCode StatementNew(struct AdbcConnection*, struct AdbcStatement*,
                            struct AdbcError* error) {
  return Fail(error, ADBC_STATUS_NOT_IMPLEMENTED, "no statements");
}

AdbcStatusCode StatementExecuteQuery(struct AdbcStatement*, struct ArrowArrayStream*,
                                     int64_t*, struct AdbcError* error) {
  return Fail(error, ADBC_STATUS_NOT_IMPLEMENTED, "no statements");
}

AdbcStatusCode StatementRelease(struct AdbcStatement*, struct AdbcError*) {
  return ADBC_STATUS_OK;
}

AdbcStatusCode DriverGetInfo(const uint32_t* codes, size_t length,
                             struct ArrowArrayStream* out, struct AdbcError* error) {
  std::vector<adbc::driver::InfoValue> all = {
      {ADBC_INFO_DRIVER_NAME, "test file driver"},
      {ADBC_INFO_DRIVER_ACCEPTS_FILESYSTEM, true},
      {ADBC_INFO_DRIVER_REQUIRES_FILESYSTEM, true},
  };
  std::vector<adbc::driver::InfoValue> rows;
  for (const auto& info : all) {
    if (!codes || std::find(codes, codes + length, info.code) != codes + length) {
      rows.push_back(info);
    }
  }
  return adbc::driver::MakeGetInfoStream(rows, out).ToAdbc(error);
}

int init_calls = 0;

/// Refuses every version below 1.2.0, as a file driver must.
AdbcStatusCode Init(int version, void* raw_driver, struct AdbcError*) {
  init_calls++;
  if (version < ADBC_VERSION_1_2_0) return ADBC_STATUS_NOT_IMPLEMENTED;
  auto* driver = static_cast<struct AdbcDriver*>(raw_driver);
  std::memset(driver, 0, ADBC_DRIVER_1_2_0_SIZE);
  driver->release = DriverRelease;
  driver->DatabaseNew = DatabaseNew;
  driver->DatabaseSetOption = DatabaseSetOption;
  driver->DatabaseInit = DatabaseInit;
  driver->DatabaseRelease = DatabaseRelease;
  driver->ConnectionNew = ConnectionNew;
  driver->ConnectionInit = ConnectionInit;
  driver->ConnectionGetOption = ConnectionGetOption;
  driver->ConnectionRelease = ConnectionRelease;
  driver->StatementNew = StatementNew;
  driver->StatementExecuteQuery = StatementExecuteQuery;
  driver->StatementRelease = StatementRelease;
  driver->DriverGetInfo = DriverGetInfo;
  driver->DatabaseSetFileSystem = DatabaseSetFileSystem;
  driver->ConnectionSetFileSystem = ConnectionSetFileSystem;
  return ADBC_STATUS_OK;
}

/// The same driver without file-system support: not a file driver.
AdbcStatusCode InitNotAFileDriver(int version, void* raw_driver,
                                  struct AdbcError* error) {
  AdbcStatusCode status = Init(version, raw_driver, error);
  if (status != ADBC_STATUS_OK) return status;
  auto* driver = static_cast<struct AdbcDriver*>(raw_driver);
  driver->DriverGetInfo = nullptr;
  driver->DatabaseSetFileSystem = nullptr;
  driver->ConnectionSetFileSystem = nullptr;
  return ADBC_STATUS_OK;
}

/// Wraps the local file system and counts opens.
struct Counting {
  struct AdbcFileSystem inner;
  std::shared_ptr<int> opens;
};

void InitCounting(struct AdbcFileSystem* out, std::shared_ptr<int> opens);

AdbcStatusCode CountingOpen(struct AdbcFileSystem* self, const char* name, size_t len,
                            uint32_t hints, struct AdbcFile** out,
                            struct AdbcError* error) {
  auto* counting = static_cast<Counting*>(self->private_data);
  (*counting->opens)++;
  return counting->inner.open(&counting->inner, name, len, hints, out, error);
}

AdbcStatusCode CountingClone(struct AdbcFileSystem* self, struct AdbcFileSystem* out,
                             struct AdbcError*) {
  InitCounting(out, static_cast<Counting*>(self->private_data)->opens);
  return ADBC_STATUS_OK;
}

void CountingRelease(struct AdbcFileSystem* self) {
  auto* counting = static_cast<Counting*>(self->private_data);
  ReleaseFs(&counting->inner);
  delete counting;
  self->release = nullptr;
}

void InitCounting(struct AdbcFileSystem* out, std::shared_ptr<int> opens) {
  auto* counting = new Counting{{}, std::move(opens)};
  ASSERT_EQ(AdbcDriverManagerLocalFileSystemInit(&counting->inner, nullptr),
            ADBC_STATUS_OK);
  std::memset(out, 0, sizeof(*out));
  out->struct_size = sizeof(*out);
  out->version = ADBC_FILESYSTEM_VERSION_1;
  out->private_data = counting;
  out->open = CountingOpen;
  out->stat = counting->inner.stat;
  out->clone = CountingClone;
  out->release = CountingRelease;
}

}  // namespace file_driver

class FileDriverTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = std::filesystem::temp_directory_path() /
           ("adbc_file_driver_" +
            std::to_string(::testing::UnitTest::GetInstance()->random_seed()) + "_" +
            ::testing::UnitTest::GetInstance()->current_test_info()->name());
    std::filesystem::create_directories(dir_);
    path_ = (dir_ / "data.txt").string();
    std::ofstream(path_, std::ios::binary) << "hello, file drivers";
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(dir_, ec);
  }

  /// A database for the test file driver, not yet initialized.
  void NewDatabase(struct AdbcDatabase* database,
                   AdbcDriverInitFunc init = file_driver::Init) {
    ASSERT_THAT(AdbcDatabaseNew(database, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcDriverManagerDatabaseSetInitFunc(database, init, &error),
                IsOkStatus(&error));
  }

  std::string FirstBytes(struct AdbcConnection* connection) {
    char value[64];
    size_t length = sizeof(value);
    EXPECT_THAT(
        AdbcConnectionGetOption(connection, "test.first_bytes", value, &length, &error),
        IsOkStatus(&error));
    return std::string(value);
  }

  std::filesystem::path dir_;
  std::string path_;
  struct AdbcError error = ADBC_ERROR_INIT;
};

TEST_F(FileDriverTest, NegotiatesOneTwo) {
  struct AdbcDriver driver = {};
  file_driver::init_calls = 0;
  // A 1.1 request cannot reach 1.2: the driver refuses 1.1 and 1.0.
  ASSERT_THAT(
      AdbcLoadDriverFromInitFunc(file_driver::Init, ADBC_VERSION_1_1_0, &driver, &error),
      IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error));
  ASSERT_EQ(file_driver::init_calls, 2);

  ASSERT_THAT(
      AdbcLoadDriverFromInitFunc(file_driver::Init, ADBC_VERSION_1_2_0, &driver, &error),
      IsOkStatus(&error));
  // 1.2 members the driver left NULL are defaulted.
  ASSERT_NE(driver.MultiResultSetNext, nullptr);
  ASSERT_NE(driver.StatementRequestSchema, nullptr);
  ASSERT_EQ(driver.DriverGetInfo, file_driver::DriverGetInfo);
  driver.release(&driver, &error);
}

TEST_F(FileDriverTest, InjectsLocalFileSystemFromUri) {
  for (const std::string& uri :
       {path_, "file:///" + std::filesystem::path(path_).generic_string()}) {
    Handle<struct AdbcDatabase> database;
    NewDatabase(&database.value);
    ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "uri", uri.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
    Handle<struct AdbcConnection> connection;
    ASSERT_THAT(AdbcConnectionNew(&connection.value, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcConnectionInit(&connection.value, &database.value, &error),
                IsOkStatus(&error));
    ASSERT_EQ(FirstBytes(&connection.value), "hello");
  }
}

TEST_F(FileDriverTest, NoUriMeansFileSystemRequired) {
  Handle<struct AdbcDatabase> database;
  NewDatabase(&database.value);
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  Handle<struct AdbcConnection> connection;
  ASSERT_THAT(AdbcConnectionNew(&connection.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection.value, &database.value, &error),
              IsStatus(ADBC_STATUS_FILESYSTEM_REQUIRED, &error));
}

TEST_F(FileDriverTest, UnknownSchemeIsNotFound) {
  Handle<struct AdbcDatabase> database;
  NewDatabase(&database.value);
  ASSERT_THAT(
      AdbcDatabaseSetOption(&database.value, "uri", "s3://bucket/data.txt", &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  Handle<struct AdbcConnection> connection;
  ASSERT_THAT(AdbcConnectionNew(&connection.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection.value, &database.value, &error),
              IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("'s3'"));
}

TEST_F(FileDriverTest, NestedSchemeIsNotFound) {
  // An archive location parses, but this manager has no zip provider.
  Handle<struct AdbcDatabase> database;
  NewDatabase(&database.value);
  std::string uri = "zip:" + std::filesystem::path(path_).generic_string() + "!/a.csv";
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "uri", uri.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  Handle<struct AdbcConnection> connection;
  ASSERT_THAT(AdbcConnectionNew(&connection.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection.value, &database.value, &error),
              IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("'zip'"));
}

TEST_F(FileDriverTest, ExplicitConnectionFileSystem) {
  auto opens = std::make_shared<int>(0);
  Handle<struct AdbcDatabase> database;
  NewDatabase(&database.value);
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "uri", path_.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));

  struct AdbcFileSystem fs;
  file_driver::InitCounting(&fs, opens);
  Handle<struct AdbcConnection> connection;
  ASSERT_THAT(AdbcConnectionNew(&connection.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionSetFileSystem(&connection.value, &fs, &error),
              IsOkStatus(&error));
  ASSERT_EQ(fs.release, nullptr) << "the manager must take ownership";
  ASSERT_THAT(AdbcConnectionInit(&connection.value, &database.value, &error),
              IsOkStatus(&error));
  ASSERT_EQ(FirstBytes(&connection.value), "hello");
  ASSERT_EQ(*opens, 1) << "the driver did not read through the given file system";
}

TEST_F(FileDriverTest, DatabaseFileSystemIsInherited) {
  auto opens = std::make_shared<int>(0);
  Handle<struct AdbcDatabase> database;
  NewDatabase(&database.value);
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "uri", path_.c_str(), &error),
              IsOkStatus(&error));
  struct AdbcFileSystem fs;
  file_driver::InitCounting(&fs, opens);
  ASSERT_THAT(AdbcDatabaseSetFileSystem(&database.value, &fs, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  // The manager must not inject its own: both connections use the
  // database's (cloned) provider.
  for (int i = 0; i < 2; i++) {
    Handle<struct AdbcConnection> connection;
    ASSERT_THAT(AdbcConnectionNew(&connection.value, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcConnectionInit(&connection.value, &database.value, &error),
                IsOkStatus(&error));
    ASSERT_EQ(FirstBytes(&connection.value), "hello");
  }
  ASSERT_EQ(*opens, 2);

  // Too late now.
  struct AdbcFileSystem late;
  ASSERT_THAT(AdbcDriverManagerLocalFileSystemInit(&late, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetFileSystem(&database.value, &late, &error),
              IsStatus(ADBC_STATUS_INVALID_STATE, &error));
  late.release(&late);
}

TEST_F(FileDriverTest, NonFileDriverRejectsAFileSystem) {
  Handle<struct AdbcDatabase> database;
  NewDatabase(&database.value, file_driver::InitNotAFileDriver);
  struct AdbcFileSystem fs;
  ASSERT_THAT(AdbcDriverManagerLocalFileSystemInit(&fs, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetFileSystem(&database.value, &fs, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error));
}

int64_t CountRows(struct ArrowArrayStream* stream) {
  int64_t rows = 0;
  struct ArrowArray array;
  while (stream->get_next(stream, &array) == 0 && array.release) {
    rows += array.length;
    array.release(&array);
  }
  stream->release(stream);
  return rows;
}

TEST_F(FileDriverTest, DriverInfoBeforeAndAfterInit) {
  Handle<struct AdbcDatabase> database;
  NewDatabase(&database.value);

  // Before init: the manager loads the driver just to answer.
  struct ArrowArrayStream stream = {};
  ASSERT_THAT(AdbcDriverManagerDatabaseGetDriverInfo(&database.value, nullptr, 0, &stream,
                                                     &error),
              IsOkStatus(&error));
  ASSERT_EQ(CountRows(&stream), 3);
  uint32_t code = ADBC_INFO_DRIVER_ACCEPTS_FILESYSTEM;
  ASSERT_THAT(
      AdbcDriverManagerDatabaseGetDriverInfo(&database.value, &code, 1, &stream, &error),
      IsOkStatus(&error));
  ASSERT_EQ(CountRows(&stream), 1);

  // Not implemented by this driver.
  ASSERT_THAT(AdbcDriverManagerDatabaseGetDriverOptionsList(
                  &database.value, ADBC_OPTION_LEVEL_ALL, &stream, &error),
              IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error));

  // The database is unaffected and initializes normally; info still works.
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDriverManagerDatabaseGetDriverInfo(&database.value, nullptr, 0, &stream,
                                                     &error),
              IsOkStatus(&error));
  ASSERT_EQ(CountRows(&stream), 3);
}

TEST_F(FileDriverTest, ManifestVersionRules) {
  auto write = [&](const std::string& adbc) {
    auto path = dir_ / "test_file_driver.toml";
    std::ofstream(path) << "manifest_version = 1\n"
                        << adbc
                        << "\n[Driver]\nshared = 'adbc_no_such_library_for_tests'\n";
    return path.string();
  };
  struct AdbcDriver driver = {};
  // A minimum this manager cannot meet fails before any library is loaded.
  ASSERT_THAT(AdbcFindLoadDriver(write("[ADBC]\nmin_version = 'v1.3.0'").c_str(), nullptr,
                                 ADBC_VERSION_1_2_0, 0, nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("requires ADBC 1.3.0"));
  if (error.release) error.release(&error);
  // A satisfiable minimum, or an unreadable version, gets as far as loading
  // the (missing) library.
  for (const char* adbc :
       {"[ADBC]\nmin_version = '1.2.0'", "[ADBC]\nversion = 'latest'"}) {
    ASSERT_THAT(AdbcFindLoadDriver(write(adbc).c_str(), nullptr, ADBC_VERSION_1_2_0, 0,
                                   nullptr, &driver, &error),
                IsStatus(ADBC_STATUS_NOT_FOUND, &error))
        << adbc;
    if (error.release) error.release(&error);
  }
}

TEST_F(FileDriverTest, LocalFileSystemReads) {
  struct AdbcFileSystem fs;
  ASSERT_THAT(AdbcDriverManagerLocalFileSystemInit(&fs, &error), IsOkStatus(&error));
  struct AdbcFile* file = nullptr;
  ASSERT_THAT(fs.open(&fs, path_.data(), path_.size(), 0, &file, &error),
              IsOkStatus(&error));
  struct AdbcFileInfo info = {};
  info.struct_size = sizeof(info);
  ASSERT_EQ(file->info(file, &info), ADBC_STATUS_OK);
  ASSERT_EQ(info.size, 19u);

  char suffix[6] = {};
  char past_end[10] = {};
  struct AdbcFileRange ranges[] = {
      {0, 5, suffix, ADBC_FILE_RANGE_SUFFIX, 1},
      {15, 10, past_end, ADBC_FILE_RANGE_UP_TO, 2},
  };
  ASSERT_THAT(file->read_ranges(file, ranges, 2, &error), IsOkStatus(&error));
  ASSERT_EQ(std::string(suffix), "ivers");
  ASSERT_EQ(std::string(past_end), "vers");
  struct AdbcFileRange exact_past_end = {15, 10, past_end, 0, 3};
  ASSERT_THAT(file->read_ranges(file, &exact_past_end, 1, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));
  if (error.release) error.release(&error);

  // Async: each range completes once, never on this thread.
  struct State {
    std::mutex mutex;
    std::condition_variable changed;
    std::vector<std::tuple<uint64_t, AdbcStatusCode, uint64_t>> done;
    bool inline_completion = false;
    std::thread::id caller = std::this_thread::get_id();
  };
  AdbcFileReadCallback record = [](void* context, uint64_t user_data,
                                   AdbcStatusCode status, uint64_t bytes,
                                   const struct AdbcError*) {
    auto* s = static_cast<State*>(context);
    std::lock_guard<std::mutex> lock(s->mutex);
    if (std::this_thread::get_id() == s->caller) s->inline_completion = true;
    s->done.emplace_back(user_data, status, bytes);
    s->changed.notify_all();
  };
  {
    State state;
    char a[5] = {}, b[5] = {};
    struct AdbcFileRange async_ranges[] = {{0, 5, a, 0, 10}, {7, 4, b, 0, 11}};
    ASSERT_THAT(file->submit_reads(file, async_ranges, 2, record, &state, &error),
                IsOkStatus(&error));
    std::unique_lock<std::mutex> lock(state.mutex);
    ASSERT_TRUE(state.changed.wait_for(lock, std::chrono::seconds(30),
                                       [&] { return state.done.size() == 2; }));
    ASSERT_FALSE(state.inline_completion);
    std::sort(state.done.begin(), state.done.end());
    using Done = std::tuple<uint64_t, AdbcStatusCode, uint64_t>;
    ASSERT_EQ(state.done, (std::vector<Done>{Done{10, ADBC_STATUS_OK, 5},
                                             Done{11, ADBC_STATUS_OK, 4}}));
    ASSERT_EQ(std::string(a, 5), "hello");
    ASSERT_EQ(std::string(b, 4), "file");
  }
  {
    // Closing at once cancels what has not started, but close returns only
    // after every callback has: each read still completes exactly once.
    State state;
    char buffers[8][4] = {};
    std::vector<struct AdbcFileRange> async_ranges;
    for (uint64_t i = 0; i < 8; i++) {
      async_ranges.push_back({i, 4, buffers[i], 0, 100 + i});
    }
    ASSERT_THAT(file->submit_reads(file, async_ranges.data(), async_ranges.size(), record,
                                   &state, &error),
                IsOkStatus(&error));
    ASSERT_EQ(file->close(file), ADBC_STATUS_OK);
    std::lock_guard<std::mutex> lock(state.mutex);
    ASSERT_EQ(state.done.size(), 8u);
    for (const auto& [user_data, status, bytes] : state.done) {
      ASSERT_TRUE(status == ADBC_STATUS_OK || status == ADBC_STATUS_CANCELLED)
          << user_data << ": " << static_cast<int>(status);
    }
  }

  struct AdbcFile* missing = nullptr;
  std::string nope = (dir_ / "missing.txt").string();
  ASSERT_THAT(fs.open(&fs, nope.data(), nope.size(), 0, &missing, &error),
              IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  if (error.release) error.release(&error);
  fs.release(&fs);
}

}  // namespace adbc

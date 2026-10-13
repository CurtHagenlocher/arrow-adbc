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

// Reports sizes and offsets of the C ABI structs as the C compiler sees
// them, for comparison with the Rust mirrors in adbc_ffi.

#include <stddef.h>
#include <stdint.h>

#include <arrow-adbc/adbc.h>
#include <arrow-adbc/adbc_filesystem.h>

struct AdbcLayoutEntry {
  const char* name;
  uint64_t value;
};

#define SIZE(T) {"sizeof " #T, sizeof(struct T)}
#define OFFSET(T, F) {#T "." #F, offsetof(struct T, F)}
#define VALUE(V) {#V, (uint64_t)(V)}

static const struct AdbcLayoutEntry kEntries[] = {
    SIZE(AdbcError),
    SIZE(AdbcDriver),
    SIZE(AdbcMultiResultSet),
    VALUE(ADBC_DRIVER_1_0_0_SIZE),
    VALUE(ADBC_DRIVER_1_1_0_SIZE),
    VALUE(ADBC_DRIVER_1_2_0_SIZE),
    OFFSET(AdbcDriver, StatementSetSubstraitPlan),
    OFFSET(AdbcDriver, ErrorGetDetailCount),
    OFFSET(AdbcDriver, StatementSetOptionInt),
    OFFSET(AdbcDriver, ErrorGetVendorCode),
    OFFSET(AdbcDriver, DriverGetOptionsList),
    OFFSET(AdbcDriver, MultiResultSetNext),
    OFFSET(AdbcDriver, MultiResultSetNextPartitions),
    OFFSET(AdbcDriver, MultiResultSetRelease),
    OFFSET(AdbcDriver, ConnectionSetWarningHandler),
    OFFSET(AdbcDriver, StatementExecuteSchemaMulti),
    OFFSET(AdbcDriver, StatementExecuteMulti),
    OFFSET(AdbcDriver, StatementRequestSchema),
    OFFSET(AdbcDriver, DriverGetInfo),
    OFFSET(AdbcDriver, DatabaseSetFileSystem),
    OFFSET(AdbcDriver, ConnectionSetFileSystem),
    VALUE(ADBC_STATUS_FILESYSTEM_REQUIRED),
    VALUE(ADBC_INFO_DRIVER_ACCEPTS_FILESYSTEM),
    VALUE(ADBC_INFO_DRIVER_REQUIRES_FILESYSTEM),
    VALUE(ADBC_INFO_DRIVER_FILE_EXTENSIONS),
    VALUE(ADBC_INFO_DRIVER_MIME_TYPES),
    SIZE(AdbcFileRange),
    OFFSET(AdbcFileRange, offset),
    OFFSET(AdbcFileRange, length),
    OFFSET(AdbcFileRange, buffer),
    OFFSET(AdbcFileRange, flags),
    OFFSET(AdbcFileRange, user_data),
    SIZE(AdbcFileInfo),
    OFFSET(AdbcFileInfo, struct_size),
    OFFSET(AdbcFileInfo, flags),
    OFFSET(AdbcFileInfo, size),
    OFFSET(AdbcFileInfo, version),
    OFFSET(AdbcFileInfo, version_length),
    SIZE(AdbcFileSystem),
    OFFSET(AdbcFileSystem, struct_size),
    OFFSET(AdbcFileSystem, version),
    OFFSET(AdbcFileSystem, private_data),
    OFFSET(AdbcFileSystem, open),
    OFFSET(AdbcFileSystem, stat),
    OFFSET(AdbcFileSystem, list),
    OFFSET(AdbcFileSystem, get_extension),
    OFFSET(AdbcFileSystem, clone),
    OFFSET(AdbcFileSystem, release),
    SIZE(AdbcFileSystemProvider),
    OFFSET(AdbcFileSystemProvider, struct_size),
    OFFSET(AdbcFileSystemProvider, version),
    OFFSET(AdbcFileSystemProvider, private_data),
    OFFSET(AdbcFileSystemProvider, set_option),
    OFFSET(AdbcFileSystemProvider, open),
    OFFSET(AdbcFileSystemProvider, release),
    SIZE(AdbcFile),
    OFFSET(AdbcFile, struct_size),
    OFFSET(AdbcFile, version),
    OFFSET(AdbcFile, private_data),
    OFFSET(AdbcFile, info),
    OFFSET(AdbcFile, read_ranges),
    OFFSET(AdbcFile, submit_reads),
    OFFSET(AdbcFile, cancel),
    OFFSET(AdbcFile, close),
    VALUE(ADBC_FILESYSTEM_VERSION_1),
    VALUE(ADBC_FILESYSTEM_PROVIDER_VERSION_1),
    VALUE(ADBC_FILE_RANGE_SUFFIX),
    VALUE(ADBC_FILE_RANGE_UP_TO),
    VALUE(ADBC_FILE_INFO_SIZE_KNOWN),
    VALUE(ADBC_FILE_OPEN_SEQUENTIAL),
    VALUE(ADBC_FILE_OPEN_RANDOM),
};

const struct AdbcLayoutEntry* AdbcLayoutEntries(size_t* count) {
  *count = sizeof(kEntries) / sizeof(kEntries[0]);
  return kEntries;
}

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

/// \file arrow-adbc/adbc_filesystem.h ADBC File System API
///
/// PROTOTYPE: this header is part of the file-driver proposal and is
/// not (yet) part of any ADBC API revision.  Names, numbering, and
/// layout may change.
///
/// A file system is a table of callbacks that a host hands to a file
/// driver (see AdbcConnectionSetFileSystem).  The file driver never
/// opens files itself; it reads all of its bytes through this table.
/// This makes storage (local disk, object stores, archives) pluggable
/// and shareable between drivers.

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <arrow-adbc/adbc.h>

#ifdef __cplusplus
extern "C" {
#endif

/// \defgroup adbc-filesystem File System
/// @{

/// \brief The version of the AdbcFileSystem and AdbcFile tables
///   defined by this header.
#define ADBC_FILESYSTEM_VERSION_1 1

/// \defgroup adbc-filesystem-flags Flags
/// @{

/// \brief Open hint: the file will be read mostly front to back.
#define ADBC_FILE_OPEN_SEQUENTIAL 1
/// \brief Open hint: the file will be read at scattered offsets.
#define ADBC_FILE_OPEN_RANDOM 2

/// \brief Range flag: offset is ignored; read the final `length`
///   bytes of the file.  This allows fetching a footer without a stat.
#define ADBC_FILE_RANGE_SUFFIX 1
/// \brief Range flag: permit a short result, but only because the file
///   ended.  Without this flag a read either fills all `length` bytes
///   or fails.
#define ADBC_FILE_RANGE_UP_TO 2

/// \brief File info flag: the size field is valid.  An unknown size is
///   not the same as a size of zero.
#define ADBC_FILE_INFO_SIZE_KNOWN 1

/// @}

/// \brief One byte range to read.
///
/// Ranges are passed in arrays and have a fixed layout; unlike the
/// other records in this header they do not carry a struct_size.
struct AdbcFileRange {
  /// \brief The absolute offset, or ignored if ADBC_FILE_RANGE_SUFFIX.
  uint64_t offset;
  /// \brief The number of bytes to read.
  uint64_t length;
  /// \brief Caller-owned destination of at least `length` bytes.  It
  ///   must remain valid and untouched until this range completes, even
  ///   if the read is cancelled.  The provider writes at most `length`
  ///   bytes.  On error, its contents are unspecified.
  void* buffer;
  /// \brief A bitfield of ADBC_FILE_RANGE values.
  uint32_t flags;
  /// \brief Echoed to the callback.  Also the key for cancel.
  uint64_t user_data;
};

/// \brief Completion callback for AdbcFile::submit_reads.
///
/// Callbacks may run on any provider thread, but never on the thread
/// inside submit_reads.  They must be tiny: they must not block, wait
/// for another read, call close, or unwind/throw.  They should only
/// resolve a future or post work to an executor.  They may submit
/// follow-up reads.
///
/// \param[in] context The context passed to submit_reads.
/// \param[in] user_data The user_data of the completed range.
/// \param[in] status ADBC_STATUS_OK, ADBC_STATUS_CANCELLED, or an error.
/// \param[in] bytes_read On success, the number of bytes written to the
///   buffer.  This equals the range length unless ADBC_FILE_RANGE_UP_TO
///   was given and the file ended.
/// \param[in] error Error details, or NULL.  Borrowed; valid only for
///   the duration of the call.
typedef void (*AdbcFileReadCallback)(void* context, uint64_t user_data,
                                     AdbcStatusCode status, uint64_t bytes_read,
                                     const struct AdbcError* error);

/// \brief Metadata about a file.
///
/// Callers zero-initialize this and set struct_size.
struct AdbcFileInfo {
  /// \brief The size of this structure as known to the caller.
  uint32_t struct_size;
  /// \brief A bitfield of ADBC_FILE_INFO values.
  uint32_t flags;
  /// \brief The size of the file in bytes, if ADBC_FILE_INFO_SIZE_KNOWN.
  uint64_t size;
  /// \brief An opaque version token (ETag, generation, mtime...), or
  ///   NULL.  Drivers may use it to key metadata caches and to detect
  ///   change across opens; it is not a snapshot guarantee.  Borrowed
  ///   from the file (for info) or valid until the next call on the
  ///   file system from the same thread (for stat).
  const uint8_t* version;
  /// \brief The length of version in bytes.
  size_t version_length;
};

struct AdbcFile;

/// \brief A file system: a store of named, immutable files.
///
/// Names are store-relative, length-delimited UTF-8.  A provider must
/// confine names to its store; a name must not escape it (for example
/// via "..").
///
/// The table is owned by whoever holds it; pass ownership by moving
/// the struct (copy it, then zero the source), following the Arrow C
/// data interface conventions.  Use clone to share one provider.
struct AdbcFileSystem {
  /// \brief The size of this structure as known to the provider.
  uint32_t struct_size;
  /// \brief The table version, e.g. ADBC_FILESYSTEM_VERSION_1.
  uint32_t version;
  /// \brief Opaque provider state.
  void* private_data;

  /// \brief Open a file.  Blocking.  May be lazy: no I/O need happen
  ///   until the first read or info.
  ///
  /// \param[in] hints A bitfield of ADBC_FILE_OPEN values.
  /// \param[out] out A pointer to the new file, freed by its close.
  /// \return ADBC_STATUS_NOT_FOUND if the file does not exist (this may
  ///   instead be reported by the first read if open is lazy).
  AdbcStatusCode (*open)(struct AdbcFileSystem* self, const char* name, size_t name_len,
                         uint32_t hints, struct AdbcFile** out, struct AdbcError* error);

  /// \brief Get metadata about a file without opening it.  Blocking.
  AdbcStatusCode (*stat)(struct AdbcFileSystem* self, const char* name, size_t name_len,
                         struct AdbcFileInfo* out, struct AdbcError* error);

  /// \brief List the entries under a prefix.  Optional (may be NULL).
  ///
  /// The result has the schema:
  ///
  /// | Field Name   | Field Type    |
  /// |--------------|---------------|
  /// | name         | utf8 not null |
  /// | size         | uint64        |
  /// | version      | binary        |
  /// | is_directory | bool not null |
  AdbcStatusCode (*list)(struct AdbcFileSystem* self, const char* prefix,
                         size_t prefix_len, struct ArrowArrayStream* out,
                         struct AdbcError* error);

  /// \brief Look up an optional extension table by name and major
  ///   version.  Optional (may be NULL).
  ///
  /// \return ADBC_STATUS_NOT_FOUND if the extension is not supported.
  AdbcStatusCode (*get_extension)(struct AdbcFileSystem* self, const char* name,
                                  uint32_t major, const void** out_table);

  /// \brief Create a new, independent handle to this file system that
  ///   shares internal state (connection pools, caches).  The new
  ///   handle has its own release.
  AdbcStatusCode (*clone)(struct AdbcFileSystem* self, struct AdbcFileSystem* out,
                          struct AdbcError* error);

  /// \brief Release this handle.  Every file opened through it must
  ///   already be closed.  Sets release to NULL.
  void (*release)(struct AdbcFileSystem* self);
};

/// \brief An open file.
///
/// Reads on one file may be issued concurrently from any thread.  There
/// is no shared cursor.  A provider implements read_ranges or
/// submit_reads (or both); a driver manager wraps a provider so that
/// drivers always see both.
struct AdbcFile {
  /// \brief The size of this structure as known to the provider.
  uint32_t struct_size;
  /// \brief The table version, e.g. ADBC_FILESYSTEM_VERSION_1.
  uint32_t version;
  /// \brief Opaque provider state.
  void* private_data;

  /// \brief Get metadata about the file, as observed at open.
  AdbcStatusCode (*info)(struct AdbcFile* self, struct AdbcFileInfo* out);

  /// \brief Read ranges, blocking until every range has completed.
  ///
  /// \return ADBC_STATUS_OK if every range was read in full (or to end
  ///   of file with ADBC_FILE_RANGE_UP_TO), else an error.  Use
  ///   submit_reads to learn the per-range outcome.
  AdbcStatusCode (*read_ranges)(struct AdbcFile* self, const struct AdbcFileRange* ranges,
                                size_t count, struct AdbcError* error);

  /// \brief Submit ranges for asynchronous reading.
  ///
  /// Admission is all-or-nothing.  If this returns an error, nothing was
  /// accepted and no callback will fire.  On success, each range gets
  /// exactly one callback (success, error, or ADBC_STATUS_CANCELLED),
  /// never on the calling thread.  The ranges array may be freed when
  /// this returns; the buffers may not.  The provider may coalesce,
  /// split, or parallelize ranges; they complete in any order.
  ///
  /// \param[in] context Passed to callback.  Must remain valid until the
  ///   last callback for this submission has returned.
  AdbcStatusCode (*submit_reads)(struct AdbcFile* self,
                                 const struct AdbcFileRange* ranges, size_t count,
                                 AdbcFileReadCallback callback, void* context,
                                 struct AdbcError* error);

  /// \brief Request cancellation of outstanding ranges with this
  ///   user_data.  Best-effort; the callback remains authoritative and
  ///   may still report success.
  void (*cancel)(struct AdbcFile* self, uint64_t user_data);

  /// \brief Cancel outstanding reads, wait until all of their callbacks
  ///   have returned, and free the file.  Must not be called from a
  ///   callback.
  AdbcStatusCode (*close)(struct AdbcFile* self);
};

/// @}

#ifdef __cplusplus
}
#endif

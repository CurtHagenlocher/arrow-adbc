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

// File systems for file drivers (PROTOTYPE, file-driver proposal): the
// built-in local provider, the uri -> location rule, and the state the
// driver manager keeps per file-driver database.

#include "adbc_driver_manager_internal.h"

#if !defined(_WIN32)
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <condition_variable>
#include <cstddef>
#include <cstring>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "arrow-adbc/adbc.h"
#include "arrow-adbc/adbc_driver_manager.h"
#include "arrow-adbc/adbc_filesystem.h"

namespace {

// ---------------------------------------------------------------------------
// The local provider

struct LocalFile {
#if defined(_WIN32)
  HANDLE handle = INVALID_HANDLE_VALUE;
#else
  int fd = -1;
#endif
  std::string path;
  uint64_t size = 0;
  std::string version;  // modification time and size

  // Async reads: one worker thread per submission. close waits for all.
  std::mutex mutex;
  std::condition_variable drained;
  int in_flight = 0;
  bool closing = false;
  std::multiset<uint64_t> cancelled;
};

AdbcStatusCode SetFsError(struct AdbcError* error, AdbcStatusCode status,
                          const std::string& message) {
  SetError(error, message);
  return status;
}

/// Read into buffer at offset until it is full or the file ends.
bool ReadAt(LocalFile* file, uint8_t* buffer, uint64_t length, uint64_t offset,
            uint64_t* total, std::string* message) {
  *total = 0;
  while (*total < length) {
    uint64_t want = std::min<uint64_t>(length - *total, 1u << 30);
#if defined(_WIN32)
    OVERLAPPED overlapped = {};
    uint64_t at = offset + *total;
    overlapped.Offset = static_cast<DWORD>(at & 0xFFFFFFFFu);
    overlapped.OffsetHigh = static_cast<DWORD>(at >> 32);
    DWORD read = 0;
    if (!ReadFile(file->handle, buffer + *total, static_cast<DWORD>(want), &read,
                  &overlapped)) {
      if (GetLastError() == ERROR_HANDLE_EOF) break;
      GetWinError(message);
      return false;
    }
#else
    ssize_t read =
        pread(file->fd, buffer + *total, want, static_cast<off_t>(offset + *total));
    if (read < 0) {
      if (errno == EINTR) continue;
      *message = std::strerror(errno);
      return false;
    }
#endif
    if (read == 0) break;
    *total += static_cast<uint64_t>(read);
  }
  return true;
}

/// Resolve a range against the file size; returns false if it cannot be
/// satisfied exactly and UP_TO is not set.
bool ResolveRange(const struct AdbcFileRange& range, uint64_t size, uint64_t* offset,
                  uint64_t* length) {
  uint64_t available;
  if (range.flags & ADBC_FILE_RANGE_SUFFIX) {
    available = std::min(range.length, size);
    *offset = size - available;
  } else if (range.offset >= size) {
    *offset = range.offset;
    available = 0;
  } else {
    *offset = range.offset;
    available = std::min(range.length, size - range.offset);
  }
  *length = available;
  return available == range.length || (range.flags & ADBC_FILE_RANGE_UP_TO);
}

/// Read one range; on success *bytes_read is set.
AdbcStatusCode ReadRange(LocalFile* file, const struct AdbcFileRange& range,
                         uint64_t* bytes_read, std::string* message) {
  uint64_t offset = 0, length = 0;
  if (!ResolveRange(range, file->size, &offset, &length)) {
    *message = "read past end of file: " + file->path + " has " +
               std::to_string(file->size) + " bytes (use ADBC_FILE_RANGE_UP_TO)";
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  uint64_t total = 0;
  if (!ReadAt(file, static_cast<uint8_t*>(range.buffer), length, offset, &total,
              message)) {
    *message = file->path + ": " + *message;
    return ADBC_STATUS_IO;
  }
  if (total < length) {
    *message = file->path + ": short read (the file changed while open)";
    return ADBC_STATUS_IO;
  }
  *bytes_read = total;
  return ADBC_STATUS_OK;
}

AdbcStatusCode LocalFileInfo(struct AdbcFile* self, struct AdbcFileInfo* out) {
  if (!self || !out) return ADBC_STATUS_INVALID_ARGUMENT;
  auto* file = static_cast<LocalFile*>(self->private_data);
  size_t available = out->struct_size;
  if (available >= offsetof(AdbcFileInfo, flags) + sizeof(uint32_t)) {
    out->flags = ADBC_FILE_INFO_SIZE_KNOWN;
  }
  if (available >= offsetof(AdbcFileInfo, size) + sizeof(uint64_t)) {
    out->size = file->size;
  }
  if (available >= offsetof(AdbcFileInfo, version_length) + sizeof(size_t)) {
    out->version = reinterpret_cast<const uint8_t*>(file->version.data());
    out->version_length = file->version.size();
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode LocalFileReadRanges(struct AdbcFile* self,
                                   const struct AdbcFileRange* ranges, size_t count,
                                   struct AdbcError* error) {
  if (!self || (!ranges && count > 0)) return ADBC_STATUS_INVALID_ARGUMENT;
  auto* file = static_cast<LocalFile*>(self->private_data);
  for (size_t i = 0; i < count; i++) {
    uint64_t bytes_read = 0;
    std::string message;
    AdbcStatusCode status = ReadRange(file, ranges[i], &bytes_read, &message);
    if (status != ADBC_STATUS_OK) return SetFsError(error, status, message);
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode LocalFileSubmitReads(struct AdbcFile* self,
                                    const struct AdbcFileRange* ranges, size_t count,
                                    AdbcFileReadCallback callback, void* context,
                                    struct AdbcError* error) {
  if (!self || !callback || (!ranges && count > 0)) {
    return SetFsError(error, ADBC_STATUS_INVALID_ARGUMENT,
                      "submit_reads: invalid arguments");
  }
  for (size_t i = 0; i < count; i++) {
    if (!ranges[i].buffer && ranges[i].length > 0) {
      return SetFsError(error, ADBC_STATUS_INVALID_ARGUMENT,
                        "submit_reads: NULL buffer for a non-empty range");
    }
  }
  if (count == 0) return ADBC_STATUS_OK;
  auto* file = static_cast<LocalFile*>(self->private_data);
  {
    std::lock_guard<std::mutex> lock(file->mutex);
    if (file->closing) {
      return SetFsError(error, ADBC_STATUS_INVALID_STATE,
                        "submit_reads: file is closing");
    }
    file->in_flight++;
  }
  std::vector<AdbcFileRange> copy(ranges, ranges + count);
  // Never complete inline: the worker thread delivers every callback.
  std::thread([file, copy = std::move(copy), callback, context]() {
    for (const auto& range : copy) {
      bool cancelled;
      {
        std::lock_guard<std::mutex> lock(file->mutex);
        auto it = file->cancelled.find(range.user_data);
        cancelled = file->closing || it != file->cancelled.end();
        if (it != file->cancelled.end()) file->cancelled.erase(it);
      }
      if (cancelled) {
        callback(context, range.user_data, ADBC_STATUS_CANCELLED, 0, nullptr);
        continue;
      }
      uint64_t bytes_read = 0;
      std::string message;
      AdbcStatusCode status = ReadRange(file, range, &bytes_read, &message);
      if (status == ADBC_STATUS_OK) {
        callback(context, range.user_data, status, bytes_read, nullptr);
      } else {
        struct AdbcError error = ADBC_ERROR_INIT;
        SetError(&error, message);
        callback(context, range.user_data, status, 0, &error);
        if (error.release) error.release(&error);
      }
    }
    std::lock_guard<std::mutex> lock(file->mutex);
    file->in_flight--;
    file->drained.notify_all();
  }).detach();
  return ADBC_STATUS_OK;
}

void LocalFileCancel(struct AdbcFile* self, uint64_t user_data) {
  if (!self) return;
  auto* file = static_cast<LocalFile*>(self->private_data);
  std::lock_guard<std::mutex> lock(file->mutex);
  file->cancelled.insert(user_data);
}

AdbcStatusCode LocalFileClose(struct AdbcFile* self) {
  if (!self) return ADBC_STATUS_INVALID_ARGUMENT;
  auto* file = static_cast<LocalFile*>(self->private_data);
  {
    // Cancel what has not started, and wait until every callback returned.
    std::unique_lock<std::mutex> lock(file->mutex);
    file->closing = true;
    file->drained.wait(lock, [file] { return file->in_flight == 0; });
  }
#if defined(_WIN32)
  CloseHandle(file->handle);
#else
  ::close(file->fd);
#endif
  delete file;
  delete self;
  return ADBC_STATUS_OK;
}

AdbcStatusCode OpenLocal(const std::string& path, LocalFile** out,
                         struct AdbcError* error) {
  auto file = std::make_unique<LocalFile>();
  file->path = path;
#if defined(_WIN32)
  file->handle = CreateFileW(Utf8Decode(path).c_str(), GENERIC_READ,
                             FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, OPEN_EXISTING,
                             FILE_ATTRIBUTE_NORMAL, nullptr);
  if (file->handle == INVALID_HANDLE_VALUE) {
    DWORD code = GetLastError();
    std::string message;
    GetWinError(&message);
    AdbcStatusCode status =
        (code == ERROR_FILE_NOT_FOUND || code == ERROR_PATH_NOT_FOUND)
            ? ADBC_STATUS_NOT_FOUND
            : (code == ERROR_ACCESS_DENIED ? ADBC_STATUS_UNAUTHORIZED : ADBC_STATUS_IO);
    return SetFsError(error, status, path + ": " + message);
  }
  BY_HANDLE_FILE_INFORMATION info;
  if (!GetFileInformationByHandle(file->handle, &info)) {
    std::string message;
    GetWinError(&message);
    CloseHandle(file->handle);
    return SetFsError(error, ADBC_STATUS_IO, path + ": " + message);
  }
  file->size = (static_cast<uint64_t>(info.nFileSizeHigh) << 32) | info.nFileSizeLow;
  uint64_t modified = (static_cast<uint64_t>(info.ftLastWriteTime.dwHighDateTime) << 32) |
                      info.ftLastWriteTime.dwLowDateTime;
#else
  file->fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (file->fd < 0) {
    AdbcStatusCode status =
        errno == ENOENT ? ADBC_STATUS_NOT_FOUND
                        : (errno == EACCES ? ADBC_STATUS_UNAUTHORIZED : ADBC_STATUS_IO);
    return SetFsError(error, status, path + ": " + std::strerror(errno));
  }
  struct stat st;
  if (fstat(file->fd, &st) != 0 || S_ISDIR(st.st_mode)) {
    ::close(file->fd);
    return SetFsError(error, ADBC_STATUS_INVALID_ARGUMENT, path + ": not a regular file");
  }
  file->size = static_cast<uint64_t>(st.st_size);
  uint64_t modified = static_cast<uint64_t>(st.st_mtime);
#endif
  file->version.assign(reinterpret_cast<const char*>(&modified), sizeof(modified));
  file->version.append(reinterpret_cast<const char*>(&file->size), sizeof(file->size));
  *out = file.release();
  return ADBC_STATUS_OK;
}

AdbcStatusCode LocalFsOpen(struct AdbcFileSystem* self, const char* name, size_t name_len,
                           uint32_t hints, struct AdbcFile** out,
                           struct AdbcError* error) {
  if (!self || !out || (!name && name_len > 0)) return ADBC_STATUS_INVALID_ARGUMENT;
  LocalFile* file = nullptr;
  CHECK_STATUS(OpenLocal(std::string(name, name_len), &file, error));
  auto* table = new AdbcFile{};
  table->struct_size = sizeof(AdbcFile);
  table->version = ADBC_FILESYSTEM_VERSION_1;
  table->private_data = file;
  table->info = LocalFileInfo;
  table->read_ranges = LocalFileReadRanges;
  table->submit_reads = LocalFileSubmitReads;
  table->cancel = LocalFileCancel;
  table->close = LocalFileClose;
  *out = table;
  return ADBC_STATUS_OK;
}

// The version token for stat lives here until the next stat on this thread.
thread_local std::string stat_version;

AdbcStatusCode LocalFsStat(struct AdbcFileSystem* self, const char* name, size_t name_len,
                           struct AdbcFileInfo* out, struct AdbcError* error) {
  if (!self || !out || (!name && name_len > 0)) return ADBC_STATUS_INVALID_ARGUMENT;
  LocalFile* file = nullptr;
  CHECK_STATUS(OpenLocal(std::string(name, name_len), &file, error));
  AdbcFile table{};
  table.private_data = file;
  LocalFileInfo(&table, out);
  stat_version = file->version;
  if (out->struct_size >= offsetof(AdbcFileInfo, version_length) + sizeof(size_t)) {
    out->version = reinterpret_cast<const uint8_t*>(stat_version.data());
  }
#if defined(_WIN32)
  CloseHandle(file->handle);
#else
  ::close(file->fd);
#endif
  delete file;
  return ADBC_STATUS_OK;
}

AdbcStatusCode LocalFsGetExtension(struct AdbcFileSystem*, const char*, uint32_t,
                                   const void** out_table) {
  if (out_table) *out_table = nullptr;
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode LocalFsClone(struct AdbcFileSystem* self, struct AdbcFileSystem* out,
                            struct AdbcError* error);

void LocalFsRelease(struct AdbcFileSystem* self) {
  if (!self || !self->release) return;
  self->release = nullptr;
}

void InitLocal(struct AdbcFileSystem* out) {
  std::memset(out, 0, sizeof(*out));
  out->struct_size = sizeof(AdbcFileSystem);
  out->version = ADBC_FILESYSTEM_VERSION_1;
  out->open = LocalFsOpen;
  out->stat = LocalFsStat;
  out->list = nullptr;  // optional; not implemented by the prototype
  out->get_extension = LocalFsGetExtension;
  out->clone = LocalFsClone;
  out->release = LocalFsRelease;
}

AdbcStatusCode LocalFsClone(struct AdbcFileSystem* self, struct AdbcFileSystem* out,
                            struct AdbcError* error) {
  if (!self || !out) return ADBC_STATUS_INVALID_ARGUMENT;
  InitLocal(out);
  return ADBC_STATUS_OK;
}

// ---------------------------------------------------------------------------
// uri -> (scheme, authority, path); the same rule as adbc_core's Location

bool HasDriveLetter(std::string_view s) {
  return s.size() >= 3 && std::isalpha(static_cast<unsigned char>(s[0])) && s[1] == ':' &&
         (s[2] == '/' || s[2] == '\\');
}

struct Location {
  std::string scheme;
  std::string authority;
  std::string path;
};

std::optional<Location> ParseLocation(std::string_view uri) {
  auto local = [](std::string_view path) {
    Location location{"file", "", std::string(path)};
    std::replace(location.path.begin(), location.path.end(), '\\', '/');
    return location;
  };
  if (uri.empty()) return std::nullopt;
  if (HasDriveLetter(uri)) return local(uri);
  // scheme:<inner location>!/<path>: a store inside another location
  // (e.g. zip:file:///C:/a.zip!/x.csv); the path follows the last "!/".
  if (auto colon = uri.find(':'); colon != std::string_view::npos && colon > 0 &&
                                  std::isalpha(static_cast<unsigned char>(uri[0])) &&
                                  uri.substr(colon + 1, 2) != "//" &&
                                  colon + 1 < uri.size()) {
    std::string scheme(uri.substr(0, colon));
    bool valid = std::all_of(scheme.begin(), scheme.end(), [](char c) {
      return std::isalnum(static_cast<unsigned char>(c)) || c == '+' || c == '-' ||
             c == '.';
    });
    if (valid) {
      std::transform(scheme.begin(), scheme.end(), scheme.begin(), [](char c) {
        return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
      });
      std::string_view rest = uri.substr(colon + 1);
      auto bang = rest.rfind("!/");
      std::string_view inner =
          bang == std::string_view::npos ? rest : rest.substr(0, bang);
      std::string path =
          bang == std::string_view::npos ? "" : std::string(rest.substr(bang + 2));
      return Location{scheme, std::string(inner), path};
    }
  }
  auto sep = uri.find("://");
  if (sep == std::string_view::npos) return local(uri);
  std::string scheme(uri.substr(0, sep));
  if (scheme.empty()) return std::nullopt;
  for (char& c : scheme) {
    if (!std::isalnum(static_cast<unsigned char>(c)) && c != '+' && c != '-' &&
        c != '.') {
      return std::nullopt;
    }
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  std::string_view rest = uri.substr(sep + 3);
  auto slash = rest.find('/');
  std::string_view authority =
      slash == std::string_view::npos ? rest : rest.substr(0, slash);
  std::string_view path =
      slash == std::string_view::npos ? std::string_view() : rest.substr(slash);
  if (scheme == "file") {
    std::string_view trimmed = path.empty() ? path : path.substr(1);
    if (HasDriveLetter(trimmed)) path = trimmed;
  } else if (!path.empty()) {
    path = path.substr(1);
  }
  return Location{scheme, std::string(authority), std::string(path)};
}

// ---------------------------------------------------------------------------
// Per-database state for file drivers

struct FileDriverState {
  std::optional<std::string> uri;
  bool database_has_file_system = false;
  bool accepts_file_system = false;
};

std::mutex state_mutex;
std::unordered_map<const AdbcDriver*, FileDriverState> states;

/// Read ADBC_INFO_DRIVER_ACCEPTS_FILESYSTEM from a GetInfo stream.
bool ReadAcceptsFileSystem(struct ArrowArrayStream* stream) {
  bool accepts = false;
  struct ArrowArray array;
  while (stream->get_next(stream, &array) == 0 && array.release) {
    // struct<info_name: uint32, info_value: dense_union<..., bool_value (1), ...>>
    if (array.n_children == 2) {
      struct ArrowArray* names = array.children[0];
      struct ArrowArray* values = array.children[1];
      const auto* codes = static_cast<const uint32_t*>(names->buffers[1]);
      const auto* type_ids = static_cast<const int8_t*>(values->buffers[0]);
      const auto* offsets = static_cast<const int32_t*>(values->buffers[1]);
      for (int64_t i = 0; i < array.length; i++) {
        int64_t row = i + array.offset;
        if (codes[names->offset + row] != ADBC_INFO_DRIVER_ACCEPTS_FILESYSTEM) continue;
        int64_t union_row = values->offset + row;
        if (type_ids[union_row] != 1 || values->n_children < 2) continue;
        struct ArrowArray* bools = values->children[1];
        int64_t bit = bools->offset + offsets[union_row];
        const auto* validity = static_cast<const uint8_t*>(bools->buffers[0]);
        const auto* data = static_cast<const uint8_t*>(bools->buffers[1]);
        bool valid = !validity || (validity[bit / 8] >> (bit % 8)) & 1;
        if (valid && ((data[bit / 8] >> (bit % 8)) & 1)) accepts = true;
      }
    }
    array.release(&array);
  }
  return accepts;
}

}  // namespace

// ---------------------------------------------------------------------------
// Internal API used by adbc_driver_manager_api.cc

bool InternalAdbcProbeAcceptsFileSystem(struct AdbcDriver* driver) {
  if (!driver->DriverGetInfo) return false;
  uint32_t code = ADBC_INFO_DRIVER_ACCEPTS_FILESYSTEM;
  struct ArrowArrayStream stream = {};
  struct AdbcError error = ADBC_ERROR_INIT;
  AdbcStatusCode status = driver->DriverGetInfo(&code, 1, &stream, &error);
  if (error.release) error.release(&error);
  if (status != ADBC_STATUS_OK || !stream.release) return false;
  bool accepts = ReadAcceptsFileSystem(&stream);
  stream.release(&stream);
  return accepts;
}

void InternalAdbcRecordFileDriverState(const struct AdbcDriver* driver,
                                       std::optional<std::string> uri,
                                       bool database_has_file_system,
                                       bool accepts_file_system) {
  std::lock_guard<std::mutex> lock(state_mutex);
  states[driver] =
      FileDriverState{std::move(uri), database_has_file_system, accepts_file_system};
}

void InternalAdbcForgetFileDriverState(const struct AdbcDriver* driver) {
  std::lock_guard<std::mutex> lock(state_mutex);
  states.erase(driver);
}

AdbcStatusCode InternalAdbcDefaultConnectionFileSystem(const struct AdbcDriver* driver,
                                                       struct AdbcFileSystem* out,
                                                       bool* provided,
                                                       struct AdbcError* error) {
  *provided = false;
  FileDriverState state;
  {
    std::lock_guard<std::mutex> lock(state_mutex);
    auto it = states.find(driver);
    if (it == states.end()) return ADBC_STATUS_OK;
    state = it->second;
  }
  // Not a file driver, or the driver inherits the database's file system,
  // or there is nothing to resolve (the driver reports FILESYSTEM_REQUIRED).
  if (!state.accepts_file_system || state.database_has_file_system || !state.uri) {
    return ADBC_STATUS_OK;
  }
  auto location = ParseLocation(*state.uri);
  if (!location) {
    SetError(error, "[Driver Manager] Cannot parse uri '" + *state.uri +
                        "' as a file-driver location");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  if (location->scheme != "file") {
    SetError(error, "[Driver Manager] No file system is registered for scheme '" +
                        location->scheme + "' (this driver manager only has 'file')");
    return ADBC_STATUS_NOT_FOUND;
  }
  if (!location->authority.empty() && location->authority != "localhost") {
    SetError(error, "[Driver Manager] file:// locations on another host ('" +
                        location->authority + "') are not supported");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
  InitLocal(out);
  *provided = true;
  return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcDriverManagerLocalFileSystemInit(struct AdbcFileSystem* out,
                                                    struct AdbcError* error) {
  if (!out) {
    SetError(error, "[Driver Manager] out must not be NULL");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  InitLocal(out);
  return ADBC_STATUS_OK;
}

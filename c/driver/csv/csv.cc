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

// An ADBC file driver for CSV files (PROTOTYPE, file-driver proposal).
//
// It never opens files itself: it reads and lists through the file system
// the host hands it (a directory on disk, a ZIP archive, an object store...)
// and refuses ADBC versions older than 1.2.0.
//
// - Database: the `uri` option names the root (a directory, or e.g.
//   `zip:file:///C:/data/a.zip!/`).
// - A statement is a scan, configured with options:
//   - `adbc.file.target`: a file (`orders/2024-01.csv`), a directory
//     (`orders`: its *.csv files), or a pattern in the last component
//     (`orders/2024-*.csv`);
//   - `adbc.file.columns`: the columns to return, as a JSON array of names;
//   - `adbc.file.limit`: the maximum number of rows (per partition when
//     executed as partitions);
//   - `adbc.file.header`: whether the first row holds column names
//     (default true).
// - ExecutePartitions returns one partition per file; the column types are
//   inferred across all files and carried in the partitions, so partitions
//   read in parallel agree on the schema.

#include <algorithm>
#include <charconv>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <arrow-adbc/adbc_filesystem.h>
#include <nanoarrow/nanoarrow.h>
#include <nanoarrow/nanoarrow.hpp>

#include "driver/framework/base_driver.h"
#include "driver/framework/objects.h"
#include "driver/framework/status.h"
#include "driver/framework/utility.h"

// Like UNWRAP_RESULT, but usable to declare: CSV_ASSIGN_OR_RETURN(auto x, f()).
#define CSV_CONCAT_IMPL(x, y) x##y
#define CSV_CONCAT(x, y) CSV_CONCAT_IMPL(x, y)
#define CSV_ASSIGN_OR_RETURN_IMPL(result, lhs, rhs) \
  auto result = (rhs);                              \
  if (!result.has_value()) {                        \
    return std::move(result).status();              \
  }                                                 \
  lhs = std::move(result).value()
#define CSV_ASSIGN_OR_RETURN(lhs, rhs) \
  CSV_ASSIGN_OR_RETURN_IMPL(CSV_CONCAT(csv_result_, __LINE__), lhs, rhs)

namespace adbc::csv {
namespace {

using driver::Option;
using driver::Result;
using driver::Status;
namespace status = adbc::driver::status;

constexpr std::string_view kOptionTarget = "adbc.file.target";
constexpr std::string_view kOptionColumns = "adbc.file.columns";
constexpr std::string_view kOptionLimit = "adbc.file.limit";
constexpr std::string_view kOptionHeader = "adbc.file.header";
constexpr std::string_view kTableTypeFile = "FILE";
constexpr std::string_view kTableTypeDirectory = "DIRECTORY";

// ---------------------------------------------------------------------------
// Locations: the same rule as adbc_core's Location and the driver manager.

bool HasDriveLetter(std::string_view s) {
  return s.size() >= 3 && std::isalpha(static_cast<unsigned char>(s[0])) && s[1] == ':' &&
         (s[2] == '/' || s[2] == '\\');
}

bool IsSchemeChar(char c) {
  return std::isalnum(static_cast<unsigned char>(c)) || c == '+' || c == '-' || c == '.';
}

/// The path part of a location: what to open in the store.
Result<std::string> LocationPath(std::string_view uri) {
  if (uri.empty()) return status::InvalidArgument("[CSV] empty uri");
  std::string path;
  if (HasDriveLetter(uri)) {
    path = std::string(uri);
  } else if (auto colon = uri.find(':');
             colon != std::string_view::npos && colon > 0 &&
             std::all_of(uri.begin(), uri.begin() + colon, IsSchemeChar) &&
             uri.substr(colon + 1, 2) != "//") {
    // scheme:<inner location>!/<path>
    std::string_view rest = uri.substr(colon + 1);
    auto bang = rest.rfind("!/");
    path = bang == std::string_view::npos ? "" : std::string(rest.substr(bang + 2));
  } else if (auto sep = uri.find("://"); sep != std::string_view::npos) {
    std::string scheme(uri.substr(0, sep));
    std::string_view rest = uri.substr(sep + 3);
    auto slash = rest.find('/');
    std::string_view p =
        slash == std::string_view::npos ? std::string_view() : rest.substr(slash);
    if (scheme == "file") {
      std::string_view trimmed = p.empty() ? p : p.substr(1);
      if (HasDriveLetter(trimmed)) p = trimmed;
    } else if (!p.empty()) {
      p = p.substr(1);
    }
    path = std::string(p);
  } else {
    path = std::string(uri);
  }
  std::replace(path.begin(), path.end(), '\\', '/');
  while (path.size() > 1 && path.back() == '/') path.pop_back();
  return path;
}

std::string Join(std::string_view root, std::string_view name) {
  while (!name.empty() && name.front() == '/') name.remove_prefix(1);
  if (root.empty() || root == "/") return std::string(root) + std::string(name);
  if (name.empty()) return std::string(root);
  return std::string(root) + "/" + std::string(name);
}

/// Shell-style matching of `*` and `?`.
bool Matches(std::string_view pattern, std::string_view name) {
  if (pattern.empty()) return name.empty();
  if (pattern[0] == '*') {
    for (size_t i = 0; i <= name.size(); i++) {
      if (Matches(pattern.substr(1), name.substr(i))) return true;
    }
    return false;
  }
  return !name.empty() && (pattern[0] == '?' || pattern[0] == name[0]) &&
         Matches(pattern.substr(1), name.substr(1));
}

bool EndsWithCsv(std::string_view name) {
  if (name.size() < 4) return false;
  std::string ext(name.substr(name.size() - 4));
  std::transform(ext.begin(), ext.end(), ext.begin(),
                 [](char c) { return static_cast<char>(std::tolower(c)); });
  return ext == ".csv";
}

// ---------------------------------------------------------------------------
// The file system

/// An owned AdbcFileSystem.
class FileSystem {
 public:
  FileSystem() = default;
  FileSystem(const FileSystem&) = delete;
  FileSystem& operator=(const FileSystem&) = delete;
  ~FileSystem() { Reset(); }

  bool valid() const { return fs_.release != nullptr; }

  /// Take ownership of `fs`, zeroing the caller's struct.
  void Take(struct AdbcFileSystem* fs) {
    Reset();
    fs_ = *fs;
    std::memset(fs, 0, sizeof(*fs));
  }

  Status CloneTo(FileSystem* out) {
    struct AdbcFileSystem clone = {};
    struct AdbcError error = ADBC_ERROR_INIT;
    AdbcStatusCode code = fs_.clone(&fs_, &clone, &error);
    if (code != ADBC_STATUS_OK) return ErrorStatus(code, &error, "clone");
    out->Take(&clone);
    return status::Ok();
  }

  /// Read a whole file.
  Result<std::string> ReadAll(const std::string& name) {
    struct AdbcError error = ADBC_ERROR_INIT;
    struct AdbcFile* file = nullptr;
    AdbcStatusCode code = fs_.open(&fs_, name.data(), name.size(),
                                   ADBC_FILE_OPEN_SEQUENTIAL, &file, &error);
    if (code != ADBC_STATUS_OK) return ErrorStatus(code, &error, name);
    struct AdbcFileInfo info = {};
    info.struct_size = sizeof(info);
    code = file->info(file, &info);
    if (code != ADBC_STATUS_OK || !(info.flags & ADBC_FILE_INFO_SIZE_KNOWN)) {
      file->close(file);
      return status::IO("[CSV] cannot determine the size of ", name);
    }
    std::string data(static_cast<size_t>(info.size), '\0');
    struct AdbcFileRange range = {0, info.size, data.data(), 0, 0};
    code = file->read_ranges(file, &range, 1, &error);
    file->close(file);
    if (code != ADBC_STATUS_OK) return ErrorStatus(code, &error, name);
    return data;
  }

  struct Entry {
    std::string name;
    bool is_directory;
  };

  /// The immediate children of a directory.
  Result<std::vector<Entry>> List(const std::string& prefix) {
    if (!fs_.list) {
      return status::NotImplemented("[CSV] the file system cannot list directories");
    }
    struct AdbcError error = ADBC_ERROR_INIT;
    nanoarrow::UniqueArrayStream stream;
    AdbcStatusCode code =
        fs_.list(&fs_, prefix.data(), prefix.size(), stream.get(), &error);
    if (code != ADBC_STATUS_OK) return ErrorStatus(code, &error, prefix);

    nanoarrow::UniqueSchema schema;
    if (stream->get_schema(stream.get(), schema.get()) != 0) {
      return status::IO("[CSV] list: cannot read the schema");
    }
    struct ArrowError na_error = {};
    nanoarrow::UniqueArrayView view;
    if (ArrowArrayViewInitFromSchema(view.get(), schema.get(), &na_error) != 0 ||
        view->n_children < 4) {
      return status::IO("[CSV] list: unexpected schema: ", na_error.message);
    }
    std::vector<Entry> entries;
    while (true) {
      nanoarrow::UniqueArray array;
      if (stream->get_next(stream.get(), array.get()) != 0) {
        return status::IO("[CSV] list: cannot read the result");
      }
      if (!array->release) break;
      if (ArrowArrayViewSetArray(view.get(), array.get(), &na_error) != 0) {
        return status::IO("[CSV] list: ", na_error.message);
      }
      for (int64_t i = 0; i < array->length; i++) {
        ArrowStringView name = ArrowArrayViewGetStringUnsafe(view->children[0], i);
        bool is_dir = ArrowArrayViewGetIntUnsafe(view->children[3], i) != 0;
        entries.push_back({std::string(name.data, name.size_bytes), is_dir});
      }
    }
    std::sort(entries.begin(), entries.end(),
              [](const Entry& a, const Entry& b) { return a.name < b.name; });
    return entries;
  }

 private:
  void Reset() {
    if (fs_.release) fs_.release(&fs_);
    std::memset(&fs_, 0, sizeof(fs_));
  }

  static Status ErrorStatus(AdbcStatusCode code, struct AdbcError* error,
                            std::string_view what) {
    std::string message = "[CSV] " + std::string(what) + ": ";
    message += error->message ? error->message : "file system error";
    if (error->release) error->release(error);
    return Status(code, std::move(message));
  }

  struct AdbcFileSystem fs_ = {};
};

// ---------------------------------------------------------------------------
// CSV parsing and typing

using Row = std::vector<std::string>;

/// RFC 4180: quoted fields, doubled quotes, LF or CRLF line ends.
std::vector<Row> ParseCsv(std::string_view data) {
  std::vector<Row> rows;
  Row row;
  std::string field;
  bool quoted = false, field_started = false;
  for (size_t i = 0; i < data.size(); i++) {
    char c = data[i];
    if (quoted) {
      if (c == '"') {
        if (i + 1 < data.size() && data[i + 1] == '"') {
          field.push_back('"');
          i++;
        } else {
          quoted = false;
        }
      } else {
        field.push_back(c);
      }
    } else if (c == '"' && field.empty()) {
      quoted = field_started = true;
    } else if (c == ',') {
      row.push_back(std::move(field));
      field.clear();
      field_started = true;
    } else if (c == '\n' || c == '\r') {
      if (c == '\r' && i + 1 < data.size() && data[i + 1] == '\n') i++;
      if (field_started || !field.empty() || !row.empty()) {
        row.push_back(std::move(field));
        rows.push_back(std::move(row));
      }
      row.clear();
      field.clear();
      field_started = false;
    } else {
      field.push_back(c);
      field_started = true;
    }
  }
  if (field_started || !field.empty() || !row.empty()) {
    row.push_back(std::move(field));
    rows.push_back(std::move(row));
  }
  return rows;
}

enum class Kind : char {
  kEmpty = 'e',
  kInt = 'i',
  kFloat = 'f',
  kBool = 'b',
  kString = 's'
};

std::optional<int64_t> ParseInt(std::string_view s) {
  int64_t value = 0;
  auto [end, ec] = std::from_chars(s.data(), s.data() + s.size(), value);
  if (ec != std::errc() || end != s.data() + s.size()) return std::nullopt;
  return value;
}

std::optional<double> ParseDouble(std::string_view s) {
  double value = 0;
  auto [end, ec] = std::from_chars(s.data(), s.data() + s.size(), value);
  if (ec != std::errc() || end != s.data() + s.size()) return std::nullopt;
  return value;
}

std::optional<bool> ParseBool(std::string_view s) {
  std::string lower(s);
  std::transform(lower.begin(), lower.end(), lower.begin(),
                 [](char c) { return static_cast<char>(std::tolower(c)); });
  if (lower == "true") return true;
  if (lower == "false") return false;
  return std::nullopt;
}

Kind KindOf(std::string_view s) {
  if (s.empty()) return Kind::kEmpty;
  if (ParseInt(s)) return Kind::kInt;
  if (ParseDouble(s)) return Kind::kFloat;
  if (ParseBool(s)) return Kind::kBool;
  return Kind::kString;
}

Kind Merge(Kind a, Kind b) {
  if (a == b || b == Kind::kEmpty) return a;
  if (a == Kind::kEmpty) return b;
  if ((a == Kind::kInt && b == Kind::kFloat) || (a == Kind::kFloat && b == Kind::kInt)) {
    return Kind::kFloat;
  }
  return Kind::kString;
}

ArrowType ArrowTypeOf(Kind kind) {
  switch (kind) {
    case Kind::kInt:
      return NANOARROW_TYPE_INT64;
    case Kind::kFloat:
      return NANOARROW_TYPE_DOUBLE;
    case Kind::kBool:
      return NANOARROW_TYPE_BOOL;
    default:
      return NANOARROW_TYPE_STRING;
  }
}

/// The columns of a table: names (from the header, or A, B, C...) and kinds.
struct Layout {
  std::vector<std::string> names;
  std::vector<Kind> kinds;
};

std::string ColumnLetter(size_t col) {
  std::string out;
  for (size_t n = col + 1; n > 0; n = (n - 1) / 26) {
    out.insert(out.begin(), static_cast<char>('A' + (n - 1) % 26));
  }
  return out;
}

/// Names from the first file; kinds merged across all files.
Layout InferLayout(const std::vector<std::vector<Row>>& files, bool header) {
  Layout layout;
  size_t width = 0;
  for (const auto& rows : files) {
    for (const auto& row : rows) width = std::max(width, row.size());
  }
  layout.kinds.assign(width, Kind::kEmpty);
  for (const auto& rows : files) {
    for (size_t r = header ? 1 : 0; r < rows.size(); r++) {
      for (size_t c = 0; c < rows[r].size(); c++) {
        layout.kinds[c] = Merge(layout.kinds[c], KindOf(rows[r][c]));
      }
    }
  }
  for (size_t c = 0; c < width; c++) {
    std::string name;
    if (header && !files.empty() && !files[0].empty() && c < files[0][0].size()) {
      name = files[0][0][c];
    }
    if (name.empty()) name = ColumnLetter(c);
    // Disambiguate duplicates.
    std::string unique = name;
    for (int n = 2; std::find(layout.names.begin(), layout.names.end(), unique) !=
                    layout.names.end();
         n++) {
      unique = name + "_" + std::to_string(n);
    }
    layout.names.push_back(unique);
  }
  return layout;
}

#define CSV_NANOARROW(EXPR)                                                \
  do {                                                                     \
    if (int _na = (EXPR); _na != NANOARROW_OK) {                           \
      return status::Internal("[CSV] nanoarrow error ", _na, ": ", #EXPR); \
    }                                                                      \
  } while (0)

Status MakeSchema(const Layout& layout, const std::vector<size_t>& selected,
                  struct ArrowSchema* out) {
  ArrowSchemaInit(out);
  CSV_NANOARROW(ArrowSchemaSetTypeStruct(out, static_cast<int64_t>(selected.size())));
  for (size_t i = 0; i < selected.size(); i++) {
    CSV_NANOARROW(
        ArrowSchemaSetType(out->children[i], ArrowTypeOf(layout.kinds[selected[i]])));
    CSV_NANOARROW(
        ArrowSchemaSetName(out->children[i], layout.names[selected[i]].c_str()));
  }
  return status::Ok();
}

/// Build one batch from a file's data rows.
Status MakeBatch(const Layout& layout, const std::vector<size_t>& selected,
                 const std::vector<Row>& rows, size_t first, std::optional<int64_t> limit,
                 struct ArrowSchema* schema, struct ArrowArray* out) {
  struct ArrowError na_error = {};
  if (ArrowArrayInitFromSchema(out, schema, &na_error) != 0) {
    return status::Internal("[CSV] ", na_error.message);
  }
  CSV_NANOARROW(ArrowArrayStartAppending(out));
  int64_t count = 0;
  for (size_t r = first; r < rows.size(); r++) {
    if (limit && count >= *limit) break;
    for (size_t i = 0; i < selected.size(); i++) {
      size_t col = selected[i];
      struct ArrowArray* child = out->children[i];
      std::string_view cell = col < rows[r].size() ? std::string_view(rows[r][col]) : "";
      if (cell.empty()) {
        CSV_NANOARROW(ArrowArrayAppendNull(child, 1));
        continue;
      }
      switch (layout.kinds[col]) {
        case Kind::kInt:
          CSV_NANOARROW(ArrowArrayAppendInt(child, *ParseInt(cell)));
          break;
        case Kind::kFloat:
          CSV_NANOARROW(ArrowArrayAppendDouble(child, *ParseDouble(cell)));
          break;
        case Kind::kBool:
          CSV_NANOARROW(ArrowArrayAppendInt(child, *ParseBool(cell) ? 1 : 0));
          break;
        default:
          CSV_NANOARROW(ArrowArrayAppendString(
              child, {cell.data(), static_cast<int64_t>(cell.size())}));
          break;
      }
    }
    CSV_NANOARROW(ArrowArrayFinishElement(out));
    count++;
  }
  if (ArrowArrayFinishBuildingDefault(out, &na_error) != 0) {
    return status::Internal("[CSV] ", na_error.message);
  }
  return status::Ok();
}

/// A stream over batches built up front.
struct BatchStream {
  nanoarrow::UniqueSchema schema;
  std::vector<nanoarrow::UniqueArray> batches;
  size_t next = 0;

  static void Export(std::unique_ptr<BatchStream> self, struct ArrowArrayStream* out) {
    out->get_schema = [](struct ArrowArrayStream* s, struct ArrowSchema* schema) {
      return ArrowSchemaDeepCopy(static_cast<BatchStream*>(s->private_data)->schema.get(),
                                 schema);
    };
    out->get_next = [](struct ArrowArrayStream* s, struct ArrowArray* array) {
      auto* self = static_cast<BatchStream*>(s->private_data);
      if (self->next < self->batches.size()) {
        ArrowArrayMove(self->batches[self->next++].get(), array);
      } else {
        array->release = nullptr;
      }
      return 0;
    };
    out->get_last_error = [](struct ArrowArrayStream*) -> const char* { return nullptr; };
    out->release = [](struct ArrowArrayStream* s) {
      delete static_cast<BatchStream*>(s->private_data);
      s->release = nullptr;
    };
    out->private_data = self.release();
  }
};

// ---------------------------------------------------------------------------
// A minimal JSON string-array codec for the columns option.

Result<std::vector<std::string>> ParseJsonStrings(std::string_view text) {
  auto invalid = [&]() {
    return status::InvalidArgument("[CSV] '", kOptionColumns,
                                   "' must be a JSON array of strings, got: ", text);
  };
  std::vector<std::string> items;
  size_t i = 0;
  auto skip_ws = [&]() {
    while (i < text.size() && std::isspace(static_cast<unsigned char>(text[i]))) i++;
  };
  skip_ws();
  if (i >= text.size() || text[i++] != '[') return invalid();
  skip_ws();
  if (i < text.size() && text[i] == ']') {
    i++;
  } else {
    while (true) {
      skip_ws();
      if (i >= text.size() || text[i++] != '"') return invalid();
      std::string item;
      while (true) {
        if (i >= text.size()) return invalid();
        char c = text[i++];
        if (c == '"') break;
        if (c == '\\') {
          if (i >= text.size()) return invalid();
          char e = text[i++];
          switch (e) {
            case '"':
            case '\\':
            case '/':
              item.push_back(e);
              break;
            case 'n':
              item.push_back('\n');
              break;
            case 't':
              item.push_back('\t');
              break;
            case 'r':
              item.push_back('\r');
              break;
            default:
              return invalid();  // \uXXXX is not needed by the prototype
          }
        } else {
          item.push_back(c);
        }
      }
      items.push_back(std::move(item));
      skip_ws();
      if (i >= text.size()) return invalid();
      char sep = text[i++];
      if (sep == ']') break;
      if (sep != ',') return invalid();
    }
  }
  skip_ws();
  if (i != text.size()) return invalid();
  return items;
}

std::string EncodeJsonStrings(const std::vector<std::string>& items) {
  std::string out = "[";
  for (size_t i = 0; i < items.size(); i++) {
    if (i > 0) out += ",";
    out += '"';
    for (char c : items[i]) {
      if (c == '"' || c == '\\') out += '\\';
      if (c == '\n') {
        out += "\\n";
      } else if (c == '\t') {
        out += "\\t";
      } else if (c == '\r') {
        out += "\\r";
      } else {
        out += c;
      }
    }
    out += '"';
  }
  return out + "]";
}

// ---------------------------------------------------------------------------
// Scans

struct ScanOptions {
  bool header = true;
  std::optional<std::vector<std::string>> columns;
  std::optional<int64_t> limit;
};

/// The selected column indices, or an error naming the unknown column.
Result<std::vector<size_t>> Select(const Layout& layout, const ScanOptions& options) {
  std::vector<size_t> selected;
  if (!options.columns) {
    for (size_t i = 0; i < layout.names.size(); i++) selected.push_back(i);
    return selected;
  }
  for (const auto& name : *options.columns) {
    auto it = std::find(layout.names.begin(), layout.names.end(), name);
    if (it == layout.names.end()) {
      return status::InvalidArgument("[CSV] no column '", name, "'");
    }
    selected.push_back(static_cast<size_t>(it - layout.names.begin()));
  }
  return selected;
}

/// A partition: one file, with the layout every partition agreed on.
struct Partition {
  std::string file;
  bool header;
  std::vector<std::string> names;  // all columns, so selection is by name
  std::string kinds;               // one Kind char per column
  std::optional<std::vector<std::string>> columns;
  std::optional<int64_t> limit;

  std::string Encode() const {
    std::string out = "csv-scan-1";
    auto field = [&](std::string_view f) {
      out.push_back('\0');
      out.append(f);
    };
    field(file);
    field(header ? "1" : "0");
    field(EncodeJsonStrings(names));
    field(kinds);
    field(columns ? EncodeJsonStrings(*columns) : "");
    field(limit ? std::to_string(*limit) : "");
    return out;
  }

  static Result<Partition> Decode(std::string_view data) {
    std::vector<std::string_view> fields;
    size_t start = 0;
    while (true) {
      size_t nul = data.find('\0', start);
      fields.push_back(data.substr(start, nul - start));
      if (nul == std::string_view::npos) break;
      start = nul + 1;
    }
    if (fields.size() != 7 || fields[0] != "csv-scan-1") {
      return status::InvalidArgument("[CSV] not a partition of the CSV driver");
    }
    Partition p;
    p.file = std::string(fields[1]);
    p.header = fields[2] == "1";
    CSV_ASSIGN_OR_RETURN(p.names, ParseJsonStrings(fields[3]));
    p.kinds = std::string(fields[4]);
    if (p.kinds.size() != p.names.size()) {
      return status::InvalidArgument("[CSV] corrupt partition");
    }
    if (!fields[5].empty()) {
      CSV_ASSIGN_OR_RETURN(auto columns, ParseJsonStrings(fields[5]));
      p.columns = std::move(columns);
    }
    if (!fields[6].empty()) {
      auto limit = ParseInt(fields[6]);
      if (!limit) return status::InvalidArgument("[CSV] corrupt partition");
      p.limit = *limit;
    }
    return p;
  }
};

}  // namespace

// ---------------------------------------------------------------------------
// Database

class CsvDatabase : public driver::BaseDatabase<CsvDatabase> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[CSV]";
  constexpr static int kMinimumAdbcVersion = ADBC_VERSION_1_2_0;

  static Status DriverInfo(const std::vector<uint32_t>& codes,
                           struct ArrowArrayStream* out) {
    std::vector<driver::InfoValue> all = {
        {ADBC_INFO_DRIVER_NAME, "ADBC CSV File Driver (prototype)"},
        {ADBC_INFO_DRIVER_VERSION, "0.1.0"},
        {ADBC_INFO_DRIVER_ADBC_VERSION, int64_t{ADBC_VERSION_1_2_0}},
        {ADBC_INFO_DRIVER_ACCEPTS_FILESYSTEM, true},
        {ADBC_INFO_DRIVER_REQUIRES_FILESYSTEM, true},
    };
    std::vector<driver::InfoValue> rows;
    for (const auto& info : all) {
      if (codes.empty() ||
          std::find(codes.begin(), codes.end(), info.code) != codes.end()) {
        rows.push_back(info);
      }
    }
    return driver::MakeGetInfoStream(rows, out);
  }

  Status SetFileSystem(struct AdbcFileSystem* fs) {
    file_system_.Take(fs);
    return status::Ok();
  }

  Status SetOptionImpl(std::string_view key, Option value) override {
    if (key == "uri") {
      CSV_ASSIGN_OR_RETURN(auto uri, value.AsString());
      uri_ = std::string(uri);
      return status::Ok();
    }
    return Base::SetOptionImpl(key, std::move(value));
  }

  Status InitImpl() override {
    if (uri_.empty()) {
      return status::InvalidState("[CSV] the database option 'uri' must name a root");
    }
    CSV_ASSIGN_OR_RETURN(root_, LocationPath(uri_));
    return status::Ok();
  }

  FileSystem& file_system() { return file_system_; }
  const std::string& root() const { return root_; }

 private:
  std::string uri_;
  std::string root_;
  FileSystem file_system_;
};

// ---------------------------------------------------------------------------
// Connection

class CsvConnection : public driver::BaseConnection<CsvConnection> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[CSV]";

  Status SetFileSystem(struct AdbcFileSystem* fs) {
    file_system_.Take(fs);
    return status::Ok();
  }

  Status InitImpl(void* parent) override {
    auto* database = static_cast<CsvDatabase*>(parent);
    if (!file_system_.valid()) {
      if (!database->file_system().valid()) {
        return Status(ADBC_STATUS_FILESYSTEM_REQUIRED,
                      "[CSV] reads through a file system provided by the host, and "
                      "none was set on the connection or the database");
      }
      UNWRAP_STATUS(database->file_system().CloneTo(&file_system_));
    }
    root_ = database->root();
    return status::Ok();
  }

  Status GetInfoImpl(const std::vector<uint32_t> codes, struct ArrowArrayStream* out) {
    return CsvDatabase::DriverInfo(codes, out);
  }

  Status GetTableTypesImpl(struct ArrowArrayStream* out) {
    return driver::MakeTableTypesStream(
        {std::string(kTableTypeFile), std::string(kTableTypeDirectory)}, out);
  }

  Status GetTableSchemaImpl(std::optional<std::string_view>,
                            std::optional<std::string_view>, std::string_view table_name,
                            struct ArrowSchema* out) {
    ScanOptions options;
    CSV_ASSIGN_OR_RETURN(auto files, ResolveTarget(table_name));
    CSV_ASSIGN_OR_RETURN(auto layout, Infer(files, options.header));
    CSV_ASSIGN_OR_RETURN(auto selected, Select(layout, options));
    return MakeSchema(layout, selected, out);
  }

  Status GetObjectsImpl(int c_depth, std::optional<std::string_view> catalog_filter,
                        std::optional<std::string_view> schema_filter,
                        std::optional<std::string_view> table_filter,
                        std::optional<std::string_view> column_filter,
                        const std::vector<std::string_view>& table_types,
                        struct ArrowArrayStream* out);

  Status ReadPartitionImpl(std::string_view serialized, struct ArrowArrayStream* out) {
    CSV_ASSIGN_OR_RETURN(auto partition, Partition::Decode(serialized));
    CSV_ASSIGN_OR_RETURN(auto data, file_system_.ReadAll(partition.file));
    std::vector<Row> rows = ParseCsv(data);
    Layout layout;
    layout.names = partition.names;
    for (char k : partition.kinds) layout.kinds.push_back(static_cast<Kind>(k));
    // Values that do not fit the agreed type (only possible if the file
    // changed) are an error, not silently nulled.
    for (size_t r = partition.header ? 1 : 0; r < rows.size(); r++) {
      for (size_t c = 0; c < rows[r].size() && c < layout.kinds.size(); c++) {
        if (Merge(layout.kinds[c], KindOf(rows[r][c])) != layout.kinds[c]) {
          return Status(
              ADBC_STATUS_INVALID_DATA,
              "[CSV] " + partition.file + " no longer matches the scan's schema");
        }
      }
    }
    ScanOptions options{partition.header, partition.columns, partition.limit};
    CSV_ASSIGN_OR_RETURN(auto selected, Select(layout, options));
    auto stream = std::make_unique<BatchStream>();
    UNWRAP_STATUS(MakeSchema(layout, selected, stream->schema.get()));
    stream->batches.emplace_back();
    UNWRAP_STATUS(MakeBatch(layout, selected, rows, partition.header ? 1 : 0,
                            partition.limit, stream->schema.get(),
                            stream->batches.back().get()));
    BatchStream::Export(std::move(stream), out);
    return status::Ok();
  }

  /// The files a target names, relative to the root.
  Result<std::vector<std::string>> ResolveTarget(std::string_view target) {
    std::string path = Join(root_, target);
    std::string_view name = target;
    auto slash = name.rfind('/');
    std::string_view last =
        slash == std::string_view::npos ? name : name.substr(slash + 1);
    bool pattern = last.find_first_of("*?") != std::string_view::npos;
    if (!pattern && EndsWithCsv(last)) return std::vector<std::string>{path};

    std::string directory = path;
    std::string_view glob = "*.csv";
    if (pattern) {
      auto dir_end = path.rfind('/');
      directory = dir_end == std::string::npos ? "" : path.substr(0, dir_end);
      glob = last;
    }
    CSV_ASSIGN_OR_RETURN(auto entries, file_system_.List(directory));
    std::vector<std::string> files;
    for (const auto& entry : entries) {
      if (entry.is_directory) continue;
      auto base = std::string_view(entry.name);
      if (auto s = base.rfind('/'); s != std::string_view::npos)
        base = base.substr(s + 1);
      if (pattern ? Matches(glob, base) : EndsWithCsv(base)) files.push_back(entry.name);
    }
    if (files.empty()) {
      return status::NotFound("[CSV] no CSV files match '", target, "'");
    }
    return files;
  }

  /// Read and parse files, inferring one layout across all of them.
  Result<Layout> Infer(const std::vector<std::string>& files, bool header) {
    std::vector<std::vector<Row>> parsed;
    for (const auto& file : files) {
      CSV_ASSIGN_OR_RETURN(auto data, file_system_.ReadAll(file));
      parsed.push_back(ParseCsv(data));
    }
    return InferLayout(parsed, header);
  }

  FileSystem& file_system() { return file_system_; }
  const std::string& root() const { return root_; }

 private:
  FileSystem file_system_;
  std::string root_;
};

namespace {

/// GetObjects: one unnamed catalog and schema; the tables are the CSV files
/// and the directories that contain them, walked from the root.
class CsvObjects : public driver::GetObjectsHelper {
 public:
  explicit CsvObjects(CsvConnection* connection) : connection_(connection) {}

  Status Load(driver::GetObjectsDepth, std::optional<std::string_view>,
              std::optional<std::string_view>, std::optional<std::string_view>,
              std::optional<std::string_view>,
              const std::vector<std::string_view>&) override {
    return status::Ok();
  }

  Status LoadCatalogs(std::optional<std::string_view>) override {
    catalog_done_ = false;
    return status::Ok();
  }

  Result<std::optional<std::string_view>> NextCatalog() override {
    if (catalog_done_) return std::nullopt;
    catalog_done_ = true;
    return std::string_view("");
  }

  Status LoadSchemas(std::string_view, std::optional<std::string_view>) override {
    schema_done_ = false;
    return status::Ok();
  }

  Result<std::optional<std::string_view>> NextSchema() override {
    if (schema_done_) return std::nullopt;
    schema_done_ = true;
    return std::string_view("");
  }

  Status LoadTables(std::string_view, std::string_view,
                    std::optional<std::string_view> table_filter,
                    const std::vector<std::string_view>& table_types) override {
    tables_.clear();
    next_table_ = 0;
    auto wanted = [&](std::string_view type) {
      return table_types.empty() ||
             std::find(table_types.begin(), table_types.end(), type) != table_types.end();
    };
    UNWRAP_STATUS(Walk("", wanted));
    if (table_filter) {
      tables_.erase(std::remove_if(tables_.begin(), tables_.end(),
                                   [&](const auto& t) {
                                     return !LikeMatch(*table_filter, t.first);
                                   }),
                    tables_.end());
    }
    return status::Ok();
  }

  Result<std::optional<Table>> NextTable() override {
    if (next_table_ >= tables_.size()) return std::nullopt;
    const auto& t = tables_[next_table_++];
    return Table{t.first, t.second};
  }

  Status LoadColumns(std::string_view, std::string_view, std::string_view table,
                     std::optional<std::string_view> column_filter) override {
    columns_.clear();
    next_column_ = 0;
    CSV_ASSIGN_OR_RETURN(auto files, connection_->ResolveTarget(table));
    CSV_ASSIGN_OR_RETURN(auto layout, connection_->Infer(files, true));
    for (const auto& name : layout.names) {
      if (!column_filter || LikeMatch(*column_filter, name)) columns_.push_back(name);
    }
    return status::Ok();
  }

  Result<std::optional<Column>> NextColumn() override {
    if (next_column_ >= columns_.size()) return std::nullopt;
    Column column;
    column.column_name = columns_[next_column_];
    column.ordinal_position = static_cast<int32_t>(++next_column_);
    return column;
  }

 private:
  /// SQL LIKE with % and _.
  static bool LikeMatch(std::string_view pattern, std::string_view value) {
    if (pattern.empty()) return value.empty();
    if (pattern[0] == '%') {
      for (size_t i = 0; i <= value.size(); i++) {
        if (LikeMatch(pattern.substr(1), value.substr(i))) return true;
      }
      return false;
    }
    return !value.empty() && (pattern[0] == '_' || pattern[0] == value[0]) &&
           LikeMatch(pattern.substr(1), value.substr(1));
  }

  /// Relative names, from the root.
  std::string Relative(const std::string& name) const {
    const std::string& root = connection_->root();
    if (!root.empty() && name.rfind(root, 0) == 0) {
      std::string rest = name.substr(root.size());
      while (!rest.empty() && rest.front() == '/') rest.erase(0, 1);
      return rest;
    }
    return name;
  }

  template <typename Wanted>
  Status Walk(const std::string& relative, Wanted&& wanted) {
    CSV_ASSIGN_OR_RETURN(auto entries, connection_->file_system().List(
                                           Join(connection_->root(), relative)));
    bool has_csv = false;
    for (const auto& entry : entries) {
      std::string rel = Relative(entry.name);
      if (entry.is_directory) {
        UNWRAP_STATUS(Walk(rel, wanted));
      } else if (EndsWithCsv(rel)) {
        has_csv = true;
        if (wanted(kTableTypeFile)) tables_.emplace_back(rel, kTableTypeFile);
      }
    }
    if (has_csv && !relative.empty() && wanted(kTableTypeDirectory)) {
      tables_.emplace_back(relative, kTableTypeDirectory);
    }
    return status::Ok();
  }

  CsvConnection* connection_;
  bool catalog_done_ = false, schema_done_ = false;
  std::vector<std::pair<std::string, std::string_view>> tables_;
  size_t next_table_ = 0;
  std::vector<std::string> columns_;
  size_t next_column_ = 0;
};

}  // namespace

Status CsvConnection::GetObjectsImpl(int c_depth,
                                     std::optional<std::string_view> catalog_filter,
                                     std::optional<std::string_view> schema_filter,
                                     std::optional<std::string_view> table_filter,
                                     std::optional<std::string_view> column_filter,
                                     const std::vector<std::string_view>& table_types,
                                     struct ArrowArrayStream* out) {
  driver::GetObjectsDepth depth;
  switch (c_depth) {
    case ADBC_OBJECT_DEPTH_CATALOGS:
      depth = driver::GetObjectsDepth::kCatalogs;
      break;
    case ADBC_OBJECT_DEPTH_DB_SCHEMAS:
      depth = driver::GetObjectsDepth::kSchemas;
      break;
    case ADBC_OBJECT_DEPTH_TABLES:
      depth = driver::GetObjectsDepth::kTables;
      break;
    default:
      depth = driver::GetObjectsDepth::kColumns;
      break;
  }
  CsvObjects helper(this);
  return driver::BuildGetObjects(&helper, depth, catalog_filter, schema_filter,
                                 table_filter, column_filter, table_types, out);
}

// ---------------------------------------------------------------------------
// Statement

class CsvStatement : public driver::BaseStatement<CsvStatement> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[CSV]";

  Status InitImpl(void* parent) {
    connection_ = static_cast<CsvConnection*>(parent);
    return status::Ok();
  }

  Status SetOptionImpl(std::string_view key, Option value) override {
    if (key == kOptionTarget) {
      CSV_ASSIGN_OR_RETURN(auto target, value.AsString());
      target_ = std::string(target);
    } else if (key == kOptionColumns) {
      CSV_ASSIGN_OR_RETURN(auto text, value.AsString());
      CSV_ASSIGN_OR_RETURN(auto columns, ParseJsonStrings(text));
      options_.columns = std::move(columns);
    } else if (key == kOptionLimit) {
      CSV_ASSIGN_OR_RETURN(auto limit, value.AsInt());
      if (limit < 0) return status::InvalidArgument("[CSV] limit must not be negative");
      options_.limit = limit;
    } else if (key == kOptionHeader) {
      CSV_ASSIGN_OR_RETURN(options_.header, value.AsBool());
    } else {
      return Base::SetOptionImpl(key, std::move(value));
    }
    return status::Ok();
  }

  Status SetSqlQueryImpl(std::string_view) {
    return status::NotImplemented(
        "[CSV] The CSV file driver does not run SQL; a statement is a scan configured "
        "with the option '",
        kOptionTarget, "'");
  }

  Status ExecuteSchemaImpl(struct ArrowSchema* schema) {
    CSV_ASSIGN_OR_RETURN(auto plan, Plan());
    CSV_ASSIGN_OR_RETURN(auto selected, Select(plan.layout, options_));
    return MakeSchema(plan.layout, selected, schema);
  }

  Result<int64_t> ExecuteQueryImpl(struct ArrowArrayStream* out) {
    CSV_ASSIGN_OR_RETURN(auto plan, Plan());
    CSV_ASSIGN_OR_RETURN(auto selected, Select(plan.layout, options_));
    auto stream = std::make_unique<BatchStream>();
    UNWRAP_STATUS(MakeSchema(plan.layout, selected, stream->schema.get()));
    std::optional<int64_t> remaining = options_.limit;
    for (const auto& rows : plan.rows) {
      if (remaining && *remaining <= 0) break;
      stream->batches.emplace_back();
      UNWRAP_STATUS(MakeBatch(plan.layout, selected, rows, options_.header ? 1 : 0,
                              remaining, stream->schema.get(),
                              stream->batches.back().get()));
      if (remaining) *remaining -= stream->batches.back()->length;
    }
    BatchStream::Export(std::move(stream), out);
    return -1;
  }

  /// One partition per file. Hides BaseStatement::ExecutePartitions, which
  /// the framework does not let drivers override through an Impl hook.
  AdbcStatusCode ExecutePartitions(struct ArrowSchema* schema,
                                   struct AdbcPartitions* partitions,
                                   int64_t* rows_affected, struct AdbcError* error) {
    auto result = ExecutePartitionsImpl(schema, partitions);
    if (!result.ok()) return result.ToAdbc(error);
    if (rows_affected) *rows_affected = -1;
    return ADBC_STATUS_OK;
  }

 private:
  struct ScanPlan {
    std::vector<std::string> files;
    std::vector<std::vector<Row>> rows;
    Layout layout;
  };

  Result<ScanPlan> Plan() {
    if (target_.empty()) {
      return status::InvalidState("[CSV] Set the statement option '", kOptionTarget,
                                  "' to a file, directory, or pattern");
    }
    ScanPlan plan;
    CSV_ASSIGN_OR_RETURN(plan.files, connection_->ResolveTarget(target_));
    for (const auto& file : plan.files) {
      CSV_ASSIGN_OR_RETURN(auto data, connection_->file_system().ReadAll(file));
      plan.rows.push_back(ParseCsv(data));
    }
    plan.layout = InferLayout(plan.rows, options_.header);
    return plan;
  }

  struct PartitionsData {
    std::vector<std::string> encoded;
    std::vector<const uint8_t*> pointers;
    std::vector<size_t> lengths;
  };

  Status ExecutePartitionsImpl(struct ArrowSchema* schema,
                               struct AdbcPartitions* partitions) {
    CSV_ASSIGN_OR_RETURN(auto plan, Plan());
    CSV_ASSIGN_OR_RETURN(auto selected, Select(plan.layout, options_));
    std::string kinds;
    for (Kind k : plan.layout.kinds) kinds.push_back(static_cast<char>(k));

    auto data = std::make_unique<PartitionsData>();
    for (const auto& file : plan.files) {
      Partition p{file,  options_.header,  plan.layout.names,
                  kinds, options_.columns, options_.limit};
      data->encoded.push_back(p.Encode());
    }
    for (const auto& e : data->encoded) {
      data->pointers.push_back(reinterpret_cast<const uint8_t*>(e.data()));
      data->lengths.push_back(e.size());
    }
    UNWRAP_STATUS(MakeSchema(plan.layout, selected, schema));
    partitions->num_partitions = data->encoded.size();
    partitions->partitions = data->pointers.data();
    partitions->partition_lengths = data->lengths.data();
    partitions->private_data = data.release();
    partitions->release = [](struct AdbcPartitions* p) {
      delete static_cast<PartitionsData*>(p->private_data);
      p->private_data = nullptr;
      p->release = nullptr;
    };
    return status::Ok();
  }

  CsvConnection* connection_ = nullptr;
  std::string target_;
  ScanOptions options_;
};

using CsvDriver = driver::Driver<CsvDatabase, CsvConnection, CsvStatement>;

}  // namespace adbc::csv

extern "C" {
ADBC_EXPORT AdbcStatusCode AdbcDriverCsvInit(int version, void* raw_driver,
                                             struct AdbcError* error) {
  return adbc::csv::CsvDriver::Init(version, raw_driver, error);
}

ADBC_EXPORT AdbcStatusCode AdbcDriverInit(int version, void* raw_driver,
                                          struct AdbcError* error) {
  return adbc::csv::CsvDriver::Init(version, raw_driver, error);
}
}

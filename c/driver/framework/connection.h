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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow-adbc/adbc.h>

#include "driver/framework/base_driver.h"
#include "driver/framework/objects.h"
#include "driver/framework/utility.h"

namespace adbc::driver {
/// \brief The CRTP base implementation of an AdbcConnection.
///
/// Derived should override and implement the Impl methods, but not others.
/// Overridden methods should defer to the superclass version at the end.
/// (The Base typedef is provided to make this easier.)  Derived should also
/// define a constexpr static symbol called kErrorPrefix that is used to
/// construct error messages.
template <typename Derived>
class Connection : public ObjectBase {
 public:
  using Base = Connection<Derived>;

  /// \brief Whether autocommit is enabled or not (by default: enabled).
  enum class AutocommitState {
    kAutocommit,
    kTransaction,
  };

  Connection() : ObjectBase() {}
  ~Connection() = default;

  /// \internal
  AdbcStatusCode Init(void* parent, AdbcError* error) override {
    if (auto status = impl().InitImpl(parent); !status.ok()) {
      return status.ToAdbc(error);
    }
    return ObjectBase::Init(parent, error);
  }

  /// \internal
  AdbcStatusCode Cancel(AdbcError* error) { return ADBC_STATUS_NOT_IMPLEMENTED; }

  /// \internal
  AdbcStatusCode Commit(AdbcError* error) {
    switch (autocommit_) {
      case AutocommitState::kAutocommit:
        return status::InvalidState(Derived::kErrorPrefix,
                                    " No active transaction, cannot commit")
            .ToAdbc(error);
      case AutocommitState::kTransaction:
        return impl().CommitImpl().ToAdbc(error);
    }
    assert(false);
    return ADBC_STATUS_INTERNAL;
  }

  /// \internal
  AdbcStatusCode GetInfo(const uint32_t* info_codes, size_t info_codes_length,
                         ArrowArrayStream* out, AdbcError* error) {
    if (!out) {
      RAISE_STATUS(error, status::InvalidArgument("out must be non-null"));
    }

    std::vector<uint32_t> codes(info_codes, info_codes + info_codes_length);
    RAISE_RESULT(error, auto infos, impl().InfoImpl(codes));
    RAISE_STATUS(error, MakeGetInfoStream(infos, out));
    return ADBC_STATUS_OK;
  }

  /// \internal
  AdbcStatusCode GetObjects(int c_depth, const char* catalog, const char* db_schema,
                            const char* table_name, const char** table_type,
                            const char* column_name, ArrowArrayStream* out,
                            AdbcError* error) {
    const auto catalog_filter =
        catalog ? std::make_optional(std::string_view(catalog)) : std::nullopt;
    const auto schema_filter =
        db_schema ? std::make_optional(std::string_view(db_schema)) : std::nullopt;
    const auto table_filter =
        table_name ? std::make_optional(std::string_view(table_name)) : std::nullopt;
    const auto column_filter =
        column_name ? std::make_optional(std::string_view(column_name)) : std::nullopt;
    std::vector<std::string_view> table_type_filter;
    while (table_type && *table_type) {
      if (*table_type) {
        table_type_filter.push_back(std::string_view(*table_type));
      }
      table_type++;
    }

    GetObjectsDepth depth = GetObjectsDepth::kColumns;
    switch (c_depth) {
      case ADBC_OBJECT_DEPTH_CATALOGS:
        depth = GetObjectsDepth::kCatalogs;
        break;
      case ADBC_OBJECT_DEPTH_COLUMNS:
        depth = GetObjectsDepth::kColumns;
        break;
      case ADBC_OBJECT_DEPTH_DB_SCHEMAS:
        depth = GetObjectsDepth::kSchemas;
        break;
      case ADBC_OBJECT_DEPTH_TABLES:
        depth = GetObjectsDepth::kTables;
        break;
      default:
        return status::InvalidArgument(Derived::kErrorPrefix,
                                       " GetObjects: invalid depth ", c_depth)
            .ToAdbc(error);
    }

    RAISE_RESULT(error, auto helper, impl().GetObjectsImpl());
    auto status = BuildGetObjects(helper.get(), depth, catalog_filter, schema_filter,
                                  table_filter, column_filter, table_type_filter, out);
    RAISE_STATUS(error, helper->Close());
    RAISE_STATUS(error, status);
    return ADBC_STATUS_OK;
  }

  /// \internal
  Result<Option> GetOption(std::string_view key) override {
    if (key == ADBC_CONNECTION_OPTION_AUTOCOMMIT) {
      switch (autocommit_) {
        case AutocommitState::kAutocommit:
          return driver::Option(ADBC_OPTION_VALUE_ENABLED);
        case AutocommitState::kTransaction:
          return driver::Option(ADBC_OPTION_VALUE_DISABLED);
      }
    } else if (key == ADBC_CONNECTION_OPTION_CURRENT_CATALOG) {
      UNWRAP_RESULT(auto catalog, impl().GetCurrentCatalogImpl());
      if (catalog) {
        return driver::Option(std::move(*catalog));
      }
      return driver::Option();
    } else if (key == ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA) {
      UNWRAP_RESULT(auto schema, impl().GetCurrentSchemaImpl());
      if (schema) {
        return driver::Option(std::move(*schema));
      }
      return driver::Option();
    }
    return Base::GetOption(key);
  }

  /// \internal
  AdbcStatusCode GetStatistics(const char* catalog, const char* db_schema,
                               const char* table_name, char approximate,
                               ArrowArrayStream* out, AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  /// \internal
  AdbcStatusCode GetStatisticNames(ArrowArrayStream* out, AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  /// \internal
  AdbcStatusCode GetTableSchema(const char* catalog, const char* db_schema,
                                const char* table_name, ArrowSchema* schema,
                                AdbcError* error) {
    if (!table_name) {
      return status::InvalidArgument(Derived::kErrorPrefix,
                                     " GetTableSchema: must provide table_name")
          .ToAdbc(error);
    }
    std::memset(schema, 0, sizeof(*schema));
    std::optional<std::string_view> catalog_param =
        catalog ? std::make_optional(std::string_view(catalog)) : std::nullopt;
    std::optional<std::string_view> db_schema_param =
        db_schema ? std::make_optional(std::string_view(db_schema)) : std::nullopt;
    std::string_view table_name_param = table_name;

    return impl()
        .GetTableSchemaImpl(catalog_param, db_schema_param, table_name_param, schema)
        .ToAdbc(error);
  }

  /// \internal
  AdbcStatusCode GetTableTypes(ArrowArrayStream* out, AdbcError* error) {
    if (!out) {
      RAISE_STATUS(error, status::InvalidArgument("out must be non-null"));
    }

    RAISE_RESULT(error, std::vector<std::string> table_types, impl().GetTableTypesImpl());
    RAISE_STATUS(error, MakeTableTypesStream(table_types, out));
    return ADBC_STATUS_OK;
  }

  /// \internal
  AdbcStatusCode ReadPartition(const uint8_t* serialized_partition,
                               size_t serialized_length, ArrowArrayStream* out,
                               AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  /// \internal
  AdbcStatusCode Release(AdbcError* error) override {
    return impl().ReleaseImpl().ToAdbc(error);
  }

  /// \internal
  AdbcStatusCode Rollback(AdbcError* error) {
    switch (autocommit_) {
      case AutocommitState::kAutocommit:
        return status::InvalidState(Derived::kErrorPrefix,
                                    " No active transaction, cannot rollback")
            .ToAdbc(error);
      case AutocommitState::kTransaction:
        return impl().RollbackImpl().ToAdbc(error);
    }
    assert(false);
    return ADBC_STATUS_INTERNAL;
  }

  /// \internal
  AdbcStatusCode SetOption(std::string_view key, Option value,
                           AdbcError* error) override {
    return impl().SetOptionImpl(key, value).ToAdbc(error);
  }

  /// \brief Commit the current transaction and begin a new transaction.
  ///
  /// Only called when autocommit is disabled.
  Status CommitImpl() { return status::NotImplemented("Commit"); }

  Result<std::optional<std::string>> GetCurrentCatalogImpl() { return std::nullopt; }

  Result<std::optional<std::string>> GetCurrentSchemaImpl() { return std::nullopt; }

  /// \brief Query the database catalog.
  ///
  /// The default implementation assumes the underlying database allows
  /// querying the catalog in a certain manner, embodied in the helper class
  /// returned here.  If the database can directly implement GetObjects, then
  /// directly override GetObjects instead of using this helper.
  Result<std::unique_ptr<GetObjectsHelper>> GetObjectsImpl() {
    return std::make_unique<GetObjectsHelper>();
  }

  Status GetTableSchemaImpl(std::optional<std::string_view> catalog,
                            std::optional<std::string_view> db_schema,
                            std::string_view table_name, ArrowSchema* schema) {
    return status::NotImplemented("GetTableSchema");
  }

  Result<std::vector<std::string>> GetTableTypesImpl() {
    return std::vector<std::string>();
  }

  Result<std::vector<InfoValue>> InfoImpl(const std::vector<uint32_t>& codes) {
    return std::vector<InfoValue>{};
  }

  Status InitImpl(void* parent) { return status::Ok(); }

  Status ReleaseImpl() { return status::Ok(); }

  Status RollbackImpl() { return status::NotImplemented("Rollback"); }

  Status SetOptionImpl(std::string_view key, Option value) {
    if (key == ADBC_CONNECTION_OPTION_AUTOCOMMIT) {
      UNWRAP_RESULT(auto enabled, value.AsBool());
      switch (autocommit_) {
        case AutocommitState::kAutocommit: {
          if (!enabled) {
            UNWRAP_STATUS(impl().ToggleAutocommitImpl(false));
            autocommit_ = AutocommitState::kTransaction;
          }
          break;
        }
        case AutocommitState::kTransaction: {
          if (enabled) {
            UNWRAP_STATUS(impl().ToggleAutocommitImpl(true));
            autocommit_ = AutocommitState::kAutocommit;
          }
          break;
        }
      }
      return status::Ok();
    }
    return status::NotImplemented(Derived::kErrorPrefix, " Unknown connection option ",
                                  key, "=", value.Format());
  }

  Status ToggleAutocommitImpl(bool enable_autocommit) {
    return status::NotImplemented(Derived::kErrorPrefix, " Cannot change autocommit");
  }

 protected:
  AutocommitState autocommit_ = AutocommitState::kAutocommit;

 private:
  Derived& impl() { return static_cast<Derived&>(*this); }
};

}  // namespace adbc::driver

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

// Filters for the CSV file driver, as a Substrait ExtendedExpression
// (PROTOTYPE, file-driver proposal).
//
// The engine sends one ExtendedExpression whose referred expressions are
// the conjuncts of its filter.  The driver reports, per conjunct, whether
// it applies it ("exact") or leaves it to the engine ("unsupported"), and
// then filters rows by the conjuncts it accepted.
//
// Supported: column references, scalar literals (boolean, integers,
// floating point, strings, typed null), comparisons (equal, not_equal, lt,
// lte, gt, gte), and, or, not, is_null, is_not_null, with SQL three-valued
// logic.  Support is decided from the expression alone, without reading
// data, so asking costs no I/O.
//
// The protobuf is decoded by hand: the driver needs a handful of messages,
// not a protobuf runtime.

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "driver/framework/status.h"

namespace adbc::csv {

/// A cell or literal value; monostate is null.
using Value = std::variant<std::monostate, int64_t, double, bool, std::string>;

/// The type of a column, for binding.
enum class ValueType { kInt, kFloat, kBool, kString };

struct FilterExpr;

class Filter {
 public:
  Filter();
  ~Filter();
  Filter(Filter&&) noexcept;
  Filter& operator=(Filter&&) noexcept;

  /// Parse a serialized ExtendedExpression.  Conjuncts the driver cannot
  /// evaluate are kept, marked unsupported.
  static driver::Result<Filter> Parse(std::string_view serialized);

  /// The serialized form, as given.
  const std::string& serialized() const { return serialized_; }

  /// Per conjunct: whether it is applied (exactly) by the driver.
  const std::vector<bool>& supported() const { return supported_; }

  /// Resolve column references against the scan's columns.  Fails if a
  /// supported conjunct names an unknown column or compares mismatched
  /// types (the engine must re-apply nothing it was told is exact).
  driver::Status Bind(const std::vector<std::string>& names,
                      const std::vector<ValueType>& types);

  /// Whether a row passes every supported conjunct.  `cell(i)` returns
  /// the value of column i.  Bind first.
  bool Matches(const std::function<Value(size_t)>& cell) const;

 private:
  std::string serialized_;
  std::vector<std::string> base_names_;
  std::vector<std::unique_ptr<FilterExpr>> conjuncts_;
  std::vector<bool> supported_;
};

}  // namespace adbc::csv

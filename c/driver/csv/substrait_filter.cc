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

#include "substrait_filter.h"

#include <algorithm>
#include <cstring>
#include <map>
#include <optional>
#include <utility>

namespace adbc::csv {

namespace status = adbc::driver::status;
using driver::Result;
using driver::Status;

// ---------------------------------------------------------------------------
// Protobuf wire format

namespace {

enum WireType : uint32_t { kVarint = 0, kFixed64 = 1, kLength = 2, kFixed32 = 5 };

/// One field of a message: its number, wire type, and payload.
struct Field {
  uint32_t number = 0;
  uint32_t wire = 0;
  uint64_t varint = 0;     // kVarint, kFixed64, kFixed32
  std::string_view bytes;  // kLength
};

/// Iterates over the fields of one message.
class Message {
 public:
  explicit Message(std::string_view data)
      : p_(reinterpret_cast<const uint8_t*>(data.data())), end_(p_ + data.size()) {}

  /// The next field; false at the end.  Sets `ok_` to false on bad input.
  bool Next(Field* field) {
    if (p_ >= end_ || !ok_) return false;
    uint64_t key = 0;
    if (!Varint(&key)) return Fail();
    field->number = static_cast<uint32_t>(key >> 3);
    field->wire = static_cast<uint32_t>(key & 7);
    field->bytes = {};
    field->varint = 0;
    switch (field->wire) {
      case kVarint:
        if (!Varint(&field->varint)) return Fail();
        return true;
      case kFixed64:
        if (end_ - p_ < 8) return Fail();
        std::memcpy(&field->varint, p_, 8);
        p_ += 8;
        return true;
      case kFixed32: {
        if (end_ - p_ < 4) return Fail();
        uint32_t v = 0;
        std::memcpy(&v, p_, 4);
        field->varint = v;
        p_ += 4;
        return true;
      }
      case kLength: {
        uint64_t length = 0;
        if (!Varint(&length) || static_cast<uint64_t>(end_ - p_) < length) return Fail();
        field->bytes = {reinterpret_cast<const char*>(p_), static_cast<size_t>(length)};
        p_ += length;
        return true;
      }
      default:
        return Fail();
    }
  }

  bool ok() const { return ok_; }

 private:
  bool Varint(uint64_t* out) {
    uint64_t value = 0;
    for (int shift = 0; shift < 64 && p_ < end_; shift += 7) {
      uint8_t b = *p_++;
      value |= static_cast<uint64_t>(b & 0x7F) << shift;
      if (!(b & 0x80)) {
        *out = value;
        return true;
      }
    }
    return false;
  }

  bool Fail() {
    ok_ = false;
    return false;
  }

  const uint8_t* p_;
  const uint8_t* end_;
  bool ok_ = true;
};

}  // namespace

// ---------------------------------------------------------------------------
// Expressions

struct FilterExpr {
  enum class Kind { kLiteral, kField, kCall, kUnsupported } kind = Kind::kUnsupported;
  Value literal;
  int32_t field = -1;    // index into the base schema
  size_t column = 0;     // bound: index into the scan's columns
  std::string function;  // without a signature suffix
  std::vector<FilterExpr> args;
  std::string why;  // for kUnsupported

  static FilterExpr Unsupported(std::string why) {
    FilterExpr e;
    e.why = std::move(why);
    return e;
  }
};

namespace {

const char* kComparisons[] = {"equal", "not_equal", "lt", "lte", "gt", "gte"};

bool IsComparison(std::string_view f) {
  return std::find(std::begin(kComparisons), std::end(kComparisons), f) !=
         std::end(kComparisons);
}

bool IsKnownFunction(std::string_view f) {
  return IsComparison(f) || f == "and" || f == "or" || f == "not" || f == "is_null" ||
         f == "is_not_null";
}

/// Literal: only scalar types a CSV column can hold.
FilterExpr ParseLiteral(std::string_view data) {
  FilterExpr e;
  e.kind = FilterExpr::Kind::kLiteral;
  Message m(data);
  Field f;
  bool have = false;
  while (m.Next(&f)) {
    switch (f.number) {
      case 1:  // boolean
        e.literal = f.varint != 0;
        have = true;
        break;
      case 2:  // i8
      case 3:  // i16
      case 5:  // i32
        e.literal = static_cast<int64_t>(static_cast<int32_t>(f.varint));
        have = true;
        break;
      case 7:  // i64
        e.literal = static_cast<int64_t>(f.varint);
        have = true;
        break;
      case 10: {  // fp32
        float v = 0;
        uint32_t bits = static_cast<uint32_t>(f.varint);
        std::memcpy(&v, &bits, 4);
        e.literal = static_cast<double>(v);
        have = true;
        break;
      }
      case 11: {  // fp64
        double v = 0;
        std::memcpy(&v, &f.varint, 8);
        e.literal = v;
        have = true;
        break;
      }
      case 12:  // string
      case 21:  // fixed_char
        e.literal = std::string(f.bytes);
        have = true;
        break;
      case 22: {  // var_char { value = 1 }
        Message vc(f.bytes);
        Field g;
        std::string value;
        while (vc.Next(&g)) {
          if (g.number == 1) value = std::string(g.bytes);
        }
        e.literal = std::move(value);
        have = true;
        break;
      }
      case 29:  // typed null
        e.literal = std::monostate{};
        have = true;
        break;
      case 50:  // nullable
      case 51:  // type_variation_reference
        break;
      default:
        return FilterExpr::Unsupported("literal type " + std::to_string(f.number));
    }
  }
  if (!m.ok() || !have) return FilterExpr::Unsupported("malformed literal");
  return e;
}

/// FieldReference: a direct reference to a top-level field of the base
/// schema (`direct_reference.struct_field.field`, no child, root).
///
/// Note that proto3 omits scalar fields equal to zero: field 0 and
/// function anchor 0 are absent from the wire, not missing.
FilterExpr ParseFieldReference(std::string_view data) {
  Message m(data);
  Field f;
  std::optional<int32_t> index;
  while (m.Next(&f)) {
    if (f.number == 1) {  // direct_reference: ReferenceSegment
      Message segment(f.bytes);
      Field s;
      while (segment.Next(&s)) {
        if (s.number != 2) return FilterExpr::Unsupported("non-struct reference");
        Message sf(s.bytes);  // StructField { field = 1; child = 2 }
        index = 0;
        Field g;
        while (sf.Next(&g)) {
          if (g.number == 1) {
            index = static_cast<int32_t>(g.varint);
          } else if (g.number == 2) {
            return FilterExpr::Unsupported("nested field reference");
          }
        }
      }
    } else if (f.number == 4) {  // root_reference
    } else {
      return FilterExpr::Unsupported("field reference kind " + std::to_string(f.number));
    }
  }
  if (!m.ok() || !index) return FilterExpr::Unsupported("malformed field reference");
  FilterExpr e;
  e.kind = FilterExpr::Kind::kField;
  e.field = *index;
  return e;
}

FilterExpr ParseExpression(std::string_view data,
                           const std::map<uint32_t, std::string>& functions);

FilterExpr ParseScalarFunction(std::string_view data,
                               const std::map<uint32_t, std::string>& functions) {
  FilterExpr e;
  e.kind = FilterExpr::Kind::kCall;
  uint32_t anchor = 0;
  Message m(data);
  Field f;
  while (m.Next(&f)) {
    if (f.number == 1) {  // function_reference
      anchor = static_cast<uint32_t>(f.varint);
    } else if (f.number == 4) {  // arguments: FunctionArgument { value = 3 }
      Message arg(f.bytes);
      Field a;
      bool value = false;
      while (arg.Next(&a)) {
        if (a.number != 3) return FilterExpr::Unsupported("non-value argument");
        e.args.push_back(ParseExpression(a.bytes, functions));
        value = true;
      }
      if (!value) return FilterExpr::Unsupported("empty argument");
    } else if (f.number == 3 || f.number == 5) {
      // output_type, options: not needed.
    } else {
      return FilterExpr::Unsupported("scalar function field " + std::to_string(f.number));
    }
  }
  if (!m.ok()) return FilterExpr::Unsupported("malformed function");
  auto it = functions.find(anchor);
  if (it == functions.end()) return FilterExpr::Unsupported("unknown function anchor");
  e.function = it->second.substr(0, it->second.find(':'));
  if (!IsKnownFunction(e.function)) {
    return FilterExpr::Unsupported("function '" + e.function + "'");
  }
  size_t arity = e.args.size();
  bool arity_ok = IsComparison(e.function)                      ? arity == 2
                  : (e.function == "and" || e.function == "or") ? arity >= 2
                                                                : arity == 1;
  if (!arity_ok) return FilterExpr::Unsupported("arity of '" + e.function + "'");
  for (const auto& arg : e.args) {
    if (arg.kind == FilterExpr::Kind::kUnsupported)
      return FilterExpr::Unsupported(arg.why);
  }
  return e;
}

FilterExpr ParseExpression(std::string_view data,
                           const std::map<uint32_t, std::string>& functions) {
  Message m(data);
  Field f;
  std::optional<FilterExpr> e;
  while (m.Next(&f)) {
    switch (f.number) {
      case 1:
        e = ParseLiteral(f.bytes);
        break;
      case 2:
        e = ParseFieldReference(f.bytes);
        break;
      case 3:
        e = ParseScalarFunction(f.bytes, functions);
        break;
      default:
        return FilterExpr::Unsupported("expression kind " + std::to_string(f.number));
    }
  }
  if (!m.ok() || !e) return FilterExpr::Unsupported("malformed expression");
  return std::move(*e);
}

// --- binding and evaluation

std::optional<ValueType> LiteralType(const Value& v) {
  if (std::holds_alternative<int64_t>(v)) return ValueType::kInt;
  if (std::holds_alternative<double>(v)) return ValueType::kFloat;
  if (std::holds_alternative<bool>(v)) return ValueType::kBool;
  if (std::holds_alternative<std::string>(v)) return ValueType::kString;
  return std::nullopt;  // null: compatible with anything
}

bool Comparable(ValueType a, ValueType b) {
  auto numeric = [](ValueType t) {
    return t == ValueType::kInt || t == ValueType::kFloat;
  };
  return a == b || (numeric(a) && numeric(b));
}

/// Bind field references; returns the expression's type (nullopt: null or
/// boolean-valued expression, handled by the caller).
Result<std::optional<ValueType>> BindExpr(FilterExpr* e,
                                          const std::vector<std::string>& base_names,
                                          const std::vector<std::string>& names,
                                          const std::vector<ValueType>& types) {
  switch (e->kind) {
    case FilterExpr::Kind::kLiteral:
      return LiteralType(e->literal);
    case FilterExpr::Kind::kField: {
      if (e->field < 0 || static_cast<size_t>(e->field) >= base_names.size()) {
        return status::InvalidArgument("[CSV] filter: field ", e->field,
                                       " is outside the base schema");
      }
      const std::string& name = base_names[e->field];
      auto it = std::find(names.begin(), names.end(), name);
      if (it == names.end()) {
        return status::InvalidArgument("[CSV] filter: no column '", name, "'");
      }
      e->column = static_cast<size_t>(it - names.begin());
      return types[e->column];
    }
    case FilterExpr::Kind::kCall: {
      std::vector<std::optional<ValueType>> arg_types;
      for (auto& arg : e->args) {
        Result<std::optional<ValueType>> t = BindExpr(&arg, base_names, names, types);
        if (!t.has_value()) return std::move(t).status();
        arg_types.push_back(t.value());
      }
      if (IsComparison(e->function) && arg_types[0] && arg_types[1] &&
          !Comparable(*arg_types[0], *arg_types[1])) {
        return status::InvalidArgument("[CSV] filter: '", e->function,
                                       "' compares mismatched types");
      }
      return ValueType::kBool;
    }
    default:
      return status::Internal("[CSV] filter: binding an unsupported expression");
  }
}

int Compare(const Value& a, const Value& b) {
  auto as_double = [](const Value& v) {
    return std::holds_alternative<int64_t>(v) ? static_cast<double>(std::get<int64_t>(v))
                                              : std::get<double>(v);
  };
  if (std::holds_alternative<int64_t>(a) && std::holds_alternative<int64_t>(b)) {
    auto x = std::get<int64_t>(a), y = std::get<int64_t>(b);
    return x < y ? -1 : x > y;
  }
  if (std::holds_alternative<std::string>(a)) {
    return std::get<std::string>(a).compare(std::get<std::string>(b));
  }
  if (std::holds_alternative<bool>(a)) {
    return static_cast<int>(std::get<bool>(a)) - static_cast<int>(std::get<bool>(b));
  }
  double x = as_double(a), y = as_double(b);
  return x < y ? -1 : x > y;
}

Value Eval(const FilterExpr& e, const std::function<Value(size_t)>& cell) {
  switch (e.kind) {
    case FilterExpr::Kind::kLiteral:
      return e.literal;
    case FilterExpr::Kind::kField:
      return cell(e.column);
    case FilterExpr::Kind::kCall:
      break;
    default:
      return std::monostate{};
  }
  const std::string& f = e.function;
  if (f == "is_null" || f == "is_not_null") {
    bool null = std::holds_alternative<std::monostate>(Eval(e.args[0], cell));
    return f == "is_null" ? null : !null;
  }
  if (f == "not") {
    Value v = Eval(e.args[0], cell);
    if (!std::holds_alternative<bool>(v)) return std::monostate{};
    return !std::get<bool>(v);
  }
  if (f == "and" || f == "or") {
    // Three-valued: a decisive value wins over null.
    bool decisive = f == "or";
    bool saw_null = false;
    for (const auto& arg : e.args) {
      Value v = Eval(arg, cell);
      if (!std::holds_alternative<bool>(v)) {
        saw_null = true;
      } else if (std::get<bool>(v) == decisive) {
        return decisive;
      }
    }
    if (saw_null) return std::monostate{};
    return !decisive;
  }
  Value a = Eval(e.args[0], cell), b = Eval(e.args[1], cell);
  if (std::holds_alternative<std::monostate>(a) ||
      std::holds_alternative<std::monostate>(b)) {
    return std::monostate{};
  }
  int c = Compare(a, b);
  if (f == "equal") return c == 0;
  if (f == "not_equal") return c != 0;
  if (f == "lt") return c < 0;
  if (f == "lte") return c <= 0;
  if (f == "gt") return c > 0;
  return c >= 0;  // gte
}

}  // namespace

// ---------------------------------------------------------------------------
// Filter

Filter::Filter() = default;
Filter::~Filter() = default;
Filter::Filter(Filter&&) noexcept = default;
Filter& Filter::operator=(Filter&&) noexcept = default;

Result<Filter> Filter::Parse(std::string_view serialized) {
  Filter filter;
  filter.serialized_ = std::string(serialized);
  std::map<uint32_t, std::string> functions;
  std::vector<std::string_view> referred;

  Message m(serialized);
  Field f;
  while (m.Next(&f)) {
    if (f.number == 2) {  // extensions: SimpleExtensionDeclaration
      Message decl(f.bytes);
      Field d;
      while (decl.Next(&d)) {
        if (d.number != 3) continue;  // extension_function
        Message ef(d.bytes);
        Field g;
        uint32_t anchor = 0;
        std::string name;
        while (ef.Next(&g)) {
          if (g.number == 2) anchor = static_cast<uint32_t>(g.varint);
          if (g.number == 3) name = std::string(g.bytes);
        }
        functions[anchor] = name;
      }
    } else if (f.number == 3) {  // referred_expr
      referred.push_back(f.bytes);
    } else if (f.number == 4) {  // base_schema: NamedStruct { names = 1 }
      Message ns(f.bytes);
      Field g;
      while (ns.Next(&g)) {
        if (g.number == 1) filter.base_names_.emplace_back(g.bytes);
      }
    }
  }
  if (!m.ok()) {
    return status::InvalidArgument("[CSV] the filter is not a valid Substrait message");
  }
  for (std::string_view ref : referred) {
    // ExpressionReference { expression = 1; measure = 2; output_names = 3 }
    Message r(ref);
    Field g;
    auto expr = std::make_unique<FilterExpr>(FilterExpr::Unsupported("no expression"));
    while (r.Next(&g)) {
      if (g.number == 1) {
        *expr = ParseExpression(g.bytes, functions);
      } else if (g.number == 2) {
        *expr = FilterExpr::Unsupported("measure");
      }
    }
    // A conjunct must be boolean-valued: a call (literals and bare columns
    // are left to the engine).
    bool supported = expr->kind == FilterExpr::Kind::kCall;
    filter.supported_.push_back(supported);
    filter.conjuncts_.push_back(std::move(expr));
  }
  return filter;
}

Status Filter::Bind(const std::vector<std::string>& names,
                    const std::vector<ValueType>& types) {
  for (size_t i = 0; i < conjuncts_.size(); i++) {
    if (!supported_[i]) continue;
    Result<std::optional<ValueType>> t =
        BindExpr(conjuncts_[i].get(), base_names_, names, types);
    if (!t.has_value()) return std::move(t).status();
  }
  return status::Ok();
}

bool Filter::Matches(const std::function<Value(size_t)>& cell) const {
  for (size_t i = 0; i < conjuncts_.size(); i++) {
    if (!supported_[i]) continue;
    Value v = Eval(*conjuncts_[i], cell);
    if (!std::holds_alternative<bool>(v) || !std::get<bool>(v)) return false;
  }
  return true;
}

}  // namespace adbc::csv

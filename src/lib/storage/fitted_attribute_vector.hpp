#pragma once

#include <vector>

#include "base_attribute_vector.hpp"
#include "types.hpp"

namespace opossum {

class FittedAttributeVector<T> : private BaseAttributeVector {
 public:
  template <typename T>
  FittedAttributeVector<T>(const size_t size) {
    _data.resize(size);
  }

  ValueID get(const size_t i) const {
    DebugAssert(i < _data.size(), "Out of bounds get() on FittedAttributeVector.");
    return _data[i];
  }

  virtual void set(const size_t i, const ValueID value_id) {
    DebugAssert(i < _data.size(), "Out of bounds set() on FittedAttributeVector.");
    _data[i] = value_id;
  }

  size_t size() const { return _data.size(); }

  AttributeVectorWidth width() const { return sizeof(T); }

 protected:
  std::vector<T> _data;
};

}  // namespace opossum

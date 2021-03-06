#pragma once

#include <vector>

#include "base_attribute_vector.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
class FittedAttributeVector : public BaseAttributeVector {
 public:
  explicit FittedAttributeVector(const size_t size) : BaseAttributeVector() { _data.resize(size); }

  ValueID get(const size_t i) const {
    DebugAssert(i < _data.size(), "Out of bounds get() on FittedAttributeVector.");
    return ValueID(_data[i]);
  }

  void set(const size_t i, const ValueID value_id) final {
    DebugAssert(i < _data.size(), "Out of bounds set() on FittedAttributeVector.");
    _data[i] = value_id;
  }

  size_t size() const { return _data.size(); }

  AttributeVectorWidth width() const { return sizeof(T); }

 protected:
  std::vector<T> _data;
};

}  // namespace opossum

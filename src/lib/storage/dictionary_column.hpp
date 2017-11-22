#pragma once

#include <algorithm>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "fitted_attribute_vector.hpp"
#include "type_cast.hpp"
#include "types.hpp"
#include "value_column.hpp"

namespace opossum {

class BaseAttributeVector;
class BaseColumn;
template <typename T>
class ValueColumn;

// Even though ValueIDs do not have to use the full width of ValueID (uint32_t), this will also work for smaller ValueID
// types (uint8_t, uint16_t) since after a down-cast INVALID_VALUE_ID will look like their numeric_limit::max()
constexpr ValueID INVALID_VALUE_ID{std::numeric_limits<ValueID::base_type>::max()};

// Dictionary is a specific column type that stores all its values in a vector
template <typename T>
class DictionaryColumn : public BaseColumn {
 public:
  /**
   * Creates a Dictionary column from a given value column.
   */
  explicit DictionaryColumn(const std::shared_ptr<BaseColumn>& base_column) {
    const auto value_column = dynamic_cast<ValueColumn<T>*>(base_column.get());
    if (!value_column) {
      throw std::logic_error("Dictionary column could not be initialized due to a type mismatch.");
    }

    const auto& values = value_column->values();

    // Build the dictionary from distinct values
    _dictionary = std::make_shared<std::vector<T>>(values.begin(), values.end());
    std::sort(_dictionary->begin(), _dictionary->end());
    _dictionary->erase(std::unique(_dictionary->begin(), _dictionary->end()), _dictionary->end());

    // Decide which size the IDs need to have based on the dictionary size
    if (_dictionary->size() < std::numeric_limits<uint8_t>::max()) {
      _attribute_vector = std::make_shared<FittedAttributeVector<uint8_t>>(value_column->size());
    } else if (_dictionary->size() < std::numeric_limits<uint16_t>::max()) {
      _attribute_vector = std::make_shared<FittedAttributeVector<uint16_t>>(value_column->size());
    } else if (_dictionary->size() < std::numeric_limits<uint32_t>::max()) {
      _attribute_vector = std::make_shared<FittedAttributeVector<uint32_t>>(value_column->size());
    } else {
      throw std::logic_error("Value IDs does not fit in 4 bytes.");
    }

    std::map<T, ValueID> value_IDs;
    size_t index = 0;
    for (const auto& value : values) {
      const auto mapIt = value_IDs.find(value);
      if (mapIt != value_IDs.end()) {
        _attribute_vector->set(index, mapIt->second);
      } else {
        const auto dictIt = std::lower_bound(_dictionary->begin(), _dictionary->end(), value);
        const auto id = ValueID(std::distance(_dictionary->begin(), dictIt));
        value_IDs[value] = id;
        _attribute_vector->set(index, id);
      }
      ++index;
    }
  }

  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const size_t i) const override { return _dictionary->at(_attribute_vector->get(i)); }

  // return the value at a certain position.
  const T get(const size_t i) const { return _dictionary->at(_attribute_vector->get(i)); }

  // dictionary columns are immutable
  void append(const AllTypeVariant&) override { throw std::logic_error("Dictionary columns are immutable."); }

  // returns an underlying dictionary
  std::shared_ptr<const std::vector<T>> dictionary() const { return _dictionary; }

  // returns an underlying data structure
  std::shared_ptr<const BaseAttributeVector> attribute_vector() const { return _attribute_vector; }

  // return the value represented by a given ValueID
  const T& value_by_value_id(ValueID value_id) const { return _dictionary->at(value_id); }

  // returns the first value ID that refers to a value >= the search value
  // returns INVALID_VALUE_ID if all values are smaller than the search value
  ValueID lower_bound(T value) const {
    const auto it = std::lower_bound(_dictionary->begin(), _dictionary->end(), value);
    if (it == _dictionary->end()) {
      return INVALID_VALUE_ID;
    } else {
      return ValueID(std::distance(_dictionary->begin(), it));
    }
  }

  // same as lower_bound(T), but accepts an AllTypeVariant
  ValueID lower_bound(const AllTypeVariant& value) const {
    const T val = type_cast<T>(value);

    if (!val) {
      return INVALID_VALUE_ID;
    }

    return lower_bound(val);
  }

  // returns the first value ID that refers to a value > the search value
  // returns INVALID_VALUE_ID if all values are smaller than or equal to the search value
  ValueID upper_bound(T value) const {
    const auto it = std::upper_bound(_dictionary->begin(), _dictionary->end(), value);
    if (it == _dictionary->end()) {
      return INVALID_VALUE_ID;
    } else {
      return ValueID(std::distance(_dictionary->begin(), it));
    }
  }

  // same as upper_bound(T), but accepts an AllTypeVariant
  ValueID upper_bound(const AllTypeVariant& value) const {
    const T val = type_cast<T>(value);

    if (!val) {
      return INVALID_VALUE_ID;
    }

    return lower_bound(val);
  }

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const { return _dictionary->size(); }

  // return the number of entries
  size_t size() const override { return _attribute_vector->size(); }

 protected:
  std::shared_ptr<std::vector<T>> _dictionary;
  std::shared_ptr<BaseAttributeVector> _attribute_vector;
};

}  // namespace opossum

#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_column.hpp"
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_column(std::shared_ptr<BaseColumn> column) { _columns.push_back(column); }

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(_columns.size() == values.size(), "Inserted number of columns does not match.");
  for (auto i = 0u; i < values.size(); ++i) {
    _columns[i]->append(values[i]);
  }
}

std::shared_ptr<BaseColumn> Chunk::get_column(ColumnID column_id) const { return _columns.at(column_id); }

uint16_t Chunk::col_count() const { return _columns.size(); }

uint32_t Chunk::size() const {
  if (_columns.empty()) {
    return 0;
  }

  return _columns[0]->size();
}

}  // namespace opossum

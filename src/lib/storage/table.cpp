#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "value_column.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const uint32_t chunk_size) {
  _chunkSize = chunk_size;

  _chunks.emplace_back();
}

void Table::add_column_definition(const std::string& name, const std::string& type) {
  DebugAssert(std::find(_columnNames.begin(), _columnNames.end(), name) == _columnNames.end(),
              "ColumnName already exists");
  _columnNames.push_back(name);
  _columnTypes.push_back(type);
}

void Table::add_column(const std::string& name, const std::string& type) {
  add_column_definition(name, type);

  for (auto& chunk : _chunks) {
    chunk.add_column(make_shared_by_column_type<BaseColumn, ValueColumn>(type));
  }
}

void Table::append(std::vector<AllTypeVariant> values) {
  bool newChunk = _chunkSize != 0 && _chunks.back().size() >= _chunkSize;

  if (newChunk) {
    create_new_chunk();
  }

  _chunks.back().append(values);
}

void Table::create_new_chunk() {
  Chunk chunk;

  for (auto& column_type : _columnTypes) {
    chunk.add_column(make_shared_by_column_type<BaseColumn, ValueColumn>(column_type));
  }

  _chunks.push_back(std::move(chunk));
}

uint16_t Table::col_count() const { return _columnNames.size(); }

uint64_t Table::row_count() const {
  auto count = 0u;

  for (const auto& chunk : _chunks) {
    count += chunk.size();
  }

  return count;
}

ChunkID Table::chunk_count() const { return ChunkID(_chunks.size()); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  for (auto i = 0u; i < _columnNames.size(); ++i) {
    if (_columnNames[i] == column_name) {
      return ColumnID(i);
    }
  }

  throw std::out_of_range("Column name not found.");
}

uint32_t Table::chunk_size() const { return _chunkSize; }

const std::vector<std::string>& Table::column_names() const { return _columnNames; }

const std::string& Table::column_name(ColumnID column_id) const { return _columnNames.at(column_id); }

const std::string& Table::column_type(ColumnID column_id) const { return _columnTypes.at(column_id); }

Chunk& Table::get_chunk(ChunkID chunk_id) { return _chunks.at(chunk_id); }

const Chunk& Table::get_chunk(ChunkID chunk_id) const { return _chunks.at(chunk_id); }

void Table::compress_chunk(ChunkID chunk_id) { throw std::runtime_error("TODO"); }

}  // namespace opossum

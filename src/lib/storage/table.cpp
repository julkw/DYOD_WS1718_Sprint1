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
  m_chunkSize = chunk_size;

  m_chunks.emplace_back();
}

void Table::add_column_definition(const std::string& name, const std::string& type) {
  DebugAssert(std::find(m_columnNames.begin(), m_columnNames.end(), name) == m_columnNames.end(),
              "ColumnName already exists");
  m_columnNames.push_back(name);
  m_columnTypes.push_back(type);
}

void Table::add_column(const std::string& name, const std::string& type) {
  add_column_definition(name, type);

  for (auto& chunk : m_chunks) {
    chunk.add_column(make_shared_by_column_type<BaseColumn, ValueColumn>(type));
  }
}

void Table::append(std::vector<AllTypeVariant> values) {
  bool newChunk = m_chunkSize != 0 && m_chunks.back().size() >= m_chunkSize;

  if (newChunk) {
    create_new_chunk();
  }

  m_chunks.back().append(values);
}

void Table::create_new_chunk() {
  m_chunks.emplace_back();

  for (auto& column_type : m_columnTypes) {
    m_chunks.back().add_column(make_shared_by_column_type<BaseColumn, ValueColumn>(column_type));
  }
}

uint16_t Table::col_count() const { return m_columnNames.size(); }

uint64_t Table::row_count() const {
  auto count = 0u;

  for (const auto& chunk : m_chunks) {
    count += chunk.size();
  }

  return count;
}

ChunkID Table::chunk_count() const { return ChunkID(m_chunks.size()); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  for (auto i = 0u; i < m_columnNames.size(); ++i) {
    if (m_columnNames[i] == column_name) {
      return ColumnID(i);
    }
  }

  DebugAssert(false, "Column name not found.");
  // an adequate error handling is missing here
  return ColumnID(-1);
}

uint32_t Table::chunk_size() const { return m_chunkSize; }

const std::vector<std::string>& Table::column_names() const { return m_columnNames; }

const std::string& Table::column_name(ColumnID column_id) const { return m_columnNames.at(column_id); }

const std::string& Table::column_type(ColumnID column_id) const { return m_columnTypes.at(column_id); }

Chunk& Table::get_chunk(ChunkID chunk_id) { return m_chunks.at(chunk_id); }

const Chunk& Table::get_chunk(ChunkID chunk_id) const { return m_chunks.at(chunk_id); }

}  // namespace opossum

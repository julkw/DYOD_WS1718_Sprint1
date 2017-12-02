#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"
#include "type_cast.hpp"

namespace opossum {

class BaseTableScanImpl {
 public:
  virtual std::shared_ptr<const Table> on_execute() = 0;
};
class Table;

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value);

  ~TableScan();

  ColumnID column_id() const;
  ScanType scan_type() const;
  const AllTypeVariant& search_value() const;

 protected:
  template <typename T>
  class TableScanImpl : public BaseTableScanImpl {
   public:
    TableScanImpl(std::shared_ptr<const Table> table, ColumnID column_id, const ScanType scan_type,
                  const AllTypeVariant search_value);

    std::shared_ptr<const Table> on_execute() override {
      const auto position_list = std::make_shared<PosList>();
      auto referenced_table = _table;

      for (auto chunk_id = ChunkID(0); chunk_id < _table->chunk_count(); ++chunk_id) {
        const auto& chunk = _table->get_chunk(chunk_id);
        const auto& column = chunk.get_column(_column_id);

        if (auto vc = std::dynamic_pointer_cast<ValueColumn<T>>(column)) {
          _scanColumn(position_list, vc, chunk_id);
        } else if (auto dc = std::dynamic_pointer_cast<DictionaryColumn<T>>(column)) {
          _scanColumn(position_list, dc, chunk_id);
        } else if (auto rc = std::dynamic_pointer_cast<ReferenceColumn>(column)) {
          _scanColumn(position_list, rc, referenced_table, chunk_id);
        } else {
          throw std::logic_error("Unkown column type.");
        }
      }

      const auto result_table = std::make_shared<opossum::Table>();

      Chunk chunk;

      for (auto column_id = ColumnID(0); column_id < _table->col_count(); ++column_id) {
        result_table->add_column(_table->column_name(column_id), _table->column_type(column_id));

        const auto column = std::make_shared<ReferenceColumn>(referenced_table, column_id, position_list);
        chunk.add_column(column);
      }

      result_table->emplace_chunk(std::move(chunk));

      return result_table;
    }

   protected:
    template <typename U>
    bool _evaluate_scan(U value, U compare_value) {
      switch (_scan_type) {
        case ScanType::OpEquals:
          return value == compare_value;
        case ScanType::OpNotEquals:
          return value != compare_value;
        case ScanType::OpGreaterThan:
          return value > compare_value;
        case ScanType::OpLessThanEquals:
          return value <= compare_value;
        case ScanType::OpGreaterThanEquals:
          return value >= compare_value;
        case ScanType::OpLessThan:
          return value < compare_value;
        default:
          throw std::logic_error("Unknown scan type.");
      }
    }

    template <typename M, typename N>
    void _appendPositionList(std::shared_ptr<PosList> position_list, M accessor, size_t size, N compare_value,
                             ChunkID chunk_id, bool all_true) {
      for (ChunkOffset chunk_offset = 0; chunk_offset < size; ++chunk_offset) {
        if (all_true || _evaluate_scan(accessor(chunk_offset), compare_value)) {
          RowID row_id;
          row_id.chunk_id = chunk_id;
          row_id.chunk_offset = chunk_offset;
          position_list->emplace_back(std::move(row_id));
        }
      }
    }

    void _scanColumn(std::shared_ptr<PosList> position_list, std::shared_ptr<ValueColumn<T>> vc,
                     const ChunkID chunk_id);
    void _scanColumn(std::shared_ptr<PosList> position_list, std::shared_ptr<DictionaryColumn<T>> dc,
                     const ChunkID chunk_id);
    void _scanColumn(std::shared_ptr<PosList> position_list, std::shared_ptr<ReferenceColumn> rc,
                     std::shared_ptr<const Table>& referenced_table, const ChunkID chunk_id);

    T _evaluateReferenceColumn(std::shared_ptr<ReferenceColumn> rc, const RowID& row_id);

    std::shared_ptr<const Table> _table;
    const ColumnID _column_id;
    ScanType _scan_type;
    const AllTypeVariant _search_value;
  };

  std::shared_ptr<const Table> _on_execute() override;
  const ColumnID _column_id;
  const ScanType _scan_type;
  const AllTypeVariant _search_value;
};

}  // namespace opossum

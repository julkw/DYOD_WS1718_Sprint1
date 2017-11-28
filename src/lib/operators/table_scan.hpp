#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "storage/table.hpp"

#include "storage/value_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/dictionary_column.hpp"

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
          const auto compare_value = opossum::type_cast<T>(_search_value);

          for (auto chunk_id = ChunkID(0); chunk_id < _table->chunk_count(); ++chunk_id) {
            const auto& chunk = _table->get_chunk(chunk_id);
            const auto& column = chunk.get_column(_column_id);

            if (auto vc = std::dynamic_pointer_cast<ValueColumn<T>>(column)) {
                const auto& values = vc->values();

                for (auto i = 0u; i < values.size(); ++i) {
                    if (_evaluate_scan(values[i], compare_value)) {
                        RowID row_id;
                        row_id.chunk_id = chunk_id;
                        row_id.chunk_offset = i;
                        position_list->emplace_back(std::move(row_id));
                    }
                }
            }
            else if (auto dc = std::dynamic_pointer_cast<DictionaryColumn<T>>(column)) {
                // TODO
            }
            else if (auto rc = std::dynamic_pointer_cast<ReferenceColumn>(column)) {
                // TODO
            }
            else {
                throw std::logic_error("Unkown column type.");
            }
          }

          const auto column = std::make_shared<ReferenceColumn>(_table, _column_id, position_list);

          Chunk chunk;
          chunk.add_column(column);

          const auto result_table = std::make_shared<opossum::Table>();
          result_table->add_column(_table->column_name(_column_id), _table->column_type(_column_id));
          result_table->emplace_chunk(std::move(chunk));

          return result_table;
      }

  protected:
      bool _evaluate_scan(T value, T compare_value) {
          // TODO
          return true;
      }

      std::shared_ptr<const Table> _table;
      const ColumnID _column_id;
      const ScanType _scan_type;
      const AllTypeVariant _search_value;
  };

  std::shared_ptr<const Table> _on_execute() override;
  const ColumnID _column_id;
  const ScanType _scan_type;
  const AllTypeVariant _search_value;
};

}  // namespace opossum

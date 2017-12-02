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
          const auto compare_value = opossum::type_cast<T>(_search_value);
          auto referenced_table = _table;

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
                const auto& id_values = dc->attribute_vector();
                ValueID dict_id = INVALID_VALUE_ID;
                bool check_attribute_vector = true;
                switch (_scan_type){
                    case ScanType::OpEquals:
                        if(dc->lower_bound(compare_value) == dc->upper_bound(compare_value)) {
                            check_attribute_vector = false;
                        } else {
                            dict_id = dc->lower_bound(compare_value);
                        }
                        break;
                    case ScanType::OpNotEquals:
                        if(dc->lower_bound(compare_value) != dc->upper_bound(compare_value)) {
                            dict_id = dc->lower_bound(compare_value);
                        }
                        // TODO
                        // in the else case every position will evaluate to true
                        // maybe cut the checks?
                        break;
                    case ScanType::OpGreaterThan:
                    case ScanType::OpLessThanEquals:
                        if(dc->lower_bound(compare_value) == dc->upper_bound(compare_value)) {
                            dict_id = dc->lower_bound(compare_value) - 1;
                            // TODO
                            // Does this throw errors when lower_bound is 0?
                        } else {
                            dict_id = dc->lower_bound(compare_value);
                        }
                        break;
                    case ScanType::OpGreaterThanEquals:
                    case ScanType::OpLessThan:
                        dict_id = dc->lower_bound(compare_value);
                        break;
                }

                if(check_attribute_vector) {
                    // TODO
                    // this is basically a copy. Maybe make a function out of it?
                    for (auto i = 0u; i < id_values->size(); ++i) {
                        if (_evaluate_scan(id_values->get(i), dict_id)) {
                            RowID row_id;
                            row_id.chunk_id = chunk_id;
                            row_id.chunk_offset = i;
                            position_list->emplace_back(std::move(row_id));
                        }
                    }
                }
            }
            else if (auto rc = std::dynamic_pointer_cast<ReferenceColumn>(column)) {
                const auto& column_pos_list = rc->pos_list();
                referenced_table = rc->referenced_table();
                for (const auto& row_id : *column_pos_list) {
                    // TODO
                    // this is incredibly ugly, but we were told not to use the [] operator.
                    // any ideas?
                    const auto& column = referenced_table->get_chunk(row_id.chunk_id).get_column(rc->referenced_column_id());
                    const T value = type_cast<T>((*column)[row_id.chunk_offset]);
                    if (_evaluate_scan(value, compare_value)) {
                        // TODO
                        // does it pose a problem that row_id is a reference?
                        position_list->push_back(row_id);
                    }
                }
            }
            else {
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
      bool _evaluate_scan(T value, T compare_value) {
          switch (_scan_type){
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
                  return false;
          }
      }

      // TODO
      // this is incredibly ugly. DO SOMETHING!
      bool _evaluate_scan(ValueID value, ValueID compare_value) {
          switch (_scan_type){
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
                  return false;
          }
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

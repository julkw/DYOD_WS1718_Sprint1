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
          auto referenced_table = _table;

          for (auto chunk_id = ChunkID(0); chunk_id < _table->chunk_count(); ++chunk_id) {
            const auto& chunk = _table->get_chunk(chunk_id);
            const auto& column = chunk.get_column(_column_id);

            if (auto vc = std::dynamic_pointer_cast<ValueColumn<T>>(column)) {
                _appendPositionList(position_list, vc, chunk_id);
            }
            else if (auto dc = std::dynamic_pointer_cast<DictionaryColumn<T>>(column)) {
                _appendPositionList(position_list, dc, chunk_id);
            }
            else if (auto rc = std::dynamic_pointer_cast<ReferenceColumn>(column)) {
                _appendPositionList(position_list, rc, referenced_table, chunk_id);
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
      template <typename U>
      bool _evaluate_scan(U value, U compare_value) {
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
                  throw std::logic_error("Unknown scan type.");
          }
      }

      void _appendPositionList(std::shared_ptr<PosList> position_list, std::shared_ptr<ValueColumn<T>> vc, const ChunkID chunk_id) {
          const auto& values = vc->values();
          const auto compare_value = opossum::type_cast<T>(_search_value);

          for (auto i = 0u; i < values.size(); ++i) {
              if (_evaluate_scan(values[i], compare_value)) {
                  RowID row_id;
                  row_id.chunk_id = chunk_id;
                  row_id.chunk_offset = i;
                  position_list->emplace_back(std::move(row_id));
              }
          }
      }

      void _appendPositionList(std::shared_ptr<PosList> position_list, std::shared_ptr<DictionaryColumn<T>> dc, const ChunkID chunk_id) {
          const auto& id_values = dc->attribute_vector();
          ValueID dict_id = INVALID_VALUE_ID;
          bool all_false = false;
          bool all_true = false;
          const auto compare_value = opossum::type_cast<T>(_search_value);

          switch (_scan_type){
              case ScanType::OpEquals: {
                  auto lower_bound = dc->lower_bound(compare_value);
                  dict_id = lower_bound;
                  all_false = lower_bound == dc->upper_bound(compare_value);
                  break; }
              case ScanType::OpNotEquals: {
                  auto lower_bound = dc->lower_bound(compare_value);
                  dict_id = lower_bound;
                  all_true = lower_bound == dc->upper_bound(compare_value);
                  break; }
              case ScanType::OpGreaterThan: {
                  dict_id = dc->upper_bound(compare_value);
                  _scan_type = ScanType::OpGreaterThanEquals;
                  all_false = dict_id == INVALID_VALUE_ID;
                  all_true = dict_id == ValueID(0);
                  break; }
              case ScanType::OpLessThanEquals: {
                  dict_id = dc->upper_bound(compare_value);
                  _scan_type = ScanType::OpLessThan;
                  all_false = dict_id == ValueID(0);
                  all_true = dict_id == INVALID_VALUE_ID;
                  break; }
              case ScanType::OpGreaterThanEquals: {
                  dict_id = dc->lower_bound(compare_value);
                  all_true = dict_id == ValueID(0);
                  all_false = dict_id == INVALID_VALUE_ID;
                  break; }
              case ScanType::OpLessThan: {
                  dict_id = dc->lower_bound(compare_value);
                  all_true = dict_id == INVALID_VALUE_ID;
                  all_false = dict_id == ValueID(0);
                  break; }
          }

          if(!all_false) {
              // TODO
              // this is basically a copy. Maybe make a function out of it?
              for (auto i = 0u; i < id_values->size(); ++i) {
                  if (all_true || _evaluate_scan(id_values->get(i), dict_id)) {
                      RowID row_id;
                      row_id.chunk_id = chunk_id;
                      row_id.chunk_offset = i;
                      position_list->emplace_back(std::move(row_id));
                  }
              }
          }
      }

      void _appendPositionList(std::shared_ptr<PosList> position_list, std::shared_ptr<ReferenceColumn> rc, std::shared_ptr<const Table>& referenced_table, const ChunkID chunk_id) {
          const auto& column_pos_list = rc->pos_list();
          referenced_table = rc->referenced_table();
          const auto compare_value = opossum::type_cast<T>(_search_value);

          for (const auto& row_id : *column_pos_list) {
              const T value = _evaluateReferenceColumn(rc, row_id);
              if (_evaluate_scan(value, compare_value)) {
                  position_list->push_back(row_id);
              }
          }
      }

      T _evaluateReferenceColumn(std::shared_ptr<ReferenceColumn> rc, const RowID& row_id) {
          const auto& chunk = rc->referenced_table()->get_chunk(row_id.chunk_id);
          const auto& column = chunk.get_column(rc->referenced_column_id());

          if (auto vc = std::dynamic_pointer_cast<ValueColumn<T>>(column)) {
              return vc->values()[row_id.chunk_offset];
          }
          else if (auto dc = std::dynamic_pointer_cast<DictionaryColumn<T>>(column)) {
              return dc->get(row_id.chunk_offset);
          }
          else {
              throw std::logic_error("Column id not a value or dictionary column.");
          }
      }

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

#include "table_scan.hpp"

#include "resolve_type.hpp"
#include "storage/table.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
                     const AllTypeVariant search_value)
: AbstractOperator(in)
, _column_id(column_id)
, _scan_type(scan_type)
, _search_value(search_value)
{
}

TableScan::~TableScan() {}

std::shared_ptr<const Table> TableScan::_on_execute() {
  const auto& input_table = _input_table_left();
  const auto& column_type = input_table->column_type(_column_id);

  auto impl = make_unique_by_column_type<BaseTableScanImpl, TableScanImpl>(column_type, input_table, _column_id, _scan_type, _search_value);
  return impl->on_execute();
}

ColumnID TableScan::column_id() const {
  return _column_id;
}

ScanType TableScan::scan_type() const {
  return _scan_type;
}

const AllTypeVariant& TableScan::search_value() const {
  return _search_value;
}

template <typename T>
TableScan::TableScanImpl<T>::TableScanImpl(std::shared_ptr<const Table> table, ColumnID column_id, const ScanType scan_type,
                                           const AllTypeVariant search_value)
: _table(table)
, _column_id(column_id)
, _scan_type(scan_type)
, _search_value(search_value)
{}

template <typename T>
void TableScan::TableScanImpl<T>:: _appendPositionList(std::shared_ptr<PosList> position_list, std::shared_ptr<ValueColumn<T>> vc, const ChunkID chunk_id) {
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

template <typename T>
void TableScan::TableScanImpl<T>::_appendPositionList(std::shared_ptr<PosList> position_list, std::shared_ptr<DictionaryColumn<T>> dc, const ChunkID chunk_id) {
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

template <typename T>
void TableScan::TableScanImpl<T>::_appendPositionList(std::shared_ptr<PosList> position_list, std::shared_ptr<ReferenceColumn> rc, std::shared_ptr<const Table>& referenced_table, const ChunkID chunk_id) {
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

template <typename T>
T TableScan::TableScanImpl<T>::_evaluateReferenceColumn(std::shared_ptr<ReferenceColumn> rc, const RowID& row_id) {
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

}  // namespace opossum

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

  auto impl = make_unique_by_column_type<BaseTableScanImpl, TableScanImpl>(column_type, *input_table, _column_id, _scan_type, _search_value);
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
TableScan::TableScanImpl<T>::TableScanImpl(const Table& table, ColumnID column_id, const ScanType scan_type,
                                           const AllTypeVariant search_value)
: _table(table)
, _column_id(column_id)
, _scan_type(scan_type)
, _search_value(search_value)
{}

}  // namespace opossum

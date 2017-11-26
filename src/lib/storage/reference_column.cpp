#include <iomanip>
#include <iterator>
#include <memory>
#include <mutex>

#include <vector>

#include "reference_column.hpp"

#include "utils/assert.hpp"

namespace opossum {
    ReferenceColumn::ReferenceColumn(const std::shared_ptr<const Table> referenced_table, const ColumnID referenced_column_id,
                    const std::shared_ptr<const PosList> pos)
    : _pos_list(pos)
    , _referenced_table(referenced_table)
    , _referenced_column_id(referenced_column_id)
    {
        // check if referenced_column_id is itself a referenceColumn?
    }

    const AllTypeVariant ReferenceColumn::operator[](const size_t i) const{
        const auto position = _pos_list->at(i);
        const auto& chunk = _referenced_table->get_chunk(position.chunk_id);
        const auto& column = chunk.get_column(_referenced_column_id);

        return (*column)[position.chunk_offset];
    }

    size_t ReferenceColumn::size() const {
        return _pos_list->size();
    }

    const std::shared_ptr<const PosList> ReferenceColumn::pos_list() const {
        return _pos_list;
    }
    const std::shared_ptr<const Table> ReferenceColumn::referenced_table() const {
        return _referenced_table;
    }

    ColumnID ReferenceColumn::referenced_column_id() const {
        return _referenced_column_id;
    }

}  // namespace opossum

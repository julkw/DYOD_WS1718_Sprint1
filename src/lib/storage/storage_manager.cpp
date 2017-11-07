#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager s;
  return s;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  DebugAssert(!has_table(name), "Table with name " + name + " already exists.");
  _tables[name] = table;
}

void StorageManager::drop_table(const std::string& name) {
  auto num_erased = _tables.erase(name);
  if (num_erased == 0) {
    DebugAssert(false, "Table with name " + name + " does not exist.");
  }
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  DebugAssert(has_table(name), "Table with name " + name + " does not exist.");
  return _tables.at(name);
}

bool StorageManager::has_table(const std::string& name) const { return _tables.count(name) != 0; }

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> names;

  for (auto& pair : _tables) {
    names.push_back(pair.first);
  }

  return names;
}

void StorageManager::print(std::ostream& out) const {
  for (const auto& pair : _tables) {
    const auto& table_name = pair.first;
    const auto& table = pair.second;
    out << table_name << std::endl
        << "#cols:" << table->col_count() << std::endl
        << "#rows:" << table->row_count() << std::endl
        << "#chunks:" << table->chunk_count() << std::endl;
  }
}

void StorageManager::reset() { get() = StorageManager(); }

}  // namespace opossum

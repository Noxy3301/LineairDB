#ifndef LINEAIRDB_INDEX_FACTORY_HPP
#define LINEAIRDB_INDEX_FACTORY_HPP

#include <lineairdb/config.h>

#include <memory>

#include "index/impl/pl_index.hpp"
#include "index/index_base.h"
#include "util/epoch_framework.hpp"

namespace LineairDB {
namespace Index {

inline std::unique_ptr<IndexBase> MakeIndex(Config c, EpochFramework& e) {
  switch (c.index_structure) {
    case Config::IndexStructure::HashTableWithPrecisionLockingIndex:
      return std::make_unique<PLIndex>(c, e);
    default:
      return std::make_unique<PLIndex>(c, e);
  }
}

}  // namespace Index
}  // namespace LineairDB

#endif /* LINEAIRDB_INDEX_FACTORY_HPP */

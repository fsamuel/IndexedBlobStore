#ifndef ALLOCATION_LOGGER_H_
#define ALLOCATION_LOGGER_H_

#include <map>
#include <mutex>
#include <set>

#include "chunked_vector.h"
#include "shm_allocator.h"

class AllocationLogger {
 public:
  using Node = ShmAllocator::Node;

  AllocationLogger();

  // Pushes an allocation operation onto the operations vector.
  void RecordAllocation(const Node& node);

  // Pushes a deallocation operation onto the operations vector.
  void RecordDeallocation(const Node& node);

  // Pushes a search operation onto the operations vector.
  void RecordSearch(const Node& node);

 private:
  enum class OperationType { Allocate, Deallocate, Search };

  std::string OperationTypeToString(OperationType type) const;

  struct Operation {
    Operation(OperationType type, const ShmAllocator::Node& node);

    std::thread::id thread_id;
    OperationType type;
    std::size_t index;
    std::size_t size;
    std::size_t version;
    std::size_t next_index;
    bool marked;
  };

  void PrintOperation(const Operation& operation) const;

  // Prints the last 100 operations performed on the allocator.
  void PrintLastOperations() const;

  // Prints the history of an operation with a particular index.
  void PrintIndexHistory(std::size_t index) const;

  thread_local static std::map<const AllocationLogger*, std::set<std::size_t>>
      skipped_nodes_;
  mutable std::mutex log_mutex_;
  ChunkedVector<Operation> operations_;
};

#endif  // ALLOCATION_LOGGER_H_
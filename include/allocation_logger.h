#ifndef ALLOCATION_LOGGER_H_
#define ALLOCATION_LOGGER_H_

#include <map>
#include <mutex>
#include <set>

#include "chunked_vector.h"

struct ShmNode;

class AllocationLogger {
 private:
  AllocationLogger();
  enum class OperationType { Allocate, Deallocate, Search };

  std::string OperationTypeToString(OperationType type) const;

  struct Operation {
    Operation(OperationType type, const ShmNode& node);

    std::thread::id thread_id;
    OperationType type;
    std::size_t index;
    std::size_t size;
    std::size_t version;
    std::size_t next_index;
    bool marked;
  };

  thread_local static std::map<const AllocationLogger*, std::set<std::size_t>>
      skipped_nodes_;
  mutable std::mutex log_mutex_;
  ChunkedVector<Operation> operations_;

 public:
  static AllocationLogger* Get();

  // Pushes an allocation operation onto the operations vector.
  void RecordAllocation(const ShmNode& node);

  // Pushes a deallocation operation onto the operations vector.
  void RecordDeallocation(const ShmNode& node);

  // Pushes a search operation onto the operations vector.
  void RecordSearch(const ShmNode& node);

  void PrintOperation(const Operation& operation) const;

  // Prints the last 100 operations performed on the allocator.
  void PrintLastOperations() const;

  // Prints the history of an operation with a particular index.
  void PrintIndexHistory(std::size_t index) const;
};

#endif  // ALLOCATION_LOGGER_H_
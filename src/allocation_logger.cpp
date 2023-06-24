#include "allocation_logger.h"
#include "shm_allocator.h"
#include "test_memory_buffer_factory.h"

thread_local std::map<const AllocationLogger*, std::set<std::size_t>>
    AllocationLogger::skipped_nodes_;

AllocationLogger* AllocationLogger::Get() {
  static AllocationLogger logger;
  return &logger;
}

AllocationLogger::AllocationLogger()
    : operations_(TestMemoryBufferFactory::Get(), "operations", 1024) {}

void AllocationLogger::RecordAllocation(const ShmNode& node) {
  operations_.emplace_back(OperationType::Allocate, node);
}

void AllocationLogger::RecordDeallocation(const ShmNode& node) {
  operations_.emplace_back(OperationType::Deallocate, node);
}

void AllocationLogger::RecordSearch(const ShmNode& node) {
  operations_.emplace_back(OperationType::Search, node);
}

AllocationLogger::Operation::Operation(OperationType type, const ShmNode& node)
    : thread_id(std::this_thread::get_id()),
      type(type),
      index(node.index),
      size(node.size.load()),
      version(node.version.load()),
      next_index(node.next_index.load()),
      marked(ShmAllocator::is_marked_reference(node.next_index)) {}

std::string AllocationLogger::OperationTypeToString(OperationType type) const {
  switch (type) {
    case OperationType::Allocate:
      return "Allocate";
    case OperationType::Deallocate:
      return "Deallocate";
    case OperationType::Search:
      return "Search";
  }
  return "Unknown";
}

void AllocationLogger::PrintOperation(const Operation& operation) const {
  std::cout << "ThreadId(" << operation.thread_id
            << "): " << OperationTypeToString(operation.type) << "("
            << ChunkManager::chunk_index(operation.index) << ", "
            << ChunkManager::offset_in_chunk(operation.index) << "), Next("
            << ChunkManager::chunk_index(operation.next_index) << ", "
            << ChunkManager::offset_in_chunk(operation.next_index)
            << "), size=" << operation.size << ", version=" << operation.version
            << ", marked = " << std::boolalpha << operation.marked << std::endl;
}

// Prints the last 100 operations performed on the allocator.
void AllocationLogger::PrintLastOperations() const {
  std::lock_guard<std::mutex> lock(log_mutex_);
  std::cout << "Last 200 operations performed on the allocator:" << std::endl;
  std::size_t op_size = operations_.size();
  for (int i = std::max<uint8_t>(0, op_size - 200); i < op_size; ++i) {
    PrintOperation(operations_[i]);
  }
}

// Prints the history of an operation with a particular index.
void AllocationLogger::PrintIndexHistory(std::size_t index) const {
  std::lock_guard<std::mutex> lock(log_mutex_);

  std::size_t input_chunk_index = ChunkManager::chunk_index(index);
  std::size_t input_offset_in_chunk = ChunkManager::offset_in_chunk(index);
  std::cout << "History of index " << input_chunk_index << ", "
            << input_offset_in_chunk << ":" << std::endl;
  std::size_t op_size = operations_.size();
  for (int i = std::max<int>(0, op_size - 2000); i < op_size; ++i) {
    const Operation& operation = operations_[i];
    std::size_t op_chunk_index = ChunkManager::chunk_index(operation.index);
    std::size_t op_offset_in_chunk =
        ChunkManager::offset_in_chunk(operation.index);
    if ((op_chunk_index == input_chunk_index) &&
        (op_offset_in_chunk == input_offset_in_chunk)) {
      PrintOperation(operation);
    }
  }
}
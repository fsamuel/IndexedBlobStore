#ifndef SHM_ALLOCATOR_H_
#define SHM_ALLOCATOR_H_

#include <algorithm>
#include <atomic>
#include <cassert>
#include <iostream>
#include <vector>

#include "chunk_manager.h"
#include "chunked_vector.h"

class AllocationLogger;

static std::mutex log_mutex;

// A simple allocator that allocates memory from a shared memory buffer. The
// allocator maintains a free list of available blocks of memory. When a block
// is allocated, it is removed from the free list. When a block is freed, it is
// added back to the free list. The allocator does not attempt to coalesce
// adjacent free blocks. The allocator is designed to be lock-free by using
// atomic operations to update the allocator state: size of a free block, or the
// head of the free list.
class ShmAllocator {
 public:
  static constexpr std::size_t InvalidIndex =
      std::numeric_limits<std::size_t>::max() >> 1;

  // Constructor that takes a reference to the shared memory buffer to be used
  // for allocation
  explicit ShmAllocator(ChunkManager&& buffer);

  explicit ShmAllocator(ShmAllocator&& other);

  // Allocate memory for n objects of type T, and return a pointer to the first
  // object
  uint8_t* Allocate(std::size_t bytes_requested);

  // Deallocate memory at the given pointer index
  bool Deallocate(std::size_t index);

  // Deallocate memory at the given pointer.
  bool Deallocate(uint8_t* ptr);

  template <typename U>
  typename std::enable_if<!std::is_same<U, uint8_t>::value, bool>::type
  Deallocate(U* ptr) {
    return Deallocate(reinterpret_cast<uin8_t*>(ptr));
  }

  // Returns the size of the allocated block at the given index.
  std::size_t GetCapacity(std::size_t index) const;

  // Returns the size of the allocated block at the given pointer.
  std::size_t GetCapacity(uint8_t* ptr);
  template <typename U>
  typename std::enable_if<!std::is_same<U, uint8_t>::value, std::size_t>::type
  GetCapacity(U* ptr) {
    return GetCapacity(reinterpret_cast<uin8_t*>(ptr));
  }

  template <typename U>
  std::uint64_t ToIndex(U* ptr) const {
    return ToIndexImpl(ptr, typename std::is_same<U, Node>::type{});
  }

  // Helper method to convert an index relative to the start of the buffer to a
  // pointer
  template <class U>
  const U* ToPtr(std::uint64_t index) const {
    if (index == InvalidIndex) {
      return nullptr;
    }
    return reinterpret_cast<const U*>(chunk_manager_.at(index));
  }

  template <class U>
  U* ToPtr(std::size_t index) {
    if (index == InvalidIndex) {
      return nullptr;
    }
    return reinterpret_cast<U*>(chunk_manager_.at(index));
  }

  ShmAllocator& operator=(ShmAllocator&& other) noexcept {
    chunk_manager_ = std::move(other.chunk_manager_);
    return *this;
  }

 private:
  // Header for the allocator state in the shared memory buffer
  struct AllocatorStateHeader {
    // Magic number for verifying the allocator state header.
    uint32_t magic_number;
    // index of the first free block in the free list
    std::atomic<std::size_t> free_list_index;
    // number of chunks in the chunk manager
    std::atomic<std::size_t> num_chunks;
  };

  AllocatorStateHeader* state() {
    return reinterpret_cast<AllocatorStateHeader*>(chunk_manager_.at(0, 0));
  }

  // Header for a free/allocated node in the allocator
  struct Node {
    // Version number for detecting state changes in the node.
    std::atomic<std::uint32_t> version;
    // The index of the chunk in the chunk manager. This is necessary to convert
    // the pointer to an index relative to the start of the chunk.
    std::size_t index;
    // Size of the block, including the header
    std::atomic<std::size_t> size;
    // index of the next free block in the free list
    std::atomic<std::size_t> next_index;
    std::size_t signature;

    bool is_allocated() const { return !is_free(); }

    bool is_free() const { return (version.load() & 0x1) == 0; }
  };

  // Given a pointer, returns the Node.
  Node* GetNode(uint8_t* ptr) const { return reinterpret_cast<Node*>(ptr) - 1; }

  // Returns the first free node in the free list, or nullptr if there are no
  // free nodes.
  Node* FirstFreeNode() { return ToPtr<Node>(state()->free_list_index.load()); }

  // Returns the next node in the free list, or nullptr if there are no more
  // nodes.
  Node* NextFreeNode(Node* node) {
    return ToPtr<Node>(node->next_index.load());
  }

  void InitializeAllocatorStateIfNecessary();

  static std::size_t CalculateBytesNeeded(std::size_t bytes) {
    // Calculate the number of bytes needed for the memory block
    return std::max<std::size_t>(sizeof(Node) + bytes, sizeof(Node));
  }

  uint8_t* NewAllocatedNode(uint8_t* buffer,
                            std::size_t index,
                            std::size_t size);

  std::uint64_t ToIndexImpl(Node* ptr, std::true_type) const;

  template <typename U>
  std::uint64_t ToIndexImpl(U* ptr, std::false_type) const;

  // Allocates space from a free node that can fit the requested size.
  //	Returns nullptr if no free node is found.
  uint8_t* AllocateFromFreeList(std::size_t bytes_needed);

  // Returns whether the highest bit in a 64-bit size_t is marked.
  static bool is_marked_reference(size_t value) {
    size_t mask = (size_t)1
                  << (sizeof(size_t) * 8 - 1);  // shift 1 to the leftmost bit
    return value & mask;  // bitwise AND, if the highest bit is set, the result
                          // is nonzero
  }

  // Clears the topmost bit in size_t
  static size_t get_unmarked_reference(size_t value) {
    return value & 0x7FFFFFFFFFFFFFFF;
  }

  // Sets the topmost bit in size_t
  static size_t get_marked_reference(size_t value) {
    size_t mask = static_cast<size_t>(1)
                  << (sizeof(size_t) * 8 - 1);  // shift 1 to the leftmost bit
    return value | mask;  // bitwise OR, will set the highest bit
  }

  // Given a size, returns the left node and right node, such that the
  // size of the left node < size, and the size of the right node >= size.
  Node* SearchBySize(std::size_t size, std::size_t index, Node** left_node);

  enum class OperationType { Allocate, Deallocate, Search };
  std::string OperationTypeToString(OperationType type) const {
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

  struct Operation {
    Operation(OperationType type, const Node& node)
        : thread_id(std::this_thread::get_id()),
          type(type),
          index(node.index),
          size(node.size.load()),
          version(node.version.load()),
          next_index(node.next_index.load()),
          marked(is_marked_reference(node.next_index)) {}
    std::thread::id thread_id;
    OperationType type;
    std::size_t index;
    std::size_t size;
    std::size_t version;
    std::size_t next_index;
    bool marked;
  };

  void PrintOperation(const Operation& operation) const {
    std::cout << "ThreadId(" << operation.thread_id
              << "): " << OperationTypeToString(operation.type) << "("
              << chunk_manager_.chunk_index(operation.index) << ", "
              << chunk_manager_.offset_in_chunk(operation.index) << "), Next("
              << chunk_manager_.chunk_index(operation.next_index) << ", "
              << chunk_manager_.offset_in_chunk(operation.next_index)
              << "), size=" << operation.size
              << ", version=" << operation.version
              << ", marked = " << std::boolalpha << operation.marked
              << std::endl;
  }

  // Prints the last 100 operations performed on the allocator.
  void PrintLastOperations() const {
    std::lock_guard<std::mutex> lock(log_mutex);
    std::cout << "Last 200 operations performed on the allocator:" << std::endl;
    std::size_t op_size = operations_.size();
    for (int i = std::max<uint8_t>(0, op_size - 200); i < op_size; ++i) {
      PrintOperation(operations_[i]);
    }
  }

  // Prints the history of an operation with a particular index.
  void PrintIndexHistory(std::size_t index) const {
    std::size_t input_chunk_index = chunk_manager_.chunk_index(index);
    std::size_t input_offset_in_chunk = chunk_manager_.offset_in_chunk(index);
    std::cout << "History of index " << input_chunk_index << ", "
              << input_offset_in_chunk << ":" << std::endl;
    std::size_t op_size = operations_.size();
    for (int i = std::max<int>(0, op_size - 2000); i < op_size; ++i) {
      const Operation& operation = operations_[i];
      std::size_t op_chunk_index = chunk_manager_.chunk_index(operation.index);
      std::size_t op_offset_in_chunk =
          chunk_manager_.offset_in_chunk(operation.index);
      if ((op_chunk_index == input_chunk_index) &&
          (op_offset_in_chunk == input_offset_in_chunk)) {
        PrintOperation(operation);
      }
    }
  }

 private:
  // Reference to the shared memory buffer used for allocation
  ChunkedVector<Operation> operations_;
  ChunkManager chunk_manager_;

  friend class AllocationLogger;
};

#endif  // SHM_ALLOCATOR_H_
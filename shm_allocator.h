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

// A simple allocator that allocates memory from a shared memory buffer. The
// allocator maintains a free list of available blocks of memory. When a block
// is allocated, it is removed from the free list. When a block is freed, it is
// added back to the free list. The allocator does not attempt to coalesce
// adjacent free blocks. The allocator is designed to be lock-free by using
// atomic operations to update the allocator state: size of a free block, or the
// head of the free list.
//
// This Allocator implements the Harris Lock-Free Linked List as the free list.
// The paper can be found here: https://timharris.uk/papers/2001-disc.pdf
// Note that this makes a few minor changes. Firstly, it's delete operation
// (allocation) differs from Harris' in that it allows for greater than or equal
// to the requested key. This implementation is not strictly linearizable but it
// does (as far as I can tell) ensure that allocations and deallocations
// complete correctly and the data structure remains in a consistent state.
// Furthermore, Harris assumes that each allocation is fresh and we don't reuse
// nodes. This is not a correct assumption when implementing an allocator as the
// same nodes can be freed and allocated at any time even in the middle of
// another operation. This implementation leaves an allocated node's next
// pointer intact while it is being deallocated allowing other threads/processes
// to follow the next pointer to the next free node.
//
// TODO(fsamuel): One thing we can do in the future is on deallocation, we can
// look at the node adjacent to the right, and if it's also free (by looking at
// the next pointer), we can remove it from the free list (by marking the next
// pointer), then changing the size of the currently allocated node, before
// deallocating it.
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
    return Deallocate(reinterpret_cast<uint8_t*>(ptr));
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
  class NodePtr;

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
    // Reference count for the node. This is used to determine when the node can
    // be coalesced.
    std::atomic<std::uint32_t> ref_count;
    // Version number for detecting state changes in the node.
    std::atomic<std::uint32_t> version;
    // The index of the chunk in the chunk manager. This is necessary to convert
    // the pointer to an index relative to the start of the chunk.
    std::size_t index;
    // Size of the block, including the header
    std::atomic<std::size_t> size;
    // index of the next free block in the free list
    std::atomic<std::size_t> next_index;

    bool is_allocated() const { return !is_free(); }

    bool is_free() const { return (version.load() & 0x1) == 0; }
  };

  // Given a pointer, returns the Node.
  Node* GetNode(uint8_t* ptr) const { return reinterpret_cast<Node*>(ptr) - 1; }

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
  std::uint64_t ToIndexImpl(U* ptr, std::false_type) const {
    Node* allocated_node = GetNode(ptr);
    return ToIndexImpl(allocated_node, std::true_type{}) + sizeof(Node);
  }

  // Allocates space from a free node that can fit the requested size.
  //	Returns nullptr if no free node is found.
  uint8_t* AllocateFromFreeList(std::size_t min_bytes_needed, std::size_t min_index);

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
  NodePtr SearchBySize(std::size_t size, std::size_t index, NodePtr* left_node);

 private:
    class NodePtr {
    public:
        explicit NodePtr(Node* ptr = nullptr) : ptr_(ptr) {
            if (ptr_) {
                ptr_->ref_count.fetch_add(1, std::memory_order_relaxed);
            }
        }

        NodePtr(const NodePtr& other) : ptr_(other.ptr_) {
            if (ptr_) {
                ptr_->ref_count.fetch_add(1, std::memory_order_relaxed);
            }
        }

        NodePtr& operator=(const NodePtr& other) {
            if (this != &other) {
                reset();
                ptr_ = other.ptr_;
                if (ptr_) {
                    ptr_->ref_count.fetch_add(1, std::memory_order_relaxed);
                }
            }
            return *this;
        }

        ~NodePtr() {
            reset();
        }

        void reset() {
            if (ptr_) {
                if (ptr_->ref_count.fetch_sub(1, std::memory_order_release) == 1) {
                    std::atomic_thread_fence(std::memory_order_acquire);
                }
                ptr_ = nullptr;
            }
        }

        Node* get() const {
            return ptr_;
        }

        Node& operator*() const {
            return *ptr_;
        }

        Node* operator->() const {
            return ptr_;
        }

        operator bool() const {
            return ptr_ != nullptr;
        }

        bool operator==(std::nullptr_t) const {
            return ptr_ == nullptr;
        }

        bool operator!=(std::nullptr_t) const {
            return ptr_ != nullptr;
        }

        bool operator==(const NodePtr& other) const {
            return ptr_ == other.ptr_;
        }

        bool operator!=(const NodePtr& other) const {
            return ptr_ != other.ptr_;
        }

    private:
        Node* ptr_;
    };

  // Reference to the shared memory buffer used for allocation
  ChunkManager chunk_manager_;

  friend class AllocationLogger;
};

#endif  // SHM_ALLOCATOR_H_
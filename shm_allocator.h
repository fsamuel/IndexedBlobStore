#ifndef SHM_ALLOCATOR_H_
#define SHM_ALLOCATOR_H_

#include <atomic>
#include <cassert>
#include <vector>

#include "chunk_manager.h"

// A simple allocator that allocates memory from a shared memory buffer. The
// allocator maintains a free list of available blocks of memory. When a block
// is allocated, it is removed from the free list. When a block is freed, it is
// added back to the free list. The allocator does not attempt to coalesce
// adjacent free blocks. The allocator is designed to be lock-free by using
// atomic operations to update the allocator state: size of a free block, or the
// head of the free list.
template <typename T>
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
  T* Allocate(std::size_t bytes_requested);

  // Deallocate memory at the given pointer index
  bool Deallocate(std::size_t index);

  // Deallocate memory at the given pointer.
  bool Deallocate(T* ptr);

  // Returns the size of the allocated block at the given index.
  std::size_t GetCapacity(std::size_t index) const;

  // Returns the size of the allocated block at the given pointer.
  std::size_t GetCapacity(T* ptr);

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
    uint32_t
        magic_number;  // Magic number for verifying the allocator state header
    std::atomic<std::size_t>
        free_list_index;  // index of the first free block in the free list
    std::atomic<std::size_t>
        num_chunks;  // number of chunks in the chunk manager
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

    bool is_free_seen(std::uint32_t* v) const {
      *v = version.load();
      return (*v & 0x1) == 0;
    }
  };

  // Given a pointer, returns the Node.
  Node* GetNode(T* ptr) const { return reinterpret_cast<Node*>(ptr) - 1; }

  // Returns the first free node in the free list, or nullptr if there are no
  // free nodes.
  Node* FirstFreeNode() { return ToPtr<Node>(state()->free_list_index.load()); }

  // Returns the next node in the free list, or nullptr if there are no more
  // nodes.
  Node* NextFreeNode(Node* node) {
    return ToPtr<Node>(node->next_index.load());
  }

  void InitializeAllocatorStateIfNecessary() {
    // Check if the allocator state header has already been initialized
    AllocatorStateHeader* state_header_ptr = state();
    if (state_header_ptr->magic_number != 0x12345678) {
      // Initialize the allocator state header
      state_header_ptr->magic_number = 0x12345678;
      state_header_ptr->free_list_index = InvalidIndex;
      state_header_ptr->num_chunks = 1;

      uint8_t* data = chunk_manager_.at(sizeof(AllocatorStateHeader));
      T* buffer = NewAllocatedNode(
          data, chunk_manager_.encode_index(0, sizeof(AllocatorStateHeader)),
          chunk_manager_.capacity() - sizeof(AllocatorStateHeader));
      // An initial allocation always starts at verison 1.
      GetNode(buffer)->version.store(1);
      Deallocate(buffer);
    }
  }

  static std::size_t CalculateBytesNeeded(std::size_t bytes) {
    // Calculate the number of objects needed based on the requested size and
    // the size of each object
    std::size_t n = bytes / sizeof(T);
    if (bytes % sizeof(T) != 0) {
      n++;
    }

    // Calculate the number of bytes needed for the memory block
    return std::max(sizeof(Node) + sizeof(T) * n, sizeof(Node));
  }

  T* NewAllocatedNode(uint8_t* buffer, std::size_t index, std::size_t size) {
    Node* allocated_node = reinterpret_cast<Node*>(buffer);
    allocated_node->size = size;
    allocated_node->index = index;
    allocated_node->next_index.store(InvalidIndex);
    allocated_node->version.fetch_add(1);
    allocated_node->signature = 0xbeefcafe;
    return reinterpret_cast<T*>(allocated_node + 1);
  }

  std::uint64_t ToIndexImpl(Node* ptr, std::true_type) const;

  template <typename U>
  std::uint64_t ToIndexImpl(U* ptr, std::false_type) const;

  // Allocates space from a free node that can fit the requested size.
  //	Returns nullptr if no free node is found.
  T* AllocateFromFreeList(std::size_t bytes_needed);

  // Returns whether the highest bit in a 64-bit size_t is marked.
  bool is_marked_reference(size_t value) {
    size_t mask = (size_t)1
                  << (sizeof(size_t) * 8 - 1);  // shift 1 to the leftmost bit
    return value & mask;  // bitwise AND, if the highest bit is set, the result
                          // is nonzero
  }

  // Clears the topmost bit in size_t
  size_t get_unmarked_reference(size_t value) {
    return value & 0x7FFFFFFFFFFFFFFF;
  }

  // Sets the topmost bit in size_t
  size_t get_marked_reference(size_t value) {
    size_t mask = (size_t)1
                  << (sizeof(size_t) * 8 - 1);  // shift 1 to the leftmost bit
    return value | mask;  // bitwise OR, will set the highest bit
  }

  // Given a size, returns the left node and right node, such that the
  // size of the left node < size, and the size of the right node >= size.
  Node* SearchBySize(std::size_t, Node** left_node);

 private:
  // Reference to the shared memory buffer used for allocation
  ChunkManager chunk_manager_;
};

template <typename T>
ShmAllocator<T>::ShmAllocator(ChunkManager&& chunk_manager)
    : chunk_manager_(std::move(chunk_manager)) {
  InitializeAllocatorStateIfNecessary();
}

template <typename T>
ShmAllocator<T>::ShmAllocator(ShmAllocator&& other)
    : chunk_manager_(std::move(other.chunk_manager_)) {}

template <typename T>
T* ShmAllocator<T>::Allocate(std::size_t bytes_requested) {
  // Calculate the number of bytes needed for the memory block
  std::size_t bytes_needed = CalculateBytesNeeded(bytes_requested);

  while (true) {
    T* data = AllocateFromFreeList(bytes_needed);

    // TODO(fsamuel): Decide if we want to split a node here.
    if (data != nullptr) {
      Node* allocated_node = GetNode(data);
      allocated_node->version.fetch_add(1);
      if (allocated_node->size > bytes_needed + sizeof(Node)) {
        std::size_t bytes_remaining = allocated_node->size - bytes_needed;
        T* buffer = NewAllocatedNode(
            reinterpret_cast<uint8_t*>(allocated_node) + bytes_needed,
            allocated_node->index + bytes_needed, bytes_remaining);
        GetNode(buffer)->version.store(1);
        Deallocate(buffer);
        allocated_node->size = bytes_needed;
      }

      return data;
    }
    // No block of sufficient size was found, resize the buffer and allocate a
    // new block.

    std::size_t last_num_chunks = state()->num_chunks.load();
    uint8_t* new_chunk_data;
    std::size_t new_chunk_size;
    if (chunk_manager_.get_or_create_chunk(last_num_chunks, &new_chunk_data,
                                           &new_chunk_size) > 0) {
      T* buffer = NewAllocatedNode(
          new_chunk_data, chunk_manager_.encode_index(last_num_chunks, 0),
          new_chunk_size);
      GetNode(buffer)->version.store(1);
      Deallocate(buffer);
      bool success = state()->num_chunks.compare_exchange_strong(
          last_num_chunks, last_num_chunks + 1);
      assert(success);
    }
  }
}

// Deallocate memory at the given pointer index
template <typename T>
bool ShmAllocator<T>::Deallocate(std::size_t index) {
  T* ptr = ToPtr<T>(index);
  return Deallocate(ptr);
}

template <typename T>
bool ShmAllocator<T>::Deallocate(T* ptr) {
  Node* node = GetNode(ptr);
  if (node == nullptr || !node->is_allocated()) {
    // The pointer is not a valid allocation.
    return false;
  }
  node->version.fetch_add(1);

  Node* left_node = nullptr;
  Node* right_node = nullptr;

  do {
    right_node = SearchBySize(node->size, &left_node);
    assert(node != right_node);
    /*
    if ((right_node != nullptr) && (right_node->index == node->index)) {
            // Double free?
            return false;
    }*/
    std::size_t right_node_index = ToIndex(right_node);
    node->next_index.store(right_node_index);
    if (left_node == nullptr) {
      if (state()->free_list_index.compare_exchange_weak(right_node_index,
                                                         node->index)) {
        return true;
      }
    } else {
      if (left_node->next_index.compare_exchange_weak(right_node_index,
                                                      node->index)) {
        return true;
      }
    }
  } while (true); /*B3*/
}

template <typename T>
std::size_t ShmAllocator<T>::GetCapacity(std::size_t index) const {
  if (index < 0) {
    return 0;
  }
  std::size_t node_header_index = index - sizeof(Node);
  const Node* current_node = ToPtr<Node>(node_header_index);
  return (current_node->size.load() - sizeof(Node)) / sizeof(T);
}

// Returns the size of the allocated block at the given pointer.
template <typename T>
std::size_t ShmAllocator<T>::GetCapacity(T* ptr) {
  if (ptr == nullptr) {
    return 0;
  }
  Node* curent_node = GetNode(ptr);
  return (curent_node->size - sizeof(Node)) / sizeof(T);
}

template <typename T>
std::uint64_t ShmAllocator<T>::ToIndexImpl(Node* ptr, std::true_type) const {
  if (ptr == nullptr) {
    return InvalidIndex;
  }
  return ptr->index;
}

// Implementation of ToIndex for other types
template <typename T>
template <typename U>
std::uint64_t ShmAllocator<T>::ToIndexImpl(U* ptr, std::false_type) const {
  Node* allocated_node = GetNode(ptr);
  return ToIndexImpl(allocated_node, std::true_type{}) + sizeof(Node);
}

template <typename T>
T* ShmAllocator<T>::AllocateFromFreeList(std::size_t bytes_needed) {
  Node* right_node = nullptr;
  std::size_t right_node_next_index = InvalidIndex;
  ;
  Node* left_node = nullptr;
  do {
    right_node = SearchBySize(bytes_needed, &left_node);
    if (right_node == nullptr) {
      return nullptr;
    }

    right_node_next_index = right_node->next_index.load();
    // We're grabbing the right node so mark it as removed from the free list.
    if (!is_marked_reference(right_node_next_index)) {
      if ((right_node->next_index.compare_exchange_weak(
              right_node_next_index,
              get_marked_reference(right_node_next_index)))) {
        break;
      }
    }
  } while (true); /*B4*/
  std::size_t right_node_index =
      right_node == nullptr ? InvalidIndex : right_node->index;
  if (left_node == nullptr) {
    if (!state()->free_list_index.compare_exchange_weak(
            right_node_index, right_node_next_index)) {
      SearchBySize(right_node->size, &left_node);
    }
  } else {
    if (!left_node->next_index.compare_exchange_weak(right_node_index,
                                                     right_node_next_index)) {
      SearchBySize(right_node->size, &left_node);
    }
  }
  return reinterpret_cast<T*>(right_node + 1);
}

template <typename T>
typename ShmAllocator<T>::Node* ShmAllocator<T>::SearchBySize(
    std::size_t size,
    Node** left_node) {
  std::size_t left_node_next_index = InvalidIndex;
  Node* right_node = nullptr;
search_again:
  do {
    Node* current_node = nullptr;
    std::size_t current_node_next_index = state()->free_list_index.load();

    /* 1: Find left_node and right_node */
    do {
      if (!is_marked_reference(current_node_next_index)) {
        *left_node = current_node;
        left_node_next_index = current_node_next_index;
      }
      current_node =
          ToPtr<Node>(get_unmarked_reference(current_node_next_index));
      if (current_node == nullptr) {
        break;
      }
      current_node_next_index = current_node->next_index.load();
    } while (is_marked_reference(current_node_next_index) ||
             (current_node->size < size));
    right_node = current_node;
    std::size_t right_node_index =
        right_node == nullptr ? InvalidIndex : right_node->index;
    /* 2: Check nodes are adjacent */
    if (left_node_next_index == right_node_index) {
      if ((right_node != nullptr) &&
          is_marked_reference(right_node->next_index.load())) {
        goto search_again; /*G1*/
      } else {
        return right_node; /*R1*/
      }
    }
    /* 3: Remove one or more marked nodes */
    // std::size_t right_node_index = right_node == nullptr ? InvalidIndex :
    // right_node->index;
    if ((*left_node) != nullptr) {
      if ((*left_node)
              ->next_index.compare_exchange_weak(left_node_next_index,
                                                 right_node_index)) {
        if (right_node != nullptr &&
            is_marked_reference(right_node->next_index.load())) {
          goto search_again;
        } else {
          return right_node;
        }
      }
    } else {
      if (state()->free_list_index.compare_exchange_weak(left_node_next_index,
                                                         right_node_index)) {
        if (right_node != nullptr &&
            is_marked_reference(right_node->next_index.load())) {
          goto search_again;
        } else {
          return right_node;
        }
      }
    }
  } while (true);
}

#endif  // SHM_ALLOCATOR_H_
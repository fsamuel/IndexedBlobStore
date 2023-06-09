#include "shm_allocator.h"

#include <iostream>
#include <set>

ShmAllocator::ShmAllocator(ChunkManager&& chunk_manager)
    : chunk_manager_(std::move(chunk_manager)) {
  InitializeAllocatorStateIfNecessary();
}

ShmAllocator::ShmAllocator(ShmAllocator&& other)
    : chunk_manager_(std::move(other.chunk_manager_)) {}

uint8_t* ShmAllocator::Allocate(std::size_t bytes_requested) {
  // Calculate the number of bytes needed for the memory block
  std::size_t bytes_needed = CalculateBytesNeeded(bytes_requested);

  while (true) {
    uint8_t* data = AllocateFromFreeList(bytes_needed);

    // TODO(fsamuel): Decide if we want to split a node here.
    if (data != nullptr) {
      Node* allocated_node = GetNode(data);
      allocated_node->version.fetch_add(1);

      // If we have enough space to split the node then split it.
      if (allocated_node->size > bytes_needed + sizeof(Node)) {
        std::size_t bytes_remaining = allocated_node->size - bytes_needed;
        uint8_t* buffer = NewAllocatedNode(
            reinterpret_cast<uint8_t*>(allocated_node) + bytes_needed,
            allocated_node->index + bytes_needed, bytes_remaining);

        Deallocate(buffer);
        allocated_node->size = bytes_needed;
      }
      return data;
    }
    // No block of sufficient size was found. We need to request a new chunk,
    // add it to the free list, and try again. Every new chunk is double the
    // size of the previous chunk. Regardless of the initial chunk size, we'll
    // keep allocating chunks until we can satisfy the request above.
    // TODO(fsamuel): A more resilient allocator would specify an upperbound to
    // allocation size and fail an allocation beyond that upper bound.
    std::size_t last_num_chunks = state()->num_chunks.load();
    uint8_t* new_chunk_data;
    std::size_t new_chunk_size;
    // Only one thread/process will end up creating a new chunk so only one new
    // free node will be added as a result of this code path. All other
    // threads/processes will not get a new chunk and will try to grab an
    // allocation from the free list again.
    if (chunk_manager_.get_or_create_chunk(last_num_chunks, &new_chunk_data,
                                           &new_chunk_size) > 0) {
      uint8_t* buffer = NewAllocatedNode(
          new_chunk_data, chunk_manager_.encode_index(last_num_chunks, 0),
          new_chunk_size);
      Deallocate(buffer);
      bool success = state()->num_chunks.compare_exchange_strong(
          last_num_chunks, last_num_chunks + 1);
      assert(success);
    }
  }
}

// Deallocate memory at the given pointer index
bool ShmAllocator::Deallocate(std::size_t index) {
  uint8_t* ptr = ToPtr<uint8_t>(index);
  return Deallocate(ptr);
}

bool ShmAllocator::Deallocate(uint8_t* ptr) {
  Node* node = GetNode(ptr);
  // The version counter on every node ensures we don't accidentally double
  // free.
  if (node == nullptr || !node->is_allocated()) {
    // The pointer is not a valid allocation.
    return false;
  }
  node->version.fetch_add(1);

  Node* left_node = nullptr;
  Node* right_node = nullptr;

  do {
    right_node = SearchBySize(node->size, node->index, &left_node);
    assert(node != right_node);

    std::size_t right_node_index = ToIndex(right_node);
    // node might still be in the free list and marked. Keep it marked just in
    // case.
    // TODO(fsamuel): Can we leak free nodes here?
    node->next_index.store(get_marked_reference(right_node_index));
    if (left_node == nullptr) {
      if (state()->free_list_index.compare_exchange_strong(right_node_index,
                                                           node->index)) {
        node->next_index.store(get_unmarked_reference(right_node_index));

        return true;
      }
    } else {
      if (left_node->next_index.compare_exchange_strong(right_node_index,
                                                        node->index)) {
        node->next_index.store(get_unmarked_reference(right_node_index));

        return true;
      }
    }
  } while (true); /*B3*/
}

std::size_t ShmAllocator::GetCapacity(std::size_t index) const {
  if (index < 0) {
    return 0;
  }
  std::size_t node_header_index = index - sizeof(Node);
  const Node* current_node = ToPtr<Node>(node_header_index);
  return current_node->size.load() - sizeof(Node);
}

// Returns the size of the allocated block at the given pointer.
std::size_t ShmAllocator::GetCapacity(uint8_t* ptr) {
  if (ptr == nullptr) {
    return 0;
  }
  Node* curent_node = GetNode(ptr);
  return curent_node->size - sizeof(Node);
}

void ShmAllocator::InitializeAllocatorStateIfNecessary() {
  // Check if the allocator state header has already been initialized
  AllocatorStateHeader* state_header_ptr = state();
  if (state_header_ptr->magic_number != 0x12345678) {
    // Initialize the allocator state header
    state_header_ptr->magic_number = 0x12345678;
    state_header_ptr->free_list_index = InvalidIndex;
    state_header_ptr->num_chunks = 1;

    uint8_t* data = chunk_manager_.at(sizeof(AllocatorStateHeader));
    uint8_t* buffer = NewAllocatedNode(
        data, chunk_manager_.encode_index(0, sizeof(AllocatorStateHeader)),
        chunk_manager_.capacity() - sizeof(AllocatorStateHeader));
    Deallocate(buffer);
  }
}

uint8_t* ShmAllocator::NewAllocatedNode(uint8_t* buffer,
                                        std::size_t index,
                                        std::size_t size) {
  Node* allocated_node = reinterpret_cast<Node*>(buffer);
  allocated_node->size = size;
  allocated_node->index = index;
  allocated_node->next_index.store(InvalidIndex);
  allocated_node->version.store(1);
  return reinterpret_cast<uint8_t*>(allocated_node + 1);
}

std::uint64_t ShmAllocator::ToIndexImpl(Node* ptr, std::true_type) const {
  if (ptr == nullptr) {
    return InvalidIndex;
  }
  return ptr->index;
}

uint8_t* ShmAllocator::AllocateFromFreeList(std::size_t bytes_needed) {
  Node* right_node = nullptr;
  std::size_t right_node_next_index = InvalidIndex;
  ;
  Node* left_node = nullptr;
  do {
    right_node = SearchBySize(bytes_needed, 0, &left_node);
    if (right_node == nullptr) {
      return nullptr;
    }

    right_node_next_index = right_node->next_index.load();
    // We're grabbing the right node so mark it as removed from the free list.
    if (!is_marked_reference(right_node_next_index)) {
      if ((right_node->next_index.compare_exchange_strong(
              right_node_next_index,
              get_marked_reference(right_node_next_index)))) {
        break;
      }
    }
  } while (true);
  std::size_t right_node_index =
      right_node == nullptr ? InvalidIndex : right_node->index;
  if (left_node == nullptr) {
    if (!state()->free_list_index.compare_exchange_strong(
            right_node_index, right_node_next_index)) {
      SearchBySize(right_node->size, right_node->index, &left_node);
    }
  } else {
    if (!left_node->next_index.compare_exchange_strong(right_node_index,
                                                       right_node_next_index)) {
      SearchBySize(right_node->size, right_node->index, &left_node);
    }
  }
  return reinterpret_cast<uint8_t*>(right_node + 1);
}

typename ShmAllocator::Node* ShmAllocator::SearchBySize(std::size_t size,
                                                        std::size_t index,
                                                        Node** left_node) {
  std::size_t left_node_next_index = InvalidIndex;
  Node* right_node = nullptr;
search_again:
  do {
    Node* current_node = nullptr;
    std::size_t current_node_next_index = state()->free_list_index.load();
    /* 1: Find left_node and right_node */
    while (true) {
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
      if (!is_marked_reference(current_node_next_index) &&
          (current_node->size >= size && current_node->index >= index)) {
        break;
      }
    }
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
    if ((*left_node) != nullptr) {
      if ((*left_node)
              ->next_index.compare_exchange_strong(left_node_next_index,
                                                   right_node_index)) {
        if (right_node != nullptr &&
            is_marked_reference(right_node->next_index.load())) {
          goto search_again;
        } else {
          return right_node;
        }
      }
    } else {
      if (state()->free_list_index.compare_exchange_strong(left_node_next_index,
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

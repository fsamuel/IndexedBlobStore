#include "shm_allocator.h"

#include <iostream>
#include <set>

ShmAllocator::ShmAllocator(ChunkManager&& chunk_manager)
    : chunk_manager_(std::move(chunk_manager)),
      operations_("operations", 1024) {
  InitializeAllocatorStateIfNecessary();
}

ShmAllocator::ShmAllocator(ShmAllocator&& other)
    : chunk_manager_(std::move(other.chunk_manager_)),
      operations_(std::move(other.operations_)) {}

uint8_t* ShmAllocator::Allocate(std::size_t bytes_requested) {
  // Calculate the number of bytes needed for the memory block
  std::size_t bytes_needed = CalculateBytesNeeded(bytes_requested);

  while (true) {
    uint8_t* data = AllocateFromFreeList(bytes_needed);

    // TODO(fsamuel): Decide if we want to split a node here.
    if (data != nullptr) {
      Node* allocated_node = GetNode(data);
      allocated_node->version.fetch_add(1);
      // allocated_node->next_index.store(InvalidIndex);

      if (allocated_node->size > bytes_needed + sizeof(Node)) {
        std::size_t bytes_remaining = allocated_node->size - bytes_needed;
        uint8_t* buffer = NewAllocatedNode(
            reinterpret_cast<uint8_t*>(allocated_node) + bytes_needed,
            allocated_node->index + bytes_needed, bytes_remaining);
        Deallocate(buffer);
        Node* left_node = nullptr;
        SearchBySize(allocated_node->size, allocated_node->index, &left_node);
        allocated_node->size = bytes_needed;
      }
      operations_.emplace_back(OperationType::Allocate, *allocated_node);
      return data;
    }
    // No block of sufficient size was found. We need to request a new chunk,
    // add it to the free list, and try again.
    std::size_t last_num_chunks = state()->num_chunks.load();
    uint8_t* new_chunk_data;
    std::size_t new_chunk_size;
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
  if (node == nullptr || !node->is_allocated()) {
    // The pointer is not a valid allocation.
    return false;
  }
  node->version.fetch_add(1);

  Node* left_node = nullptr;
  Node* right_node = nullptr;

  do {
    right_node = SearchBySize(node->size, node->index, &left_node);
    // assert(node != right_node);
    if ((right_node != nullptr) && (right_node->index == node->index)) {
      operations_.emplace_back(OperationType::Deallocate, *node);
      // Acquire a lock
      std::lock_guard<std::mutex> lock(log_mutex);
      PrintIndexHistory(node->index);

      node->next_index.store(get_unmarked_reference(node->next_index.load()));
      // Double free?
      return true;
    }
    std::size_t right_node_index = ToIndex(right_node);
    node->next_index.store(right_node_index);
    if (left_node == nullptr) {
      if (state()->free_list_index.compare_exchange_strong(right_node_index,
                                                           node->index)) {
        operations_.emplace_back(OperationType::Deallocate, *node);
        return true;
      }
    } else {
      if (left_node->next_index.compare_exchange_strong(right_node_index,
                                                        node->index)) {
        operations_.emplace_back(OperationType::Deallocate, *node);

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
  allocated_node->signature = 0xbeefcafe;
  return reinterpret_cast<uint8_t*>(allocated_node + 1);
}

std::uint64_t ShmAllocator::ToIndexImpl(Node* ptr, std::true_type) const {
  if (ptr == nullptr) {
    return InvalidIndex;
  }
  return ptr->index;
}

// Implementation of ToIndex for other types
template <typename U>
std::uint64_t ShmAllocator::ToIndexImpl(U* ptr, std::false_type) const {
  Node* allocated_node = GetNode(ptr);
  return ToIndexImpl(allocated_node, std::true_type{}) + sizeof(Node);
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
        /*
                {
                    std::lock_guard<std::mutex> lock(log_mutex);
                    std::cout << "Allocated node(" <<
           chunk_manager_.chunk_index(right_node->index) << "," <<
           chunk_manager_.offset_in_chunk(right_node->index) << ") with size "
           << right_node->size << std::endl;
                }
                */
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
    std::set<std::size_t> skipped_nodes;
    std::vector<Operation> operations;
    bool printed = false;
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
      if (skipped_nodes.insert(current_node_next_index).second) {
        operations.emplace_back(OperationType::Search, *current_node);
      } else if (!printed) {
        operations.emplace_back(OperationType::Search, *current_node);
        // PrintLastOperations();
        /*
        std::lock_guard<std::mutex> lock(log_mutex);
        for (const auto& operation : operations) {
           PrintIndexHistory(operation.index);
           PrintOperation(operation);
        }*/
        printed = true;
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
    // std::size_t right_node_index = right_node == nullptr ? InvalidIndex :
    // right_node->index;
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

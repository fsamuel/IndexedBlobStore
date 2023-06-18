#include "shm_allocator.h"

#include <iostream>
#include <set>

#include "allocation_logger.h"
#include "shm_node.h"

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
    uint8_t* data = AllocateFromFreeList(bytes_needed, 0, false);

    if (data != nullptr) {
      ShmNodePtr allocated_node = GetNode(data);
      allocated_node->version.fetch_add(1);

      // If we have enough space to split the node then split it.
      bool should_split_node =
          (allocated_node->size > bytes_needed + sizeof(ShmNode));
      if (should_split_node) {
        std::size_t bytes_remaining = allocated_node->size - bytes_needed;
        ShmNodePtr node = NewAllocatedNode(
            reinterpret_cast<uint8_t*>(allocated_node.get()) + bytes_needed,
            allocated_node->index + bytes_needed, bytes_remaining);

        DeallocateNode(std::move(node));
        allocated_node->size = bytes_needed;
      }
      AllocationLogger::Get()->RecordAllocation(*allocated_node);
      return data;
    }
    // No block of sufficient size was found. We need to request a new chunk,
    // add it to the free list, and try again. Every new chunk is double the
    // size of the previous chunk. Regardless of the initial chunk size, we'll
    // keep allocating chunks until we can satisfy the request above.
    // TODO(fsamuel): A more resilient allocator would specify an upperbound to
    // allocation size and fail an allocation beyond that upper bound.
    RequestNewFreeNodeFromChunkManager();
  }
}

std::size_t ShmAllocator::GetCapacity(std::size_t index) const {
  if (index < 0) {
    return 0;
  }
  std::size_t node_header_index = index - sizeof(ShmNode);
  const ShmNode* current_node = ToPtr<ShmNode>(node_header_index);
  return current_node->size.load() - sizeof(ShmNode);
}

// Returns the size of the allocated block at the given pointer.
std::size_t ShmAllocator::GetCapacity(uint8_t* ptr) {
  if (ptr == nullptr) {
    return 0;
  }
  ShmNodePtr current_node = GetNode(ptr);
  return current_node->size - sizeof(ShmNode);
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
    ShmNodePtr node = NewAllocatedNode(
        data, chunk_manager_.encode_index(0, sizeof(AllocatorStateHeader)),
        chunk_manager_.capacity() - sizeof(AllocatorStateHeader));
    DeallocateNode(std::move(node));
  }
}

ShmNodePtr ShmAllocator::NewAllocatedNode(uint8_t* buffer,
                                          std::size_t index,
                                          std::size_t size) {
  ShmNode* allocated_node = reinterpret_cast<ShmNode*>(buffer);
  allocated_node->size = size;
  allocated_node->index = index;
  allocated_node->next_index.store(InvalidIndex);
  allocated_node->version.store(1);
  allocated_node->ref_count.store(0);
  ShmNodePtr node(allocated_node);
  return node;
}

bool ShmAllocator::DeallocateNode(ShmNodePtr node) {
  // The version counter on every node ensures we don't accidentally double
  // free.
  if (node == nullptr || !node->is_allocated()) {
    // The pointer is not a valid allocation.
    return false;
  }

  // TODO(fsamuel): This is currently broken.
  // node = CoalesceWithRightNodeIfPossible(std::move(node));

  node->version.fetch_add(1);

  AllocationLogger::Get()->RecordDeallocation(*node);

  ShmNodePtr left_node;
  ShmNodePtr right_node;

  do {
    right_node = SearchBySize(node->size, node->index, &left_node);
    assert(node != right_node);

    std::size_t right_node_index = ToIndex(right_node.get());
    // node might still be in the free list and marked. Keep it marked just in
    // case.
    // TODO(fsamuel): Can we leak free nodes here? Is it possible that
    // SearchBySize would swing a pointer earlier in the free list after node,
    // removing the node from the free list? The only way to do that is if
    // left_node's next index has changed (since we known it's unmarked).
    std::size_t right_node_index_marked =
        get_marked_reference(right_node_index);
    node->next_index.store(right_node_index_marked);
    if (left_node == nullptr) {
      if (state()->free_list_index.compare_exchange_strong(right_node_index,
                                                           node->index)) {
        node->next_index.compare_exchange_strong(
            right_node_index_marked, get_unmarked_reference(right_node_index));
        return true;
      }
    } else {
      if (left_node->next_index.compare_exchange_strong(right_node_index,
                                                        node->index)) {
        node->next_index.compare_exchange_strong(
            right_node_index_marked, get_unmarked_reference(right_node_index));
        return true;
      }
    }
  } while (true);  // B3
}

std::uint64_t ShmAllocator::ToIndexImpl(ShmNode* ptr, std::true_type) const {
  if (ptr == nullptr) {
    return InvalidIndex;
  }
  return ptr->index;
}

uint8_t* ShmAllocator::AllocateFromFreeList(std::size_t min_bytes_needed,
                                            std::size_t min_index,
                                            bool exact_match) {
  ShmNodePtr right_node;
  std::size_t right_node_next_index = InvalidIndex;

  ShmNodePtr left_node;
  do {
    right_node = SearchBySize(min_bytes_needed, min_index, &left_node);
    if (right_node == nullptr ||
        (exact_match && (right_node->size != min_bytes_needed ||
                         right_node->index != min_index))) {
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
      ShmNodePtr new_left_node;
      SearchBySize(right_node->size, right_node->index, &new_left_node);
    }
  } else {
    if (!left_node->next_index.compare_exchange_strong(right_node_index,
                                                       right_node_next_index)) {
      ShmNodePtr new_left_node;
      SearchBySize(right_node->size, right_node->index, &new_left_node);
    }
  }
  return reinterpret_cast<uint8_t*>(right_node.get() + 1);
}

void ShmAllocator::RequestNewFreeNodeFromChunkManager() {
  std::size_t last_num_chunks = state()->num_chunks.load();
  uint8_t* new_chunk_data;
  std::size_t new_chunk_size;
  // Only one thread/process will end up creating a new chunk so only one new
  // free node will be added as a result of this code path. All other
  // threads/processes will not get a new chunk and will try to grab an
  // allocation from the free list again.
  if (chunk_manager_.get_or_create_chunk(last_num_chunks, &new_chunk_data,
                                         &new_chunk_size) == 0) {
    return;
  }
  ShmNodePtr node = NewAllocatedNode(
      new_chunk_data, chunk_manager_.encode_index(last_num_chunks, 0),
      new_chunk_size);
  DeallocateNode(std::move(node));
  bool success = state()->num_chunks.compare_exchange_strong(
      last_num_chunks, last_num_chunks + 1);
  assert(success);
}

ShmNodePtr ShmAllocator::SearchBySize(std::size_t size,
                                      std::size_t index,
                                      ShmNodePtr* left_node) {
  std::size_t left_node_next_index = InvalidIndex;
  ShmNodePtr right_node;
search_again:
  do {
    ShmNodePtr current_node;
    std::size_t current_node_next_index = state()->free_list_index.load();
    // 1: Find left_node and right_node
    while (true) {
      if (!is_marked_reference(current_node_next_index)) {
        *left_node = std::move(current_node);
        left_node_next_index = current_node_next_index;
      }
      current_node = ShmNodePtr(
          ToPtr<ShmNode>(get_unmarked_reference(current_node_next_index)));
      if (current_node == nullptr) {
        break;
      }
      current_node_next_index = current_node->next_index.load();
      if (!is_marked_reference(current_node_next_index) &&
          (current_node->size >= size && current_node->index >= index)) {
        break;
      }
    }
    right_node = std::move(current_node);
    std::size_t right_node_index =
        right_node == nullptr ? InvalidIndex : right_node->index;
    // 2: Check nodes are adjacent
    if (left_node_next_index == right_node_index) {
      if ((right_node != nullptr) &&
          is_marked_reference(right_node->next_index.load())) {
        goto search_again;  // G1
      } else {
        return right_node;  // R1
      }
    }
    // 3: Remove one or more marked nodes
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

bool ShmAllocator::IsNodeReachable(std::size_t index) {
  std::size_t first_free_node_index = state()->free_list_index.load();
  ShmNodePtr current_node =
      first_free_node_index == InvalidIndex
          ? ShmNodePtr(nullptr)
          : ShmNodePtr(
                ToPtr<ShmNode>(get_unmarked_reference(first_free_node_index)));

  while (current_node != nullptr) {
    if (current_node->index == index) {
      return true;
    }
    std::size_t current_node_next_index = current_node->next_index.load();
    current_node = ShmNodePtr(
        ToPtr<ShmNode>(get_unmarked_reference(current_node_next_index)));
  }

  return false;
}

ShmNodePtr ShmAllocator::CoalesceWithRightNodeIfPossible(ShmNodePtr node) {
  // node's refcount > 1 if it's not removed from the free list but just marked
  // for removal. We cannot resize it in this case.
  while (true) {
    bool reachable = IsNodeReachable(node->index);
    if (!reachable && node->ref_count.load() == 1) {
      break;
    }
  }

  std::size_t encoded_node_index = node->index;
  std::size_t chunk_index = ChunkManager::chunk_index(encoded_node_index);
  std::size_t chunk_size = chunk_manager_.chunk_size_at_index(chunk_index);
  std::size_t offset = ChunkManager::offset_in_chunk(encoded_node_index);
  std::size_t node_size = node->size;
  // Check to see if there's a node to the right of this one.
  if (offset + node_size >= chunk_size) {
    return node;
  }
  // See if the adjacent node is on the free list. If it is allocate it.
  ShmNodePtr adjacent_node = GetNode(reinterpret_cast<uint8_t*>(node.get()) +
                                     node_size + sizeof(ShmNode));
  if (adjacent_node == nullptr || !adjacent_node->is_free()) {
    return node;
  }

  // Allocate the adjacent node to make sure no other thread will try to
  // allocate it.
  uint8_t* data =
      AllocateFromFreeList(adjacent_node->size, adjacent_node->index, true);
  if (data == nullptr) {
    return node;
  }
  while (true) {
    bool reachable = IsNodeReachable(adjacent_node->index);
    if (!reachable && adjacent_node->ref_count.load() == 1) {
      break;
    }
  }

  node->size.fetch_add(adjacent_node->size);
  return node;
}
#include "blob_store.h"

BlobStore::BlobStore(SharedMemoryBuffer&& metadataBuffer,
                     ChunkManager&& dataBuffer)
    : allocator_(std::move(dataBuffer)), metadata_(std::move(metadataBuffer)) {
  if (metadata_.empty()) {
    metadata_.emplace_back();
  }
}

BlobStore::~BlobStore() {}

void BlobStore::Drop(size_t index) {
  if (index == BlobStore::InvalidIndex) {
    return;
  }
  BlobMetadata* metadata = metadata_.at(index);
  // Set a tombstone on the blob to ensure that no new locks are acquired on
  // it. We cannot drop a blob that is on the free list.
  if (metadata == nullptr || !metadata->SetTombstone()) {
    return;
  }

  // Check if a lock is held on the blob, and if so, return. It will be
  // dropped when the lock is released.
  if (metadata->lock_state.load() != 0) {
    return;
  }
  size_t allocated_offset = metadata->offset;

  BlobMetadata& free_list_head = metadata_[0];
  while (true) {
    ssize_t first_free_index = free_list_head.next_free_index.load();
    ssize_t tombstone = 0;
    if (!metadata->next_free_index.compare_exchange_weak(tombstone,
                                                         first_free_index)) {
      continue;
    }

    // If the head of the free list has changed, undo the change we made if
    // possible and try again.
    if (!free_list_head.next_free_index.compare_exchange_weak(first_free_index,
                                                              index)) {
      metadata->next_free_index.store(tombstone);
      continue;
    }

    allocator_.Deallocate(allocator_.ToPtr<char>(allocated_offset));
    return;
  }
}

size_t BlobStore::FindFreeSlot() {
  while (true) {
    BlobMetadata& free_list_head = metadata_[0];
    ssize_t free_index = free_list_head.next_free_index.load();
    if (free_index == 0) {
      return metadata_.emplace_back();
    }
    ssize_t next_free_index = metadata_[free_index].next_free_index.load();
    if (free_list_head.next_free_index.compare_exchange_weak(free_index,
                                                             next_free_index)) {
      // Make sure the tombstone bit is not set for the recycled metadata.
      BlobMetadata& metadata = metadata_[free_index];
      metadata.next_free_index.store(-1);
      return free_index;
    }
  }
}

// Returns the number of free slots in the metadata vector
size_t BlobStore::GetFreeSlotCount() const {
  size_t count = 0;
  for (size_t i = 1; i < metadata_.size(); i++) {
    const BlobMetadata& metadata = metadata_[i];
    if (metadata.is_deleted()) {
      count++;
    }
  }
  return count;
}

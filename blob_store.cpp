#include "blob_store.h"

constexpr std::int32_t WRITE_LOCK_FLAG = 0x80000000;

namespace {
void SpinWait() {
#if defined(_WIN32)
  Sleep(0);
#else
  std::this_thread::yield();
#endif
}
}  // namespace

BlobStore::BlobStore(BufferFactory* buffer_factory,
                     const std::string& name_prefix,
                     std::size_t requested_chunk_size,
                     ChunkManager&& dataBuffer)
    : allocator_(std::move(dataBuffer)),
      metadata_(buffer_factory, name_prefix, requested_chunk_size) {
  if (metadata_.empty()) {
    metadata_.emplace_back();
  }
}

BlobStore::~BlobStore() {}

uint8_t* BlobStore::GetRaw(size_t index, size_t* offset) {
  if (index == BlobStore::InvalidIndex) {
    return nullptr;
  }
  BlobMetadata* metadata = metadata_.at(index);
  if (metadata == nullptr || metadata->is_deleted() || metadata->size == 0) {
    return nullptr;
  }

  std::size_t offset_value = metadata->offset;
  if (offset != nullptr) {
    *offset = offset_value;
  }
  return allocator_.ToPtr<uint8_t>(offset_value);
}

bool BlobStore::CompareAndSwap(std::size_t index,
                               std::size_t expected_offset,
                               std::size_t new_offset) {
  if (index == BlobStore::InvalidIndex) {
    return false;
  }

  BlobMetadata* metadata = metadata_.at(index);
  if (metadata == nullptr || metadata->is_deleted()) {
    return false;
  }

  return metadata->offset.compare_exchange_weak(expected_offset, new_offset);
}

std::size_t BlobStore::Clone(std::size_t index) {
  // This is only safe if the calling object is holding a read or write lock.
  BlobMetadata& metadata = metadata_[index];
  size_t clone_index = FindFreeSlot();
  uint8_t* ptr = allocator_.Allocate(metadata.size);
  size_t offset;
  const uint8_t* obj = GetRaw(index, &offset);
  // Blobs are trivially copyable and standard layout so memcpy should be
  // safe.
  memcpy(ptr, obj, metadata.size);
  BlobMetadata& clone_metadata = metadata_[clone_index];
  clone_metadata.size = metadata.size;
  clone_metadata.offset = allocator_.ToIndex(ptr);
  assert(clone_metadata.offset != ShmAllocator::InvalidIndex);

  clone_metadata.lock_state = 0;
  clone_metadata.next_free_index = -1;
  return clone_index;
}

// Gets the size of the blob stored at the speific index.
std::size_t BlobStore::GetSize(std::size_t index) {
  if (index == BlobStore::InvalidIndex) {
    return 0;
  }

  BlobMetadata* metadata = metadata_.at(index);
  if (metadata == nullptr || metadata->is_deleted()) {
    return 0;
  }

  return metadata->size;
}

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

bool BlobStore::AcquireReadLock(std::size_t index) {
  if (index == BlobStore::InvalidIndex) {
    return false;
  }
  while (true) {
    BlobMetadata* metadata = metadata_.at(index);
    // It's possible that the blob was deleted while we were waiting for the
    // lock.
    if (metadata == nullptr || metadata->is_deleted()) {
      return false;
    }
    int state = metadata->lock_state.load(std::memory_order_acquire);
    if (state >= 0) {
      if (metadata->lock_state.compare_exchange_weak(
              state, state + 1, std::memory_order_acquire)) {
        break;
      }
    }
    SpinWait();
  }

  return true;
}

bool BlobStore::AcquireWriteLock(std::size_t index) {
  if (index == BlobStore::InvalidIndex) {
    return false;
  }
  while (true) {
    BlobMetadata* metadata = metadata_.at(index);
    // It's possible that the blob was deleted while we were waiting for the
    // lock.
    if (metadata == nullptr || metadata->is_deleted()) {
      return false;
    }
    std::int32_t expected = 0;
    if (metadata->lock_state.compare_exchange_weak(expected, WRITE_LOCK_FLAG,
                                                   std::memory_order_acquire)) {
      break;
    }
    SpinWait();
  }
  return true;
}

void BlobStore::Unlock(std::size_t index) {
  if (index == BlobStore::InvalidIndex) {
    return;
  }
  BlobMetadata* metadata = metadata_.at(index);
  if (metadata == nullptr) {
    return;
  }

  std::int32_t expected;

  while (true) {
    expected = metadata->lock_state.load();
    std::int32_t new_state =
        std::max<int32_t>((expected & ~WRITE_LOCK_FLAG) - 1, 0);

    if (metadata->lock_state.compare_exchange_weak(expected, new_state)) {
      break;
    }
    SpinWait();
  }
  // Check if the blob was tombstoned and is now ready to be reused.
  if (metadata->is_tombstone() && metadata->lock_state.load() == 0) {
    Drop(index);
  }
}

void BlobStore::DowngradeWriteLock(std::size_t index) {
  if (index == BlobStore::InvalidIndex) {
    return;
  }
  BlobMetadata* metadata = metadata_.at(index);
  if (metadata == nullptr || metadata->is_deleted()) {
    return;
  }

  if (metadata->lock_state > 0) {
    return;
  }

  while (true) {
    std::int32_t expected = metadata->lock_state.load() & WRITE_LOCK_FLAG;
    if (metadata->lock_state.compare_exchange_weak(expected, 1)) {
      break;
    }
    SpinWait();
  }
}

void BlobStore::UpgradeReadLock(std::size_t index) {
  if (index == BlobStore::InvalidIndex) {
    return;
  }
  BlobMetadata* metadata = metadata_.at(index);
  if (metadata == nullptr || metadata->is_deleted()) {
    return;
  }
  // We're already holding a write lock.
  if (metadata->lock_state == WRITE_LOCK_FLAG) {
    return;
  }
  while (true) {
    std::int32_t expected = 1;
    if (metadata->lock_state.compare_exchange_weak(expected, WRITE_LOCK_FLAG)) {
      break;
    }
    SpinWait();
  }
}
#ifndef BLOB_STORE_H_
#define BLOB_STORE_H_

#include <sys/types.h>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>
#include <type_traits>

#include "blob_store_base.h"
#include "blob_store_object.h"
#include "blob_metadata.h"
#include "chunked_vector.h"
#include "shm_allocator.h"
#include "storage_traits.h"
#include "string_slice.h"
#include "utils.h"

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

class BlobStore;

// BlobStore is a class that manages the storage and retrieval of objects
// (blobs) in shared memory. It supports storing, getting, and deleting
// objects while maintaining a compact memory footprint.
class BlobStore : public BlobStoreBase {
 public:
  using Allocator = ShmAllocator;

  static constexpr std::size_t InvalidIndex =
      std::numeric_limits<std::size_t>::max();

  // Constructor that initializes the BlobStore with the provided metadata and
  // data shared memory buffers.
  BlobStore(BufferFactory* buffer_factory,
            const std::string& name_prefix,
            std::size_t requested_chunk_size,
            ChunkManager&& dataBuffer);

  // BlobStore destructor
  ~BlobStore();

  // Creates a new object of type T with the provided arguments into the
  // BlobStore and returns a BlobStoreObject.
  template <typename T, typename... Args>
  typename std::enable_if<
      std::is_standard_layout<typename StorageTraits<T>::StorageType>::value &&
          std::is_trivially_copyable<
              typename StorageTraits<T>::StorageType>::value,
      BlobStoreObject<T>>::type
  New(Args&&... args);

  template <typename T>
  typename std::enable_if<
      std::is_standard_layout<typename StorageTraits<T>::StorageType>::value &&
          std::is_trivially_copyable<
              typename StorageTraits<T>::StorageType>::value,
      BlobStoreObject<T>>::type
  New(std::initializer_list<typename StorageTraits<T>::ElementType> initList);

  template <typename T, typename... Args>
  typename std::enable_if<
      std::is_standard_layout<typename StorageTraits<T>::StorageType>::value &&
          std::is_trivially_copyable<
              typename StorageTraits<T>::StorageType>::value,
      BlobStoreObject<T>>::type
  NewImpl(Args&&... args);

  template <typename T>
  typename std::enable_if<
      std::is_standard_layout<typename StorageTraits<T>::StorageType>::value &&
          std::is_trivially_copyable<
              typename StorageTraits<T>::StorageType>::value,
      BlobStoreObject<T>>::type
  NewImpl(
      std::initializer_list<typename StorageTraits<T>::ElementType> initList);

  // Gets the object of type T at the specified index.
  template <typename T>
  BlobStoreObject<T> GetMutable(size_t index) {
    return BlobStoreObject<T>(this, index);
  }

  // Gets the object of type T at the specified index as a constant.
  template <typename T>
  BlobStoreObject<const T> Get(size_t index) const {
    return const_cast<BlobStore*>(this)->GetMutable<const T>(index);
  }

  // Drops the object at the specified index, freeing the associated memory.
  void Drop(size_t index);

  template <typename U>
  void Drop(BlobStoreObject<U>&& object) {
    size_t index = object.Index();
    // This ensures that we're no longer holding a lock on this object.
    object = nullptr;
    Drop(index);
  }

  // Returns the number of stored objects in the BlobStore.
  size_t GetSize() const {
    // The first slot in the metadata vector is always reserved for the free
    // list.
    size_t size = metadata_.size() - 1;
    size -= GetFreeSlotCount();
    return size;
  }

  // Returns whether the BlobStore is empty.
  bool IsEmpty() const { return GetSize() == 0; }

  // Iterator class for BlobStore
  class Iterator {
   public:
    Iterator(BlobStore* store, size_t index) : store_(store), index_(index) {
      AdvanceToValidIndex();
    }

    size_t size() const { return store_->metadata_[index_].size; }

    size_t index() const { return index_; }

    Iterator& operator++() {
      ++index_;
      AdvanceToValidIndex();
      return *this;
    }

    Iterator operator++(int) {
      Iterator temp = *this;
      ++(*this);
      return temp;
    }

    Iterator& operator--() {
      do {
        --index_;
      } while (store_->metadata_[index_].next_free_index != -1);
      return *this;
    }

    Iterator operator--(int) {
      Iterator temp = *this;
      --(*this);
      return temp;
    }

    bool operator==(const Iterator& other) const {
      return store_ == other.store_ && index_ == other.index_;
    }

    bool operator!=(const Iterator& other) const { return !(*this == other); }

    template <typename T>
    BlobStoreObject<const T> Get() const {
      return store_->Get<const T>(index_);
    }

    template <typename T>
    BlobStoreObject<T> GetMutable() {
      return store_->GetMutable<T>(index_);
    }

   private:
    void AdvanceToValidIndex() {
      while (true) {
        BlobMetadata* metadata = store_->metadata_.at(index_);
        if (metadata == nullptr) {
          break;
        }

        if (!metadata->is_deleted()) {
          break;
        }
        ++index_;
      }
    }

    BlobStore* store_;
    size_t index_;
  };

  Iterator begin() { return Iterator(this, 1); }

  Iterator end() { return Iterator(this, metadata_.size()); }

 private:
  using MetadataVector = ChunkedVector<BlobMetadata>;

  // Returns the index of the first free slot in the metadata vector.
  size_t FindFreeSlot();

  // Returns the number of free slots in the metadata vector.
  size_t GetFreeSlotCount() const;



  // BlobStoreBase implementation:
  uint8_t* GetRaw(size_t index, size_t* offset) override;
  std::size_t Clone(std::size_t index) override;
  bool CompareAndSwap(std::size_t index,
      std::size_t expected_offset,
      std::size_t new_offset) override;
  std::size_t GetSize(std::size_t index) override;
  bool AcquireReadLock(std::size_t index) override;
  bool AcquireWriteLock(std::size_t index) override;
  void Unlock(std::size_t index) override;
  void DowngradeWriteLock(std::size_t index) override;
  void UpgradeReadLock(std::size_t index) override;

  Allocator allocator_;
  MetadataVector metadata_;

  template <typename T>
  friend class BlobStoreObject;
};

template <typename T, typename... Args>
typename std::enable_if<
    std::is_standard_layout<typename StorageTraits<T>::StorageType>::value &&
        std::is_trivially_copyable<
            typename StorageTraits<T>::StorageType>::value,
    BlobStoreObject<T>>::type
BlobStore::New(Args&&... args) {
  return NewImpl<T>(std::forward<Args>(args)...);
}

template <typename T>
typename std::enable_if<
    std::is_standard_layout<typename StorageTraits<T>::StorageType>::value &&
        std::is_trivially_copyable<
            typename StorageTraits<T>::StorageType>::value,
    BlobStoreObject<T>>::type
BlobStore::New(
    std::initializer_list<typename StorageTraits<T>::ElementType> initList) {
  return NewImpl<T>(initList);
}

template <typename T, typename... Args>
typename std::enable_if<
    std::is_standard_layout<typename StorageTraits<T>::StorageType>::value &&
        std::is_trivially_copyable<
            typename StorageTraits<T>::StorageType>::value,
    BlobStoreObject<T>>::type
BlobStore::NewImpl(Args&&... args) {
  using StorageType = typename StorageTraits<T>::StorageType;
  size_t index = FindFreeSlot();
  size_t size = StorageTraits<T>::size(std::forward<Args>(args)...);
  uint8_t* ptr = allocator_.Allocate(size);
  utils::Construct(reinterpret_cast<StorageType*>(ptr),
                   std::forward<Args>(args)...);
  BlobMetadata& metadata = metadata_[index];
  metadata.size = size;
  metadata.offset = allocator_.ToIndex(ptr);
  assert(metadata.offset != ShmAllocator::InvalidIndex);
  metadata.lock_state = 0;
  metadata.next_free_index = -1;
  return BlobStoreObject<T>(this, index);
}

template <typename T>
typename std::enable_if<
    std::is_standard_layout<typename StorageTraits<T>::StorageType>::value &&
        std::is_trivially_copyable<
            typename StorageTraits<T>::StorageType>::value,
    BlobStoreObject<T>>::type
BlobStore::NewImpl(
    std::initializer_list<typename StorageTraits<T>::ElementType> initList) {
  using ElementType = typename StorageTraits<T>::ElementType;
  size_t index = FindFreeSlot();
  size_t size = initList.size() * sizeof(ElementType);
  uint8_t* ptr = allocator_.Allocate(size);
  std::uninitialized_copy(initList.begin(), initList.end(),
                          reinterpret_cast<ElementType*>(ptr));
  BlobMetadata& metadata = metadata_[index];
  metadata.size = size;
  metadata.offset = allocator_.ToIndex(ptr);
  assert(metadata.offset != ShmAllocator::InvalidIndex);
  metadata.lock_state = 0;
  metadata.next_free_index = -1;
  return BlobStoreObject<T>(this, index);
}

#endif  // BLOB_STORE_H_
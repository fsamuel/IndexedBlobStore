#ifndef BLOB_STORE_BASE_H_
#define BLOB_STORE_BASE_H_

#include <cstddef>

// This class defines the interface for a blob store. A blob store is a data
// structure that stores a collection of objects of any type that can be
// serialized to disk.
class BlobStoreBase {
 public:
  // Gets the object of type T at the specified index as a raw pointer.
  virtual uint8_t* GetRaw(size_t index, size_t* offset) = 0;

  // Compares the offset of the object at the specified index with the expected
  // offset. If they are equal, the offset is updated to the new offset.
  virtual bool CompareAndSwap(std::size_t index,
                              std::size_t expected_offset,
                              std::size_t new_offset) = 0;

  // Copies the object at the specified index to a new index.
  virtual size_t Clone(size_t index) = 0;

  virtual size_t GetSize(size_t index) = 0;

  // Acquires a read lock for the object at the specified index.
  virtual bool AcquireReadLock(size_t index) = 0;

  // Acquires a write lock for the object at the specified index.
  virtual bool AcquireWriteLock(size_t index) = 0;

  // Unlocks the object at the specified index.
  virtual void Unlock(size_t index) = 0;

  // Downgrades a write lock to a read lock at the specified index.
  virtual void DowngradeWriteLock(size_t index) = 0;

  // Upgrades a read lock to a write lock at the specified index.
  // Note that this is dangerous and should only be used when the caller knows
  // that no other thread is holding a read lock.
  virtual void UpgradeReadLock(size_t index) = 0;
};

#endif  // BLOB_STORE_BASE_H_
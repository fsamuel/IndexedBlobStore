#ifndef BLOB_METADATA_H_
#define BLOB_METADATA_H_

#include <atomic>

namespace blob_store {

#ifdef _WIN64
typedef __int64 ssize_t;
#else
typedef int ssize_t;
#endif

struct BlobMetadata {
  // The size of the type stored.
  // TODO(fsamuel): Can we get this from the allocator? Or perhaps BlobStore is
  // the allocator.
  size_t size;

  // The offset of the blob in the shared memory buffer.
  std::atomic<std::size_t> offset;

  // The lock state of the blob.
  // TODO(fsamuel): We should probably get rid of this.
  std::atomic<int> lock_state;

  // This field can take one of three states:
  // -  -1 if the slot is occupied
  // -   0 if the slot is tombstoned or at the end of the free list.
  // -   A positive number indicating the index of the next free slot in the
  // free list.
  //  A tombstoned slot is used to indicate that the blob has been dropped but
  //  is not yet ready to be reused.
  // This can happen if there is a pending read or write operation on the
  // blob.
  std::atomic<ssize_t> next_free_index;

  BlobMetadata() : size(0), offset(0), lock_state(0), next_free_index(0) {}

  BlobMetadata(size_t size, size_t count, std::size_t offset)
      : size(size), offset(offset), lock_state(0), next_free_index(-1) {}

  BlobMetadata(const BlobMetadata& other)
      : size(other.size),
        offset(other.offset.load()),
        lock_state(0),
        next_free_index(other.next_free_index.load()) {}

  bool is_deleted() const { return next_free_index.load() != -1; }

  bool is_tombstone() const { return next_free_index.load() == 0; }

  bool SetTombstone() {
    ssize_t expected = next_free_index.load();
    // If the slot is already tombstoned, or on the free list then we don't
    // need to do anything.
    if (expected != -1) {
      return false;
    }
    return next_free_index.compare_exchange_strong(expected, 0);
  }
};

}  // namespace blob_store

#endif  // BLOB_METADATA_H_
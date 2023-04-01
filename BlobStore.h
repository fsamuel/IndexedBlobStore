#ifndef __BLOB_STORE_H_
#define __BLOB_STORE_H_

#include <cstdio>
#include <sys/types.h>
#include <string>
#include <cstring>

#include "SharedMemoryVector.h"
#include "SharedMemoryAllocator.h"

#ifdef _WIN64
typedef __int64 ssize_t;
#else
typedef int ssize_t;
#endif

// BlobStore is a class that manages the storage and retrieval of objects
// (blobs) in shared memory. It supports storing, getting, and deleting
// objects while maintaining a compact memory footprint.
class BlobStore {
public:
    using Allocator = SharedMemoryAllocator<char>;
    using offset_type = typename Allocator::offset_type;
    using index_type = typename SharedMemoryVector<offset_type>::size_type;

    static constexpr index_type InvalidIndex = static_cast<index_type>(-1);

    struct BlobMetadata {
        size_t size;
        offset_type offset;
        ssize_t nextFreeIndex; // -1 if the slot is occupied, or the index of the next free slot in the free list
    };

    using BlobMetadataAllocator = SharedMemoryAllocator<BlobMetadata>;
    using MetadataVector = SharedMemoryVector<BlobMetadata, BlobMetadataAllocator>;

    // Constructor that initializes the BlobStore with the provided metadata and data shared memory buffers.
    BlobStore(SharedMemoryBuffer&& metadataBuffer, SharedMemoryBuffer&& dataBuffer);

    // Puts an object of size `size` into the BlobStore and returns its index.
    // The object's memory location is stored in `ptr`.
    size_t Put(size_t size, char*& ptr);

    // Puts an object of type T with the provided arguments into the BlobStore and returns its index.
    template <typename T, typename... Args>
    size_t Put(size_t size, Args&&... args) {
        size_t index = findFreeSlot();
        char* ptr = allocator.allocate(size);
        allocator.construct(reinterpret_cast<T*>(ptr), std::forward<Args>(args)...);
        metadata[index] = { size, allocator.ToOffset(ptr), -1 };
        return index;
    }

    // Gets the object of type T at the specified index.
    template<typename T>
    T* Get(size_t index) {
        if (index >= metadata.size() || metadata[index].nextFreeIndex != -1) {
            return nullptr;
        }
        BlobMetadata& metadataEntry = metadata[index];
        if (metadataEntry.size == 0) {
            return nullptr;
        }
        return allocator.ToPtr<T>(metadataEntry.offset);
    }

    // Gets the object of type T at the specified index as a constant.
    template<typename T>
    const T* Get(size_t index) const {
        return const_cast<BlobStore*>(this)->Get<T>(index);
    }

    // Gets the object as a char pointer at the specified index.
    char* operator[](size_t index) {
        return Get<char>(index);
    }

    // Gets the object as a constant char pointer at the specified index.
    const char* operator[](size_t index) const {
        return Get<char>(index);
    }

    // Drops the object at the specified index, freeing the associated memory.
    void Drop(size_t index);

    // Compacts the BlobStore, removing any unused space between objects.
    void Compact();

    // Returns the number of stored objects in the BlobStore.
    size_t GetSize() const {
        // The first slot in the metadata vector is always reserved for the free list.
        size_t size = metadata.size() - 1;
        size -= freeSlotCount();
        return size;
    }

private:
    // Returns the index of the first free slot in the metadata vector.
    size_t findFreeSlot();

    // Returns the number of free slots in the metadata vector.
    size_t freeSlotCount() const;

    Allocator allocator;
    BlobMetadataAllocator metadataAllocator;
    MetadataVector metadata;
};

#endif  // __BLOB_STORE_H_
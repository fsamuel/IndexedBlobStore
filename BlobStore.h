#ifndef __BLOB_STORE_H_
#define __BLOB_STORE_H_

#include <cstdio>
#include <sys/types.h>
#include <string>

#include "SharedMemoryVector.h"
#include "SharedMemoryAllocator.h"

#ifdef _WIN64
typedef __int64 ssize_t;
#else
typedef int ssize_t;
#endif

class BlobStore {
public:
    using Allocator = SharedMemoryAllocator<char>;
    using offset_type = typename Allocator::offset_type;

    struct BlobMetadata {
        size_t size;
        offset_type offset;
        ssize_t nextFreeIndex; // -1 if the slot is occupied, or the index of the next free slot in the free list
    };

    using BlobMetadataAllocator = SharedMemoryAllocator<BlobMetadata>;
    using MetadataVector = SharedMemoryVector<BlobMetadata, BlobMetadataAllocator>;

    BlobStore(SharedMemoryBuffer&& metadataBuffer, SharedMemoryBuffer&& dataBuffer) :
        allocator(std::move(dataBuffer)),
        metadataAllocator(std::move(metadataBuffer)),
        metadata(metadataAllocator)
    {
        if (metadata.empty()) {
            metadata.push_back({ 0, -1, -1 });
        }
    }

    size_t Put(size_t size, char*& ptr) {
        size_t index = findFreeSlot();
        ptr = allocator.allocate(size);
        metadata[index] = { size, allocator.ToOffset(ptr), -1};
        return index;
    }

    char* Get(size_t index) {
        if (index >= metadata.size() || metadata[index].nextFreeIndex != -1) {
            return nullptr;
        }
        BlobMetadata& metadataEntry = metadata[index];
        return allocator.ToPtr<char>(metadataEntry.offset);
    }

    void Drop(size_t index) {
        if (index >= metadata.size() || metadata[index].nextFreeIndex != -1) {
            return;
        }
        allocator.deallocate(allocator.ToPtr<char>(metadata[index].offset));
        metadata[index].nextFreeIndex = metadata[0].nextFreeIndex;
        metadata[0].nextFreeIndex = index;
    }

    void compact() {
        std::string bufferName = allocator.bufferName();
        SharedMemoryBuffer tempBuffer(bufferName + "_data_compact");
        Allocator newAllocator(std::move(tempBuffer));
        for (size_t i = 0; i < metadata.size(); ++i) {
            BlobMetadata& metadataEntry = metadata[i];

            // Skip free slots
            if (metadataEntry.nextFreeIndex != -1) {
                continue;
            }

            // Allocate memory in the new allocator and copy the data
            char* newPtr = newAllocator.allocate(metadataEntry.size);
            std::memcpy(newPtr, allocator.ToPtr<char>(metadataEntry.offset), metadataEntry.size);

            // Update metadata with the new pointer
            metadataEntry.offset = newAllocator.ToOffset(newPtr);;
        }

        // Swap the new allocator with the old one and remove the old shared memory file
       // std::swap(allocator, newAllocator);
        allocator = std::move(newAllocator);
        std::remove(bufferName.c_str());

        // Rename the new shared memory file to the original name
        std::rename((bufferName + "_data_compact").c_str(), bufferName.c_str());

    }

private:
    size_t findFreeSlot() {
        if (metadata[0].nextFreeIndex != -1) {
            size_t freeIndex = metadata[0].nextFreeIndex;
            metadata[0].nextFreeIndex = metadata[freeIndex].nextFreeIndex;
            return freeIndex;
        }
        else {
            metadata.push_back({ 0, -1, -1 });
            return metadata.size() - 1;
        }
    }
    Allocator allocator;
    BlobMetadataAllocator metadataAllocator;
    MetadataVector metadata;
};


        #endif  // __BLOB_STORE_H_
#include "BlobStore.h"

BlobStore::BlobStore(SharedMemoryBuffer&& metadataBuffer, SharedMemoryBuffer&& dataBuffer) :
    allocator(std::move(dataBuffer)),
    metadataAllocator(std::move(metadataBuffer)),
    metadata(metadataAllocator)
{
    if (metadata.empty()) {
        metadata.push_back({ 0, -1, 0 });
    }
}

size_t BlobStore::Put(size_t size, char*& ptr) {
    size_t index = findFreeSlot();
    ptr = allocator.allocate(size);
    metadata[index] = { size, allocator.ToOffset(ptr), -1 };
    return index;
}

void BlobStore::Drop(size_t index) {
    if (index >= metadata.size() || metadata[index].nextFreeIndex != -1) {
        return;
    }
    allocator.deallocate(allocator.ToPtr<char>(metadata[index].offset));
    metadata[index].nextFreeIndex = metadata[0].nextFreeIndex;
    metadata[0].nextFreeIndex = index;
}

void BlobStore::Compact() {
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

size_t BlobStore::findFreeSlot() {
    if (metadata[0].nextFreeIndex != 0) {
        size_t freeIndex = metadata[0].nextFreeIndex;
        metadata[0].nextFreeIndex = metadata[freeIndex].nextFreeIndex;
        return freeIndex;
    }
    else {
        metadata.push_back({ 0, -1, -1 });
        return metadata.size() - 1;
    }
}

// Returns the number of free slots in the metadata vector
size_t BlobStore::freeSlotCount() const {
	size_t count = 0;
	size_t index = metadata[0].nextFreeIndex;
    // The free list is a circular linked list with a dummy node at the head.
    while (index !=0) {
		++count;
		index = metadata[index].nextFreeIndex;
	}
	return count;
}
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

	BlobStore(SharedMemoryBuffer&& metadataBuffer, SharedMemoryBuffer&& dataBuffer);

	size_t Put(size_t size, char*& ptr);

	char* Get(size_t index);

	void Drop(size_t index);

	void Compact();

	size_t size() const { 
		// The first slot in the metadata vector is always reserved for the free list.
		return metadata.size() - 1;
	}

private:
	size_t findFreeSlot();

	Allocator allocator;
	BlobMetadataAllocator metadataAllocator;
	MetadataVector metadata;
};


#endif  // __BLOB_STORE_H_
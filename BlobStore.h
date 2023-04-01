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
	using index_type = typename SharedMemoryVector<offset_type>::size_type;

	static constexpr index_type InvalidIndex = static_cast<index_type>(-1);

	struct BlobMetadata {
		size_t size;
		offset_type offset;
		ssize_t nextFreeIndex; // -1 if the slot is occupied, or the index of the next free slot in the free list
	};

	using BlobMetadataAllocator = SharedMemoryAllocator<BlobMetadata>;
	using MetadataVector = SharedMemoryVector<BlobMetadata, BlobMetadataAllocator>;

	BlobStore(SharedMemoryBuffer&& metadataBuffer, SharedMemoryBuffer&& dataBuffer);

	size_t Put(size_t size, char*& ptr);

	template <typename T, typename... Args>
	size_t Put(size_t size, Args&&... args) {
		size_t index = findFreeSlot();
		char* ptr = allocator.allocate(size);
		allocator.construct(reinterpret_cast<T*>(ptr), std::forward<Args>(args)...);
		metadata[index] = { size, allocator.ToOffset(ptr), -1 };
		return index;
	}

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

	template<typename T>
	const T* Get(size_t index) const {
		return const_cast<BlobStore*>(this)->Get<T>(index);
	}

	char* operator[](size_t index) {
		return Get<char>(index);
	}

	const char* operator[](size_t index) const {
		return Get<char>(index);
	}

	void Drop(size_t index);

	void Compact();

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
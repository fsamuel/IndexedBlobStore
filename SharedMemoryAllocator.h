#ifndef __SHARED_MEMORY_ALLOCATOR_H_
#define __SHARED_MEMORY_ALLOCATOR_H_

#include "SharedMemoryBuffer.h"

template <typename T>
class SharedMemoryAllocator {
public:
	using size_type = std::size_t;
	using offset_type = std::ptrdiff_t;

	// Helper method to convert a pointer to an offset relative to the start of the buffer
	offset_type ToOffset(const void* ptr) const {
		return reinterpret_cast<const char*>(ptr) - reinterpret_cast<const char*>(m_buffer.data());
	}

	// Helper method to convert an offset relative to the start of the buffer to a pointer
	template<class U>
	const U* ToPtr(offset_type offset) const {
		return reinterpret_cast<const U*>(reinterpret_cast<const char*>(m_buffer.data()) + offset);
	}

	template<class U>
	U* ToPtr(offset_type offset)  {
		return reinterpret_cast<U*>(reinterpret_cast<char*>(m_buffer.data()) + offset);
	}

	// Header for the allocator state in the shared memory buffer
	struct AllocatorStateHeader {
		uint32_t magicNumber;      // Magic number for verifying the allocator state header
		offset_type freeListOffset; // Offset of the first free block in the free list
		offset_type allocationOffset; // Offset of the first allocation block.
	};

	// Constructor that takes a reference to the shared memory buffer to be used for allocation
	explicit SharedMemoryAllocator(SharedMemoryBuffer&& buffer)
		: m_buffer(buffer) {
		// Check if the buffer is large enough to hold the allocator state header
		if (m_buffer.size() < sizeof(AllocatorStateHeader)) {
			m_buffer.resize(sizeof(AllocatorStateHeader));
		}

		initializeAllocatorStateIfNecessary();
	}

	AllocatorStateHeader* state() {
		return reinterpret_cast<AllocatorStateHeader*>(m_buffer.data());
	}

	const std::string& bufferName() const {
		return m_buffer.name();
	}

	// Allocate memory for n objects of type T, and return a pointer to the first object
	T* allocate(size_type bytes) {
		// Calculate the number of bytes needed for the memory block
		size_type bytesNeeded = calculateBytesNeeded(bytes);

		initializeAllocatorStateIfNecessary();

		offset_type freeListOffset = state()->freeListOffset;

		// Search the free list for a block of sufficient size
		offset_type* prevFreeListOffsetPtr = nullptr;
		while (freeListOffset != -1) {
			// Get a pointer to the current block and its size
			FreeNodeHeader* currentBlockPtr = ToPtr<FreeNodeHeader>(freeListOffset);
			size_type blockSize = currentBlockPtr->size;

			if (blockSize > bytesNeeded + sizeof(FreeNodeHeader)) {
				currentBlockPtr->size = blockSize - bytesNeeded;
				offset_type dataOffset = freeListOffset + blockSize - bytesNeeded;
				return newAllocatedNodeAtOffset(dataOffset, bytesNeeded);

			}
			else if (blockSize >= bytesNeeded) {
				// Found a block that is large enough, remove it from the free list and return a pointer to its data
				if (prevFreeListOffsetPtr) {
					*prevFreeListOffsetPtr = currentBlockPtr->nextOffset;
				}
				else {
					if (state()->freeListOffset == freeListOffset) {
						state()->freeListOffset = currentBlockPtr->nextOffset;
					}
					freeListOffset = currentBlockPtr->nextOffset;
				}

				// Return a pointer to the data in the new block
				AllocatedNodeHeader* nodeHeaderPtr = reinterpret_cast<AllocatedNodeHeader*>(currentBlockPtr);
				return newAllocatedNodeAtOffset(ToOffset(nodeHeaderPtr), max(bytesNeeded, blockSize));
			}

			// Move to the next block in the free list
			prevFreeListOffsetPtr = &(currentBlockPtr->nextOffset);
			freeListOffset = currentBlockPtr->nextOffset;
		}

		// No block of sufficient size was found, resize the buffer and allocate a new block
		offset_type dataOffset = m_buffer.size();
		m_buffer.resize(m_buffer.size() + bytesNeeded);

		// Return a pointer to the data in the new block
		return newAllocatedNodeAtOffset(dataOffset, bytesNeeded);
	}

	// Deallocate memory at the given pointer offset
	void deallocate(offset_type offset) {
		if (offset < 0)
			return;
		T* ptr = ToPtr<T>(offset);
		deallocate(ptr);
	}

	// Deallocate memory at the given pointer
	void deallocate(T* ptr) {
		// Calculate the offset of the allocated node header
		offset_type nodeHeaderOffset = ToOffset(reinterpret_cast<AllocatedNodeHeader*>(ptr) - 1);
		AllocatedNodeHeader* currentNode = ToPtr<AllocatedNodeHeader>(nodeHeaderOffset);
		if (currentNode->prevOffset != -1) {
			AllocatedNodeHeader* prevNode = ToPtr<AllocatedNodeHeader>(currentNode->prevOffset);
			prevNode->nextOffset = currentNode->nextOffset;
		}
		if (currentNode->nextOffset != -1) {
			AllocatedNodeHeader* nextNode = ToPtr<AllocatedNodeHeader>(currentNode->nextOffset);
			nextNode->prevOffset = currentNode->prevOffset;

		}
		// Add the block to the free list
		AllocatorStateHeader* stateHeaderPtr = reinterpret_cast<AllocatorStateHeader*>(m_buffer.data());
		if (stateHeaderPtr->allocationOffset == nodeHeaderOffset) {
			if (currentNode->nextOffset != nodeHeaderOffset) {
				stateHeaderPtr->allocationOffset = currentNode->nextOffset;
			}
			else {
				stateHeaderPtr->allocationOffset = -1;
			}
		}
		FreeNodeHeader* freeNodePtr = ToPtr<FreeNodeHeader>(nodeHeaderOffset);
		freeNodePtr->size = currentNode->size;
		freeNodePtr->nextOffset = stateHeaderPtr->freeListOffset;
		stateHeaderPtr->freeListOffset = ToOffset(freeNodePtr);
	}

	size_type capacity(offset_type offset) const {
		if (offset < 0) {
			return 0;
		}
		offset_type nodeHeaderOffset = offset - sizeof(AllocatedNodeHeader);
		const AllocatedNodeHeader* currentNode = ToPtr<AllocatedNodeHeader>(nodeHeaderOffset);
		return (currentNode->size - sizeof(AllocatedNodeHeader)) / sizeof(T);
	}

	size_type capacity(T* ptr) {
		if (ptr == nullptr) {
			return 0;
		}
		offset_type nodeHeaderOffset = ToOffset(reinterpret_cast<AllocatedNodeHeader*>(ptr) - 1);
		AllocatedNodeHeader* currentNode = ToPtr<AllocatedNodeHeader>(nodeHeaderOffset);
		return currentNode->size;
	}

	template<typename U, typename... Args>
	void construct(U* p, Args&&... args)
	{
		new (p) U(std::forward<Args>(args)...);
	}

	template<typename U>
	void destroy(U* p)
	{
		p->~U();
	}

	// Return an iterator to the first allocated object
	auto begin() {
		AllocatorStateHeader* stateHeaderPtr = reinterpret_cast<AllocatorStateHeader*>(m_buffer.data());
		if (stateHeaderPtr->allocationOffset == -1)
			return Iterator(nullptr, this);
		return Iterator(ToPtr<AllocatedNodeHeader>(stateHeaderPtr->allocationOffset), this);
	}

	auto begin() const {
		const AllocatorStateHeader* stateHeaderPtr = reinterpret_cast<const AllocatorStateHeader*>(m_buffer.data());
		if (stateHeaderPtr->allocationOffset == -1)
			return ConstIterator(nullptr, this);
		return ConstIterator(ToPtr<AllocatedNodeHeader>(stateHeaderPtr->allocationOffset), this);
	}

	// Return an iterator to the end of the allocated objects
	auto end() {
		return Iterator(nullptr, this);
	}

	auto end() const {
		return ConstIterator(nullptr, this);
	}

	auto first() {
		auto lastAllocation = begin();
		auto firstAllocation = --begin();
		if (firstAllocation == end())
			return lastAllocation;
		return firstAllocation;
	}

	auto first() const{
		auto lastAllocation = begin();
		auto firstAllocation = --begin();
		if (firstAllocation == end())
			return lastAllocation;
		return firstAllocation;
	}

	auto last() {
		return begin();
	}

	auto last() const{
		return begin();
	}

private:
	// Header for an allocated node in the allocator
	struct AllocatedNodeHeader {
		size_type size; // Size of the allocated block, including the header
		offset_type nextOffset; // Offset of the next allocation.
		offset_type prevOffset; // Offset of the previous allocation.
	};
	// Header for a free node in the allocator
	struct FreeNodeHeader {
		size_type size;        // Size of the free block, including the header
		offset_type nextOffset; // Offset of the next free block in the free list
	};

	void initializeAllocatorStateIfNecessary() {
		// Check if the allocator state header has already been initialized
		AllocatorStateHeader* stateHeaderPtr = reinterpret_cast<AllocatorStateHeader*>(m_buffer.data());
		if (stateHeaderPtr->magicNumber != 0x12345678) {
			// Initialize the allocator state header
			stateHeaderPtr->magicNumber = 0x12345678;
			stateHeaderPtr->freeListOffset = -1;
			stateHeaderPtr->allocationOffset = -1;
		}
	}

	static size_type calculateBytesNeeded(size_type bytes) {
		// Calculate the number of objects needed based on the requested size and the size of each object
		size_type n = bytes / sizeof(T);
		if (bytes % sizeof(T) != 0) {
			n++;
		}

		// Calculate the number of bytes needed for the memory block
		return sizeof(AllocatedNodeHeader) + sizeof(T) * n;
	}

	T* newAllocatedNodeAtOffset(offset_type offset, size_type size) {
		AllocatedNodeHeader* nodeHeaderPtr = ToPtr<AllocatedNodeHeader>(offset);
		nodeHeaderPtr->size = size;
		if (state()->allocationOffset != -1) {
			AllocatedNodeHeader* nextNode = ToPtr<AllocatedNodeHeader>(state()->allocationOffset);
			nodeHeaderPtr->nextOffset = state()->allocationOffset;
			nodeHeaderPtr->prevOffset = nextNode->prevOffset;
			nextNode->prevOffset = offset;
		} else {
			nodeHeaderPtr->nextOffset = offset;
			nodeHeaderPtr->prevOffset = offset;
		}
		state()->allocationOffset = offset;

		return ToPtr<T>(offset + sizeof(AllocatedNodeHeader));
	}


	// Iterator class for iterating over the allocated nodes in the allocator
	class Iterator {
	public:
		Iterator(AllocatedNodeHeader* nodePtr,
			SharedMemoryAllocator* allocator) :
			m_nodePtr(nodePtr), m_allocator(allocator) {}

		T& operator*() const {
			return *reinterpret_cast<T*>(m_nodePtr + 1);
		}

		T* operator->() const {
			return reinterpret_cast<T*>(m_nodePtr + 1);
		}

		Iterator& operator++() {
			if (m_nodePtr == nullptr)
				return *this;

			if ((m_nodePtr->nextOffset == -1) || (m_nodePtr->nextOffset == m_allocator->ToOffset(m_nodePtr))) {
				m_nodePtr = nullptr;
			} else {
				m_nodePtr = m_allocator->ToPtr<AllocatedNodeHeader>(m_nodePtr->nextOffset);
			}
			return *this;
		}

		Iterator& operator--() {
			if (m_nodePtr == nullptr)
				return *this;

			if ((m_nodePtr->prevOffset == -1) || (m_nodePtr->prevOffset == m_allocator->ToOffset(m_nodePtr))) {
				m_nodePtr = nullptr;
			}
			else {
				m_nodePtr = m_allocator->ToPtr<AllocatedNodeHeader>(m_nodePtr->prevOffset);
			}
			return *this;
		}

		bool operator==(const Iterator& other) const {
			return m_nodePtr == other.m_nodePtr;
		}

		bool operator!=(const Iterator& other) const {
			return m_nodePtr != other.m_nodePtr;
		}

	private:
		AllocatedNodeHeader* m_nodePtr;
		SharedMemoryAllocator* m_allocator;
	};

	// Iterator class for iterating over the allocated nodes in the allocator
	class ConstIterator {
	public:
		ConstIterator(const AllocatedNodeHeader* nodePtr,
			const SharedMemoryAllocator* allocator) :
			m_nodePtr(nodePtr), m_allocator(allocator) {}

		const T& operator*() const {
			return *reinterpret_cast<const T*>(m_nodePtr + 1);
		}

		const T* operator->() const {
			return reinterpret_cast<const T*>(m_nodePtr + 1);
		}

		ConstIterator& operator++() {
			if (m_nodePtr == nullptr)
				return *this;
			if (m_nodePtr->nextOffset == -1) {
				m_nodePtr = nullptr;
			}
			else {
				m_nodePtr = m_allocator->ToPtr<AllocatedNodeHeader>(m_nodePtr->nextOffset);
			}
			return *this;
		}

		ConstIterator& operator--() {
			if (m_nodePtr == nullptr)
				return *this;

			if ((m_nodePtr->prevOffset == -1) || (m_nodePtr->prevOffset == m_allocator->ToOffset(m_nodePtr))) {
				m_nodePtr = nullptr;
			}
			else {
				m_nodePtr = m_allocator->ToPtr<AllocatedNodeHeader>(m_nodePtr->prevOffset);
			}
			return *this;
		}

		bool operator==(const ConstIterator& other) const {
			return m_nodePtr == other.m_nodePtr;
		}

		bool operator!=(const ConstIterator& other) const {
			return m_nodePtr != other.m_nodePtr;
		}

	private:
		const AllocatedNodeHeader* m_nodePtr;
		const SharedMemoryAllocator* m_allocator;
	};


	SharedMemoryBuffer m_buffer;  // Reference to the shared memory buffer used for allocation
};



#endif  // __SHARED_MEMORY_ALLOCATOR_H_
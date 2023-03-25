#ifndef __SHARED_MEMORY_ALLOCATOR_H_
#define __SHARED_MEMORY_ALLOCATOR_H_

template <typename T>
class SharedMemoryAllocator {
public:
    using size_type = std::size_t;
    using offset_type = std::ptrdiff_t;

    // Constructor that takes a reference to the shared memory buffer to be used for allocation
    explicit SharedMemoryAllocator(SharedMemoryBuffer& buffer)
        : m_buffer(buffer) {
        // Check if the buffer is large enough to hold the allocator state header
        if (m_buffer.size() < sizeof(AllocatorStateHeader)) {
            m_buffer.resize(sizeof(AllocatorStateHeader));
            //throw std::runtime_error("Shared memory buffer is too small for the allocator state header");
        }

        // Check if the allocator state header has already been initialized
        AllocatorStateHeader* stateHeaderPtr = reinterpret_cast<AllocatorStateHeader*>(m_buffer.data());
        if (stateHeaderPtr->magicNumber != 0x12345678) {
            // Initialize the allocator state header
            stateHeaderPtr->magicNumber = 0x12345678;
            stateHeaderPtr->freeListOffset = -1;
            stateHeaderPtr->allocationOffset = -1;
        }
    }

    // Allocate memory for n objects of type T, and return a pointer to the first object
    T* allocate(size_type bytes) {
        // Calculate the number of objects needed based on the requested size and the size of each object
        size_type n = bytes / sizeof(T);
        if (bytes % sizeof(T) != 0) {
            n++;
        }

        // Calculate the number of bytes needed for the memory block
        size_type bytesNeeded = sizeof(AllocatedNodeHeader) + sizeof(T) * n;

        // Check if the allocator state header has already been initialized
        AllocatorStateHeader* stateHeaderPtr = reinterpret_cast<AllocatorStateHeader*>(m_buffer.data());
        offset_type freeListOffset = -1;
        if (stateHeaderPtr->magicNumber != 0x12345678) {
            // Initialize the allocator state header
            stateHeaderPtr->magicNumber = 0x12345678;
            stateHeaderPtr->freeListOffset = -1;   
            stateHeaderPtr->allocationOffset = -1;
        } else {
            // Get the free list offset from the allocator state header
            freeListOffset = stateHeaderPtr->freeListOffset;
        }

        // Search the free list for a block of sufficient size
        offset_type* prevFreeListOffsetPtr = nullptr;
        while (freeListOffset != -1) {
            // Get a pointer to the current block and its size
            FreeNodeHeader* currentBlockPtr = reinterpret_cast<FreeNodeHeader*>(offsetToPtr(freeListOffset));
            size_type blockSize = currentBlockPtr->size;

            if (blockSize >= bytesNeeded) {
                // Found a block that is large enough, remove it from the free list and return a pointer to its data
                if (prevFreeListOffsetPtr) {
                    *prevFreeListOffsetPtr = currentBlockPtr->nextOffset;
                }
                else {
                    freeListOffset = currentBlockPtr->nextOffset;
                }

                AllocatedNodeHeader* nodeHeaderPtr = reinterpret_cast<AllocatedNodeHeader*>(currentBlockPtr);
                nodeHeaderPtr->size = bytesNeeded;
                nodeHeaderPtr->nextOffset = stateHeaderPtr->allocationOffset;
                nodeHeaderPtr->prevOffset = -1;
                if (stateHeaderPtr->allocationOffset != -1) {
                    AllocatedNodeHeader* prev = reinterpret_cast<AllocatedNodeHeader*>(offsetToPtr(stateHeaderPtr->allocationOffset));
                    prev->nextOffset = ptrToOffset(nodeHeaderPtr);
                }
   
                stateHeaderPtr->allocationOffset = ptrToOffset(nodeHeaderPtr);
                return reinterpret_cast<T*>(offsetToPtr(freeListOffset + sizeof(AllocatedNodeHeader)));
            }

            // Move to the next block in the free list
            prevFreeListOffsetPtr = &(currentBlockPtr->nextOffset);
            freeListOffset = currentBlockPtr->nextOffset;
        }

        // No block of sufficient size was found, resize the buffer and allocate a new block
        offset_type dataOffset = m_buffer.size();
        m_buffer.resize(m_buffer.size() + bytesNeeded);

        AllocatedNodeHeader* nodeHeaderPtr = reinterpret_cast<AllocatedNodeHeader*>(offsetToPtr(dataOffset));
        nodeHeaderPtr->size = bytesNeeded;
        nodeHeaderPtr->nextOffset = stateHeaderPtr->allocationOffset;
        nodeHeaderPtr->prevOffset = -1;
        stateHeaderPtr->allocationOffset = ptrToOffset(nodeHeaderPtr);

        // Return a pointer to the data in the new block
        return reinterpret_cast<T*>(offsetToPtr(dataOffset + sizeof(AllocatedNodeHeader)));
    }

    // Deallocate memory at the given pointer with the given size
    void deallocate(T* ptr) {
        // Calculate the offset of the allocated node header
        offset_type nodeHeaderOffset = ptrToOffset(reinterpret_cast<AllocatedNodeHeader*>(ptr) - 1);
        AllocatedNodeHeader* currentNode = reinterpret_cast<AllocatedNodeHeader*>(offsetToPtr(nodeHeaderOffset));
        if (currentNode->prevOffset != -1) {
            AllocatedNodeHeader* prevNode = reinterpret_cast<AllocatedNodeHeader*>(offsetToPtr(currentNode->prevOffset));
            prevNode->nextOffset = currentNode->nextOffset;
        }
        // Add the block to the free list
        AllocatorStateHeader* stateHeaderPtr = reinterpret_cast<AllocatorStateHeader*>(m_buffer.data());
        if (stateHeaderPtr->allocationOffset == nodeHeaderOffset) {
            stateHeaderPtr->allocationOffset = -1;
        }
        FreeNodeHeader* freeNodePtr = reinterpret_cast<FreeNodeHeader*>(offsetToPtr(nodeHeaderOffset));
        freeNodePtr->size = currentNode->size;
        freeNodePtr->nextOffset = stateHeaderPtr->freeListOffset;
        stateHeaderPtr->freeListOffset = ptrToOffset(freeNodePtr);
    }

    // Return an iterator to the first allocated object
    auto begin() {
        AllocatorStateHeader* stateHeaderPtr = reinterpret_cast<AllocatorStateHeader*>(m_buffer.data());
        if (stateHeaderPtr->allocationOffset == -1)
            return Iterator(nullptr, this);
        return Iterator(
            reinterpret_cast<AllocatedNodeHeader*>(
                offsetToPtr(stateHeaderPtr->allocationOffset)), this);
    }

    // Return an iterator to the end of the allocated objects
    auto end() {
        return Iterator(nullptr, this);
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

    // Header for the allocator state in the shared memory buffer
    struct AllocatorStateHeader {
        uint32_t magicNumber;      // Magic number for verifying the allocator state header
        offset_type freeListOffset; // Offset of the first free block in the free list
        offset_type allocationOffset; // Offset of the first allocation block.
    };

    // Helper method to convert a pointer to an offset relative to the start of the buffer
    offset_type ptrToOffset(const void* ptr) const {
        return reinterpret_cast<const char*>(ptr) - m_buffer.data();
    }

    // Helper method to convert an offset relative to the start of the buffer to a pointer
    void* offsetToPtr(offset_type offset) const {
        return reinterpret_cast<char*>(m_buffer.data()) + offset;
    }

    // Iterator class for iterating over the allocated nodes in the allocator
    class Iterator {
    public:
        Iterator(AllocatedNodeHeader* nodePtr,
                 SharedMemoryAllocator* allocator) : 
            m_nodePtr(nodePtr), m_allocator(allocator)  {}

        T& operator*() const {
            return *reinterpret_cast<T*>(m_nodePtr + 1);
        }

        T* operator->() const {
            return reinterpret_cast<T*>(m_nodePtr + 1);
        }

        Iterator& operator++() {
            if (m_nodePtr == nullptr)
                return *this;
            if (m_nodePtr->nextOffset == -1) {
                m_nodePtr = nullptr;
            }
            else {
                m_nodePtr = reinterpret_cast<AllocatedNodeHeader*>(m_allocator->offsetToPtr(m_nodePtr->nextOffset));
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


    SharedMemoryBuffer& m_buffer;  // Reference to the shared memory buffer used for allocation
 };



#endif  // __SHARED_MEMORY_ALLOCATOR_H_
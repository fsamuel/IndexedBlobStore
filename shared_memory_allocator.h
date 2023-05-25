#ifndef SHARED_MEMORY_ALLOCATOR_H_
#define SHARED_MEMORY_ALLOCATOR_H_

#include <atomic>
#include <vector>

#include "shared_memory_buffer.h"

class SharedMemoryAllocatorObserver {
public:
	virtual void OnBufferResize() = 0;
};

// A simple allocator that allocates memory from a shared memory buffer. The allocator
// maintains a free list of available blocks of memory. When a block is allocated, it is
// removed from the free list. When a block is freed, it is added back to the free list.
// The allocator does not attempt to coalesce adjacent free blocks.
// The allocator is designed to be lock-free by using atomic operations to update the
// allocator state: size of a free block, or the head of the free list.
template <typename T>
class SharedMemoryAllocator {
public:
	using size_type = std::size_t;
	using offset_type = std::ptrdiff_t;

	// Constructor that takes a reference to the shared memory buffer to be used for allocation
	explicit SharedMemoryAllocator(SharedMemoryBuffer&& buffer);

	explicit SharedMemoryAllocator(SharedMemoryAllocator&& other);

	const std::string& buffer_name() const {
		return buffer_.name();
	}

	// Allocate memory for n objects of type T, and return a pointer to the first object
	T* Allocate(size_type bytes);

	// Deallocate memory at the given pointer offset
	void Deallocate(offset_type offset);

	// Deallocate memory at the given pointer.
	void Deallocate(T* ptr);

	// Returns the size of the allocated block at the given offset.
	size_type GetCapacity(offset_type offset) const;

	// Returns the size of the allocated block at the given pointer.
	size_type GetCapacity(T* ptr);

	// Helper method to convert a pointer to an offset relative to the start of the buffer
	offset_type ToOffset(const void* ptr) const {
		return reinterpret_cast<const char*>(ptr) - reinterpret_cast<const char*>(buffer_.data());
	}

	// Helper method to convert an offset relative to the start of the buffer to a pointer
	template<class U>
	const U* ToPtr(offset_type offset) const {
		if (offset == -1) {
			return nullptr;
		}
		return reinterpret_cast<const U*>(reinterpret_cast<const char*>(buffer_.data()) + offset);
	}

	template<class U>
	U* ToPtr(offset_type offset) {
		if (offset == -1) {
			return nullptr;
		}
		return reinterpret_cast<U*>(reinterpret_cast<char*>(buffer_.data()) + offset);
	}

	void AddObserver(SharedMemoryAllocatorObserver* observer) {
		observers_.push_back(observer);
	}

	void RemoveObserver(SharedMemoryAllocatorObserver* observer) {
		observers_.erase(std::remove(observers_.begin(), observers_.end(), observer), observers_.end());
	}

	SharedMemoryAllocator& operator=(SharedMemoryAllocator&& other) noexcept {
		buffer_ = std::move(other.buffer_);
		return *this;
	}

private:
	// Header for the allocator state in the shared memory buffer
	struct AllocatorStateHeader {
		uint32_t magic_number;      // Magic number for verifying the allocator state header
		std::atomic<offset_type> free_list_offset; // Offset of the first free block in the free list
	};

	AllocatorStateHeader* state() {
		return reinterpret_cast<AllocatorStateHeader*>(buffer_.data());
	}

	void NotifyObserversOfResize() {
		for (SharedMemoryAllocatorObserver* observer : observers_) {
			observer->OnBufferResize();
		}
	}

	// Header for an allocated node in the allocator
	struct AllocatedNodeHeader {
		size_type size; // Size of the allocated block, including the header
	};

	// Given a pointer, returns the AllocatedNodeHeader.
	AllocatedNodeHeader* GetNode(T* ptr) {
		return reinterpret_cast<AllocatedNodeHeader*>(ptr) - 1;
	}

	// Header for a free node in the allocator
	struct FreeNodeHeader {
		// Size of the free block, including the header
		std::atomic<size_type> size;
		// Offset of the next free block in the free list
		std::atomic<offset_type> next_offset;
	};

	// Returns the first free node in the free list, or nullptr if there are no free nodes.
	FreeNodeHeader* FirstFreeNode() {
		return ToPtr<FreeNodeHeader>(state()->free_list_offset);
	}

	// Returns the next node in the free list, or nullptr if there are no more nodes.
	FreeNodeHeader* NextFreeNode(FreeNodeHeader* node) {
		return ToPtr<FreeNodeHeader>(node->next_offset);
	}

	void InitializeAllocatorStateIfNecessary() {
		// Check if the allocator state header has already been initialized
		AllocatorStateHeader* state_header_ptr = state();
		if (state_header_ptr->magic_number != 0x12345678) {
			// Initialize the allocator state header
			state_header_ptr->magic_number = 0x12345678;
			state_header_ptr->free_list_offset = -1;
		}
	}

	static size_type CalculateBytesNeeded(size_type bytes) {
		// Calculate the number of objects needed based on the requested size and the size of each object
		size_type n = bytes / sizeof(T);
		if (bytes % sizeof(T) != 0) {
			n++;
		}

		// Calculate the number of bytes needed for the memory block
		return std::max(sizeof(AllocatedNodeHeader) + sizeof(T) * n, sizeof(FreeNodeHeader)) ;
	}

	T* NewAllocatedNodeAtOffset(offset_type offset, size_type size) {
		AllocatedNodeHeader* allocated_node = ToPtr<AllocatedNodeHeader>(offset);
		allocated_node->size = size;
		return ToPtr<T>(offset + sizeof(AllocatedNodeHeader));
	}

private:

	SharedMemoryBuffer buffer_;  // Reference to the shared memory buffer used for allocation
	std::vector<SharedMemoryAllocatorObserver*> observers_;
};

template<typename T>
SharedMemoryAllocator<T>::SharedMemoryAllocator(SharedMemoryBuffer&& buffer)
	: buffer_(std::move(buffer)) {
	// Check if the buffer is large enough to hold the allocator state header
	if (buffer_.size() < sizeof(AllocatorStateHeader)) {
		buffer_.Resize(sizeof(AllocatorStateHeader));
	}

	InitializeAllocatorStateIfNecessary();
}

template<typename T>
SharedMemoryAllocator<T>::SharedMemoryAllocator(SharedMemoryAllocator&& other)
	: buffer_(std::move(other.buffer_)),
	observers_(std::move(other.observers_)) {
}

template<typename T>
T* SharedMemoryAllocator<T>::Allocate(size_type bytes) {
	// Calculate the number of bytes needed for the memory block
	size_type bytes_needed = CalculateBytesNeeded(bytes);

	InitializeAllocatorStateIfNecessary();

	FreeNodeHeader* current_free_node = FirstFreeNode();

	// Search the free list for a block of sufficient size
	FreeNodeHeader* prev_free_node = nullptr;
	while (current_free_node != nullptr) {
		size_type free_node_size = current_free_node->size;

		// If we've found a block greater than the size we need (and can accommodate a head
		// a second block, split it into two blocks. The first piece will be returned to the
		// free list, and the second piece will be returned to the caller.
		if (free_node_size > bytes_needed + sizeof(FreeNodeHeader)) {
			size_type new_free_node_size = free_node_size - bytes_needed;
			if (!current_free_node->size.compare_exchange_weak(free_node_size, new_free_node_size)) {
				// The block was resized by another thread, go back to the free list and try again.
				current_free_node = FirstFreeNode();
				continue;
			}
			offset_type data_offset = ToOffset(current_free_node) + free_node_size - bytes_needed;
			return NewAllocatedNodeAtOffset(data_offset, bytes_needed);
		}

		// The block is large enough to use for allocation but NOT large enough to split.
		if (free_node_size >= bytes_needed) {
			// Found a block that is large enough, remove it from the free list and return a pointer to its data
			if (prev_free_node != nullptr) {
				offset_type current_free_node_offset = ToOffset(current_free_node);
				if (!prev_free_node->next_offset.compare_exchange_weak(current_free_node_offset, current_free_node->next_offset)) {
					// The block was removed from the free list by another thread, go back to the
					// beginning of the free list and try again.
					current_free_node = FirstFreeNode();
					continue;
				}
			}
			else {
				// The block we found is the first block in the free list, update the free list
				// offset. Again this needs to be done atomically or we need to move back to the
				// beginning of the free list and try again.
				// This design likey results in a lot of fragmentation. We should consider
				// using a free list that is sorted by size
				offset_type current_free_node_offset = ToOffset(current_free_node);
				if (!state()->free_list_offset.compare_exchange_weak(current_free_node_offset, current_free_node->next_offset)) {
					// The block was removed from the free list by another thread, go back to the
					// beginning of the free list and try again.
					current_free_node = FirstFreeNode();
					continue;
				}
			}

			// Return a pointer to the data in the new block
			AllocatedNodeHeader* node_header_ptr = reinterpret_cast<AllocatedNodeHeader*>(current_free_node);
			return NewAllocatedNodeAtOffset(ToOffset(node_header_ptr), std::max(bytes_needed, free_node_size));
		}

		// Move to the next block in the free list
		prev_free_node = current_free_node;
		current_free_node = NextFreeNode(current_free_node);
	}

	// No block of sufficient size was found, resize the buffer and allocate a new block.
	// TODO(fsamuel): We can probably use something like ChunkedVector for resizing the buffer.
	// We just need to make sure an allocation fully fits into a chunk.
	// Idea: When we need a new chunk, create a free list node at the beginning of the chunk for
	// the full size of the chunk. This will allow us to use the free list to find a chunk that
	// is large enough for an allocation.
	// We don't want to insert a new node multiple times for a single chunk, so we need to make
	// sure that the free list head encodes the last chunk that was allocated.
	offset_type data_offset = buffer_.size();
	buffer_.Resize(buffer_.size() + bytes_needed);

	// Return a pointer to the data in the new block
	T* new_node = NewAllocatedNodeAtOffset(data_offset, bytes_needed);
	NotifyObserversOfResize();

	return new_node;
}

// Deallocate memory at the given pointer offset
template<typename T>
void SharedMemoryAllocator<T>::Deallocate(offset_type offset) {
	if (offset < 0)
		return;
	T* ptr = ToPtr<T>(offset);
	Deallocate(ptr);
}

template<typename T>
void SharedMemoryAllocator<T>::Deallocate(T* ptr) {
	AllocatedNodeHeader* current_node = GetNode(ptr);
	offset_type node_header_offset = ToOffset(current_node);

	// Add the block to the free list
	while (true) {
		// Get the current head of the free list
		offset_type free_list_offset = state()->free_list_offset;
		// Set the next pointer of the new free node to the current head of the free list
		FreeNodeHeader* free_node_ptr = ToPtr<FreeNodeHeader>(node_header_offset);
		free_node_ptr->next_offset.store(free_list_offset);
		// Try to set the head of the free list to the new free node
		if (state()->free_list_offset.compare_exchange_weak(free_list_offset, node_header_offset)) {
			break;
		}
		// The head of the free list was updated by another thread, try again
	}
}

template<typename T>
typename SharedMemoryAllocator<T>::size_type SharedMemoryAllocator<T>::GetCapacity(offset_type offset) const {
	if (offset < 0) {
		return 0;
	}
	offset_type node_header_offset = offset - sizeof(AllocatedNodeHeader);
	const AllocatedNodeHeader* current_node = ToPtr<AllocatedNodeHeader>(node_header_offset);
	return (current_node->size - sizeof(AllocatedNodeHeader)) / sizeof(T);
}

// Returns the size of the allocated block at the given pointer.
template<typename T>
typename SharedMemoryAllocator<T>::size_type SharedMemoryAllocator<T>::GetCapacity(T* ptr) {
	if (ptr == nullptr) {
		return 0;
	}
	offset_type node_header_offset = ToOffset(reinterpret_cast<AllocatedNodeHeader*>(ptr) - 1);
	AllocatedNodeHeader* curent_node = ToPtr<AllocatedNodeHeader>(node_header_offset);
	return (curent_node->size - sizeof(AllocatedNodeHeader)) / sizeof(T);
}

#endif  // SHARED_MEMORY_ALLOCATOR_H_
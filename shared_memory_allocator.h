#ifndef SHARED_MEMORY_ALLOCATOR_H_
#define SHARED_MEMORY_ALLOCATOR_H_

#include "shared_memory_buffer.h"
#include <vector>

#ifdef _WIN32
#define NOMINMAX
#include <Windows.h>
#else
#include <unistd.h>
#include <sys/mman.h>
#endif

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

	// Helper method to convert a pointer to an offset relative to the start of the buffer
	offset_type ToOffset(const void* ptr) const {
		return reinterpret_cast<const char*>(ptr) - reinterpret_cast<const char*>(buffer_.data());
	}

	// Helper method to convert an offset relative to the start of the buffer to a pointer
	template<class U>
	const U* ToPtr(offset_type offset) const {
		return reinterpret_cast<const U*>(reinterpret_cast<const char*>(buffer_.data()) + offset);
	}

	template<class U>
	U* ToPtr(offset_type offset) {
		return reinterpret_cast<U*>(reinterpret_cast<char*>(buffer_.data()) + offset);
	}

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

	void AddObserver(SharedMemoryAllocatorObserver* observer) {
		observers_.push_back(observer);
	}

	void RemoveObserver(SharedMemoryAllocatorObserver* observer) {
		observers_.erase(std::remove(observers_.begin(), observers_.end(), observer), observers_.end());
	}

	template<typename U, typename... Args>
	void Construct(U* p, Args&&... args)
	{
		new (p) U(std::forward<Args>(args)...);
	}

	template<typename T, std::size_t N>
	void Construct(std::array<T, N>* p, std::initializer_list<T> ilist)
	{
		*p = ilist;
	}

	template<typename U>
	void Destroy(U* p)
	{
		p->~U();
	}

	SharedMemoryAllocator& operator=(SharedMemoryAllocator&& other) noexcept {
		buffer_ = std::move(other.buffer_);
		return *this;
	}


private:
	// Header for the allocator state in the shared memory buffer
	struct AllocatorStateHeader {
		uint32_t magic_number;      // Magic number for verifying the allocator state header
		offset_type free_list_offset; // Offset of the first free block in the free list
		offset_type allocation_offset; // Offset of the first allocation block.
	};

	AllocatorStateHeader* state() {
		return reinterpret_cast<AllocatorStateHeader*>(buffer_.data());
	}

	void NotifyObserversOfResize() {
		for (SharedMemoryAllocatorObserver* observer : observers_) {
			observer->OnBufferResize();
		}
	}

	// We should allocate new chunks in multiples of the page size.
	// The first chunk should always be at least as big as the page size.
	// This is because we need to store the allocator state header in the first chunk.
	// Each subsequent chunk should be double the size of the previous chunk.
	size_t GetPageSize() {
#ifdef _WIN32
		SYSTEM_INFO system_info;
		GetSystemInfo(&system_info);
		return system_info.dwPageSize;
#else
		return sysconf(_SC_PAGE_SIZE);
#endif
	}

	size_t RoundUpToPageSize(size_t size) {
		size_t page_size = GetPageSize();
		return ((size + page_size - 1) / page_size) * page_size;
	}

	// Header for an allocated node in the allocator
	struct AllocatedNodeHeader {
		size_type size; // Size of the allocated block, including the header
		offset_type next_offset; // Offset of the next allocation.
		offset_type prev_offset; // Offset of the previous allocation.
	};

	// Given a pointer, returns the AllocatedNodeHeader.
	AllocatedNodeHeader* GetNode(T* ptr) {
		return reinterpret_cast<AllocatedNodeHeader*>(ptr) - 1;
	}

	// Returns the next node in the allocation list, or nullptr if there are no more nodes.
	AllocatedNodeHeader* NextNode(AllocatedNodeHeader* node) {
		offset_type next_offset = node->next_offset;
		if (next_offset == -1) {
			return nullptr;
		}
		return ToPtr<AllocatedNodeHeader>(next_offset);
	}

	// Returns the previous node in the allocation list, or nullptr if there 
	// are no more nodes.
	AllocatedNodeHeader* PreviousNode(AllocatedNodeHeader* node) {
		offset_type prev_offset = node->prev_offset;
		if (prev_offset == -1) {
			return nullptr;
		}
		return ToPtr<AllocatedNodeHeader>(prev_offset);
	}

	// Header for a free node in the allocator
	struct FreeNodeHeader {
		size_type size;        // Size of the free block, including the header
		offset_type next_offset; // Offset of the next free block in the free list
	};

	// Returns the first free node in the free list, or nullptr if there are no free nodes.
	FreeNodeHeader* FirstFreeNode() {
		AllocatorStateHeader* state_header_ptr = state();
		offset_type free_list_offset = state_header_ptr->free_list_offset;
		if (free_list_offset == -1) {
			return nullptr;
		}
		return ToPtr<FreeNodeHeader>(free_list_offset);
	}

	// Returns the next node in the free list, or nullptr if there are no more nodes.
	FreeNodeHeader* NextFreeNode(FreeNodeHeader* node) {
		offset_type next_offset = node->next_offset;
		if (next_offset == -1) {
			return nullptr;
		}
		return ToPtr<FreeNodeHeader>(next_offset);
	}

	void InitializeAllocatorStateIfNecessary() {
		// Check if the allocator state header has already been initialized
		AllocatorStateHeader* state_header_ptr = state();
		if (state_header_ptr->magic_number != 0x12345678) {
			// Initialize the allocator state header
			state_header_ptr->magic_number = 0x12345678;
			state_header_ptr->free_list_offset = -1;
			state_header_ptr->allocation_offset = -1;
		}
	}

	static size_type CalculateBytesNeeded(size_type bytes) {
		// Calculate the number of objects needed based on the requested size and the size of each object
		size_type n = bytes / sizeof(T);
		if (bytes % sizeof(T) != 0) {
			n++;
		}

		// Calculate the number of bytes needed for the memory block
		return sizeof(AllocatedNodeHeader) + sizeof(T) * n;
	}

	T* NewAllocatedNodeAtOffset(offset_type offset, size_type size) {
		AllocatedNodeHeader* node_header_ptr = ToPtr<AllocatedNodeHeader>(offset);
		node_header_ptr->size = size;
		// Add the node to the front of the allocation list
		if (state()->allocation_offset != -1) {
			AllocatedNodeHeader* next_node = ToPtr<AllocatedNodeHeader>(state()->allocation_offset);
			node_header_ptr->next_offset = state()->allocation_offset;
			node_header_ptr->prev_offset = next_node->prev_offset;
			next_node->prev_offset = offset;
		}
		else {
			node_header_ptr->next_offset = -1;
			node_header_ptr->prev_offset = -1;
		}
		state()->allocation_offset = offset;

		return ToPtr<T>(offset + sizeof(AllocatedNodeHeader));
	}

public:

	// Iterator class for iterating over the allocated nodes in the allocator
	class Iterator {
	public:
		Iterator(AllocatedNodeHeader* nodePtr,
			SharedMemoryAllocator* allocator) :
			node_ptr_(nodePtr), allocator_(allocator) {}

		T& operator*() const {
			return *reinterpret_cast<T*>(node_ptr_ + 1);
		}

		T* operator->() const {
			return reinterpret_cast<T*>(node_ptr_ + 1);
		}

		Iterator& operator++() {
			if (node_ptr_ == nullptr)
				return *this;
			node_ptr_ = allocator_->NextNode(node_ptr_);
			return *this;
		}

		bool operator==(const Iterator& other) const {
			return node_ptr_ == other.node_ptr_;
		}

		bool operator!=(const Iterator& other) const {
			return node_ptr_ != other.node_ptr_;
		}

	private:
		AllocatedNodeHeader* node_ptr_;
		SharedMemoryAllocator* allocator_;
	};

	// Return an iterator to the first allocated object
	Iterator begin();

	// Return an iterator to the end of the allocated objects
	Iterator end();

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

	//offset_type free_list_offset = state()->free_list_offset;
	FreeNodeHeader* current_free_node = FirstFreeNode();

	// Search the free list for a block of sufficient size
	FreeNodeHeader* prev_free_node = nullptr;
	while (current_free_node != nullptr) {
		size_type block_size = current_free_node->size;

		// If we've found a block greater than the size we need (and can accommodate a head
		// a second block, split it into two blocks. The first piece will be returned to the
		// free list, and the second piece will be returned to the caller.
		if (block_size > bytes_needed + sizeof(FreeNodeHeader)) {
			// TODO(fsamuel): This should be done atomically. We don't want another thread to
			// allocate the same block before we have a chance to split it. If we fail to 
			// resize the block, we should go back to the free list and try again.
			current_free_node->size = block_size - bytes_needed;
			offset_type data_offset = ToOffset(current_free_node) + block_size - bytes_needed;
			return NewAllocatedNodeAtOffset(data_offset, bytes_needed);

		}
		// The block is large enough to use for allocation but NOT large enough to split.
		else if (block_size >= bytes_needed) {
			// Found a block that is large enough, remove it from the free list and return a pointer to its data
			if (prev_free_node != nullptr) {
				// TODO(fsamuel): Single pointer update, we should do this atomically or move
				// back to the beginning of the free list and try again.
				prev_free_node->next_offset = current_free_node->next_offset;
			}
			else {
				// The block we found is the first block in the free list, update the free list
				// offset. Again this needs to be done atomically or we need to move back to the
				// beginning of the free list and try again.
				// This design likey results in a lot of fragmentation. We should consider
				// using a free list that is sorted by size.
				if (state()->free_list_offset == ToOffset(current_free_node)) {
					state()->free_list_offset = current_free_node->next_offset;
				}
			}

			// Return a pointer to the data in the new block
			AllocatedNodeHeader* node_header_ptr = reinterpret_cast<AllocatedNodeHeader*>(current_free_node);
			return NewAllocatedNodeAtOffset(ToOffset(node_header_ptr), std::max(bytes_needed, block_size));
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

	// TODO(fsamuel): AllocatedNodes should not have next and previous pointers set.
	// This makes thread-safety difficult. We should consider removing these pointers
	AllocatedNodeHeader* prev_node = PreviousNode(current_node);
	if (prev_node != nullptr) {
		prev_node->next_offset = current_node->next_offset;
	}
	AllocatedNodeHeader* next_node = NextNode(current_node);
	if (next_node != nullptr) {
		next_node->prev_offset = current_node->prev_offset;
	}

	// Add the block to the free list
	AllocatorStateHeader* state_header_ptr = state();
	// If the allocation is at the front of the allocation list,
	// update the allocation offset to refer to the next allocation.
	if (state_header_ptr->allocation_offset == node_header_offset) {
		state_header_ptr->allocation_offset = current_node->next_offset;
	}
	FreeNodeHeader* free_node_ptr = ToPtr<FreeNodeHeader>(node_header_offset);
	free_node_ptr->size = current_node->size;
	free_node_ptr->next_offset = state_header_ptr->free_list_offset;
	// TODO(fsamuel): This can be done atomically. If this is stale, we can just try again.
	state_header_ptr->free_list_offset = ToOffset(free_node_ptr);
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

template<typename T>
typename SharedMemoryAllocator<T>::Iterator SharedMemoryAllocator<T>::begin() {
	AllocatorStateHeader* state_header_ptr = state();
	if (state_header_ptr->allocation_offset == -1)
		return Iterator(nullptr, this);
	return Iterator(ToPtr<AllocatedNodeHeader>(state_header_ptr->allocation_offset), this);
}

template<typename T>
typename SharedMemoryAllocator<T>::Iterator SharedMemoryAllocator<T>::end() {
	return Iterator(nullptr, this);
}

#endif  // SHARED_MEMORY_ALLOCATOR_H_
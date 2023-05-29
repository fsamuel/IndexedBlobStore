#ifndef SHARED_MEMORY_ALLOCATOR_H_
#define SHARED_MEMORY_ALLOCATOR_H_

#include <atomic>
#include <vector>

#include "chunk_manager.h"

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
	struct AllocatedNodeHeader;
	struct FreeNodeHeader;
	static constexpr std::size_t InvalidIndex = std::numeric_limits<std::size_t>::max();


	// Constructor that takes a reference to the shared memory buffer to be used for allocation
	explicit SharedMemoryAllocator(ChunkManager&& buffer);

	explicit SharedMemoryAllocator(SharedMemoryAllocator&& other);

	// Allocate memory for n objects of type T, and return a pointer to the first object
	T* Allocate(std::size_t bytes_requested);

	// Deallocate memory at the given pointer index
	bool Deallocate(std::size_t index);

	// Deallocate memory at the given pointer.
	bool Deallocate(T* ptr);

	// Returns the size of the allocated block at the given index.
	std::size_t GetCapacity(std::size_t index) const;

	// Returns the size of the allocated block at the given pointer.
	std::size_t GetCapacity(T* ptr);

	// Helper method to convert a pointer to an index relative to the start of the buffer
	std::uint64_t ToIndex(AllocatedNodeHeader* ptr) const;

	std::uint64_t ToIndex(FreeNodeHeader* ptr) const;

	template<typename U>
	std::uint64_t ToIndex(U* ptr) const {
		return ToIndexImpl(ptr, typename std::is_same<U, AllocatedNodeHeader>::type{},
			typename std::is_same<U, FreeNodeHeader>::type{});
	}

	// Helper method to convert an index relative to the start of the buffer to a pointer
	template<class U>
	const U* ToPtr(std::uint64_t index) const {
		if (index == InvalidIndex) {
			return nullptr;
		}
		return reinterpret_cast<const U*>(chunk_manager_.at(index));
	}

	template<class U>
	U* ToPtr(std::size_t index) {
		if (index == InvalidIndex) {
			return nullptr;
		}
		return reinterpret_cast<U*>(chunk_manager_.at(index));
	}

	void AddObserver(SharedMemoryAllocatorObserver* observer) {
		observers_.push_back(observer);
	}

	void RemoveObserver(SharedMemoryAllocatorObserver* observer) {
		observers_.erase(std::remove(observers_.begin(), observers_.end(), observer), observers_.end());
	}

	SharedMemoryAllocator& operator=(SharedMemoryAllocator&& other) noexcept {
		chunk_manager_ = std::move(other.chunk_manager_);
		return *this;
	}

private:
	// Header for the allocator state in the shared memory buffer
	struct AllocatorStateHeader {
		uint32_t magic_number;      // Magic number for verifying the allocator state header
		std::atomic<std::size_t> free_list_index; // index of the first free block in the free list
		std::atomic<std::size_t> num_chunks; // number of chunks in the chunk manager
	};

	AllocatorStateHeader* state() {
		uint8_t* buffer;
		std::size_t buffer_size;
		chunk_manager_.get_or_create_chunk(0, &buffer, &buffer_size);
		return reinterpret_cast<AllocatorStateHeader*>(buffer);
	}

	void NotifyObserversOfResize() {
		for (SharedMemoryAllocatorObserver* observer : observers_) {
			observer->OnBufferResize();
		}
	}

	// Header for an allocated node in the allocator
	struct AllocatedNodeHeader {
		std::size_t size; // Size of the allocated block, including the header
		// The index of the chunk in the chunk manager. This is necessary to convert
		// the pointer to an index relative to the start of the chunk.
		std::size_t chunk_index;
		// The magic number is used to verify that the header is valid.
		std::size_t signature;
	};

	// Given a pointer, returns the AllocatedNodeHeader.
	AllocatedNodeHeader* GetNode(T* ptr) {
		return reinterpret_cast<AllocatedNodeHeader*>(ptr) - 1;
	}

	// Header for a free node in the allocator
	struct FreeNodeHeader {
		// Size of the free block, including the header
		std::atomic<std::size_t> size;
		// index of the next free block in the free list
		std::atomic<std::size_t> next_index;
		std::size_t chunk_index; // index of the chunk in the chunk manager
		// For now we'll set it to 0xdeadbeef to make it easier to debug
		std::uint32_t signature; // signature for verifying the free node header
	};

	// Returns the first free node in the free list, or nullptr if there are no free nodes.
	FreeNodeHeader* FirstFreeNode() {
		return ToPtr<FreeNodeHeader>(state()->free_list_index);
	}

	// Returns the next node in the free list, or nullptr if there are no more nodes.
	FreeNodeHeader* NextFreeNode(FreeNodeHeader* node) {
		return ToPtr<FreeNodeHeader>(node->next_index);
	}

	void InitializeAllocatorStateIfNecessary() {
		// Check if the allocator state header has already been initialized
		AllocatorStateHeader* state_header_ptr = state();
		if (state_header_ptr->magic_number != 0x12345678) {
			// Initialize the allocator state header
			state_header_ptr->magic_number = 0x12345678;
			state_header_ptr->free_list_index = InvalidIndex;
			state_header_ptr->num_chunks = 1;
			AllocatedNodeHeader* allocated_node = reinterpret_cast<AllocatedNodeHeader*>(chunk_manager_.at(sizeof(AllocatorStateHeader)));
			allocated_node->size = chunk_manager_.capacity() - sizeof(AllocatorStateHeader);
			allocated_node->chunk_index = 0;
			allocated_node->signature = 0xBEEFCAFE;
			Deallocate(reinterpret_cast<T*>(allocated_node + 1));
		}

	}

	static std::size_t CalculateBytesNeeded(std::size_t bytes) {
		// Calculate the number of objects needed based on the requested size and the size of each object
		std::size_t n = bytes / sizeof(T);
		if (bytes % sizeof(T) != 0) {
			n++;
		}

		// Calculate the number of bytes needed for the memory block
		return std::max(sizeof(AllocatedNodeHeader) + sizeof(T) * n, sizeof(FreeNodeHeader)) ;
	}

	T* NewAllocatedNodeAtIndex(std::size_t index, std::size_t size) {
		AllocatedNodeHeader* allocated_node = ToPtr<AllocatedNodeHeader>(index);
		allocated_node->size = size;
		allocated_node->chunk_index = chunk_manager_.chunk_index(index);
		allocated_node->signature = 0xBEEFCAFE;
		return ToPtr<T>(index + sizeof(AllocatedNodeHeader));
	}

	std::uint64_t ToIndexImpl(AllocatedNodeHeader* ptr, std::true_type, std::false_type) const;

	std::uint64_t ToIndexImpl(FreeNodeHeader* ptr, std::false_type, std::true_type) const;

	template<typename U>
	std::uint64_t ToIndexImpl(U* ptr, std::false_type, std::false_type) const;

private:

	ChunkManager chunk_manager_;  // Reference to the shared memory buffer used for allocation
	std::vector<SharedMemoryAllocatorObserver*> observers_;
};

template<typename T>
SharedMemoryAllocator<T>::SharedMemoryAllocator(ChunkManager&& chunk_manager)
	: chunk_manager_(std::move(chunk_manager)) {

	InitializeAllocatorStateIfNecessary();
}

template<typename T>
SharedMemoryAllocator<T>::SharedMemoryAllocator(SharedMemoryAllocator&& other)
	: chunk_manager_(std::move(other.chunk_manager_)),
	observers_(std::move(other.observers_)) {
}

template<typename T>
T* SharedMemoryAllocator<T>::Allocate(std::size_t bytes_requested) {
	// Calculate the number of bytes needed for the memory block
	std::size_t bytes_needed = CalculateBytesNeeded(bytes_requested);

	InitializeAllocatorStateIfNecessary();

	FreeNodeHeader* current_free_node = FirstFreeNode();

	// Search the free list for a block of sufficient size
	FreeNodeHeader* prev_free_node = nullptr;
	while (current_free_node != nullptr) {
		std::size_t free_node_size = current_free_node->size;

		// If we've found a block greater than the size we need (and can accommodate a head
		// a second block, split it into two blocks. The first piece will be returned to the
		// free list, and the second piece will be returned to the caller.
		if (free_node_size > bytes_needed + sizeof(FreeNodeHeader)) {
			std::size_t new_free_node_size = free_node_size - bytes_needed;
			if (!current_free_node->size.compare_exchange_weak(free_node_size, new_free_node_size)) {
				// The block was resized by another thread, go back to the free list and try again.
				current_free_node = FirstFreeNode();
				continue;
			}
			std::size_t data_index = ToIndex(current_free_node) + free_node_size - bytes_needed;
			return NewAllocatedNodeAtIndex(data_index, bytes_needed);
		}

		// The block is large enough to use for allocation but NOT large enough to split.
		if (free_node_size >= bytes_needed) {
			// Found a block that is large enough, remove it from the free list and return a pointer to its data
			if (prev_free_node != nullptr) {
				std::size_t current_free_node_index = ToIndex(current_free_node);
				if (!prev_free_node->next_index.compare_exchange_weak(current_free_node_index, current_free_node->next_index)) {
					// The block was removed from the free list by another thread, go back to the
					// beginning of the free list and try again.
					current_free_node = FirstFreeNode();
					continue;
				}
			}
			else {
				// The block we found is the first block in the free list, update the free list
				// index. Again this needs to be done atomically or we need to move back to the
				// beginning of the free list and try again.
				// This design likey results in a lot of fragmentation. We should consider
				// using a free list that is sorted by size
				std::size_t current_free_node_index = ToIndex(current_free_node);
				if (!state()->free_list_index.compare_exchange_weak(current_free_node_index, current_free_node->next_index)) {
					// The block was removed from the free list by another thread, go back to the
					// beginning of the free list and try again.
					current_free_node = FirstFreeNode();
					continue;
				}
			}

			// Return a pointer to the data in the new block
			//AllocatedNodeHeader* node_header_ptr = reinterpret_cast<AllocatedNodeHeader*>(current_free_node);
			return NewAllocatedNodeAtIndex(ToIndex(current_free_node), std::max(bytes_needed, free_node_size));
		}

		// Move to the next block in the free list
		prev_free_node = current_free_node;
		current_free_node = NextFreeNode(current_free_node);
	}

	// No block of sufficient size was found, resize the buffer and allocate a new block.

	std::size_t last_num_chunks = state()->num_chunks.load();
	uint8_t* new_chunk_data;
	std::size_t new_chunk_size;
	if (chunk_manager_.get_or_create_chunk(last_num_chunks, &new_chunk_data, &new_chunk_size)) {
		AllocatedNodeHeader* allocated_node = reinterpret_cast<AllocatedNodeHeader*>(new_chunk_data);
		allocated_node->size = new_chunk_size;
		allocated_node->chunk_index = last_num_chunks;
		allocated_node->signature = 0xBEEFCAFE;
		Deallocate(chunk_manager_.encode_index(allocated_node->chunk_index, sizeof(AllocatedNodeHeader)));
		state()->num_chunks.compare_exchange_weak(last_num_chunks, last_num_chunks + 1);
	}
	// TODO(fsamuel): This might lead to stack overflow. We should consider
	// a loop that allocates a new chunk and then tries to allocate the block.
	return Allocate(bytes_requested);
}

// Deallocate memory at the given pointer index
template<typename T>
bool SharedMemoryAllocator<T>::Deallocate(std::size_t index) {
	if (index < 0)
		return false;
	T* ptr = ToPtr<T>(index);
	return Deallocate(ptr);
}

template<typename T>
bool SharedMemoryAllocator<T>::Deallocate(T* ptr) {
	AllocatedNodeHeader* current_node = GetNode(ptr);
	if (current_node == nullptr || current_node->signature != 0xBEEFCAFE) {
		// The pointer is not a valid allocation
		return false;
	}
	std::size_t chunk_index = current_node->chunk_index;
	std::size_t node_header_index = ToIndex(current_node);

	// Add the block to the free list
	while (true) {
		// Get the current head of the free list
		std::size_t free_list_index = state()->free_list_index;
		// Set the next pointer of the new free node to the current head of the free list
		FreeNodeHeader* free_node_ptr = ToPtr<FreeNodeHeader>(node_header_index);
		if (free_node_ptr->signature == 0xdeadbeef) {
			// This block was already freed, this is a double free.
			return false;
		}
		free_node_ptr->chunk_index = chunk_index;
		free_node_ptr->next_index.store(free_list_index);
		free_node_ptr->signature = 0xdeadbeef;
		// Try to set the head of the free list to the new free node
		if (state()->free_list_index.compare_exchange_weak(free_list_index, node_header_index)) {
			return true;
		}
		// The head of the free list was updated by another thread, try again
	}
}

template<typename T>
std::size_t SharedMemoryAllocator<T>::GetCapacity(std::size_t index) const {
	if (index < 0) {
		return 0;
	}
	std::size_t node_header_index = index - sizeof(AllocatedNodeHeader);
	const AllocatedNodeHeader* current_node = ToPtr<AllocatedNodeHeader>(node_header_index);
	return (current_node->size - sizeof(AllocatedNodeHeader)) / sizeof(T);
}

// Returns the size of the allocated block at the given pointer.
template<typename T>
std::size_t SharedMemoryAllocator<T>::GetCapacity(T* ptr) {
	if (ptr == nullptr) {
		return 0;
	}
	std::size_t node_header_index = ToIndex(reinterpret_cast<AllocatedNodeHeader*>(ptr) - 1);
	AllocatedNodeHeader* curent_node = ToPtr<AllocatedNodeHeader>(node_header_index);
	return (curent_node->size - sizeof(AllocatedNodeHeader)) / sizeof(T);
}

template<typename T>
std::uint64_t SharedMemoryAllocator<T>::ToIndex(AllocatedNodeHeader* ptr) const {
	uint8_t* data;
	std::size_t chunk_size;
	chunk_manager_.get_or_create_chunk(ptr->chunk_index, &data, &chunk_size);
	return chunk_manager_.encode_index(ptr->chunk_index, reinterpret_cast<uint8_t*>(ptr) - data);
}

template<typename T>
std::uint64_t SharedMemoryAllocator<T>::ToIndex(FreeNodeHeader* ptr) const {
	uint8_t* data;
	std::size_t chunk_size;
	chunk_manager_.get_or_create_chunk(ptr->chunk_index, &data, &chunk_size);
	return chunk_manager_.encode_index(ptr->chunk_index, reinterpret_cast<uint8_t*>(ptr) - data);
}

template<typename T>
std::uint64_t SharedMemoryAllocator<T>::ToIndexImpl(AllocatedNodeHeader* ptr, std::true_type, std::false_type) const {
	uint8_t* data;
	std::size_t chunk_size;
	chunk_manager_.get_or_create_chunk(ptr->chunk_index, &data, &chunk_size);
	return chunk_manager_.encode_index(ptr->chunk_index, reinterpret_cast<uint8_t*>(ptr) - data);
}

// Implementation of ToIndex for FreeNodeHeader*
template<typename T>
std::uint64_t SharedMemoryAllocator<T>::ToIndexImpl(FreeNodeHeader* ptr, std::false_type, std::true_type) const {
	uint8_t* data;
	std::size_t chunk_size;
	chunk_manager_.get_or_create_chunk(ptr->chunk_index, &data, &chunk_size);
	return chunk_manager_.encode_index(ptr->chunk_index, reinterpret_cast<uint8_t*>(ptr) - data);
}

// Implementation of ToIndex for other types
template<typename T>
template<typename U>
std::uint64_t SharedMemoryAllocator<T>::ToIndexImpl(U* ptr, std::false_type, std::false_type) const {
	AllocatedNodeHeader* allocated_node = reinterpret_cast<AllocatedNodeHeader*>(
		reinterpret_cast<uint8_t*>(ptr) - sizeof(AllocatedNodeHeader));
	return ToIndexImpl(allocated_node, std::true_type{}, std::false_type{}) + sizeof(AllocatedNodeHeader);
}

#endif  // SHARED_MEMORY_ALLOCATOR_H_
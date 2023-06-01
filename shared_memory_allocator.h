#ifndef SHARED_MEMORY_ALLOCATOR_H_
#define SHARED_MEMORY_ALLOCATOR_H_

#include <atomic>
#include <cassert>
#include <vector>

#include "chunk_manager.h"

// A simple allocator that allocates memory from a shared memory buffer. The allocator
// maintains a free list of available blocks of memory. When a block is allocated, it is
// removed from the free list. When a block is freed, it is added back to the free list.
// The allocator does not attempt to coalesce adjacent free blocks.
// The allocator is designed to be lock-free by using atomic operations to update the
// allocator state: size of a free block, or the head of the free list.
template <typename T>
class SharedMemoryAllocator {
public:

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

	template<typename U>
	std::uint64_t ToIndex(U* ptr) const {
		return ToIndexImpl(ptr, typename std::is_same<U, Node>::type{});
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
		return reinterpret_cast<AllocatorStateHeader*>(chunk_manager_.at(0, 0));
	}

	// Header for a free/allocated node in the allocator
	struct Node {
		// Version number for detecting state changes in the node.
		std::atomic<std::uint32_t> version;
		// The index of the chunk in the chunk manager. This is necessary to convert
		// the pointer to an index relative to the start of the chunk.
		std::size_t chunk_index;
		// Size of the block, including the header
		std::atomic<std::size_t> size;
		// index of the next free block in the free list
		std::atomic<std::size_t> next_index;
		std::size_t signature;

		bool is_allocated() const {
			return !is_free();
		}

		bool is_free() const {
			return (version.load() & 0x1) == 0;
		}

		bool is_free_seen(std::uint32_t* v) const {
			*v = version.load();
			return (*v & 0x1) == 0;
		}
	};

	// Given a pointer, returns the Node.
	Node* GetNode(T* ptr) const {
		return reinterpret_cast<Node*>(ptr) - 1;
	}

	// Returns the first free node in the free list, or nullptr if there are no free nodes.
	Node* FirstFreeNode() {
		return ToPtr<Node>(state()->free_list_index.load());
	}

	// Returns the next node in the free list, or nullptr if there are no more nodes.
	Node* NextFreeNode(Node* node) {
		return ToPtr<Node>(node->next_index.load());
	}

	void InitializeAllocatorStateIfNecessary() {
		// Check if the allocator state header has already been initialized
		AllocatorStateHeader* state_header_ptr = state();
		if (state_header_ptr->magic_number != 0x12345678) {
			// Initialize the allocator state header
			state_header_ptr->magic_number = 0x12345678;
			state_header_ptr->free_list_index = InvalidIndex;
			state_header_ptr->num_chunks = 1;

			uint8_t* data = chunk_manager_.at(sizeof(AllocatorStateHeader));
			T* buffer = NewAllocatedNode(data, 0, chunk_manager_.capacity() - sizeof(AllocatorStateHeader));
			// An initial allocation always starts at verison 1.
			GetNode(buffer)->version.store(1);
			Deallocate(buffer);
		}

	}

	static std::size_t CalculateBytesNeeded(std::size_t bytes) {
		// Calculate the number of objects needed based on the requested size and the size of each object
		std::size_t n = bytes / sizeof(T);
		if (bytes % sizeof(T) != 0) {
			n++;
		}

		// Calculate the number of bytes needed for the memory block
		return std::max(sizeof(Node) + sizeof(T) * n, sizeof(Node)) ;
	}

	T* NewAllocatedNode(uint8_t* buffer, std::size_t chunk_index, std::size_t size) {
		Node* allocated_node = reinterpret_cast<Node*>(buffer);
		allocated_node->size = size;
		allocated_node->chunk_index = chunk_index;
		allocated_node->next_index.store(InvalidIndex);
		allocated_node->version.fetch_add(1);
		allocated_node->signature = 0xbeefcafe;
		return reinterpret_cast<T*>(allocated_node + 1);
	}

	std::uint64_t ToIndexImpl(Node* ptr, std::true_type) const;


	template<typename U>
	std::uint64_t ToIndexImpl(U* ptr, std::false_type) const;

	// Allocates space from a free node that can fit the requested size.
    //	Returns nullptr if no free node is found.
	T* AllocateFromFreeNode(std::size_t bytes_needed);


private:
	// Reference to the shared memory buffer used for allocation
	ChunkManager chunk_manager_;  
};

template<typename T>
SharedMemoryAllocator<T>::SharedMemoryAllocator(ChunkManager&& chunk_manager)
	: chunk_manager_(std::move(chunk_manager)) {

	InitializeAllocatorStateIfNecessary();
}

template<typename T>
SharedMemoryAllocator<T>::SharedMemoryAllocator(SharedMemoryAllocator&& other)
	: chunk_manager_(std::move(other.chunk_manager_)) {
}

template<typename T>
T* SharedMemoryAllocator<T>::Allocate(std::size_t bytes_requested) {
	// Calculate the number of bytes needed for the memory block
	std::size_t bytes_needed = CalculateBytesNeeded(bytes_requested);

	while (true) {
		T* data = AllocateFromFreeNode(bytes_needed);
		if (data != nullptr) {
			return data;
		}
		// No block of sufficient size was found, resize the buffer and allocate a new block.

		std::size_t last_num_chunks = state()->num_chunks.load();
		uint8_t* new_chunk_data;
		std::size_t new_chunk_size;
		if (chunk_manager_.get_or_create_chunk(last_num_chunks, &new_chunk_data, &new_chunk_size) > 0) {
			T* buffer = NewAllocatedNode(new_chunk_data, last_num_chunks, new_chunk_size);
			GetNode(buffer)->version.store(1);
			Deallocate(buffer);
			bool success = state()->num_chunks.compare_exchange_strong(last_num_chunks, last_num_chunks + 1);
			assert(success);
		}
	}
}

// Deallocate memory at the given pointer index
template<typename T>
bool SharedMemoryAllocator<T>::Deallocate(std::size_t index) {
	T* ptr = ToPtr<T>(index);
	return Deallocate(ptr);
}

template<typename T>
bool SharedMemoryAllocator<T>::Deallocate(T* ptr) {
	Node* node = GetNode(ptr);
	if (node == nullptr || !node->is_allocated()) {
		// The pointer is not a valid allocation.
		return false;
	}

	std::size_t node_header_index = ToIndex(node);
	assert(node_header_index != InvalidIndex);

	// This should only be done once.
	node->version.fetch_add(1);

	// Add the block to the free list
	while (true) {
		// Get the current head of the free list
		std::size_t free_list_index = state()->free_list_index.load();
		// Set the next pointer of the new free node to the current head of the free list
		node->next_index.store(free_list_index);
		assert(node->signature == 0xbeefcafe);
//		assert(node->is_free());
		std::uint32_t version;
		if (!node->is_free_seen(&version)) {
			return false;
		}
		// Try to set the head of the free list to the new free node
		if (state()->free_list_index.compare_exchange_weak(free_list_index, node_header_index)) {
			
			// The head of the free list was updated, the block was successfully freed	
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
	std::size_t node_header_index = index - sizeof(Node);
	const Node* current_node = ToPtr<Node>(node_header_index);
	return (current_node->size.load() - sizeof(Node)) / sizeof(T);
}

// Returns the size of the allocated block at the given pointer.
template<typename T>
std::size_t SharedMemoryAllocator<T>::GetCapacity(T* ptr) {
	if (ptr == nullptr) {
		return 0;
	}
	Node* curent_node = GetNode(ptr);
	return (curent_node->size - sizeof(Node)) / sizeof(T);
}

template<typename T>
std::uint64_t SharedMemoryAllocator<T>::ToIndexImpl(Node* ptr, std::true_type) const {
	std::uint32_t version = ptr->version.load();
	std::size_t chunk_index = ptr->chunk_index;
	const uint8_t* data = chunk_manager_.get_chunk_start(chunk_index);
	if (ptr->version.load() != version || data == nullptr) {
		return InvalidIndex;
	}
	return chunk_manager_.encode_index(chunk_index, reinterpret_cast<uint8_t*>(ptr) - data);
}

// Implementation of ToIndex for other types
template<typename T>
template<typename U>
std::uint64_t SharedMemoryAllocator<T>::ToIndexImpl(U* ptr, std::false_type) const {
	Node* allocated_node = GetNode(ptr);
	return ToIndexImpl(allocated_node, std::true_type{}) + sizeof(Node);
}

template<typename T>
T* SharedMemoryAllocator<T>::AllocateFromFreeNode(std::size_t bytes_needed) {
	Node* prev_free_node = nullptr;
	std::uint32_t prev_free_node_version = 0;
	Node* current_free_node = FirstFreeNode();
	//if (current_free_)
	//assert(current_free_node->signature == 0xbeefcafe);
	// Search the free list for a block of sufficient sizes
	while (current_free_node != nullptr) {
		assert(current_free_node->signature == 0xbeefcafe);
		std::uint32_t version = current_free_node->version.load();
		std::size_t free_node_size = current_free_node->size.load();
		if (current_free_node->version.load() != version ||
			!current_free_node->is_free()) {
			assert(current_free_node->signature == 0xbeefcafe);
			current_free_node = FirstFreeNode();
			continue;
		}


		// If we've found a block greater than the size we need (and can accommodate a head
		// a second block, split it into two blocks. The first piece will be returned to the
		// free list, and the second piece will be returned to the caller.
		if (free_node_size > bytes_needed + sizeof(Node)) {
			std::size_t new_free_node_size = free_node_size - bytes_needed;
			if (!current_free_node->size.compare_exchange_weak(free_node_size, new_free_node_size)) {
				// The block was resized by another thread, go back to the free list and try again.
				prev_free_node = nullptr;
				prev_free_node_version = 0;
				current_free_node = FirstFreeNode();
				continue;
			}
			std::size_t chunk_index = current_free_node->chunk_index;
			uint8_t* data = reinterpret_cast<uint8_t*>(current_free_node) +
				free_node_size - bytes_needed;
			T* node_data = NewAllocatedNode(data, chunk_index, bytes_needed);
			GetNode(node_data)->version.store(1);
			return node_data;
		}

		// The block is large enough to use for allocation but NOT large enough to split.
		if (free_node_size >= bytes_needed) {
			// Found a block that is large enough, remove it from the free list and return a pointer to its data.
			if (prev_free_node != nullptr) {
				std::size_t current_free_node_index = ToIndex(current_free_node);
				if (current_free_node_index == InvalidIndex || !prev_free_node->next_index.compare_exchange_weak(current_free_node_index, current_free_node->next_index.load()) || !prev_free_node->is_free() || prev_free_node->version.load() != prev_free_node_version) {
					// Checking prev_free_node's node_type is safe because we know that
					// both free and allocated nodes have the same structure, and we 
					// do not currently coalesce nodes.
					// The block was removed from the free list by another thread, go back to the
					// beginning of the free list and try again.
					prev_free_node = nullptr;
					prev_free_node_version = 0;
					current_free_node = FirstFreeNode();
					continue;
				}
			}
			else {
				// The block we found is the first block in the free list, update the free
				// list index. Again this needs to be done atomically or we need to move
				// back to the beginning of the free list and try again. This design
				// likely results in a lot of fragmentation. We should consider using a 
				// free list that is sorted by size
				std::size_t current_free_node_index = ToIndex(current_free_node);
				if (current_free_node_index == InvalidIndex || !state()->free_list_index.compare_exchange_weak(current_free_node_index, current_free_node->next_index.load())) {
					// The block was removed from the free list by another thread, go back to the
					// beginning of the free list and try again.
					prev_free_node = nullptr;
					prev_free_node_version = 0;
					current_free_node = FirstFreeNode();
					continue;
				}
			}

			// Return a pointer to the data in the new block
			std::size_t chunk_index = current_free_node->chunk_index;
			uint32_t version;
			if (!current_free_node->is_free_seen(&version)) {
				assert(false);
			}
			return NewAllocatedNode(reinterpret_cast<uint8_t*>(current_free_node), chunk_index, std::max(bytes_needed, free_node_size));
		}

		// Move to the next block in the free list
		prev_free_node = current_free_node;
		prev_free_node_version = current_free_node->version.load();
		current_free_node = NextFreeNode(current_free_node);
	}

	return nullptr;
}

#endif  // SHARED_MEMORY_ALLOCATOR_H_
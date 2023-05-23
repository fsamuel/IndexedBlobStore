#ifndef SHARED_MEMORY_ALLOCATOR_H_
#define SHARED_MEMORY_ALLOCATOR_H_

#include "shared_memory_buffer.h"
#include <vector>

#ifdef _WIN32
#include <Windows.h>
#undef max
#else
#include <unistd.h>
#include <sys/mman.h>
#endif

class SharedMemoryAllocatorObserver {
public:
	virtual void OnBufferResize() = 0;
};

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
	U* ToPtr(offset_type offset)  {
		return reinterpret_cast<U*>(reinterpret_cast<char*>(buffer_.data()) + offset);
	}

	// Header for the allocator state in the shared memory buffer
	struct AllocatorStateHeader {
		uint32_t magic_number;      // Magic number for verifying the allocator state header
		offset_type free_list_offset; // Offset of the first free block in the free list
		offset_type allocation_offset; // Offset of the first allocation block.
	};

	// Constructor that takes a reference to the shared memory buffer to be used for allocation
	explicit SharedMemoryAllocator(SharedMemoryBuffer&& buffer)
		: buffer_(std::move(buffer)) {
		// Check if the buffer is large enough to hold the allocator state header
		if (buffer_.size() < sizeof(AllocatorStateHeader)) {
			buffer_.Resize(sizeof(AllocatorStateHeader));
		}

		InitializeAllocatorStateIfNecessary();
	}

	explicit SharedMemoryAllocator(SharedMemoryAllocator&& other)
		: buffer_(std::move(other.buffer_)),
		  observers_(std::move(other.observers_)){
	}

	AllocatorStateHeader* state() {
		return reinterpret_cast<AllocatorStateHeader*>(buffer_.data());
	}

	const std::string& buffer_name() const {
		return buffer_.name();
	}

	void AddObserver(SharedMemoryAllocatorObserver* observer) {
		observers_.push_back(observer);
	}

	void RemoveObserver(SharedMemoryAllocatorObserver* observer) {
		observers_.erase(std::remove(observers_.begin(), observers_.end(), observer), observers_.end());
	}

	// Allocate memory for n objects of type T, and return a pointer to the first object
	T* Allocate(size_type bytes) {
		// Calculate the number of bytes needed for the memory block
		size_type bytes_needed = CalculateBytesNeeded(bytes);

		InitializeAllocatorStateIfNecessary();

		offset_type free_list_offset = state()->free_list_offset;

		// Search the free list for a block of sufficient size
		offset_type* prev_free_list_offset_ptr = nullptr;
		while (free_list_offset != -1) {
			// Get a pointer to the current block and its size
			FreeNodeHeader* current_block_ptr = ToPtr<FreeNodeHeader>(free_list_offset);
			size_type block_size = current_block_ptr->size;

			if (block_size > bytes_needed + sizeof(FreeNodeHeader)) {
				current_block_ptr->size = block_size - bytes_needed;
				offset_type dataOffset = free_list_offset + block_size - bytes_needed;
				return NewAllocatedNodeAtOffset(dataOffset, bytes_needed);

			}
			else if (block_size >= bytes_needed) {
				// Found a block that is large enough, remove it from the free list and return a pointer to its data
				if (prev_free_list_offset_ptr) {
					*prev_free_list_offset_ptr = current_block_ptr->next_offset;
				}
				else {
					if (state()->free_list_offset == free_list_offset) {
						state()->free_list_offset = current_block_ptr->next_offset;
					}
					free_list_offset = current_block_ptr->next_offset;
				}

				// Return a pointer to the data in the new block
				AllocatedNodeHeader* node_header_ptr = reinterpret_cast<AllocatedNodeHeader*>(current_block_ptr);
				return NewAllocatedNodeAtOffset(ToOffset(node_header_ptr), std::max(bytes_needed, block_size));
			}

			// Move to the next block in the free list
			prev_free_list_offset_ptr = &(current_block_ptr->next_offset);
			free_list_offset = current_block_ptr->next_offset;
		}

		// No block of sufficient size was found, resize the buffer and allocate a new block
		offset_type data_offset = buffer_.size();
		buffer_.Resize(buffer_.size() + bytes_needed);

		// Return a pointer to the data in the new block
		T* new_node = NewAllocatedNodeAtOffset(data_offset, bytes_needed);
		NotifyObserversOfResize();

		return new_node;
	}

	// Deallocate memory at the given pointer offset
	void Deallocate(offset_type offset) {
		if (offset < 0)
			return;
		T* ptr = ToPtr<T>(offset);
		Deallocate(ptr);
	}

	// Deallocate memory at the given pointer
	void Deallocate(T* ptr) {
		// Calculate the offset of the allocated node header
		offset_type node_header_offset = ToOffset(reinterpret_cast<AllocatedNodeHeader*>(ptr) - 1);
		AllocatedNodeHeader* current_node = ToPtr<AllocatedNodeHeader>(node_header_offset);
		if (current_node->prev_offset != -1) {
			AllocatedNodeHeader* prev_node = ToPtr<AllocatedNodeHeader>(current_node->prev_offset);
			prev_node->next_offset = current_node->next_offset;
		}
		if (current_node->next_offset != -1) {
			AllocatedNodeHeader* next_node = ToPtr<AllocatedNodeHeader>(current_node->next_offset);
			next_node->prev_offset = current_node->prev_offset;

		}
		// Add the block to the free list
		AllocatorStateHeader* state_header_ptr = reinterpret_cast<AllocatorStateHeader*>(buffer_.data());
		if (state_header_ptr->allocation_offset == node_header_offset) {
			if (current_node->next_offset != node_header_offset) {
				state_header_ptr->allocation_offset = current_node->next_offset;
			}
			else {
				state_header_ptr->allocation_offset = -1;
			}
		}
		FreeNodeHeader* free_node_ptr = ToPtr<FreeNodeHeader>(node_header_offset);
		free_node_ptr->size = current_node->size;
		free_node_ptr->next_offset = state_header_ptr->free_list_offset;
		state_header_ptr->free_list_offset = ToOffset(free_node_ptr);
	}

	size_type GetCapacity(offset_type offset) const {
		if (offset < 0) {
			return 0;
		}
		offset_type node_header_offset = offset - sizeof(AllocatedNodeHeader);
		const AllocatedNodeHeader* current_node = ToPtr<AllocatedNodeHeader>(node_header_offset);
		return (current_node->size - sizeof(AllocatedNodeHeader)) / sizeof(T);
	}

	size_type GetCapacity(T* ptr) {
		if (ptr == nullptr) {
			return 0;
		}
		offset_type node_header_offset = ToOffset(reinterpret_cast<AllocatedNodeHeader*>(ptr) - 1);
		AllocatedNodeHeader* curent_node = ToPtr<AllocatedNodeHeader>(node_header_offset);
		return (curent_node->size - sizeof(AllocatedNodeHeader)) / sizeof(T);
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

	// Return an iterator to the first allocated object
	auto begin() {
		AllocatorStateHeader* state_header_ptr = reinterpret_cast<AllocatorStateHeader*>(buffer_.data());
		if (state_header_ptr->allocation_offset == -1)
			return Iterator(nullptr, this);
		return Iterator(ToPtr<AllocatedNodeHeader>(state_header_ptr->allocation_offset), this);
	}

	auto begin() const {
		const AllocatorStateHeader* state_header_ptr = reinterpret_cast<const AllocatorStateHeader*>(buffer_.data());
		if (state_header_ptr->allocation_offset == -1)
			return ConstIterator(nullptr, this);
		return ConstIterator(ToPtr<AllocatedNodeHeader>(state_header_ptr->allocation_offset), this);
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
		auto last_allocation = begin();
		auto first_allocation = --begin();
		if (first_allocation == end())
			return last_allocation;
		return first_allocation;
	}

	auto last() {
		return begin();
	}

	auto last() const{
		return begin();
	}

private:
	void NotifyObserversOfResize() {
		for (SharedMemoryAllocatorObserver* observer : observers_) {
			observer->OnBufferResize();
		}
	}

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
	// Header for a free node in the allocator
	struct FreeNodeHeader {
		size_type size;        // Size of the free block, including the header
		offset_type next_offset; // Offset of the next free block in the free list
	};

	void InitializeAllocatorStateIfNecessary() {
		// Check if the allocator state header has already been initialized
		AllocatorStateHeader* state_header_ptr = reinterpret_cast<AllocatorStateHeader*>(buffer_.data());
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
		if (state()->allocation_offset != -1) {
			AllocatedNodeHeader* next_node = ToPtr<AllocatedNodeHeader>(state()->allocation_offset);
			node_header_ptr->next_offset = state()->allocation_offset;
			node_header_ptr->prev_offset = next_node->prev_offset;
			next_node->prev_offset = offset;
		} else {
			node_header_ptr->next_offset = offset;
			node_header_ptr->prev_offset = offset;
		}
		state()->allocation_offset = offset;

		return ToPtr<T>(offset + sizeof(AllocatedNodeHeader));
	}

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

			if ((node_ptr_->next_offset == -1) || (node_ptr_->next_offset == allocator_->ToOffset(node_ptr_))) {
				node_ptr_ = nullptr;
			} else {
				node_ptr_ = allocator_->ToPtr<AllocatedNodeHeader>(node_ptr_->next_offset);
			}
			return *this;
		}

		Iterator& operator--() {
			if (node_ptr_ == nullptr)
				return *this;

			if ((node_ptr_->prev_offset == -1) || (node_ptr_->prev_offset == allocator_->ToOffset(node_ptr_))) {
				node_ptr_ = nullptr;
			}
			else {
				node_ptr_ = allocator_->ToPtr<AllocatedNodeHeader>(node_ptr_->prev_offset);
			}
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

	// Iterator class for iterating over the allocated nodes in the allocator
	class ConstIterator {
	public:
		ConstIterator(const AllocatedNodeHeader* nodePtr,
			const SharedMemoryAllocator* allocator) :
			node_ptr_(nodePtr), allocator_(allocator) {}

		const T& operator*() const {
			return *reinterpret_cast<const T*>(node_ptr_ + 1);
		}

		const T* operator->() const {
			return reinterpret_cast<const T*>(node_ptr_ + 1);
		}

		ConstIterator& operator++() {
			if (node_ptr_ == nullptr)
				return *this;
			if (node_ptr_->next_offset == -1) {
				node_ptr_ = nullptr;
			}
			else {
				node_ptr_ = allocator_->ToPtr<AllocatedNodeHeader>(node_ptr_->next_offset);
			}
			return *this;
		}

		ConstIterator& operator--() {
			if (node_ptr_ == nullptr)
				return *this;

			if ((node_ptr_->prev_offset == -1) || (node_ptr_->prev_offset == allocator_->ToOffset(node_ptr_))) {
				node_ptr_ = nullptr;
			}
			else {
				node_ptr_ = allocator_->ToPtr<AllocatedNodeHeader>(node_ptr_->prev_offset);
			}
			return *this;
		}

		bool operator==(const ConstIterator& other) const {
			return node_ptr_ == other.node_ptr_;
		}

		bool operator!=(const ConstIterator& other) const {
			return node_ptr_ != other.node_ptr_;
		}

	private:
		const AllocatedNodeHeader* node_ptr_;
		const SharedMemoryAllocator* allocator_;
	};

	SharedMemoryBuffer buffer_;  // Reference to the shared memory buffer used for allocation
	std::vector<SharedMemoryAllocatorObserver*> observers_;
};

#endif  // SHARED_MEMORY_ALLOCATOR_H_
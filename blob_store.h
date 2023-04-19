#ifndef BLOB_STORE_H_
#define BLOB_STORE_H_

#include <cstdio>
#include <sys/types.h>
#include <string>
#include <cstring>
#include <iostream>

#include "shared_Memory_allocator.h"
#include "shared_memory_vector.h"

#ifdef _WIN64
typedef __int64 ssize_t;
#else
typedef int ssize_t;
#endif

class BlobStore;

class BlobStoreObserver {
public:
	// Indicates that memory has been reallocated either through compaction
	// or buffer remapping. This means that any pointers to memory in the
	// allocator are no longer valid.
	virtual void OnMemoryReallocated() = 0;

	// Indicates that a Blob with the given index has been removed from the store.
	virtual void OnDroppedBlob(size_t index) = 0;
};


// BlobStoreObject is a wrapper around BlobStore that provides a safe way to access
// objects stored in a BlobStore instance. It automatically updates its internal
// pointer to the object whenever the memory in BlobStore is reallocated, ensuring
// that the clients don't store stale pointers. It also implements the BlobStoreObserver
// interface to receive notifications from the BlobStore when the memory is reallocated.
//
// Usage:
//   BlobStoreObject<MyClass> obj(&blobStore, index);
//   obj->myMethod();
//   MyClass& obj = *obj;
//
template <typename T>
class BlobStoreObject : public BlobStoreObserver {
public:
	// Constructor: Initializes the object with a nullptr.
	BlobStoreObject()
		: store_(nullptr)
		, index_(BlobStore::InvalidIndex)
		, ptr_(nullptr) {
	}

	// Constructor: Initializes the object with a pointer to the BlobStore and the
	// index of the object. It also registers itself as an observer to the BlobStore
	// and updates the internal pointer to the object.
	BlobStoreObject(BlobStore* store, size_t index);

	// Destructor: Unregisters the object from the BlobStore's observer list.
	~BlobStoreObject() {
		if (store_) {
			store_->RemoveObserver(this);
		}
	}

	// Arrow operator: Provides access to the object's methods.
	T* operator->() {
		return ptr_;
	}

	const T* operator->() const {
		return ptr_;
	}

	// Dereference operator: Provides access to the object itself.
	T& operator*() {
		return *ptr_;
	}

	const T& operator*() const {
		return *ptr_;
	}

	// Returns the index of the Blob.
	size_t Index() const {
		return index_;
	}

	template<typename U>
	BlobStoreObject<U> To() {
		return BlobStoreObject<U>(store_, index_);
	}

	BlobStoreObject& operator=(const BlobStoreObject& other) {
		store_ = other.store_;
		index_ = other.index_;
		ptr_ = other.ptr_;
		return *this;
	}

	BlobStoreObject& operator=(std::nullptr_t) {
		store_ = nullptr;
		index_ = BlobStore::InvalidIndex;
		ptr_ = nullptr;
		return *this;
	}

	bool operator!() const // Enables "if (!sp) ..."
	{
		return store_ == nullptr || index_ == BlobStore::InvalidIndex || ptr_ == nullptr;
	}

	inline friend bool operator==(const BlobStoreObject& lhs, const T* rhs)
	{
		return lhs.ptr_ == rhs;
	}

	inline friend bool operator==(const T* lhs, const BlobStoreObject& rhs)
	{
		return lhs == rhs.ptr_;
	}

	inline friend bool operator!=(const BlobStoreObject& lhs, const T* rhs)
	{
		return lhs.ptr_ != rhs;
	}

	inline friend bool operator!=(const T* lhs, const BlobStoreObject& rhs)
	{
		return lhs != rhs.ptr_;
	}

	inline friend bool operator==(const BlobStoreObject& lhs, const BlobStoreObject& rhs)
	{
		return lhs.ptr_ == rhs.ptr_;
	}

	inline friend bool operator!=(const BlobStoreObject& lhs, const BlobStoreObject& rhs)
	{
		return lhs.ptr_ != rhs.ptr_;
	}

	// OnMemoryReallocated: Method from BlobStoreObserver interface that gets called
	// when the memory in the BlobStore is reallocated. Updates the internal pointer
	// to the object.
	void OnMemoryReallocated() override {
		UpdatePointer();
	}

	// OnDroppedBlob: Method from BlobStoreObserver interface that gets called when a blob
	// is dropped from the BlobStore. If the dropped blob is the one that this object points to
	// then it sets the internal pointer to nullptr.
	void OnDroppedBlob(size_t index) override {
		if (index == index_) {
			ptr_ = nullptr;
			index = BlobStore::InvalidIndex;
		}
	}

private:
	// UpdatePointer: Helper method to update the internal pointer to the object using
	// the BlobStore's GetPointer method.
	void UpdatePointer();

	BlobStore* store_; // Pointer to the BlobStore instance
	size_t index_;     // Index of the object in the BlobStore
	T* ptr_;           // Pointer to the object
};


// BlobStore is a class that manages the storage and retrieval of objects
// (blobs) in shared memory. It supports storing, getting, and deleting
// objects while maintaining a compact memory footprint.
class BlobStore : public SharedMemoryAllocatorObserver {
public:
	using Allocator = SharedMemoryAllocator<char>;
	using offset_type = typename Allocator::offset_type;
	using index_type = typename SharedMemoryVector<offset_type>::size_type;

	static constexpr index_type InvalidIndex = static_cast<index_type>(-1);

	// Constructor that initializes the BlobStore with the provided metadata and data shared memory buffers.
	BlobStore(SharedMemoryBuffer&& metadataBuffer, SharedMemoryBuffer&& dataBuffer);

	// BlobStore destructor
	~BlobStore();

	// Puts an object of type T with the provided arguments into the BlobStore and returns a BlobStoreObject.
	template <typename T, typename... Args>
	BlobStoreObject<T> Put(Args&&... args);

	template <typename T, typename... Args>
	BlobStoreObject<T> Put(size_t size, Args&&... args);

	// Gets the object of type T at the specified index.
	template<typename T>
	T* Get(size_t index) {
		if (index >= metadata_.size() || metadata_[index].next_free_index != -1) {
			return nullptr;
		}
		BlobMetadata& metadata_entry = metadata_[index];
		if (metadata_entry.size == 0) {
			return nullptr;
		}
		return allocator_.ToPtr<T>(metadata_entry.offset);
	}

	// Gets the object of type T at the specified index as a constant.
	template<typename T>
	const T* Get(size_t index) const {
		return const_cast<BlobStore*>(this)->Get<T>(index);
	}

	// Gets the object as a char pointer at the specified index.
	char* operator[](size_t index) {
		return Get<char>(index);
	}

	// Gets the object as a constant char pointer at the specified index.
	const char* operator[](size_t index) const {
		return Get<char>(index);
	}

	// Used for Debugging Purposes; prints out the value stored at the specified index.
	template<typename T>
	void Print(size_t index) {
		T* ptr = Get<T>(index);
		if (ptr == nullptr) {
			std::cout << "Invalid index" << std::endl;
			return;
		}
		std::cout << *ptr << std::endl;
	}

	// Drops the object at the specified index, freeing the associated memory.
	void Drop(size_t index);

	// Compacts the BlobStore, removing any unused space between objects.
	void Compact();

	// Returns the number of stored objects in the BlobStore.
	size_t GetSize() const {
		// The first slot in the metadata vector is always reserved for the free list.
		size_t size = metadata_.size() - 1;
		size -= GetFreeSlotCount();
		return size;
	}

	// Returns whether the BlobStore is empty.
	bool IsEmpty() const {
		return GetSize() == 0;
	}

	// Adds a BlobStoreObserver.
	void AddObserver(BlobStoreObserver* observer);

	// Removes a BlobStoreObserver.
	void RemoveObserver(BlobStoreObserver* observer);

	// Iterator class for BlobStore
	class Iterator {
	public:
		using iterator_category = std::bidirectional_iterator_tag;
		using value_type = char;
		using difference_type = std::ptrdiff_t;
		using pointer = value_type*;
		using reference = value_type&;

		Iterator(BlobStore* store, size_t index) : store_(store), index_(index) {
			AdvanceToValidIndex();
		}

		size_t size() const {
			return store_->metadata_[index_].size;
		}

		size_t index() const {
			return index_;
		}

		Iterator& operator++() {
			++index_;
			AdvanceToValidIndex();
			return *this;
		}

		Iterator operator++(int) {
			Iterator temp = *this;
			++(*this);
			return temp;
		}

		Iterator& operator--() {
			do {
				--index_;
			} while (store_->metadata_[index_].next_free_index != -1);
			return *this;
		}

		Iterator operator--(int) {
			Iterator temp = *this;
			--(*this);
			return temp;
		}

		bool operator==(const Iterator& other) const {
			return store_ == other.store_ && index_ == other.index_;
		}

		bool operator!=(const Iterator& other) const {
			return !(*this == other);
		}

		reference operator*() {
			return *store_->Get<char>(index_);
		}

		pointer operator->() {
			return store_->Get<char>(index_);
		}

	private:
		void AdvanceToValidIndex() {
			while (index_ < store_->metadata_.size() && store_->metadata_[index_].next_free_index != -1) {
				++index_;
			}
		}

		BlobStore* store_;
		size_t index_;
	};

	Iterator begin() {
		return Iterator(this, 1);
	}

	Iterator end() {
		return Iterator(this, metadata_.size());
	}

	Iterator cbegin() const {
		return Iterator(const_cast<BlobStore*>(this), 1);
	}

	Iterator cend() const {
		return Iterator(const_cast<BlobStore*>(this), metadata_.size());
	}

private:
	struct BlobMetadata {
		size_t size;
		offset_type offset;
		ssize_t next_free_index; // -1 if the slot is occupied, or the index of the next free slot in the free list
	};

	using BlobMetadataAllocator = SharedMemoryAllocator<BlobMetadata>;
	using MetadataVector = SharedMemoryVector<BlobMetadata, BlobMetadataAllocator>;

	// Returns the index of the first free slot in the metadata vector.
	size_t FindFreeSlot();

	// Returns the number of free slots in the metadata vector.
	size_t GetFreeSlotCount() const;

	// SharedMemoryAllocatorObserver overrides.
	void OnBufferResize() override;

	void NotifyObserversOnMemoryReallocated();
	void NotifyObserversOnDroppedBlob(size_t index);

	Allocator allocator_;
	BlobMetadataAllocator metadata_allocator_;
	MetadataVector metadata_;
	std::vector<BlobStoreObserver*> observers_;
};

template<typename T>
BlobStoreObject<T>::BlobStoreObject(BlobStore* store, size_t index)
	: store_(store), index_(index), ptr_(nullptr) {
	if (store_) {
		store_->AddObserver(this);
		UpdatePointer();
	}
}

template<typename T>
void BlobStoreObject<T>::UpdatePointer() {
	ptr_ = store_->Get<T>(index_);
}

template <typename T, typename... Args>
BlobStoreObject<T> BlobStore::Put(Args&&... args) {
	size_t index = FindFreeSlot();
	char* ptr = allocator_.Allocate(sizeof(T));
	allocator_.Construct(reinterpret_cast<T*>(ptr), std::forward<Args>(args)...);
	metadata_[index] = { sizeof(T), allocator_.ToOffset(ptr), -1 };
	return BlobStoreObject<T>(this, index);
}

template <typename T, typename... Args>
BlobStoreObject<T> BlobStore::Put(size_t size, Args&&... args) {
	size_t index = FindFreeSlot();
	char* ptr = allocator_.Allocate(size);
	allocator_.Construct(reinterpret_cast<T*>(ptr), std::forward<Args>(args)...);
	metadata_[index] = { size, allocator_.ToOffset(ptr), -1 };
	return BlobStoreObject<T>(this, index);
}

#endif  // BLOB_STORE_H_
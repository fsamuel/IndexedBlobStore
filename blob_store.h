#ifndef BLOB_STORE_H_
#define BLOB_STORE_H_

#include <cstdio>
#include <sys/types.h>
#include <string>
#include <cstring>
#include <iostream>
#include <type_traits>

#include "rwlock.h"
#include "shared_memory_allocator.h"
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
class BlobStoreObject {
public:
	using non_const_T = typename std::remove_const<T>::type;

	// Returns a BlobStoreObject pointing to nullptr.
	static BlobStoreObject<T> CreateNull() {
		return BlobStoreObject<T>(nullptr, BlobStore::InvalidIndex);
	}

	BlobStoreObject() : control_block_(nullptr) {}

	BlobStoreObject(const BlobStoreObject& other)
		: control_block_(other.control_block_) {
		if (control_block_ != nullptr) {
			control_block_->IncrementRefCount();
		}
	}

	// Move constructor
	BlobStoreObject(BlobStoreObject&& other)
		: control_block_(other.control_block_) {
		other.control_block_ = nullptr;
	}

	// Constructor: creates a new ControlBlock with the provided store and index.
	// The ControlBlock starts with a refcount of 1.
	BlobStoreObject(BlobStore* store, size_t index);

	// Destructor: Decrements the ControlBlock and destroys it if the refcount
	// goes to zero.
	~BlobStoreObject() {
		if (control_block_ && control_block_->DecrementRefCount()) {
			delete control_block_;
		}
	}

	// Arrow operator: Provides access to the object's methods.
	T* operator->() {
		return control_block_->ptr_;
	}

	const T* operator->() const {
		return control_block_->ptr_;
	}

	// Dereference operator: Provides access to the object itself.
	T& operator*() {
		return *control_block_->ptr_;
	}

	const T& operator*() const {
		return *control_block_->ptr_;
	}

	// Operator[] implementation for array types.
	template<typename U = T>
	typename std::enable_if<std::is_array<U>::value, typename std::remove_extent<U>::type&>::type
		operator[](size_t i) {
		return (*control_block_->ptr_)[i];
	}

	template<typename U = T>
	typename std::enable_if<std::is_array<U>::value, const typename std::remove_extent<U>::type&>::type
		operator[](size_t i) const {
		return (*control_block_->ptr_)[i];
	}

	template<typename U = T>
	typename std::enable_if<!std::is_array<U>::value, T&>::type
		operator[](size_t i) {
		return (*control_block_->ptr_)[i];
	}

	template<typename U = T>
	typename std::enable_if<!std::is_array<U>::value, const T&>::type
		operator[](size_t i) const {
		return (*control_block_->ptr_)[i];
	}

	// Returns the index of the Blob.
	size_t Index() const
	{
		return control_block_->index_;
	}

	// Returns the total size in bytes of this object.
	size_t GetSize() const {
		if (control_block_ == nullptr || control_block_->store_ == nullptr) {
			return 0;
		}
		return control_block_->store_->GetSize(control_block_->index_);
	}

	// Returns the number of elements this object is composed of if it's an array.
	size_t GetElementCount() const {
		if (control_block_ == nullptr || control_block_->store_ == nullptr) {
			return 0;
		}
		return control_block_->store_->GetElementCount(control_block_->index_);
	}

	// Casts BlobStoreObject<T> to a const-preserving BlobStoreObject<U>.
	template <typename U>
	auto To() -> typename std::conditional <
		std::is_const<T>::value,
		BlobStoreObject< typename std::add_const<U>::type >,
		BlobStoreObject<U>
	>::type {
		using const_preserving_U = typename std::conditional<
			std::is_const<T>::value,
			typename std::add_const<U>::type,
			U
		>::type;
		return BlobStoreObject<const_preserving_U>(reinterpret_cast<BlobStoreObject<const_preserving_U>::ControlBlock*>(control_block_));
	}

	BlobStoreObject<const T> Downgrade()&& {
		ControlBlock* control_block = control_block_;
		control_block_ = nullptr;
		if (control_block != nullptr) {
			control_block->DowngradeLock();
		}
		return BlobStoreObject<const T>(reinterpret_cast<BlobStoreObject<const T>::ControlBlock*>(control_block));
	}

	BlobStoreObject<non_const_T> Upgrade()&& {
		ControlBlock* control_block = control_block_;
		control_block_ = nullptr;
		if (control_block != nullptr) {
			control_block->UpgradeLock();
		}
		return BlobStoreObject<non_const_T>(reinterpret_cast<BlobStoreObject<non_const_T>::ControlBlock*>(control_block));
	}

	BlobStoreObject& operator=(const BlobStoreObject& other) {
		if (this != &other) {
			if (control_block_ && control_block_->DecrementRefCount()) {
				delete control_block_;
			}
			control_block_ = other.control_block_;
			if (control_block_) {
				control_block_->IncrementRefCount();
			}
		}
		return *this;
	}

	BlobStoreObject& operator=(BlobStoreObject&& other) {
		if (this != &other) {
			if (control_block_ && control_block_->DecrementRefCount()) {
				delete control_block_;
			}
			control_block_ = other.control_block_;
			other.control_block_ = nullptr;
		}
		return *this;
	}

	BlobStoreObject& operator=(std::nullptr_t) {
		if (control_block_ && control_block_->DecrementRefCount()) {
			delete control_block_;
		}
		control_block_ = nullptr;
		return *this;
	}

	bool operator!() const // Enables "if (!sp) ..."
	{
		return !control_block_ || control_block_->store_ == nullptr || control_block_->index_ == BlobStore::InvalidIndex || control_block_->ptr_ == nullptr;
	}

	inline friend bool operator==(const BlobStoreObject& lhs, const T* rhs)
	{
		if (lhs.control_block_ == nullptr) {
			return rhs == nullptr;
		}
		return lhs.control_block_->ptr_ == rhs;
	}

	inline friend bool operator==(const T* lhs, const BlobStoreObject& rhs)
	{
		if (rhs.control_block_ == nullptr) {
			return lhs == nullptr;
		}
		return lhs == rhs.control_block_->ptr_;
	}

	inline friend bool operator!=(const BlobStoreObject& lhs, const T* rhs)
	{
		if (lhs.control_block_ == nullptr) {
			return rhs != nullptr;
		}
		return lhs.control_block_->ptr_ != rhs;
	}

	inline friend bool operator!=(const T* lhs, const BlobStoreObject& rhs)
	{
		if (rhs.control_block_ == nullptr) {
			return lhs != nullptr;
		}
		return lhs != rhs->control_block_->ptr_;
	}

	inline friend bool operator==(const BlobStoreObject& lhs, const BlobStoreObject& rhs)
	{
		return lhs.control_block_ == rhs.control_block_;
	}

	inline friend bool operator!=(const BlobStoreObject& lhs, const BlobStoreObject& rhs)
	{
		return lhs.control_block_ != rhs.control_block_;
	}

private:
	struct ControlBlock;

	BlobStoreObject(ControlBlock* control_block) :
		control_block_(control_block) {
		if (control_block_) {
			control_block_->IncrementRefCount();
		}
	}

	struct ControlBlock : public BlobStoreObserver {
	public:
		ControlBlock(BlobStore* store, size_t index);

		~ControlBlock() {
			if (store_ != nullptr) {
				store_->RemoveObserver(this);
			}

		}

		void IncrementRefCount() {
			++ref_count_;
		}

		bool DecrementRefCount() {
			if (--ref_count_ == 0) {
				lock_->unlock();
				return true;
			}
			return false;
		}

		void DowngradeLock() {
			lock_->DowngradeWriteToReadLock();
			--ref_count_;
		}

		void UpgradeLock() {
			lock_->UpgradeReadToWriteLock();
			--ref_count_;
		}

		BlobStore* store_;				// Pointer to the BlobStore instance
		size_t index_;					// Index of the object in the BlobStore
		T* ptr_;						// Pointer to the object
		std::unique_ptr<RWLock> lock_;	// RWLock for this object.
		std::atomic<size_t> ref_count_; // Number of smart pointers to this object.

	private:

		// UpdatePointer: Helper method to update the internal pointer to the object using
		// the BlobStore's GetPointer method.
		void UpdatePointer() {
			ptr_ = store_->GetRaw<T>(index_);
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
				index_ = BlobStore::InvalidIndex;
				ptr_ = nullptr;
			}
		}
	};

	ControlBlock* control_block_;

	template<typename U>
	friend class BlobStoreObject;
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

	// Returns the metadata buffer name.
	const std::string& GetMetadataBufferName() const {
		return metadata_allocator_.buffer_name();
	}

	// Creates a new object of type T with the provided arguments into the BlobStore and returns a BlobStoreObject.
	template <typename T, typename... Args>
	BlobStoreObject<T> New(Args&&... args);

	template <typename T>
	BlobStoreObject<T[]> NewArray(size_t count);

	// Gets the object of type T at the specified index.
	template<typename T>
	BlobStoreObject<T> GetMutable(size_t index) {
		return BlobStoreObject<T>(this, index);
	}

	// Gets the object of type T at the specified index as a constant.
	template<typename T>
	BlobStoreObject<const T> Get(size_t index) const {
		return const_cast<BlobStore*>(this)->GetMutable<const T>(index);
	}

	// Gets the size of the blob stored at the speific index.
	size_t GetSize(size_t index) {
		if (index == BlobStore::InvalidIndex || index >= metadata_.size() || metadata_[index].next_free_index != -1) {
			return 0;
		}
		BlobMetadata& metadata_entry = metadata_[index];
		return metadata_entry.size * metadata_entry.count;
	}

	// Gets the number of elements stored in this blob if the type is an array.
	size_t GetElementCount(size_t index) {
		if (index == BlobStore::InvalidIndex || index >= metadata_.size() || metadata_[index].next_free_index != -1) {
			return 0;
		}
		BlobMetadata& metadata_entry = metadata_[index];
		return metadata_entry.count;
	}

	// Drops the object at the specified index, freeing the associated memory.
	void Drop(size_t index);

	template<typename U>
	void Drop(BlobStoreObject<U>&& object) {
		Drop(object.Index());
	}

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
		using pointer = const value_type*;
		using reference = const value_type&;

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

		template<typename T>
		BlobStoreObject<const T> Get() const {
			return store_->Get<const T>(index_);
		}

		template<typename T>
		BlobStoreObject<T> GetMutable() {
			return store_->GetMutable<T>(index_);
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
		size_t size;			      // The size of the type stored.
		size_t count;			      // The number of elements of type stored.
		offset_type offset;           // The offset of the blob in the shared memory buffer.
		std::atomic<int> lock_state;  // The lock state of the blob.
		ssize_t next_free_index;      // -1 if the slot is occupied, or the index of the next free slot in the free list
		BlobMetadata() : size(0), count(0), offset(0), lock_state(0), next_free_index(0) {}

		BlobMetadata(size_t size, size_t count, offset_type offset) : size(size), count(count), offset(offset), lock_state(0), next_free_index(-1) {}

		BlobMetadata(const BlobMetadata& other) : size(other.size), count(other.count), offset(other.offset), lock_state(0), next_free_index(other.next_free_index) {}
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

	// Gets the object of type T at the specified index as a raw pointer.
	template<typename T>
	T* GetRaw(size_t index) {
		if (index == BlobStore::InvalidIndex || index >= metadata_.size() || metadata_[index].next_free_index != -1) {
			return nullptr;
		}
		BlobMetadata& metadata_entry = metadata_[index];
		if (metadata_entry.size == 0 || metadata_entry.count == 0) {
			return nullptr;
		}
		return allocator_.ToPtr<T>(metadata_entry.offset);
	}

	// Creates an RWLock for the object at the specified index.
	std::unique_ptr<RWLock> CreateLock(size_t index) {
		if (index == BlobStore::InvalidIndex || index >= metadata_.size() || metadata_[index].next_free_index != -1) {
			return nullptr;
		}
		BlobMetadata& metadata_entry = metadata_[index];
		return std::make_unique<RWLock>(&metadata_entry.lock_state);
	}

	Allocator allocator_;
	BlobMetadataAllocator metadata_allocator_;
	MetadataVector metadata_;
	std::vector<BlobStoreObserver*> observers_;

	template<typename T>
	friend class BlobStoreObject;
};

template <typename T, typename... Args>
BlobStoreObject<T> BlobStore::New(Args&&... args) {
	size_t index = FindFreeSlot();
	char* ptr = allocator_.Allocate(sizeof(T));
	allocator_.Construct(reinterpret_cast<T*>(ptr), std::forward<Args>(args)...);
	BlobMetadata& metadata = metadata_[index];
	metadata.size = sizeof(T);
	metadata.count = 1;
	metadata.offset = allocator_.ToOffset(ptr);
	metadata.lock_state = 0;
	metadata.next_free_index = -1;
	return BlobStoreObject<T>(this, index);
}

template <typename T>
BlobStoreObject<T[]> BlobStore::NewArray(size_t count) {
	size_t index = FindFreeSlot();
	T* ptr = reinterpret_cast<T*>(allocator_.Allocate(sizeof(T) * count));
	for (size_t i = 0; i < count; ++i) {
		new (&ptr[i]) T();
	}
	BlobMetadata& metadata = metadata_[index];
	metadata.size = sizeof(T);
	metadata.count = count;
	metadata.offset = allocator_.ToOffset(ptr);
	metadata.lock_state = 0;
	metadata.next_free_index = -1;
	return BlobStoreObject<T[]>(this, index);
}

template<typename T>
BlobStoreObject<T>::BlobStoreObject(BlobStore* store, size_t index) :
	control_block_(index == BlobStore::InvalidIndex ? nullptr : new ControlBlock(store, index)) {}

template<typename T>
BlobStoreObject<T>::BlobStoreObject::ControlBlock::ControlBlock(BlobStore* store, size_t index)
	: store_(store), index_(index), lock_(store->CreateLock(index)), ref_count_(1) {
	if (std::is_const<T>::value) {
		lock_->lockRead();
	}
	else {
		lock_->lockWrite();
	}
	if (store_ != nullptr) {
		store_->AddObserver(this);
	}
	UpdatePointer();
}
#endif  // BLOB_STORE_H_
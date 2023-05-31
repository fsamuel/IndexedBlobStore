#ifndef BLOB_STORE_H_
#define BLOB_STORE_H_

#include <cstdio>
#include <sys/types.h>
#include <string>
#include <cstring>
#include <iostream>
#include <type_traits>

#include "chunked_vector.h"
#include "shared_memory_allocator.h"
#include "storage_traits.h"
#include "string_slice.h"
#include "utils.h"

#ifdef _WIN64
typedef __int64 ssize_t;
#else
typedef int ssize_t;
#endif

constexpr std::int32_t WRITE_LOCK_FLAG = 0x80000000;

namespace {
	void SpinWait() {
#if defined(_WIN32)
		Sleep(0);
#else
		std::this_thread::yield();
#endif
	}
}

class BlobStore;

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
	using StorageType = typename StorageTraits<T>::StorageType;
	using ElementType = typename StorageTraits<T>::ElementType;
	using NonConstStorageType = typename StorageTraits<non_const_T>::StorageType;

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

	// Returns the blob_store associated with this object.
	BlobStore* GetBlobStore() const {
		return control_block_->store_;
	}

	// Arrow operator: Provides access to the object's methods.
	StorageType* operator->() {
		return control_block_->ptr_;
	}

	const StorageType* operator->() const {
		return control_block_->ptr_;
	}

	// Dereference operator: Provides access to the object itself.
	StorageType& operator*() {
		return *control_block_->ptr_;
	}

	const StorageType& operator*() const {
		return *control_block_->ptr_;
	}

	ElementType& operator[](size_t i) {
		return (*control_block_->ptr_)[i];
	}

	
	const ElementType& operator[](size_t i) const {
		return (*control_block_->ptr_)[i];
	}

	BlobStoreObject<typename std::remove_const<T>::type> Clone() const {
		return control_block_->store_->Clone<T>(control_block_->index_);// .To<std::remove_const<V>::type >();
	}

	template<typename U>
	typename std::enable_if<
		std::is_same<
		    typename std::remove_const<U>::type,
		    typename std::remove_const<T>::type
		>::value,
		bool
	>::type CompareAndSwap(BlobStoreObject<U> other) {
		if (*this == nullptr || other == nullptr) {
			return false;
		}
		size_t offset = control_block_->offset_;
		size_t other_offset = other.control_block_->offset_;
		return control_block_->store_->CompareAndSwap(control_block_->index_, offset, other_offset) &&
			control_block_->store_->CompareAndSwap(other.control_block_->index_, other_offset, offset);
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

	// Casts BlobStoreObject<T> to a const-preserving BlobStoreObject<U>.
	template <typename U>
	auto To() & -> typename std::conditional <
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

	// Casts BlobStoreObject<T> to a const-preserving BlobStoreObject<U>.
	template <typename U>
	auto To() && -> typename std::conditional <
		std::is_const<T>::value,
		BlobStoreObject< typename std::add_const<U>::type >,
		BlobStoreObject<U>
	>::type {
		using const_preserving_U = typename std::conditional<
			std::is_const<T>::value,
			typename std::add_const<U>::type,
			U
		>::type;
		ControlBlock* control_block = control_block_;
		control_block_ = nullptr;
	    BlobStoreObject<const_preserving_U> new_ptr(reinterpret_cast<BlobStoreObject<const_preserving_U>::ControlBlock*>(control_block));
		control_block->DecrementRefCount();
		return new_ptr;
	}

	BlobStoreObject<const T> Downgrade() && {
		ControlBlock* control_block = control_block_;
		control_block_ = nullptr;
		// We should only return a BlobStoreObject<const T> cast of this control_block if it has a refcount of 1.
		// Otherwise, we should return a nullptr BlobStoreObject<const T>.
		if (control_block == nullptr || control_block->ref_count_ != 1) {
			return BlobStoreObject<const T>();
		}
		control_block->DowngradeLock();
		return BlobStoreObject<const T>(reinterpret_cast<BlobStoreObject<const T>::ControlBlock*>(control_block));
	}

	// Upgrades a const T BlobStoreObject to a non-const T BlobStoreObject.
	// This is only valid if the BlobStoreObject is the only owner of the Blob.
	BlobStoreObject<non_const_T> Upgrade()&& {
		ControlBlock* control_block = control_block_;
		control_block_ = nullptr;
		// We should only return a BlobStoreObject<non_const_T> cast of this control_block if it has a refcount of 1.
		// Otherwise, we should return a nullptr BlobStoreObject<non_const_T>.
		if (control_block == nullptr || control_block->ref_count_ != 1) {
			return BlobStoreObject<non_const_T>();
		}
		control_block->UpgradeLock();
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

	inline friend bool operator==(const BlobStoreObject& lhs, const StorageType* rhs)
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

	inline friend bool operator!=(const BlobStoreObject& lhs, const StorageType* rhs)
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

	struct ControlBlock {
	public:
		ControlBlock(BlobStore* store, size_t index);

		~ControlBlock() {
		}

		void IncrementRefCount() {
			++ref_count_;
		}

		bool DecrementRefCount() {
			if (--ref_count_ == 0) {
				store_->Unlock(index_);
				return true;
			}
			return false;
		}

		void DowngradeLock() {
			store_->DowngradeWriteLock(index_);
			--ref_count_;
		}

		void UpgradeLock() {
			store_->UpgradeReadLock(index_);
			--ref_count_;
		}

		BlobStore* store_;				// Pointer to the BlobStore instance
		size_t index_;					// Index of the object in the BlobStore
		size_t offset_;                 // Offset of the object in the shared memory buffer.
		StorageType* ptr_;				// Pointer to the object
		std::atomic<size_t> ref_count_; // Number of smart pointers to this object.
	};

	ControlBlock* control_block_;

	template<typename U>
	friend class BlobStoreObject;
};

// BlobStore is a class that manages the storage and retrieval of objects
// (blobs) in shared memory. It supports storing, getting, and deleting
// objects while maintaining a compact memory footprint.
class BlobStore {
public:
	using Allocator = SharedMemoryAllocator<char>;

	static constexpr std::size_t InvalidIndex = std::numeric_limits<std::size_t>::max();

	// Constructor that initializes the BlobStore with the provided metadata and data shared memory buffers.
	BlobStore(SharedMemoryBuffer&& metadataBuffer, ChunkManager&& dataBuffer);

	// BlobStore destructor
	~BlobStore();

	// Creates a new object of type T with the provided arguments into the BlobStore and returns a BlobStoreObject.
	template <typename T, typename... Args>
	typename std::enable_if<
		std::is_standard_layout<typename StorageTraits<T>::StorageType>::value &&
		std::is_trivially_copyable<typename StorageTraits<T>::StorageType>::value,
		BlobStoreObject<T>
	>::type New(Args&&... args);

	template <typename T>
	typename std::enable_if<
        std::is_standard_layout<typename StorageTraits<T>::StorageType>::value &&
		std::is_trivially_copyable<typename StorageTraits<T>::StorageType>::value,		BlobStoreObject<T>
	>::type New(std::initializer_list<typename StorageTraits<T>::ElementType> initList);

	template <typename T, typename... Args>
	typename std::enable_if<
		std::is_standard_layout<typename StorageTraits<T>::StorageType>::value&&
		std::is_trivially_copyable<typename StorageTraits<T>::StorageType>::value,		BlobStoreObject<T>
	>::type NewImpl(Args&&... args);

	template <typename T>
	typename std::enable_if<
		std::is_standard_layout<typename StorageTraits<T>::StorageType>::value&&
		std::is_trivially_copyable<typename StorageTraits<T>::StorageType>::value,		BlobStoreObject<T>
	>::type NewImpl(std::initializer_list<typename StorageTraits<T>::ElementType> initList);

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

	template <typename T>
	typename std::enable_if<
		std::conjunction<
		    std::is_standard_layout<T>,
		    std::is_trivially_copyable<T>
		>::value,
		BlobStoreObject<typename std::remove_const<T>::type>
	>::type Clone(size_t index) {
		// This is only safe if the calling object is holding a read or write lock.
		BlobMetadata& metadata = metadata_[index];
		size_t clone_index = FindFreeSlot();
		char* ptr = allocator_.Allocate(metadata.size);
		size_t offset;
		const T* obj = GetRaw<T>(index, &offset);
		// Blobs are trivially copyable and standard layout so memcpy should be safe.
		memcpy(ptr, obj, metadata.size);
		BlobMetadata& clone_metadata = metadata_[clone_index];
		clone_metadata.size = metadata.size;
		clone_metadata.offset = allocator_.ToIndex(ptr);
		clone_metadata.lock_state = 0;
		clone_metadata.next_free_index = -1;
		return BlobStoreObject<typename std::remove_const<T>::type>(this, clone_index);
	}

	bool CompareAndSwap(size_t index, std::size_t expected_offset, std::size_t new_offset) {
		if (index == BlobStore::InvalidIndex) {
			return false;
		}

		BlobMetadata* metadata = metadata_.at(index);
		if (metadata == nullptr || metadata->is_deleted()) {
			return false;
		}

		return metadata->offset.compare_exchange_weak(expected_offset, new_offset);
	}

	// Gets the size of the blob stored at the speific index.
	size_t GetSize(size_t index) {
		if (index == BlobStore::InvalidIndex) {
			return 0;
		}
		
		BlobMetadata* metadata = metadata_.at(index);
		if (metadata == nullptr || metadata->is_deleted()) {
			return 0;
		}
		
		return metadata->size;
	}

	// Drops the object at the specified index, freeing the associated memory.
	void Drop(size_t index);

	template<typename U>
	void Drop(BlobStoreObject<U>&& object) {
		size_t index = object.Index();
		// This ensures that we're no longer holding a lock on this object.
		object = nullptr;
		Drop(index);
	}
		
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
			while (true) {
				BlobMetadata* metadata = store_->metadata_.at(index_);
				if (metadata == nullptr) {
					break;
				}

				if (!metadata->is_deleted()) {
					break;
				}
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
		// The size of the type stored.
		// TODO(fsamuel): Can we get this from the allocator?
		size_t size;
		// The offset of the blob in the shared memory buffer.
		std::atomic<std::size_t> offset;
		// The lock state of the blob.
		std::atomic<int> lock_state;
		// This field can take one of three states:
		// -  -1 if the slot is occupied
		// -   0 if the slot is tombstoned or at the end of the free list.
		// -   A positive number indicating the index of the next free slot in the free list.
		//  A tombstoned slot is used to indicate that the blob has been dropped but is not yet ready to be reused.
		// This can happen if there is a pending read or write operation on the blob.
		std::atomic<ssize_t> next_free_index;          

		BlobMetadata() : size(0), offset(0), lock_state(0), next_free_index(0) {}

		BlobMetadata(size_t size, size_t count, std::size_t offset) : size(size), offset(offset), lock_state(0), next_free_index(-1) {}

		BlobMetadata(const BlobMetadata& other) : size(other.size), offset(other.offset.load()), lock_state(0), next_free_index(other.next_free_index.load()) {}

		bool is_deleted() const {
			return next_free_index.load() != -1;
		}

		bool is_tombstone() const {
			return next_free_index.load() == 0;
		}

		bool SetTombstone() {
			ssize_t expected = next_free_index.load();
			// If the slot is already tombstoned, or on the free list then we don't need to do anything.
			if (expected != -1) {
				return false;
			}
			return next_free_index.compare_exchange_strong(expected, 0);
		}
	};

	using BlobMetadataAllocator = SharedMemoryAllocator<BlobMetadata>;
	using MetadataVector = ChunkedVector<BlobMetadata>;

	// Returns the index of the first free slot in the metadata vector.
	size_t FindFreeSlot();

	// Returns the number of free slots in the metadata vector.
	size_t GetFreeSlotCount() const;

	// Gets the object of type T at the specified index as a raw pointer.
	template<typename T>
	T* GetRaw(size_t index, size_t* offset) {
		if (index == BlobStore::InvalidIndex) {
			return nullptr;
		}
		BlobMetadata* metadata = metadata_.at(index);
		if (metadata == nullptr ||
			metadata->is_deleted() ||
			metadata->size == 0) {
			return nullptr;
		}
		
		std::size_t offset_value = metadata->offset;
		if (offset != nullptr) {
			*offset = offset_value;
		}
		return allocator_.ToPtr<T>(offset_value);
	}

	// Acquires a read lock for the object at the specified index.
	bool AcquireReadLock(size_t index) {
		if (index == BlobStore::InvalidIndex) {
			return false;
		}
		while (true) {
			BlobMetadata* metadata = metadata_.at(index);
			// It's possible that the blob was deleted while we were waiting for the lock.
			if (metadata == nullptr || metadata->is_deleted()) {
				return false;
			}
			int state = metadata->lock_state.load(std::memory_order_acquire);
			if (state >= 0) {
				if (metadata->lock_state.compare_exchange_weak(state, state + 1, std::memory_order_acquire)) {
					break;
				}
			}
			SpinWait();
		}

		return true;
	}

	// Acquires a write lock for the object at the specified index.
	bool AcquireWriteLock(size_t index) {
		if (index == BlobStore::InvalidIndex) {
			return false;
		}
		while (true) {
			BlobMetadata* metadata = metadata_.at(index);
			// It's possible that the blob was deleted while we were waiting for the lock.
			if (metadata == nullptr || metadata->is_deleted()) {
				return false;
			}
			std::int32_t expected = 0;
			if (metadata->lock_state.compare_exchange_weak(expected, WRITE_LOCK_FLAG, std::memory_order_acquire)) {
				break;
			}
			SpinWait();
		}
		return true;
	}

	// Unlocks the object at the specified index.
	void Unlock(size_t index) {
		if (index == BlobStore::InvalidIndex) {
			return;
		}
		BlobMetadata* metadata = metadata_.at(index);
		if (metadata == nullptr) {
			return;
		}
			
		std::int32_t expected;

		while (true) {
			expected = metadata->lock_state.load();
			std::int32_t new_state = std::max<int32_t>((expected & ~WRITE_LOCK_FLAG) - 1, 0);

			if (metadata->lock_state.compare_exchange_weak(expected, new_state)) {
				break;
			}
			SpinWait();
		}
		// Check if the blob was tombstoned and is now ready to be reused.
		if (metadata->is_tombstone() && metadata->lock_state == 0) {
			Drop(index);
		}
	}

	// Downgrades a write lock to a read lock at the specified index.
	void DowngradeWriteLock(size_t index) {
		if (index == BlobStore::InvalidIndex) {
			return;
		}
		BlobMetadata* metadata = metadata_.at(index);
		if (metadata == nullptr || metadata->is_deleted()) {
			return;
		}

		if (metadata->lock_state > 0) {
			return;
		}

		while (true) {
			std::int32_t expected = metadata->lock_state.load() & WRITE_LOCK_FLAG;
			if (metadata->lock_state.compare_exchange_weak(expected, 1)) {
				break;
			}
			SpinWait();
		}
	}

	// Upgrades a read lock to a write lock at the specified index.
	// Note that this is dangerous and should only be used when the caller knows that no other thread is holding a read lock.
	void UpgradeReadLock(size_t index) {
		if (index == BlobStore::InvalidIndex) {
			return;
		}
		BlobMetadata* metadata = metadata_.at(index);
		if (metadata == nullptr || metadata->is_deleted()) {
			return;
		}
		// We're already holding a write lock.
		if (metadata->lock_state == WRITE_LOCK_FLAG) {
			return;
		}
		while (true) {
			std::int32_t expected = 1;
			if (metadata->lock_state.compare_exchange_weak(expected, WRITE_LOCK_FLAG)) {
				break;
			}
			SpinWait();
		}
	}

	Allocator allocator_;
	MetadataVector metadata_;

	template<typename T>
	friend class BlobStoreObject;
};

template <typename T, typename... Args>
typename std::enable_if<
	std::is_standard_layout<typename StorageTraits<T>::StorageType>::value &&
	std::is_trivially_copyable<typename StorageTraits<T>::StorageType>::value,
	BlobStoreObject<T>
>::type BlobStore::New(Args&&... args) {
	return NewImpl<T>(std::forward<Args>(args)...);
}

template <typename T>
typename std::enable_if<
	std::is_standard_layout<typename StorageTraits<T>::StorageType>::value &&
	std::is_trivially_copyable<typename StorageTraits<T>::StorageType>::value,
	BlobStoreObject<T>
>::type BlobStore::New(std::initializer_list<typename StorageTraits<T>::ElementType> initList) {
	return NewImpl<T>(initList);
}

template <typename T, typename... Args>
typename std::enable_if<
	std::is_standard_layout<typename StorageTraits<T>::StorageType>::value&&
	std::is_trivially_copyable<typename StorageTraits<T>::StorageType>::value,
	BlobStoreObject<T>
>::type BlobStore::NewImpl(Args&&... args) {
	using StorageType = typename StorageTraits<T>::StorageType;
	size_t index = FindFreeSlot();
	size_t size = StorageTraits<T>::size(std::forward<Args>(args)...);
	char* ptr = allocator_.Allocate(size);
	utils::Construct(reinterpret_cast<StorageType*>(ptr), std::forward<Args>(args)...);
	BlobMetadata& metadata = metadata_[index];
	metadata.size = size;
	metadata.offset = allocator_.ToIndex(ptr);
	metadata.lock_state = 0;
	metadata.next_free_index = -1;
	return BlobStoreObject<T>(this, index);
}

template <typename T>
typename std::enable_if<
	std::is_standard_layout<typename StorageTraits<T>::StorageType>::value&&
	std::is_trivially_copyable<typename StorageTraits<T>::StorageType>::value,
	BlobStoreObject<T>
>::type BlobStore::NewImpl(std::initializer_list<typename StorageTraits<T>::ElementType> initList) {
	using StorageType = typename StorageTraits<T>::StorageType;
	using ElementType = typename StorageTraits<T>::ElementType;
	size_t index = FindFreeSlot();
	size_t size = initList.size() * sizeof(ElementType);
	char* ptr = allocator_.Allocate(size);
	std::uninitialized_copy(initList.begin(), initList.end(), reinterpret_cast<ElementType*>(ptr));
	BlobMetadata& metadata = metadata_[index];
	metadata.size = size;
	metadata.offset = allocator_.ToIndex(ptr);
	metadata.lock_state = 0;
	metadata.next_free_index = -1;
	return BlobStoreObject<T>(this, index);
}

template<typename T>
BlobStoreObject<T>::BlobStoreObject(BlobStore* store, size_t index) :
	control_block_(index == BlobStore::InvalidIndex ? nullptr : new ControlBlock(store, index)) {}

template<typename T>
BlobStoreObject<T>::BlobStoreObject::ControlBlock::ControlBlock(BlobStore* store, size_t index)
	: store_(store), index_(index), ptr_(nullptr), ref_count_(1) {
	bool success = false;
	if (std::is_const<T>::value) {
		success = store_->AcquireReadLock(index_);
	}
	else {
		success = store_->AcquireWriteLock(index_);
	}
	// If we failed to acquire the lock, then the blob was deleted while we were constructing the object.
	if (!success) {
		std::cout << "Failed to acquire lock." << std::endl;
		index_ = BlobStore::InvalidIndex;
		return;
	}
	ptr_ = store_->GetRaw<StorageType>(index_, &offset_);
}
#endif  // BLOB_STORE_H_
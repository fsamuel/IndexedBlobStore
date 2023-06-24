#ifndef BLOB_STORE_OBJECT_H_
#define BLOB_STORE_OBJECT_H_

#include <atomic>
#include <type_traits>

#include "blob_store_base.h"
#include "storage_traits.h"

// BlobStoreObject is a wrapper around BlobStore that provides a safe way to
// access objects stored in a BlobStore instance. It automatically updates its
// internal pointer to the object whenever the memory in BlobStore is
// reallocated, ensuring that the clients don't store stale pointers. It also
// implements the BlobStoreObserver interface to receive notifications from the
// BlobStore when the memory is reallocated.
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
    BlobStoreObject(BlobStoreBase* store, size_t index);

    // Destructor: Decrements the ControlBlock and destroys it if the refcount
    // goes to zero.
    ~BlobStoreObject() {
        if (control_block_ && control_block_->DecrementRefCount()) {
            delete control_block_;
        }
    }

    // Returns the blob_store associated with this object.
    BlobStoreBase* GetBlobStore() const { return control_block_->store_; }

    // Arrow operator: Provides access to the object's methods.
    StorageType* operator->() {
        assert(control_block_ != nullptr);
        assert(control_block_->ptr_ != nullptr);
        return control_block_->ptr_;
    }

    const StorageType* operator->() const {
        assert(control_block_ != nullptr);
        assert(control_block_->ptr_ != nullptr);
        return control_block_->ptr_;
    }

    // Dereference operator: Provides access to the object itself.
    StorageType& operator*() {
        assert(control_block_ != nullptr);
        assert(control_block_->ptr_ != nullptr);
        return *control_block_->ptr_;
    }

    const StorageType& operator*() const {
        assert(control_block_ != nullptr);
        assert(control_block_->ptr_ != nullptr);
        return *control_block_->ptr_;
    }

    ElementType& operator[](size_t i) {
        assert(control_block_ != nullptr);
        assert(control_block_->ptr_ != nullptr);
        return (*control_block_->ptr_)[i];
    }

    const ElementType& operator[](size_t i) const {
        assert(control_block_ != nullptr);
        assert(control_block_->ptr_ != nullptr);
        return (*control_block_->ptr_)[i];
    }

    BlobStoreObject<typename std::remove_const<T>::type> Clone() const {
        size_t clone_index =  control_block_->store_->Clone(control_block_->index_);
        return BlobStoreObject<typename std::remove_const<T>::type>(
			control_block_->store_, clone_index);
    }

    template <typename U>
    typename std::enable_if<
        std::is_same<typename std::remove_const<U>::type,
        typename std::remove_const<T>::type>::value,
        bool>::type
        CompareAndSwap(BlobStoreObject<U> other) {
        if (*this == nullptr || other == nullptr) {
            return false;
        }
        size_t offset = control_block_->offset_;
        size_t other_offset = other.control_block_->offset_;
        return control_block_->store_->CompareAndSwap(control_block_->index_,
            offset, other_offset) &&
            control_block_->store_->CompareAndSwap(other.control_block_->index_,
                other_offset, offset);
    }

    // Returns the index of the Blob.
    size_t Index() const { return control_block_->index_; }

    // Returns the total size in bytes of this object.
    size_t GetSize() const {
        if (control_block_ == nullptr || control_block_->store_ == nullptr) {
            return 0;
        }
        return control_block_->store_->GetSize(control_block_->index_);
    }

    // Casts BlobStoreObject<T> to a const-preserving BlobStoreObject<U>.
    template <typename U>
    auto To() & -> typename std::conditional<
        std::is_const<T>::value,
        BlobStoreObject<typename std::add_const<U>::type>,
        BlobStoreObject<U>>::type {
        using const_preserving_U =
            typename std::conditional<std::is_const<T>::value,
            typename std::add_const<U>::type, U>::type;
        return BlobStoreObject<const_preserving_U>(
            reinterpret_cast<BlobStoreObject<const_preserving_U>::ControlBlock*>(
                control_block_));
    }

    // Casts BlobStoreObject<T> to a const-preserving BlobStoreObject<U>.
    template <typename U>
    auto To() && -> typename std::conditional<
        std::is_const<T>::value,
        BlobStoreObject<typename std::add_const<U>::type>,
        BlobStoreObject<U>>::type {
        using const_preserving_U =
            typename std::conditional<std::is_const<T>::value,
            typename std::add_const<U>::type, U>::type;
        ControlBlock* control_block = control_block_;
        control_block_ = nullptr;
        BlobStoreObject<const_preserving_U> new_ptr(
            reinterpret_cast<BlobStoreObject<const_preserving_U>::ControlBlock*>(
                control_block));
        control_block->DecrementRefCount();
        return new_ptr;
    }

    BlobStoreObject<const T> Downgrade()&& {
        ControlBlock* control_block = control_block_;
        control_block_ = nullptr;
        // We should only return a BlobStoreObject<const T> cast of this
        // control_block if it has a refcount of 1. Otherwise, we should return a
        // nullptr BlobStoreObject<const T>.
        if (control_block == nullptr || control_block->ref_count_ != 1) {
            return BlobStoreObject<const T>();
        }
        control_block->DowngradeLock();
        BlobStoreObject<const T> downgraded_obj = BlobStoreObject<const T>(
            reinterpret_cast<typename BlobStoreObject<const T>::ControlBlock*>(
                control_block));
        control_block->ref_count_.fetch_sub(1);
        return downgraded_obj;
    }

    // Upgrades a const T BlobStoreObject to a non-const T BlobStoreObject.
    // This is only valid if the BlobStoreObject is the only owner of the Blob.
    BlobStoreObject<non_const_T> Upgrade()&& {
        ControlBlock* control_block = control_block_;
        control_block_ = nullptr;
        // We should only return a BlobStoreObject<non_const_T> cast of this
        // control_block if it has a refcount of 1. Otherwise, we should return a
        // nullptr BlobStoreObject<non_const_T>.
        if (control_block == nullptr || control_block->ref_count_ != 1) {
            return BlobStoreObject<non_const_T>();
        }
        control_block->UpgradeLock();
        BlobStoreObject<non_const_T> upgraded_object = BlobStoreObject<non_const_T>(
            reinterpret_cast<BlobStoreObject<non_const_T>::ControlBlock*>(
                control_block));
        control_block->ref_count_.fetch_sub(1);
        return upgraded_object;
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
            // TODO(fsamuel): It seems there are occasional cases where we double
            // delete control_block_.
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

    bool operator!() const  // Enables "if (!sp) ..."
    {
        return !control_block_ || control_block_->store_ == nullptr ||
            control_block_->index_ == BlobStore::InvalidIndex ||
            control_block_->ptr_ == nullptr;
    }

    inline friend bool operator==(const BlobStoreObject& lhs,
        const StorageType* rhs) {
        if (lhs.control_block_ == nullptr) {
            return rhs == nullptr;
        }
        return lhs.control_block_->ptr_ == rhs;
    }

    inline friend bool operator==(const T* lhs, const BlobStoreObject& rhs) {
        if (rhs.control_block_ == nullptr) {
            return lhs == nullptr;
        }
        return lhs == rhs.control_block_->ptr_;
    }

    inline friend bool operator!=(const BlobStoreObject& lhs,
        const StorageType* rhs) {
        if (lhs.control_block_ == nullptr) {
            return rhs != nullptr;
        }
        return lhs.control_block_->ptr_ != rhs;
    }

    inline friend bool operator!=(const T* lhs, const BlobStoreObject& rhs) {
        if (rhs.control_block_ == nullptr) {
            return lhs != nullptr;
        }
        return lhs != rhs->control_block_->ptr_;
    }

    inline friend bool operator==(const BlobStoreObject& lhs,
        const BlobStoreObject& rhs) {
        return lhs.control_block_ == rhs.control_block_;
    }

    inline friend bool operator!=(const BlobStoreObject& lhs,
        const BlobStoreObject& rhs) {
        return lhs.control_block_ != rhs.control_block_;
    }

private:
    struct ControlBlock;

    BlobStoreObject(ControlBlock* control_block) : control_block_(control_block) {
        if (control_block_) {
            control_block_->IncrementRefCount();
        }
    }

    struct ControlBlock {
    public:
        ControlBlock(BlobStoreBase* store, size_t index);

        ~ControlBlock() {}

        void IncrementRefCount() { ref_count_.fetch_add(1); }

        bool DecrementRefCount() {
            size_t prev_ref_count = ref_count_.fetch_sub(1);
            if (prev_ref_count == 1) {
                store_->Unlock(index_);
                return true;
            }
            return false;
        }

        void DowngradeLock() { store_->DowngradeWriteLock(index_); }

        void UpgradeLock() { store_->UpgradeReadLock(index_); }

        // Pointer to the BlobStore instance
        BlobStoreBase* const store_;
        // Index of the object in the BlobStore
        size_t index_;
        // Offset of the object in the shared memory buffer.
        size_t offset_;
        // Pointer to the object
        StorageType* ptr_;
        // Number of smart pointers to this object.
        std::atomic<size_t> ref_count_;
    };

    ControlBlock* control_block_;

    template <typename U>
    friend class BlobStoreObject;
};


template <typename T>
BlobStoreObject<T>::BlobStoreObject(BlobStoreBase* store, size_t index)
    : control_block_(index == BlobStore::InvalidIndex
        ? nullptr
        : new ControlBlock(store, index)) {}

template <typename T>
BlobStoreObject<T>::BlobStoreObject::ControlBlock::ControlBlock(
    BlobStoreBase* store,
    size_t index)
    : store_(store), index_(index), ptr_(nullptr), ref_count_(1) {
    bool success = false;
    if (std::is_const<T>::value) {
        success = store_->AcquireReadLock(index_);
    }
    else {
        success = store_->AcquireWriteLock(index_);
    }
    // If we failed to acquire the lock, then the blob was deleted while we were
    // constructing the object.
    if (!success) {
        index_ = BlobStore::InvalidIndex;
        return;
    }
    ptr_ = reinterpret_cast<StorageType*>(store_->GetRaw(index_, &offset_));
}

#endif  // BLOB_STORE_OBJECT_H_
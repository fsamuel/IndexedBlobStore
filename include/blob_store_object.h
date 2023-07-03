#ifndef BLOB_STORE_OBJECT_H_
#define BLOB_STORE_OBJECT_H_

#include <atomic>
#include <type_traits>

#include "blob_store_base.h"
#include "storage_traits.h"

namespace blob_store {

// The BlobStoreObject class provides a type-safe smart pointer for managing the
// lifecycle and access of a Blob stored within a BlobStore. It uses RAII
// (Resource Acquisition Is Initialization) principle to handle resource
// allocation and deallocation.
//
// The BlobStoreObject is thread-safe due to usage of atomic operations for
// reference counting. It supports moving, copying, upgrading (non-const to
// const) and downgrading (const to non-const) operations on the smart pointer,
// while ensuring atomicity of operations.
//
// Usage Example:
//   BlobStoreObject<MyClass> obj(&blobStore, index);
//   obj->myMethod();
//   MyClass& objRef = *obj;
//
template <typename T>
class BlobStoreObject {
 public:
  using non_const_T = typename std::remove_const<T>::type;
  using StorageType = typename StorageTraits<T>::StorageType;
  using ElementType = typename StorageTraits<T>::ElementType;
  using NonConstStorageType = typename StorageTraits<non_const_T>::StorageType;

  // Default constructor that creates an empty BlobStoreObject with a null
  // control block.
  BlobStoreObject() : control_block_(nullptr) {}

  // Copy constructor that duplicates the BlobStoreObject by creating a new
  // reference to the same ControlBlock. It increments the refcount to reflect
  // the new reference.
  BlobStoreObject(const BlobStoreObject& other)
      : control_block_(other.control_block_) {
    if (control_block_ != nullptr) {
      control_block_->IncrementRefCount();
    }
  }

  // Move constructor that transfers ownership of a BlobStoreObject without
  // creating a new reference. The moved-from BlobStoreObject will have a null
  // ControlBlock after the operation.
  BlobStoreObject(BlobStoreObject&& other)
      : control_block_(other.control_block_) {
    other.control_block_ = nullptr;
  }

  // Constructs a BlobStoreObject from a BlobStore and an index. The
  // BlobStoreObject will hold a pointer to the BlobStore and the index of the
  // Blob in the store. The created ControlBlock starts with a refcount of 1.
  BlobStoreObject(BlobStoreBase* store, size_t index);

  // Destructor that decrements the refcount of the ControlBlock. If the
  // refcount reaches zero, it means there are no BlobStoreObjects pointing to
  // the Blob, so the Blob can be safely deleted.
  ~BlobStoreObject() {
    if (control_block_ && control_block_->DecrementRefCount()) {
      delete control_block_;
    }
  }

  // Accessor for the BlobStore associated with this object.
  BlobStoreBase* GetBlobStore() const { return control_block_->store_; }

  // Arrow operators provide access to the methods of the actual object stored.
  // If control_block_ or the stored object pointer is null, it raises an
  // assertion failure.
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

  // Clone this BlobStoreObject, creating a new blob in the BlobStore
  // with the same content and returning a BlobStoreObject pointing to it.
  BlobStoreObject<typename std::remove_const<T>::type> Clone() const {
    size_t clone_index = control_block_->store_->Clone(control_block_->index_);
    return BlobStoreObject<typename std::remove_const<T>::type>(
        control_block_->store_, clone_index);
  }

  // Attempt to atomically swap the contents of two BlobStoreObjects if they are
  // of the same underlying type.
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

  // Returns the index of the Blob within the BlobStore.
  size_t Index() const { return control_block_->index_; }

  // Returns the total size in bytes of the Blob within the BlobStore.
  size_t GetSize() const {
    if (control_block_ == nullptr || control_block_->store_ == nullptr) {
      return 0;
    }
    return control_block_->store_->GetSize(control_block_->index_);
  }

  // Casts BlobStoreObject<T> to a const-preserving BlobStoreObject<U>.
  // The casting mechanism ensures type safety.
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

  // Downgrades the BlobStoreObject to const and removes the ownership,
  // if the refcount equals 1, otherwise returns an empty BlobStoreObject.
  BlobStoreObject<const T> Downgrade() && {
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
  // This operation is valid if the BlobStoreObject is the sole owner of the
  // Blob. Otherwise, it returns an empty BlobStoreObject.
  BlobStoreObject<non_const_T> Upgrade() && {
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

  // Overloaded copy assignment operator.
  // Decrements the control block reference count of the current object and
  // deletes it if necessary. Then, makes this object point to the control block
  // of the other object and increments its refcount.
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

  // Overloaded move assignment operator.
  // Decrements the control block reference count of the current object and
  // deletes it if necessary. Then, takes ownership of the control block of the
  // other object.
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

  // Overloaded nullptr assignment operator.
  // Decrements the control block reference count of the current object and
  // deletes it if necessary. Then, sets the control block of this object to
  // nullptr.
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
  } else {
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

}  // namespace blob_store

#endif  // BLOB_STORE_OBJECT_H_
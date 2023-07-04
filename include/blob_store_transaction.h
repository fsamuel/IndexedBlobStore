#ifndef BLOB_STORE_TRANSACTION_H_
#define BLOB_STORE_TRANSACTION_H_

#include <optional>
#include <unordered_map>

#include "blob_store.h"

namespace blob_store {

// Head points to the latest version of a BlobStore-indexed data structure.
struct HeadNode {
  // The version of the transaction.
  std::size_t version;
  // The index of the root node.
  std::size_t root_index;
  // The index of the previous head.
  std::size_t previous;

  HeadNode(std::size_t version)
      : version(version),
        root_index(BlobStore::InvalidIndex),
        previous(BlobStore::InvalidIndex) {}

  HeadNode()
      : version(0),
        root_index(BlobStore::InvalidIndex),
        previous(BlobStore::InvalidIndex) {}
};

static_assert(std::is_trivially_copyable<HeadNode>::value,
              "HeadNode is trivially copyable");
static_assert(std::is_standard_layout<HeadNode>::value,
              "HeadNode is standard layout");

void PrintNode(BlobStoreObject<const HeadNode> node);

class Transaction {
 public:
  Transaction(BlobStore* blob_store, size_t head_index)
      : blob_store_(blob_store) {
    old_head_ = blob_store_->Get<HeadNode>(head_index);
    new_head_ = old_head_.Clone();
    ++new_head_->version;
    new_head_->previous = new_head_.Index();
    OperationInfo operation_info{OperationType::MUTATED_OBJECT,
                                 old_head_.Index(), new_head_.Index()};
    new_objects_.emplace(new_head_.Index(), operation_info);
  }

  // Aborts the transaction. All new objects are dropped.
  void Abort() && {
    for (const auto& obj : new_objects_) {
      blob_store_->Drop(obj.first);
    }
  }

  // Commits the transaction. Returns true if the commit was successful, false
  // otherwise.
  bool Commit() && {
    if (!old_head_.CompareAndSwap(new_head_)) {
      std::move(*this).Abort();
      return false;
    }
    return true;
  }

  template <typename T>
  BlobStoreObject<const T> GetRootNode() const {
    return blob_store_->Get<T>(new_head_->root_index);
  }

  // Sets the new_head's root to the provided index.
  void SetRootNode(size_t index) { new_head_->root_index = index; }

  // Returns a new object of type T. The object is initialized with the provided
  // arguments. The newly created object is tracked by the transaction and will
  // be deleted if the transaction is aborted.
  template <typename T, typename... Args>
  BlobStoreObject<T> New(Args&&... args) {
    BlobStoreObject<T> object =
        blob_store_->New<T>(std::forward<Args>(args)...);
    OperationInfo operation_info{OperationType::NEW_OBJECT,
                                 BlobStore::InvalidIndex, object.Index()};
    new_objects_.emplace(object.Index(), operation_info);
    return object;
  }

  // Returns a mutable version of the provided object. If the object is already
  // mutable, it is returned as-is. If the version of the node matches the
  // version of the transaction, then upgrade the pointer to a mutable version.
  // Otherwise, clone the node and set the version to the transaction's version.
  template <typename T>
  BlobStoreObject<typename std::remove_const<T>::type> GetMutable(
      BlobStoreObject<typename std::add_const<T>::type> object) {
    if (new_objects_.count(object.Index()) > 0) {
      return std::move(object).Upgrade();
    }
    auto new_object = object.Clone();
    OperationInfo operation_info{OperationType::MUTATED_OBJECT, object.Index(),
                                 new_object.Index()};
    discarded_objects_.emplace(object.Index(), operation_info);
    new_objects_.emplace(new_object.Index(), operation_info);
    return new_object;
  }

  // A non-const node is already mutable.
  template <typename T>
  BlobStoreObject<typename std::remove_const<T>::type> GetMutable(
      BlobStoreObject<typename std::remove_const<T>::type> node) {
    return node;
  }

  // Record that the object is no longer needed by the transaction. The object
  // will be deleted if the transaction is committed.
  template <typename T>
  void Drop(BlobStoreObject<T> obj) {
    // The object type doesn't matter when looking up the object in the new
    // objects set.
    if (new_objects_.count(obj.Index()) > 0) {
      new_objects_.erase(obj.Index());
    }
    OperationInfo operation_info{OperationType::DELETED_OBJECT, obj.Index(),
                                 BlobStore::InvalidIndex};
    discarded_objects_.emplace(obj.Index(), operation_info);
  }

 private:
  enum class OperationType { NEW_OBJECT, DELETED_OBJECT, MUTATED_OBJECT };
  struct OperationInfo {
    OperationType type;
    std::size_t old_index;
    std::size_t new_index;
  };

  BlobStore* blob_store_;
  // Holding onto the old head ensures we retain a snapshot of the tree.
  BlobStoreObject<const HeadNode> old_head_;
  BlobStoreObject<HeadNode> new_head_;
  std::unordered_map<size_t, OperationInfo> new_objects_;
  std::unordered_map<size_t, OperationInfo> discarded_objects_;
};

}  // namespace blob_store

#endif  // BLOB_STORE_TRANSACTION_H_
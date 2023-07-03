#ifndef BLOB_STORE_TRANSACTION_H_
#define BLOB_STORE_TRANSACTION_H_

#include <unordered_set>

#include "blob_store.h"

namespace blob_store {

class Transaction {
 public:
  Transaction(BlobStore* blob_store) : blob_store_(blob_store) {}

  // Aborts the transaction. All new objects are dropped.
  void Abort() && {
    for (const std::size_t& object_index : new_objects_) {
      blob_store_->Drop(object_index);
    }
  }

  // Commits the transaction. Returns true if the commit was successful, false
  // otherwise.
  virtual bool Commit() && = 0;

  // Returns a new object of type T. The object is initialized with the provided
  // arguments. The newly created object is tracked by the transaction and will
  // be deleted if the transaction is aborted.
  template <typename T, typename... Args>
  BlobStoreObject<T> New(Args&&... args) {
    BlobStoreObject<T> object =
        blob_store_->New<T>(std::forward<Args>(args)...);
    new_objects_.insert(object.Index());
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
    discarded_objects_.insert(object.Index());
    new_objects_.insert(new_object.Index());
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
    discarded_objects_.insert(obj.Index());
  }

 protected:
  BlobStore* blob_store_;
  std::unordered_set<size_t> new_objects_;
  std::unordered_set<size_t> discarded_objects_;
};

}  // namespace blob_store

#endif  // BLOB_STORE_TRANSACTION_H_
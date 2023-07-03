#ifndef B_PLUS_TREE_TRANSACTION_H_
#define B_PLUS_TREE_TRANSACTION_H_

#include <cstddef>
#include <unordered_set>

#include "b_plus_tree_base.h"
#include "b_plus_tree_iterator.h"
#include "b_plus_tree_nodes.h"

namespace b_plus_tree {

template <typename KeyType, typename ValueType, std::size_t Order>
class Transaction {
 public:
  using Iterator = TreeIterator<KeyType, ValueType, Order>;
  using BaseNode = BaseNode<Order>;
  using BPlusTreeBase = BPlusTreeBase<KeyType, ValueType, Order>;

  Transaction(BPlusTreeBase* tree, BlobStore* blob_store)
      : tree_(tree), blob_store_(blob_store) {
    old_head_ = blob_store_->Get<HeadNode>(1);
    new_head_ = old_head_.Clone();
    ++new_head_->version;
    new_head_->previous = new_head_.Index();
    new_objects_.insert(new_head_.Index());
  }

  void Insert(const KeyType& key, const ValueType& value) {
    BlobStoreObject<KeyType> key_ptr = New<KeyType>(key);
    BlobStoreObject<ValueType> value_ptr = New<ValueType>(value);
    Insert(std::move(key_ptr).Downgrade(), std::move(value_ptr).Downgrade());
  }

  void Insert(BlobStoreObject<const KeyType> key,
              BlobStoreObject<const ValueType> value) {
    tree_->Insert(this, std::move(key), std::move(value));
  }

  Iterator Search(const KeyType& key) { return tree_->Search(new_head_, key); }

  BlobStoreObject<const ValueType> Delete(const KeyType& key) {
    return tree_->Delete(this, key);
  }

  // Aborts the transaction. All new objects are dropped.
  void Abort() && {
    for (const std::size_t& object_index : new_objects_) {
      blob_store_->Drop(object_index);
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

  BlobStoreObject<const BaseNode> GetNewRoot() const {
    return blob_store_->Get<BaseNode>(new_head_->root_index);
  }

  // Sets the new_head's root to the provided index.
  void SetNewRoot(size_t index) { new_head_->root_index = index; }

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

 private:
  BPlusTreeBase* tree_;
  BlobStore* blob_store_;
  // Holding onto the old head ensures we retain a snapshot of the tree.
  BlobStoreObject<const HeadNode> old_head_;
  BlobStoreObject<HeadNode> new_head_;
  std::unordered_set<size_t> new_objects_;
  std::unordered_set<size_t> discarded_objects_;
  template <typename KeyType, typename ValueType, std::size_t Order, typename T>
  friend struct TransactionHelper;
};

}  // namespace b_plus_tree

#endif  // B_PLUS_TREE_TRANSACTION_H_
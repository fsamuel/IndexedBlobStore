#ifndef B_PLUS_TREE_TRANSACTION_H_
#define B_PLUS_TREE_TRANSACTION_H_

#include <cstddef>
#include <unordered_set>

#include "b_plus_tree_base.h"
#include "b_plus_tree_iterator.h"
#include "b_plus_tree_nodes.h"

#include "blob_store_transaction.h"

namespace b_plus_tree {

template <typename KeyType, typename ValueType, std::size_t Order>
class Transaction : public blob_store::Transaction {
 public:
  using Iterator = TreeIterator<KeyType, ValueType, Order>;
  using BaseNode = BaseNode<Order>;
  using BPlusTreeBase = BPlusTreeBase<KeyType, ValueType, Order>;

  Transaction(BPlusTreeBase* tree, BlobStore* store)
      : blob_store::Transaction(store), tree_(tree) {
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

  // Commits the transaction. Returns true if the commit was successful, false
  // otherwise.
  bool Commit() && override {
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

 private:
  BPlusTreeBase* tree_;
  // Holding onto the old head ensures we retain a snapshot of the tree.
  BlobStoreObject<const HeadNode> old_head_;
  BlobStoreObject<HeadNode> new_head_;
};

}  // namespace b_plus_tree

#endif  // B_PLUS_TREE_TRANSACTION_H_
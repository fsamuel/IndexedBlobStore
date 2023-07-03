#ifndef B_PLUS_TREE_TRANSACTION_H_
#define B_PLUS_TREE_TRANSACTION_H_

#include <cstddef>
#include <unordered_set>

#include "b_plus_tree_base.h"
#include "b_plus_tree_iterator.h"
#include "b_plus_tree_nodes.h"

#include "blob_store_transaction.h"

namespace b_plus_tree {

using blob_store::HeadNode;

template <typename KeyType, typename ValueType, std::size_t Order>
class Transaction : public blob_store::Transaction {
 public:
  using Iterator = TreeIterator<KeyType, ValueType, Order>;
  using BaseNode = BaseNode<Order>;
  using BPlusTreeBase = BPlusTreeBase<KeyType, ValueType, Order>;

  Transaction(BPlusTreeBase* tree, BlobStore* store, size_t head_index)
      : blob_store::Transaction(store, head_index), tree_(tree) {}

  void Insert(const KeyType& key, const ValueType& value) {
    BlobStoreObject<KeyType> key_ptr = New<KeyType>(key);
    BlobStoreObject<ValueType> value_ptr = New<ValueType>(value);
    Insert(std::move(key_ptr).Downgrade(), std::move(value_ptr).Downgrade());
  }

  void Insert(BlobStoreObject<const KeyType> key,
              BlobStoreObject<const ValueType> value) {
    tree_->Insert(this, std::move(key), std::move(value));
  }

  Iterator Search(const KeyType& key) { return tree_->Search(this, key); }

  BlobStoreObject<const ValueType> Delete(const KeyType& key) {
    return tree_->Delete(this, key);
  }

 private:
  BPlusTreeBase* tree_;
};

}  // namespace b_plus_tree

#endif  // B_PLUS_TREE_TRANSACTION_H_
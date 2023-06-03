#ifndef B_PLUS_TREE_BASE_H_
#define B_PLUS_TREE_BASE_H_

#include "blob_store.h"
#include "nodes.h"
#include "tree_iterator.h"

template <typename KeyType, typename ValueType, std::size_t Order>
class Transaction;

template <typename KeyType, typename ValueType, std::size_t Order>
class BPlusTreeBase {
 public:
  using Transaction = Transaction<KeyType, ValueType, Order>;
  using Iterator = TreeIterator<KeyType, ValueType, Order>;

  virtual void Insert(Transaction* transaction,
                      BlobStoreObject<const KeyType> key,
                      BlobStoreObject<const ValueType> value) = 0;
  virtual BlobStoreObject<const ValueType> Delete(Transaction* transaction,
                                                  const KeyType& key) = 0;
  virtual Iterator Search(BlobStoreObject<HeadNode> head,
                          const KeyType& key) = 0;
};

#endif  // B_PLUS_TREE_BASE_H_
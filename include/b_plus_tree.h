#ifndef B_PLUS_TREE_H_
#define B_PLUS_TREE_H_

#include <limits>
#include <queue>

#include "b_plus_tree_base.h"
#include "b_plus_tree_iterator.h"
#include "b_plus_tree_nodes.h"
#include "b_plus_tree_transaction.h"
#include "blob_store.h"

namespace b_plus_tree {

// InsertionBundle represents output of an insertion operation.
// It is either empty or contains a BlobStoreObject of the newly cloned node,
// the key to be inserted into the parent node, and the BlobStoreObect of the
// new right sibling node.
template <typename KeyType, typename NodeType>
struct InsertionBundle {
  InsertionBundle(BlobStoreObject<NodeType> new_left_node,
                  BlobStoreObject<const KeyType> new_key,
                  BlobStoreObject<NodeType> new_right_node)
      : new_key(new_key),
        new_left_node(new_left_node),
        new_right_node(new_right_node) {}

  BlobStoreObject<const KeyType> new_key;
  BlobStoreObject<NodeType> new_left_node;
  BlobStoreObject<NodeType> new_right_node;
};

template <typename KeyType, typename ValueType, std::size_t Order>
class BPlusTree : public BPlusTreeBase<KeyType, ValueType, Order> {
 private:
  using BaseNode = BaseNode<Order>;
  using InternalNode = InternalNode<Order>;
  using LeafNode = LeafNode<Order>;
  using Transaction = BPlusTreeBase<KeyType, ValueType, Order>::Transaction;

  using InsertionBundle = InsertionBundle<KeyType, BaseNode>;
  using Iterator = TreeIterator<KeyType, ValueType, Order>;

 public:
  BPlusTree(BlobStore& blob_store) : blob_store_(blob_store) {
    if (blob_store_.IsEmpty()) {
      CreateRoot();
    }
  }

  Transaction CreateTransaction() { return Transaction(this, &blob_store_); }

  // BPlusTreeBase implementation.
  void Insert(Transaction* transaction,
              BlobStoreObject<const KeyType> key,
              BlobStoreObject<const ValueType> value) override;
  Iterator Search(BlobStoreObject<HeadNode> head, const KeyType& key) override;
  BlobStoreObject<const ValueType> Delete(Transaction* transaction,
                                          const KeyType& key) override;

  // Returns an iterator to the first element greater than or equal to key.
  Iterator Search(const KeyType& key);

  // Inserts a key-value pair into the tree. Returns true if the key-value pair
  // was inserted, false if the key already existed in the tree or there was a
  // conflicting operation in progress.
  bool Insert(const KeyType& key, const ValueType& value);

  // Deletes a key-value pair from the tree. Returns true if the operation was
  // successful, false if there was a conflicting operation in progress. If
  // deleted_value is not null, the deleted value is stored in deleted_value.
  BlobStoreObject<const ValueType> Delete(const KeyType& key);

  // Prints the tree in a human-readable format in breadth-first order.
  void Print(size_t version = std::numeric_limits<size_t>::max());

 private:
  BlobStore& blob_store_;

  void CreateRoot() {
    auto head = blob_store_.New<HeadNode>();
    auto root = blob_store_.New<LeafNode>();
    head->set_version(0);
    head->root_index = root.Index();
    head->previous = BlobStore::InvalidIndex;
  }

  // Searches for the provided key in the provided subtree rooted at node.
  // Returns an iterator starting at the first key >= key. If the key is not
  // found, the iterator will be invalid. If the key is found, the path from
  // the leaf to the root of the tree is returned in path_to_root.
  Iterator Search(BlobStoreObject<const BaseNode> node,
                  const KeyType& key,
                  std::vector<size_t> path_to_root);

  // Split a leaf node into two leaf nodes and a middle key, all returned in
  // InsertionBundle. left_node is modified directly.
  InsertionBundle SplitLeafNode(Transaction* transaction,
                                BlobStoreObject<LeafNode> left_node);

  // Split an internal node into two internal nodes nodes and a middle key, all
  // returned in InsertionBundle. left_node is modified directly.
  InsertionBundle SplitInternalNode(Transaction* transaction,
                                    BlobStoreObject<InternalNode> left_node);

  // Inserts key and value into the leaf node |node|. This method accepts both
  // const and non-const leaves. If the leaf is const (we're holding a read
  // lock), we clone the node and insert into the clone. If the leaf is
  // non-const, we insert directly into the node.
  template <typename U>
  typename std::enable_if<
      std::is_same<
          typename std::remove_const<U>::type,
          typename BPlusTree<KeyType, ValueType, Order>::LeafNode>::value,
      InsertionBundle>::type
  InsertIntoLeaf(Transaction* transaction,
                 BlobStoreObject<U> node,
                 BlobStoreObject<const KeyType> key,
                 BlobStoreObject<const ValueType> value);

  // Insert new_key and new_child into node with the assumption that node is
  // not full.
  void InsertKeyChildIntoInternalNode(BlobStoreObject<InternalNode> node,
                                      BlobStoreObject<const KeyType> new_key,
                                      BlobStoreObject<BaseNode> new_child);

  InsertionBundle Insert(Transaction* transaction,
                         BlobStoreObject<const BaseNode> node,
                         BlobStoreObject<const KeyType> key,
                         BlobStoreObject<const ValueType> value);

  BlobStoreObject<const ValueType> Delete(
      Transaction* transaction,
      BlobStoreObject<BaseNode>* parent_node,
      size_t child_index,
      const KeyType& key);

  BlobStoreObject<const ValueType> DeleteFromLeafNode(
      BlobStoreObject<LeafNode> node,
      const KeyType& key);

  BlobStoreObject<const ValueType> DeleteFromInternalNode(
      Transaction* transaction,
      BlobStoreObject<InternalNode> node,
      const KeyType& key);

  // Borrow a key from the left sibling of node and return the new right
  // sibling.
  bool BorrowFromLeftSibling(Transaction* transaction,
                             BlobStoreObject<InternalNode> parent_node,
                             BlobStoreObject<const BaseNode> left_sibling,
                             BlobStoreObject<const BaseNode> right_sibling,
                             size_t child_index,
                             BlobStoreObject<BaseNode>* out_right_sibling);

  // Borrow a key from the right sibling of node and return the new left
  // sibling.
  bool BorrowFromRightSibling(Transaction* transaction,
                              BlobStoreObject<InternalNode> parent_node,
                              BlobStoreObject<const BaseNode> left_sibling,
                              BlobStoreObject<const BaseNode> right_sibling,
                              size_t child_index,
                              BlobStoreObject<BaseNode>* out_left_sibling);

  // Returns the key of the successor of node.
  BlobStoreObject<const KeyType> GetSuccessorKey(
      BlobStoreObject<const BaseNode> node,
      const KeyType& key);

  // Merges the right child into the left child. The parent key separating the
  // two children is merged into the left child.
  void MergeInternalNodes(BlobStoreObject<InternalNode> left_child,
                          BlobStoreObject<const InternalNode> right_child,
                          size_t parent_key);

  // Merges the right child into the left child. Leaves contain all the keys so
  // we don't need to pass the parent key.
  void MergeLeafNodes(BlobStoreObject<LeafNode> left_child,
                      BlobStoreObject<const LeafNode> right_child);

  // Merges the provded child with its left or right sibling depending on
  // whether child is the rightmost child of its parent. The new child is
  // returned in out_child.
  void MergeChildWithLeftOrRightSibling(Transaction* transaction,
                                        BlobStoreObject<InternalNode> parent,
                                        size_t child_index,
                                        BlobStoreObject<const BaseNode> child,
                                        BlobStoreObject<BaseNode>* out_child);

  // If the left or right sibling of child has more than the minimum number of
  // keys, borrow a key from the sibling. Otherwise, merge the child with its
  // sibling. The new child is returned in new_child.
  void RebalanceChildWithLeftOrRightSibling(
      Transaction* transaction,
      BlobStoreObject<InternalNode> parent,
      size_t child_index,
      BlobStoreObject<const BaseNode> child,
      BlobStoreObject<BaseNode>* new_child);
};

template <typename KeyType, typename ValueType, size_t Order>
bool BPlusTree<KeyType, ValueType, Order>::Insert(const KeyType& key,
                                                  const ValueType& value) {
  while (true) {
    Transaction txn(CreateTransaction());
    BlobStoreObject<KeyType> key_ptr = txn.New<KeyType>(key);
    BlobStoreObject<ValueType> value_ptr = txn.New<ValueType>(value);
    txn.Insert(std::move(key_ptr).Downgrade(),
               std::move(value_ptr).Downgrade());
    if (std::move(txn).Commit()) {
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::Insert(
    Transaction* transaction,
    BlobStoreObject<const KeyType> key,
    BlobStoreObject<const ValueType> value) {
  BlobStoreObject<const BaseNode> root = transaction->GetNewRoot();
  InsertionBundle bundle = Insert(transaction, std::move(root), key, value);
  if (bundle.new_right_node != nullptr) {
    BlobStoreObject<InternalNode> new_root = transaction->New<InternalNode>(1);
    new_root->children[0] = bundle.new_left_node.Index();
    new_root->children[1] = bundle.new_right_node.Index();
    new_root->set_num_keys(1);
    new_root->set_key(0, bundle.new_key.Index());
    transaction->SetNewRoot(new_root.Index());
  } else {
    transaction->SetNewRoot(bundle.new_left_node.Index());
  }
}

template <typename KeyType, typename ValueType, size_t Order>
typename BPlusTree<KeyType, ValueType, Order>::Iterator
BPlusTree<KeyType, ValueType, Order>::Search(const KeyType& key) {
  BlobStoreObject<const HeadNode> head = blob_store_.Get<HeadNode>(1);
  if (head->root_index == BlobStore::InvalidIndex) {
    return Iterator(&blob_store_, std::vector<size_t>(), 0);
  }
  return Search(blob_store_.Get<BaseNode>(head->root_index), key,
                std::vector<size_t>());
}

template <typename KeyType, typename ValueType, size_t Order>
typename BPlusTree<KeyType, ValueType, Order>::Iterator
BPlusTree<KeyType, ValueType, Order>::Search(BlobStoreObject<HeadNode> head,
                                             const KeyType& key) {
  BlobStoreObject<const BaseNode> root =
      blob_store_.Get<BaseNode>(head->root_index);
  if (head->root_index == BlobStore::InvalidIndex) {
    return Iterator(&blob_store_, std::vector<size_t>(), 0);
  }
  return Search(std::move(root), key, std::vector<size_t>());
}

template <typename KeyType, typename ValueType, size_t Order>
typename BPlusTree<KeyType, ValueType, Order>::Iterator
BPlusTree<KeyType, ValueType, Order>::Search(
    BlobStoreObject<const BaseNode> node,
    const KeyType& key,
    std::vector<size_t> path_to_root) {
  path_to_root.push_back(node.Index());

  BlobStoreObject<const KeyType> key_found;
  size_t key_index = node->Search(&blob_store_, key, &key_found);

  if (node->is_leaf()) {
    return Iterator(&blob_store_, std::move(path_to_root), key_index);
  }

  if (key_index < node->num_keys() && key == *key_found) {
    BlobStoreObject<const BaseNode> child;
    GetChild(node.To<InternalNode>(), key_index + 1, &child);
    return Search(std::move(child), key, std::move(path_to_root));
  }
  BlobStoreObject<const BaseNode> child;
  GetChild(node.To<InternalNode>(), key_index, &child);
  return Search(std::move(child), key, std::move(path_to_root));
}

template <typename KeyType, typename ValueType, size_t Order>
typename BPlusTree<KeyType, ValueType, Order>::InsertionBundle
BPlusTree<KeyType, ValueType, Order>::SplitLeafNode(
    Transaction* transaction,
    BlobStoreObject<LeafNode> left_node) {
  // Create a new right node tracked by the provided transaction.
  BlobStoreObject<LeafNode> new_right_node = transaction->New<LeafNode>();

  // Find the middle key.
  size_t middle_key_index = (left_node->num_keys() - 1) / 2;
  BlobStoreObject<const KeyType> middle_key;
  GetKey(left_node, middle_key_index, &middle_key);

  // Copy the middle keys/values onward to the right node.
  new_right_node->set_num_keys(left_node->num_keys() - middle_key_index);
  for (int i = 0; i < new_right_node->num_keys(); ++i) {
    new_right_node->set_key(i, left_node->get_key(middle_key_index + i));
    new_right_node->values[i] = left_node->values[middle_key_index + i];
    left_node->set_key(middle_key_index + i, BlobStore::InvalidIndex);
    left_node->values[middle_key_index + i] = BlobStore::InvalidIndex;
  }
  left_node->values[middle_key_index] = BlobStore::InvalidIndex;

  // Update the key count of the left_node.
  left_node->set_num_keys(middle_key_index);
  left_node->set_key(middle_key_index, BlobStore::InvalidIndex);

  return InsertionBundle(left_node.To<BaseNode>(), middle_key,
                         new_right_node.To<BaseNode>());
}

template <typename KeyType, typename ValueType, size_t Order>
typename BPlusTree<KeyType, ValueType, Order>::InsertionBundle
BPlusTree<KeyType, ValueType, Order>::SplitInternalNode(
    Transaction* transaction,
    BlobStoreObject<InternalNode> left_node) {
  BlobStoreObject<InternalNode> new_right_node =
      transaction->New<InternalNode>(Order);

  size_t middle_key_index = (left_node->num_keys() - 1) / 2;
  BlobStoreObject<const KeyType> middle_key;
  GetKey(left_node, middle_key_index, &middle_key);

  new_right_node->set_num_keys(left_node->num_keys() - middle_key_index - 1);
  for (int i = 0; i < new_right_node->num_keys(); ++i) {
    new_right_node->set_key(i, left_node->get_key(middle_key_index + i + 1));
    new_right_node->children[i] = left_node->children[middle_key_index + i + 1];
    left_node->set_key(middle_key_index + i + 1, BlobStore::InvalidIndex);
    left_node->children[middle_key_index + i + 1] = BlobStore::InvalidIndex;
  }
  new_right_node->children[new_right_node->num_keys()] =
      left_node->children[middle_key_index + new_right_node->num_keys() + 1];
  left_node->children[middle_key_index + new_right_node->num_keys() + 1] =
      BlobStore::InvalidIndex;

  left_node->set_num_keys(middle_key_index);
  left_node->set_key(middle_key_index, BlobStore::InvalidIndex);
  return InsertionBundle(left_node.To<BaseNode>(), middle_key,
                         new_right_node.To<BaseNode>());
}

template <typename KeyType, typename ValueType, size_t Order>
template <typename U>
typename std::enable_if<
    std::is_same<
        typename std::remove_const<U>::type,
        typename BPlusTree<KeyType, ValueType, Order>::LeafNode>::value,
    typename BPlusTree<KeyType, ValueType, Order>::InsertionBundle>::type
BPlusTree<KeyType, ValueType, Order>::InsertIntoLeaf(
    Transaction* transaction,
    BlobStoreObject<U> node,
    BlobStoreObject<const KeyType> key,
    BlobStoreObject<const ValueType> value) {
  BlobStoreObject<LeafNode> new_left_node =
      transaction->GetMutable<U>(std::move(node));

  if (new_left_node->is_full()) {
    InsertionBundle bundle =
        SplitLeafNode(transaction, std::move(new_left_node));
    // We pass the recursive InsertIntoLeaf a non-const LeafNode so we won't
    // clone it.
    if (*key >= *bundle.new_key) {
      bundle.new_right_node =
          InsertIntoLeaf(transaction,
                         std::move(bundle.new_right_node).To<LeafNode>(),
                         std::move(key), std::move(value))
              .new_left_node;
    } else {
      bundle.new_left_node =
          InsertIntoLeaf(transaction,
                         std::move(bundle.new_left_node).To<LeafNode>(),
                         std::move(key), std::move(value))
              .new_left_node;
    }
    return bundle;
  }
  // Shift the keys and values right.
  size_t i = new_left_node->num_keys();
  for (; i > 0; --i) {
    BlobStoreObject<const KeyType> key_ptr;
    GetKey(new_left_node, i - 1, &key_ptr);
    if (*key >= *key_ptr) {
      break;
    }
    new_left_node->set_key(i, new_left_node->get_key(i - 1));
    new_left_node->values[i] = new_left_node->values[i - 1];
  }
  new_left_node->set_key(i, key.Index());
  new_left_node->values[i] = value.Index();
  new_left_node->increment_num_keys();
  return InsertionBundle(new_left_node.To<BaseNode>(),
                         BlobStoreObject<const KeyType>(),
                         BlobStoreObject<BaseNode>());
}

template <typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::InsertKeyChildIntoInternalNode(
    BlobStoreObject<InternalNode> node,
    BlobStoreObject<const KeyType> new_key,
    BlobStoreObject<BaseNode> new_child) {
  size_t i = node->num_keys();
  for (; i > 0; --i) {
    BlobStoreObject<const KeyType> key_ptr;
    GetKey(node, i - 1, &key_ptr);
    if (*new_key >= *key_ptr) {
      break;
    }
    node->set_key(i, node->get_key(i - 1));
    node->children[i + 1] = node->children[i];
  }
  node->set_key(i, new_key.Index());
  node->children[i + 1] = new_child.Index();
  node->increment_num_keys();
}

template <typename KeyType, typename ValueType, size_t Order>
typename BPlusTree<KeyType, ValueType, Order>::InsertionBundle
BPlusTree<KeyType, ValueType, Order>::Insert(
    Transaction* transaction,
    BlobStoreObject<const BaseNode> node,
    BlobStoreObject<const KeyType> key,
    BlobStoreObject<const ValueType> value) {
  if (node->is_leaf()) {
    return InsertIntoLeaf(transaction, std::move(node).To<LeafNode>(),
                          std::move(key), std::move(value));
  }

  // Find the child node where the new key-value should be inserted.
  auto internal_node = std::move(node).To<InternalNode>();

  // TODO(fsamuel): This is constructing a string just to search it. This is
  // inefficient.
  BlobStoreObject<const KeyType> key_found;
  size_t key_index = internal_node->Search(&blob_store_, *key, &key_found);

  // Don't hold onto the child node longer than necessary to avoid failing to
  // upgrade its lock.
  BlobStoreObject<const BaseNode> child;
  GetChildConst(internal_node, key_index, &child);
  InsertionBundle child_node_bundle =
      Insert(transaction, std::move(child), key, value);
  BlobStoreObject<InternalNode> new_internal_node =
      transaction->GetMutable<InternalNode>(std::move(internal_node));

  new_internal_node->children[key_index] =
      child_node_bundle.new_left_node.Index();
  // If the child node bundle has a new right node, then that means that a split
  // occurred to insert the key/value pair. We need to find a place to insert
  // the new middle key.
  if (child_node_bundle.new_right_node != nullptr) {
    // The internal node is full so we need to recursively split to find a
    if (new_internal_node->is_full()) {
      // insert the new child node and its minimum key into the parent node
      // recursively
      InsertionBundle node_bundle =
          SplitInternalNode(transaction, new_internal_node);

      // The parent node is full so we need to split it and insert the new node
      // into the parent node or its new sibling.
      if (*child_node_bundle.new_key < *node_bundle.new_key) {
        InsertKeyChildIntoInternalNode(
            node_bundle.new_left_node.To<InternalNode>(),
            std::move(child_node_bundle.new_key),
            std::move(child_node_bundle.new_right_node));
      } else {
        InsertKeyChildIntoInternalNode(
            node_bundle.new_right_node.To<InternalNode>(),
            std::move(child_node_bundle.new_key),
            std::move(child_node_bundle.new_right_node));
      }
      // return the new node and its middle key to be inserted into the parent
      // node recursively
      return node_bundle;
    }
    // insert the new child node and its minimum key into the parent node
    for (size_t j = new_internal_node->num_keys(); j > key_index; --j) {
      new_internal_node->children[j + 1] = new_internal_node->children[j];
      new_internal_node->set_key(j, new_internal_node->get_key(j - 1));
    }
    new_internal_node->children[key_index + 1] =
        child_node_bundle.new_right_node.Index();
    new_internal_node->set_key(key_index, child_node_bundle.new_key.Index());
    new_internal_node->increment_num_keys();
  }
  // No split occurred so nothing to return.
  return InsertionBundle(new_internal_node.To<BaseNode>(),
                         BlobStoreObject<const KeyType>(),
                         BlobStoreObject<BaseNode>());
}

template <typename KeyType, typename ValueType, size_t Order>
BlobStoreObject<const ValueType> BPlusTree<KeyType, ValueType, Order>::Delete(
    const KeyType& key) {
  while (true) {
    Transaction txn(CreateTransaction());
    BlobStoreObject<const ValueType> deleted = txn.Delete(key);
    if (std::move(txn).Commit()) {
      return deleted;
    }
  }
}

template <typename KeyType, typename ValueType, size_t Order>
BlobStoreObject<const ValueType> BPlusTree<KeyType, ValueType, Order>::Delete(
    Transaction* transaction,
    const KeyType& key) {
  BlobStoreObject<const BaseNode> root = transaction->GetNewRoot();
  BlobStoreObject<BaseNode> new_root =
      transaction->GetMutable<BaseNode>(std::move(root));

  if (new_root->is_leaf()) {
    // If the root is a leaf node, then we can just delete the key from the leaf
    // node.
    transaction->SetNewRoot(new_root.Index());
    return DeleteFromLeafNode(new_root.To<LeafNode>(), key);
  }

  // Find the child node where the key should be deleted.
  BlobStoreObject<const KeyType> key_found;
  size_t key_index = new_root->Search(&blob_store_, key, &key_found);

  BlobStoreObject<const ValueType> deleted;
  // As part of the Delete operation, the root node may have been deleted and
  // replaced with a new root node.
  if (key_index < new_root->num_keys() && key == *key_found) {
    deleted = Delete(transaction, &new_root, key_index + 1, key);
  } else {
    deleted = Delete(transaction, &new_root, key_index, key);
  }
  transaction->SetNewRoot(new_root.Index());
  return deleted;
}

template <typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::Print(size_t version) {
  struct NodeWithLevel {
    BlobStoreObject<const BaseNode> node;
    size_t level;
  };
  std::queue<NodeWithLevel> queue;
  BlobStoreObject<const HeadNode> head = blob_store_.Get<HeadNode>(1);
  // Find the head with the given version
  while (head->previous != BlobStore::InvalidIndex &&
         head->get_version() > version) {
    head = blob_store_.Get<HeadNode>(head->previous);
  }
  PrintNode(head);
  queue.push({blob_store_.Get<BaseNode>(head->root_index), 1});
  while (!queue.empty()) {
    NodeWithLevel node_with_level = queue.front();
    queue.pop();
    if (node_with_level.node->is_internal()) {
      BlobStoreObject<const InternalNode> internal_node =
          node_with_level.node.To<InternalNode>();
      for (size_t i = 0; i <= internal_node->num_keys(); ++i) {
        BlobStoreObject<const BaseNode> child_ptr;
        GetChild(internal_node, i, &child_ptr);
        queue.push({std::move(child_ptr), node_with_level.level + 1});
      }
      std::cout << std::string(node_with_level.level, ' ');
      PrintNode<KeyType>(internal_node);
    } else {
      BlobStoreObject<const LeafNode> leaf_node =
          node_with_level.node.To<LeafNode>();
      std::cout << std::string(node_with_level.level, ' ');
      PrintNode<KeyType>(leaf_node);
    }
  }
}

template <typename KeyType, typename ValueType, size_t Order>
BlobStoreObject<const ValueType>
BPlusTree<KeyType, ValueType, Order>::DeleteFromLeafNode(
    BlobStoreObject<LeafNode> node,
    const KeyType& key) {
  BlobStoreObject<const KeyType> key_found;
  size_t key_index = node->Search(&blob_store_, key, &key_found);

  if (!key_found) {
    return BlobStoreObject<const ValueType>();
  }

  BlobStoreObject<const ValueType> deleted_value;
  GetValue(node, key_index, &deleted_value);

  // Shift keys and values to fill the gap
  for (size_t j = key_index + 1; j < node->num_keys(); j++) {
    node->set_key(j - 1, node->get_key(j));
    node->values[j - 1] = node->values[j];
  }
  node->decrement_num_keys();

  return deleted_value;  // Key successfully removed
}

template <typename KeyType, typename ValueType, size_t Order>
BlobStoreObject<const ValueType>
BPlusTree<KeyType, ValueType, Order>::DeleteFromInternalNode(
    Transaction* transaction,
    BlobStoreObject<InternalNode> node,
    const KeyType& key) {
  BlobStoreObject<const KeyType> key_found;
  size_t key_index = node->Search(&blob_store_, key, &key_found);

  BlobStoreObject<BaseNode> internal_node_base = node.To<BaseNode>();

  // We found the first key larger or equal to the node we're looking for.
  // Case 1: key == *current_key: we need to delete the key.
  if (key_index < node->num_keys() && key == *key_found) {
    // The key/value pair is in the right child. Recurse down
    // to delete the key/value pair first.
    auto deleted_value =
        Delete(transaction, &internal_node_base, key_index + 1, key);
    // We need to update current key to a new successor since we just deleted
    // the successor to this node. We shouldn't refer to nodes that don't exist.
    BlobStoreObject<const KeyType> key_found;
    size_t key_index = node->Search(&blob_store_, key, &key_found);

    if (key_index < node->num_keys() && key == *key_found) {
      // Can there ever be a null successor? That means there is no successor at
      // all. That shouldn't happen I think.
      auto key_ptr = GetSuccessorKey(node.To<const BaseNode>(), key);
      node->set_key(key_index, key_ptr.Index());
    }
    return deleted_value;
  }
  // Delete at the left child if the current key is larger.
  return Delete(transaction, &internal_node_base, key_index, key);
}

template <typename KeyType, typename ValueType, size_t Order>
bool BPlusTree<KeyType, ValueType, Order>::BorrowFromLeftSibling(
    Transaction* transaction,
    BlobStoreObject<InternalNode> parent_node,
    BlobStoreObject<const BaseNode> left_sibling,
    BlobStoreObject<const BaseNode> right_sibling,
    size_t child_index,
    BlobStoreObject<BaseNode>* out_right_sibling) {
  BlobStoreObject<BaseNode> new_left_sibling =
      transaction->GetMutable<BaseNode>(std::move(left_sibling));
  BlobStoreObject<BaseNode> new_right_sibling =
      transaction->GetMutable<BaseNode>(std::move(right_sibling));
  *out_right_sibling = new_right_sibling;

  parent_node->children[child_index - 1] = new_left_sibling.Index();
  parent_node->children[child_index] = new_right_sibling.Index();

  // Move keys and children in the child node to make space for the borrowed key
  for (size_t i = new_right_sibling->num_keys(); i > 0; --i) {
    new_right_sibling->keys[i] = new_right_sibling->keys[i - 1];
  }

  if (new_right_sibling->is_internal()) {
    auto new_right_sibling_internal_node = new_right_sibling.To<InternalNode>();
    auto new_left_sibling_internal_node = new_left_sibling.To<InternalNode>();

    for (size_t i = new_right_sibling_internal_node->num_keys(); i >= 0; --i) {
      new_right_sibling_internal_node->children[i + 1] =
          new_right_sibling_internal_node->children[i];
    }
    new_right_sibling_internal_node->children[0] =
        new_left_sibling_internal_node
            ->children[new_left_sibling_internal_node->num_keys()];
    new_left_sibling_internal_node
        ->children[new_left_sibling_internal_node->num_keys()] =
        BlobStore::InvalidIndex;
    new_right_sibling->set_key(0, parent_node->get_key(child_index - 1));
  } else {
    auto new_right_sibling_leaf_node = new_right_sibling.To<LeafNode>();
    auto new_left_sibling_leaf_node = new_left_sibling.To<LeafNode>();

    // Move keys and children in the child node to make space for the borrowed
    // key
    for (size_t i = new_right_sibling_leaf_node->num_keys(); i > 0; --i) {
      new_right_sibling_leaf_node->values[i] =
          new_right_sibling_leaf_node->values[i - 1];
    }
    new_right_sibling_leaf_node->values[0] =
        new_left_sibling_leaf_node
            ->values[new_left_sibling_leaf_node->num_keys() - 1];
    new_right_sibling->set_key(0,
                               new_left_sibling_leaf_node->get_key(
                                   new_left_sibling_leaf_node->num_keys() - 1));
  }

  // We want to move this out so we need to return the last key of the left
  // sibling that we're bumping up.
  parent_node->set_key(child_index - 1, new_left_sibling->get_key(
                                            new_left_sibling->num_keys() - 1));

  new_left_sibling->set_key(new_left_sibling->num_keys() - 1,
                            BlobStore::InvalidIndex);
  new_right_sibling->increment_num_keys();

  new_left_sibling->decrement_num_keys();

  return true;
}

template <typename KeyType, typename ValueType, size_t Order>
bool BPlusTree<KeyType, ValueType, Order>::BorrowFromRightSibling(
    Transaction* transaction,
    BlobStoreObject<InternalNode> parent_node,
    BlobStoreObject<const BaseNode> left_sibling,
    BlobStoreObject<const BaseNode> right_sibling,
    size_t child_index,
    BlobStoreObject<BaseNode>* out_left_sibling) {
  BlobStoreObject<BaseNode> new_left_sibling =
      transaction->GetMutable<BaseNode>(std::move(left_sibling));
  BlobStoreObject<BaseNode> new_right_sibling =
      transaction->GetMutable<BaseNode>(std::move(right_sibling));

  *out_left_sibling = new_left_sibling;

  parent_node->children[child_index] = new_left_sibling.Index();
  parent_node->children[child_index + 1] = new_right_sibling.Index();

  size_t key_index;
  if (new_left_sibling->is_internal()) {
    auto new_left_internal_node = new_left_sibling.To<InternalNode>();
    auto new_right_internal_node = new_right_sibling.To<InternalNode>();

    new_left_internal_node->set_key(new_left_sibling->num_keys(),
                                    parent_node->get_key(child_index));
    new_left_internal_node->children[new_left_internal_node->num_keys() + 1] =
        new_right_internal_node->children[0];

    for (int i = 1; i <= new_right_internal_node->num_keys(); ++i) {
      new_right_internal_node->children[i - 1] =
          new_right_internal_node->children[i];
    }
    new_right_internal_node->children[new_right_internal_node->num_keys()] =
        BlobStore::InvalidIndex;
    key_index = new_right_sibling->get_key(0);
  } else {
    auto new_left_leaf_node = new_left_sibling.To<LeafNode>();
    auto new_right_leaf_node = new_right_sibling.To<LeafNode>();

    new_left_leaf_node->set_key(new_left_leaf_node->num_keys(),
                                new_right_leaf_node->get_key(0));
    new_left_leaf_node->values[new_left_leaf_node->num_keys()] =
        new_right_leaf_node->values[0];

    for (int i = 1; i < new_right_leaf_node->num_keys(); ++i) {
      new_right_leaf_node->values[i - 1] = new_right_leaf_node->values[i];
    }
    new_right_leaf_node->values[new_right_leaf_node->num_keys() - 1] =
        BlobStore::InvalidIndex;

    key_index = new_right_sibling->get_key(1);
  }

  for (int i = 1; i < new_right_sibling->num_keys(); ++i) {
    new_right_sibling->keys[i - 1] = new_right_sibling->keys[i];
  }
  new_right_sibling->keys[new_right_sibling->num_keys() - 1] =
      BlobStore::InvalidIndex;

  new_left_sibling->increment_num_keys();
  new_right_sibling->decrement_num_keys();

  parent_node->set_key(child_index, key_index);

  return true;
}

template <typename KeyType, typename ValueType, size_t Order>
BlobStoreObject<const ValueType> BPlusTree<KeyType, ValueType, Order>::Delete(
    Transaction* transaction,
    BlobStoreObject<BaseNode>* parent_node,
    size_t child_index,
    const KeyType& key) {
  BlobStoreObject<InternalNode> parent_internal_node =
      parent_node->To<InternalNode>();
  BlobStoreObject<const BaseNode> const_child;
  GetChildConst(parent_internal_node, child_index, &const_child);
  BlobStoreObject<BaseNode> child;
  // The current child where we want to delete a node is too small.
  if (const_child->will_underflow()) {
    // Rebalancing might involve one of three operations:
    //     1. Borrowing a key from the left sibling.
    //     2. Borrowing a key from the right sibling.
    //     3. Merging the left or right sibling with the current child.
    // Rebalancing might drop the current child and move all its keys to its
    // left sibling if this child is the rightmost child within the parent. This
    // is why child is an output parameter. If we end up merging nodes, we might
    // remove a key from the parent which is why the child must be rebalanced
    // before the recursive calls below.
    RebalanceChildWithLeftOrRightSibling(transaction, parent_internal_node,
                                         child_index, std::move(const_child),
                                         &child);

    if (parent_internal_node->num_keys() == 0) {
      // The root node is empty, so make the left child the new root node
      // This is okay to drop since this is a new clone.
      transaction->Drop(std::move(parent_internal_node));
      *parent_node = child;
    }
  } else {
    child = transaction->GetMutable<BaseNode>(std::move(const_child));
    parent_internal_node->children[child_index] = child.Index();
  }

  // The current child where we want to delete a node is a leaf node.
  if (child->is_leaf()) {
    return DeleteFromLeafNode(std::move(child).To<LeafNode>(), key);
  }

  return DeleteFromInternalNode(transaction,
                                std::move(child).To<InternalNode>(), key);
}

template <typename KeyType, typename ValueType, size_t Order>
BlobStoreObject<const KeyType>
BPlusTree<KeyType, ValueType, Order>::GetSuccessorKey(
    BlobStoreObject<const BaseNode> node,
    const KeyType& key) {
  if (node->is_leaf()) {
    BlobStoreObject<const KeyType> key_found;
    node->Search(&blob_store_, key, &key_found);
    return key_found;
  }
  BlobStoreObject<const InternalNode> internal_node =
      std::move(node).To<InternalNode>();
  for (int i = 0; i <= internal_node->num_keys(); ++i) {
    BlobStoreObject<const BaseNode> child;
    GetChild(internal_node, i, &child);  // Get the leftmost child
    auto key_ptr = GetSuccessorKey(std::move(child), std::move(key));
    if (key_ptr != nullptr)
      return key_ptr;
  }
  // We should never get here unless the tree has a problem.
  return BlobStoreObject<const KeyType>();
}

template <typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::MergeInternalNodes(
    BlobStoreObject<InternalNode> left_node,
    BlobStoreObject<const InternalNode> right_node,
    size_t parent_key) {
  // Move the key from the parent node down to the left sibling node
  left_node->set_key(left_node->num_keys(), parent_key);
  left_node->children[left_node->num_keys() + 1] = right_node->children[0];
  left_node->increment_num_keys();

  // Move all keys and child pointers from the right sibling node to the left
  // sibling node
  for (size_t i = 0; i < right_node->num_keys(); ++i) {
    left_node->set_key(left_node->num_keys(), right_node->get_key(i));
    left_node->children[left_node->num_keys() + 1] =
        right_node->children[i + 1];
    left_node->increment_num_keys();
  }
}

template <typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::MergeLeafNodes(
    BlobStoreObject<LeafNode> left_node,
    BlobStoreObject<const LeafNode> right_node) {
  // Move all keys and values from the right sibling node to the left sibling
  // node
  for (size_t i = 0; i < right_node->num_keys(); ++i) {
    left_node->set_key(left_node->num_keys(), right_node->get_key(i));
    left_node->values[left_node->num_keys()] = right_node->values[i];
    left_node->increment_num_keys();
  }
}

template <typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::MergeChildWithLeftOrRightSibling(
    Transaction* transaction,
    BlobStoreObject<InternalNode> parent,
    size_t child_index,
    BlobStoreObject<const BaseNode> child,
    BlobStoreObject<BaseNode>* out_child) {
  BlobStoreObject<BaseNode> left_child;
  BlobStoreObject<const BaseNode> right_child;
  size_t key_index_in_parent;
  if (child_index < parent->num_keys()) {
    key_index_in_parent = child_index;
    left_child = transaction->GetMutable<BaseNode>(std::move(child));
    parent->children[child_index] = left_child.Index();
    GetChildConst(parent, child_index + 1, &right_child);
    *out_child = left_child;
  } else {
    key_index_in_parent = child_index - 1;
    BlobStoreObject<const BaseNode> const_left_child;
    GetChildConst(parent, child_index - 1, &const_left_child);
    left_child = transaction->GetMutable<BaseNode>(std::move(const_left_child));
    parent->children[child_index - 1] = left_child.Index();
    right_child = child;
    *out_child = left_child;
  }

  if (left_child->is_leaf()) {
    MergeLeafNodes(left_child.To<LeafNode>(), right_child.To<const LeafNode>());
  } else {
    MergeInternalNodes(left_child.To<InternalNode>(),
                       right_child.To<const InternalNode>(),
                       parent->get_key(key_index_in_parent));
  }
  transaction->Drop(std::move(right_child));

  // Update the parent node by removing the key that was moved down and the
  // pointer to the right sibling node
  for (size_t i = key_index_in_parent; i < parent->num_keys() - 1; ++i) {
    parent->set_key(i, parent->get_key(i + 1));
    parent->children[i + 1] = parent->children[i + 2];
  }
  // The parent could underflow if we don't do something before getting this
  // far.
  parent->decrement_num_keys();
}

template <typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::RebalanceChildWithLeftOrRightSibling(
    Transaction* transaction,
    BlobStoreObject<InternalNode> parent,
    size_t child_index,
    BlobStoreObject<const BaseNode> child,
    BlobStoreObject<BaseNode>* new_child) {
  BlobStoreObject<const BaseNode> left_sibling;
  if (child_index > 0) {
    GetChildConst(parent, child_index - 1, &left_sibling);
  }
  if (left_sibling != nullptr && !left_sibling->will_underflow()) {
    BorrowFromLeftSibling(transaction, parent, std::move(left_sibling),
                          std::move(child), child_index, new_child);
    return;
  }

  BlobStoreObject<const BaseNode> right_sibling;
  if ((child_index + 1) <= parent->num_keys()) {
    GetChildConst(parent, child_index + 1, &right_sibling);
  }
  if (right_sibling != nullptr && !right_sibling->will_underflow()) {
    BorrowFromRightSibling(transaction, parent, std::move(child),
                           std::move(right_sibling), child_index, new_child);
    return;
  }

  MergeChildWithLeftOrRightSibling(transaction, parent, child_index,
                                   std::move(child), new_child);
}

}  //  namespace b_plus_tree

#endif  // B_PLUS_TREE_H_
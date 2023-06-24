#ifndef TREE_ITERATOR_H_
#define TREE_ITERATOR_H_

#include "blob_store.h"
#include "tree_nodes.h"

// Iterator class for BPlusTree
template <typename KeyType, typename ValueType, std::size_t Order>
class TreeIterator {
 public:
  using BaseNode = BaseNode<Order>;
  using InternalNode = InternalNode<Order>;
  using LeafNode = LeafNode<Order>;

  TreeIterator(BlobStore* store,
               std::vector<size_t> path_to_root,
               size_t key_index)
      : store_(store),
        path_to_root_(std::move(path_to_root)),
        key_index_(key_index) {
    leaf_node_ = store_->Get<LeafNode>(path_to_root_.back());
    path_to_root_.pop_back();
    if (key_index_ >= leaf_node_->num_keys()) {
      AdvanceToNextNode();
    }
  }

  TreeIterator& operator++() {
    ++key_index_;
    if (key_index_ > leaf_node_->num_keys() - 1) {
      AdvanceToNextNode();
    }
    return *this;
  }

  TreeIterator operator++(int) {
    TreeIterator temp = *this;
    ++(*this);
    return temp;
  }

  bool operator==(const TreeIterator& other) const {
    return store_ == other.store_ && leaf_node_ == other.leaf_node_ &&
           key_index_ == other.key_index_;
  }

  bool operator!=(const TreeIterator& other) const { return !(*this == other); }

  BlobStoreObject<const KeyType> GetKey() const {
    if (leaf_node_ != nullptr) {
      return BlobStoreObject<const KeyType>(store_,
                                            leaf_node_->get_key(key_index_));
    }
    return BlobStoreObject<const KeyType>();
  }

  BlobStoreObject<const ValueType> GetValue() const {
    if (leaf_node_ != nullptr) {
      return BlobStoreObject<const ValueType>(store_,
                                              leaf_node_->values[key_index_]);
    }
    return BlobStoreObject<const ValueType>();
  }

 private:
  // Advances leaf_node_ to the next leaf node in the tree. If there are no more
  // leaf nodes, leaf_node_ is set to nullptr. This function also updates
  // path_to_root_ to reflect the new path to the root of the tree.
  void AdvanceToNextNode() {
    if (path_to_root_.empty()) {
      leaf_node_ = nullptr;
      return;
    }
    BlobStoreObject<const BaseNode> current_node = leaf_node_.To<BaseNode>();
    BlobStoreObject<const InternalNode> parent_node =
        store_->Get<InternalNode>(path_to_root_.back());
    while (parent_node != nullptr &&
           current_node.Index() ==
               parent_node->children[parent_node->num_keys()]) {
      // Move up to the parent node until we find a node that is not the
      // rightmost child
      current_node = parent_node.To<BaseNode>();
      path_to_root_.pop_back();
      if (path_to_root_.empty()) {
        leaf_node_ = nullptr;
        return;
      }
      parent_node = store_->Get<InternalNode>(path_to_root_.back());
    }

    // Find the index of the child node that corresponds to the current node
    int child_index = 0;
    while (child_index < parent_node->num_keys() &&
           parent_node->children[child_index] != current_node.Index()) {
      ++child_index;
    }
    // Follow the rightmost child of the parent node to find the next element
    BlobStoreObject<const BaseNode> next_node =
        store_->Get<BaseNode>(parent_node->children[child_index + 1]);
    while (!next_node->is_leaf()) {
      path_to_root_.push_back(next_node.Index());
      next_node =
          store_->Get<BaseNode>(next_node.To<InternalNode>()->children[0]);
    }
    leaf_node_ = next_node.To<LeafNode>();
    key_index_ = 0;
  }

  BlobStore* store_;
  std::vector<size_t> path_to_root_;
  BlobStoreObject<const LeafNode> leaf_node_;
  size_t key_index_;
};

#endif  // TREE_ITERATOR_H_
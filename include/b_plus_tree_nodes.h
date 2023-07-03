#ifndef B_PLUS_TREE_NODES_H_
#define B_PLUS_TREE_NODES_H_

#include <array>
#include <cstddef>
#include <cstdint>

#include "blob_store.h"
#include "fixed_string.h"
#include "storage_traits.h"

namespace b_plus_tree {

using blob_store::BlobStore;
using blob_store::BlobStoreObject;

enum class NodeType : uint8_t { INTERNAL, LEAF };

template <std::size_t Order = 4>
struct BaseNode {
  // The type of node this is: internal or leaf.
  NodeType type;

  // The number of keys in the node.
  std::size_t n;
  // The keys in the node.
  std::array<std::size_t, Order - 1> keys;

  BaseNode(NodeType type, std::size_t n) : type(type), n(n) {
    // Initialize all keys to invalid index.
    for (size_t i = 0; i < Order - 1; ++i) {
      keys[i] = BlobStore::InvalidIndex;
    }
  }

  bool is_leaf() const { return type == NodeType::LEAF; }
  bool is_internal() const { return type == NodeType::INTERNAL; }

  // Returns whether the node has the maximum number of keys it can hold.
  bool is_full() const { return n == Order - 1; }

  // Returns whether the node has the minimum number of keys it can hold.
  bool will_underflow() const { return n == (Order - 1) / 2; }

  // Returns the number of keys in the node.
  size_t num_keys() const { return n; }

  // Increments the number of keys in the node.
  void increment_num_keys() {
    ++n;
    assert(n < Order);
  }

  // Decrements the number of keys in the node.
  void decrement_num_keys() { --n; }

  // Sets the number of keys in the node.
  void set_num_keys(size_t num_keys) { n = num_keys; }

  // Returns the key at the given index.
  size_t get_key(size_t index) const { return keys[index]; }

  // Sets the key at the given index.
  void set_key(size_t index, size_t key) { keys[index] = key; }

  // Returns the first key in the node that is greater than or equal to the
  // given key and its index in the node. This is potentially more expensive
  // than necessary with strings as a temporary string is created.
  template <typename KeyType,
            typename U = typename StorageTraits<KeyType>::SearchType>
  typename std::enable_if<
      std::is_same<U, KeyType>::value ||
          std::is_convertible<U, typename StorageTraits<KeyType>::SearchType>::
              value ||
          std::is_same<U, typename StorageTraits<KeyType>::StorageType>::value,
      size_t>::type
  Search(BlobStore* store,
         const U& search_key,
         BlobStoreObject<const KeyType>* key_in_node) const {
    assert(num_keys() < Order);
    auto it = std::lower_bound(keys.begin(), keys.begin() + num_keys(),
                               search_key, [store](size_t lhs, const U& rhs) {
                                 return *store->Get<KeyType>(lhs) < rhs;
                               });

    size_t index = std::distance(keys.begin(), it);
    if (index < num_keys()) {
      *key_in_node = store->Get<KeyType>(*it);
    } else {
      *key_in_node = BlobStoreObject<const KeyType>();
    }
    return index;
  }
};

static_assert(std::is_trivially_copyable<BaseNode<>>::value,
              "BaseNode is trivially copyable");
static_assert(std::is_standard_layout<BaseNode<>>::value,
              "BaseNode is standard layout");

template <std::size_t Order = 4>
struct InternalNode {
  BaseNode<Order> base;
  std::array<std::size_t, Order> children;
  explicit InternalNode(std::size_t n) : base(NodeType::INTERNAL, n) {
    for (size_t i = 0; i < Order; ++i) {
      children[i] = BlobStore::InvalidIndex;
    }
  }

  bool is_leaf() const { return base.is_leaf(); }
  bool is_internal() const { return base.is_internal(); }
  bool is_full() const { return base.is_full(); }
  bool will_underflow() const { return base.will_underflow(); }
  size_t num_keys() const { return base.num_keys(); }
  void increment_num_keys() { base.increment_num_keys(); }
  void decrement_num_keys() { base.decrement_num_keys(); }
  void set_num_keys(size_t num_keys) { base.set_num_keys(num_keys); }
  size_t get_key(size_t index) const { return base.get_key(index); }
  void set_key(size_t index, size_t key) { base.set_key(index, key); }

  template <typename KeyType,
            typename U = typename StorageTraits<KeyType>::SearchType>
  typename std::enable_if<
      std::is_same<U, KeyType>::value ||
          std::is_convertible<U, typename StorageTraits<KeyType>::SearchType>::
              value ||
          std::is_same<U, typename StorageTraits<KeyType>::StorageType>::value,
      size_t>::type
  Search(BlobStore* store,
         const U& search_key,
         BlobStoreObject<const KeyType>* key_in_node) const {
    return base.Search<KeyType, U>(store, search_key, key_in_node);
  }
};

static_assert(std::is_trivially_copyable<InternalNode<>>::value,
              "InternalNode is trivially copyable");
static_assert(std::is_standard_layout<InternalNode<>>::value,
              "InternalNode is standard layout");

template <std::size_t Order = 4>
struct LeafNode {
  BaseNode<Order> base;
  std::array<std::size_t, Order - 1> values;

  LeafNode(std::size_t num_keys = 0) : base(NodeType::LEAF, num_keys) {
    for (size_t i = 0; i < Order - 1; ++i) {
      values[i] = BlobStore::InvalidIndex;
    }
  }

  bool is_leaf() const { return base.is_leaf(); }
  bool is_internal() const { return base.is_internal(); }
  bool is_full() const { return base.is_full(); }
  bool will_underflow() const { return base.will_underflow(); }
  size_t num_keys() const { return base.num_keys(); }
  void increment_num_keys() { base.increment_num_keys(); }
  void decrement_num_keys() { base.decrement_num_keys(); }
  void set_num_keys(size_t num_keys) { base.set_num_keys(num_keys); }
  size_t get_key(size_t index) const { return base.get_key(index); }
  void set_key(size_t index, size_t key) { base.set_key(index, key); }

  template <typename KeyType,
            typename U = typename StorageTraits<KeyType>::SearchType>
  typename std::enable_if<
      std::is_same<U, KeyType>::value ||
          std::is_convertible<U, typename StorageTraits<KeyType>::SearchType>::
              value ||
          std::is_same<U, typename StorageTraits<KeyType>::StorageType>::value,
      size_t>::type
  Search(BlobStore* store,
         const U& search_key,
         BlobStoreObject<const KeyType>* key_in_node) const {
    return base.Search<KeyType, U>(store, search_key, key_in_node);
  }
};

static_assert(std::is_trivially_copyable<LeafNode<>>::value,
              "LeafNode is trivially copyable");
static_assert(std::is_standard_layout<LeafNode<>>::value,
              "LeafNode is standard layout");

// Grab the key at the provided key_index. This method accepts all node types
// and works for both const and non-const nodes.
template <typename KeyType, typename U>
static void GetKey(const BlobStoreObject<U>& node,
                   size_t key_index,
                   BlobStoreObject<const KeyType>* key_ptr) {
  if (node == nullptr || key_index > node->num_keys() - 1) {
    *key_ptr = BlobStoreObject<const KeyType>();
    return;
  }
  *key_ptr = BlobStoreObject<const KeyType>(node.GetBlobStore(),
                                            node->get_key(key_index));
}

// Returns the value stored at position value_index in node.
// node can be a const or non-const LeafNode.
template <typename ValueType, typename U>
static void GetValue(const BlobStoreObject<U>& node,
                     size_t value_index,
                     BlobStoreObject<const ValueType>* value_ptr) {
  if (node == nullptr || value_index > node->num_keys() - 1) {
    *value_ptr = BlobStoreObject<const ValueType>();
    return;
  }
  *value_ptr = BlobStoreObject<const ValueType>(node.GetBlobStore(),
                                                node->values[value_index]);
}

// Returns the child at the given index of the given node preserving constness.
template <std::size_t Order>
void GetChild(const BlobStoreObject<InternalNode<Order>>& node,
              size_t child_index,
              BlobStoreObject<BaseNode<Order>>* child_ptr) {
  if (node == nullptr || child_index > node->num_keys()) {
    *child_ptr = BlobStoreObject<BaseNode<Order>>();
    return;
  }
  *child_ptr = BlobStoreObject<BaseNode<Order>>(node.GetBlobStore(),
                                                node->children[child_index]);
}

template <std::size_t Order>
void GetChild(const BlobStoreObject<const InternalNode<Order>>& node,
              size_t child_index,
              BlobStoreObject<const BaseNode<Order>>* child_ptr) {
  if (node == nullptr || child_index > node->num_keys()) {
    *child_ptr = BlobStoreObject<const BaseNode<Order>>();
    return;
  }
  *child_ptr = BlobStoreObject<const BaseNode<Order>>(
      node.GetBlobStore(), node->children[child_index]);
}

// Grab the child at the provided child_index. This method accepts only
// internal nodes and works for both const and non-const nodes
template <typename U, std::size_t Order>
void GetChildConst(const BlobStoreObject<U>& node,
                   size_t child_index,
                   BlobStoreObject<const BaseNode<Order>>* child_ptr) {
  if (node == nullptr || child_index > node->num_keys()) {
    *child_ptr = BlobStoreObject<const BaseNode<Order>>();
    return;
  }
  *child_ptr = BlobStoreObject<const BaseNode<Order>>(
      node.GetBlobStore(), node->children[child_index]);
}

// Prints a BlobStoreObject<BaseNode> in a human-readable format.
template <typename KeyType, std::size_t Order>
void PrintNode(BlobStoreObject<const InternalNode<Order>> node) {
  if (node == nullptr) {
    std::cout << "NULL Node" << std::endl;
    return;
  }
  std::cout << "Internal node (Index = " << node.Index()
            << ", n = " << node->num_keys() << ") ";
  for (size_t i = 0; i < node->num_keys(); ++i) {
    BlobStoreObject<const KeyType> key_ptr;
    GetKey(node, i, &key_ptr);
    std::cout << *key_ptr << " ";
  }
  std::cout << std::endl;
}

template <typename KeyType, std::size_t Order>
void PrintNode(BlobStoreObject<const LeafNode<Order>> node) {
  if (node == nullptr) {
    std::cout << "NULL Node" << std::endl;
    return;
  }
  std::cout << "Leaf node (Index = " << node.Index()
            << ", n = " << node->num_keys() << ") ";
  for (size_t i = 0; i < node->num_keys(); ++i) {
    BlobStoreObject<const KeyType> key_ptr;
    GetKey(node, i, &key_ptr);
    std::cout << *key_ptr << " ";
  }
  std::cout << std::endl;
}

template <std::size_t Order>
void PrintNode(BlobStoreObject<const BaseNode<Order>> node) {
  if (node == nullptr) {
    std::cout << "NULL Node" << std::endl;
    return;
  }
  if (node->is_internal()) {
    PrintNode(node.To<InternalNode>());
  }
  PrintNode(node.To<LeafNode>());
}

}  // namespace b_plus_tree

#endif  // B_PLUS_TREE_NODES_H_
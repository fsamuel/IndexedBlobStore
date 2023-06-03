#ifndef TRANSACTION_H_
#define TRANSACTION_H_

#include <cstddef>
#include <functional>
#include <unordered_set>

#include "b_plus_tree_base.h"
#include "nodes.h"
#include "tree_iterator.h"

// Enumerating the different types of objects stored in a transaction.
enum class ObjectType { LeafNode, InternalNode, HeadNode, KeyValue, DontCare };

struct ObjectInfo {
  size_t index;
  ObjectType type;
};

struct ObjectInfoEqual {
  bool operator()(const ObjectInfo& lhs, const ObjectInfo& rhs) const {
    return lhs.index == rhs.index;
  }
};

struct ObjectInfoHash {
  std::size_t operator()(const ObjectInfo& object_info) const {
    return std::hash<size_t>()(object_info.index);
  }
};

template <typename KeyType, typename ValueType, std::size_t Order>
class Transaction;

template <typename KeyType, typename ValueType, std::size_t Order, typename T>
struct TransactionHelper {
  using Transaction = Transaction<KeyType, ValueType, Order>;
  using non_const_T = typename std::remove_const<T>::type;

  template <typename... Args>
  static BlobStoreObject<T> New(Transaction* txn, Args&&... args) {
    BlobStoreObject<T> object =
        txn->blob_store_->New<T>(std::forward<Args>(args)...);
    txn->new_objects_.insert({object.Index(), ObjectType::KeyValue});
    return object;
  }

  static BlobStoreObject<typename std::remove_const<T>::type> GetMutable(
      Transaction* txn,
      BlobStoreObject<typename std::add_const<T>::type> object) {
    if (txn->new_objects_.count(object.Index()) > 0) {
      return std::move(object).Upgrade();
    }
    auto new_object = object.Clone();
    txn->discarded_objects_.insert(object.Index());
    txn->new_objects_.insert({new_object.Index(), ObjectType::KeyValue});
    return new_object;
  }
};

template <typename KeyType, typename ValueType, std::size_t Order>
struct TransactionHelper<KeyType, ValueType, Order, HeadNode> {
  using Transaction = Transaction<KeyType, ValueType, Order>;

  template <typename... Args>
  static BlobStoreObject<HeadNode> New(Transaction* txn, Args&&... args) {
    BlobStoreObject<HeadNode> node =
        txn->blob_store_->New<HeadNode>(std::forward<Args>(args)...);
    txn->new_objects_.insert({node.Index(), ObjectType::HeadNode});
    return node;
  }

  static BlobStoreObject<HeadNode> GetMutable(
      Transaction* txn,
      BlobStoreObject<const HeadNode> node) {
    if (node->get_version() == txn->GetVersion()) {
      return std::move(node).Upgrade();
    }
    auto new_node = node.Clone();
    new_node->set_version(txn->GetVersion());
    txn->new_objects_.insert({new_node.Index(), ObjectType::HeadNode});
    txn->discarded_objects_.insert(node.Index());
    return new_node;
  }
};

template <typename KeyType, typename ValueType, std::size_t Order>
struct TransactionHelper<KeyType, ValueType, Order, BaseNode<Order>> {
  using Transaction = Transaction<KeyType, ValueType, Order>;
  using BaseNode = BaseNode<Order>;
  using InternalNode = InternalNode<Order>;
  using LeafNode = LeafNode<Order>;

  template <typename... Args>
  static BlobStoreObject<BaseNode> New(Transaction* txn, Args&&... args) {
    // We should never be creating a new BaseNode. This is just a placeholder to
    // make the compiler happy.
    return BlobStoreObject<BaseNode>();
  }

  static BlobStoreObject<BaseNode> GetMutable(
      Transaction* txn,
      BlobStoreObject<const BaseNode> node) {
    if (node->is_leaf()) {
      return TransactionHelper<KeyType, ValueType, Order, LeafNode>::GetMutable(
                 txn, std::move(node).To<LeafNode>())
          .To<BaseNode>();
    }
    return TransactionHelper<KeyType, ValueType, Order, InternalNode>::
        GetMutable(txn, std::move(node).To<InternalNode>())
            .To<BaseNode>();
  }
};

template <typename KeyType, typename ValueType, std::size_t Order>
struct TransactionHelper<KeyType, ValueType, Order, LeafNode<Order>> {
  using Transaction = Transaction<KeyType, ValueType, Order>;
  using LeafNode = LeafNode<Order>;

  template <typename... Args>
  static BlobStoreObject<LeafNode> New(Transaction* txn, Args&&... args) {
    BlobStoreObject<LeafNode> node =
        txn->blob_store_->New<LeafNode>(std::forward<Args>(args)...);
    node->set_version(txn->GetVersion());
    txn->new_objects_.insert({node.Index(), ObjectType::LeafNode});
    return node;
  }

  static BlobStoreObject<LeafNode> GetMutable(
      Transaction* txn,
      BlobStoreObject<const LeafNode> node) {
    if (node->get_version() == txn->GetVersion()) {
      return std::move(node).Upgrade();
    }
    auto new_node = node.Clone();
    new_node->set_version(txn->GetVersion());
    txn->new_objects_.insert({new_node.Index(), ObjectType::LeafNode});
    txn->discarded_objects_.insert(node.Index());
    return new_node;
  }
};

template <typename KeyType, typename ValueType, std::size_t Order>
struct TransactionHelper<KeyType, ValueType, Order, InternalNode<Order>> {
  using Transaction = Transaction<KeyType, ValueType, Order>;
  using InternalNode = InternalNode<Order>;

  template <typename... Args>
  static BlobStoreObject<InternalNode> New(Transaction* txn, Args&&... args) {
    BlobStoreObject<InternalNode> node =
        txn->blob_store_->New<InternalNode>(std::forward<Args>(args)...);
    txn->new_objects_.insert({node.Index(), ObjectType::InternalNode});
    node->set_version(txn->GetVersion());
    return node;
  }

  static BlobStoreObject<InternalNode> GetMutable(
      Transaction* txn,
      BlobStoreObject<const InternalNode> node) {
    if (node->get_version() == txn->GetVersion()) {
      return std::move(node).Upgrade();
    }
    auto new_node = node.Clone();
    new_node->set_version(txn->GetVersion());
    txn->new_objects_.insert({new_node.Index(), ObjectType::InternalNode});
    txn->discarded_objects_.insert(node.Index());
    return new_node;
  }
};

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
    new_head_->set_version(new_head_->get_version() + 1);
    new_head_->previous = new_head_.Index();
    new_objects_.insert({new_head_.Index(), ObjectType::HeadNode});
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

  void Abort() && {
    for (const ObjectInfo& object_info : new_objects_) {
      blob_store_->Drop(object_info.index);
    }
  }

  bool Commit() && {
    if (!old_head_.CompareAndSwap(new_head_)) {
      std::move(*this).Abort();
      return false;
    }
    return true;
  }

  size_t GetVersion() const { return new_head_->get_version(); }

  BlobStoreObject<const BaseNode> GetNewRoot() const {
    return blob_store_->Get<BaseNode>(new_head_->root_index);
  }

  // Sets the new_head's root to the provided index.
  void SetNewRoot(size_t index) { new_head_->root_index = index; }

  template <typename T, typename... Args>
  BlobStoreObject<T> New(Args&&... args) {
    return TransactionHelper<KeyType, ValueType, Order, T>::New(
        this, std::forward<Args>(args)...);
  }

  template <typename T>
  BlobStoreObject<typename std::remove_const<T>::type> GetMutable(
      BlobStoreObject<typename std::add_const<T>::type> node) {
    return TransactionHelper<
        KeyType, ValueType, Order,
        typename std::remove_const<T>::type>::GetMutable(this, std::move(node));
  }

  template <typename T>
  BlobStoreObject<typename std::remove_const<T>::type> GetMutable(
      BlobStoreObject<typename std::remove_const<T>::type> node) {
    return node;
  }

  template <typename T>
  void Drop(BlobStoreObject<T> obj) {
    // The object type doesn't matter when looking up the object in the new
    // objects set.
    if (new_objects_.count({obj.Index(), ObjectType::DontCare}) > 0) {
      new_objects_.erase({obj.Index(), ObjectType::DontCare});
    }
    discarded_objects_.insert(obj.Index());
  }

 private:
  BPlusTreeBase* tree_;
  BlobStore* blob_store_;
  // Holding onto the old head ensures we retain a snapshot of the tree.
  BlobStoreObject<const HeadNode> old_head_;
  BlobStoreObject<HeadNode> new_head_;
  std::unordered_set<ObjectInfo, ObjectInfoHash, ObjectInfoEqual> new_objects_;
  std::unordered_set<size_t> discarded_objects_;
  template <typename KeyType, typename ValueType, std::size_t Order, typename T>
  friend struct TransactionHelper;
};

#endif  // TRANSACTION_H_
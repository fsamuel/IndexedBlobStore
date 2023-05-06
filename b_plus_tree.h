#ifndef B_PLUS_TREE_H_
#define B_PLUS_TREE_H_

#include <assert.h>
#include <algorithm>
#include <array>
#include "blob_store.h"
#include <queue>

class BaseNode;

template <typename KeyType, typename ValueType>
using KeyValuePair = std::pair<BlobStoreObject<const KeyType>, BlobStoreObject<const ValueType>>;


// InsertionBundle represents output of an insertion operation.
// It is either empty or contains a BlobStoreObject of the newly cloned node,
// the key to be inserted into the parent node, and the BlobStoreObect of the
// new right sibling node.
template<typename KeyType, typename NodeType>
struct InsertionBundle {
	InsertionBundle(BlobStoreObject<NodeType> new_left_node, BlobStoreObject<const KeyType> new_key, BlobStoreObject<NodeType> new_right_node)
		: new_left_node(new_left_node), new_key(new_key), new_right_node(new_right_node) {}
	BlobStoreObject<const KeyType> new_key;
	BlobStoreObject<NodeType> new_left_node;
	BlobStoreObject<NodeType> new_right_node;

	bool empty() const { return !new_left_node && !new_right_node; }
};

template <typename KeyType, typename ValueType, std::size_t Order>
class BPlusTree {
private:
	struct BPlusTreeHeader {
		size_t version;
		BlobStore::index_type root_index;
		BlobStore::index_type previous_header;
	};
	enum class NodeType : uint8_t { INTERNAL, LEAF };

	struct BaseNode {
		// The type of node this is: internal or leaf.
		NodeType type;
		// The number of keys in the node.
		std::size_t n;
		// The version of this node.
		std::size_t version;
		// The keys in the node.
		std::array<BlobStore::index_type, Order - 1> keys;

		BaseNode(NodeType type, std::size_t n) : type(type), n(n), version(0) {
			// Initialize all keys to invalid index.
			for (size_t i = 0; i < Order - 1; ++i) {
				keys[i] = BlobStore::InvalidIndex;
			}
		}

		BaseNode(const BaseNode& other)
			:type(other.type), n(other.n), version(other.version), keys(other.keys) {}

		// Returns whether the node has the maximum number of keys it can hold.
		bool is_full() const { return n == Order - 1; }

		// Returns whether the node has the minimum number of keys it can hold.
		bool will_underflow() const { return n == (Order - 1) / 2; }
		
		// Returns the number of keys in the node.
		size_t num_keys() const { return n; }

		// Increments the number of keys in the node.
		void increment_num_keys() { ++n; }

		// Decrements the number of keys in the node.
		void decrement_num_keys() { --n; }

		// Returns the version of the node.
		size_t get_version() const {
			return version;
		}

		// Sets the version of the node.
		void set_version(size_t new_version) {
			version = new_version;
		}

		// Sets the number of keys in the node.
		void set_num_keys(size_t num_keys) { n = num_keys; }

		// Returns the key at the given index.
		size_t get_key(size_t index) const { return keys[index]; }

		// Sets the key at the given index.
		void set_key(size_t index, size_t key) { keys[index] = key; }
	};

	//static_assert(std::is_trivially_copyable<BaseNode>::value, "BaseNode is trivially copyable");
	static_assert(std::is_standard_layout<BaseNode>::value, "BaseNode is standard layout");


	struct InternalNode {
		BaseNode base;
		std::array<BlobStore::index_type, Order> children;
		explicit InternalNode(std::size_t n)
			: base(NodeType::INTERNAL, n) {
			for (size_t i = 0; i < Order; ++i) {
				children[i] = BlobStore::InvalidIndex;
			}
		}

		InternalNode(const InternalNode& other) : base(other.base), children(other.children) {}

		bool is_full() const { return base.is_full(); }
		bool will_underflow() const { return base.will_underflow(); }
		size_t num_keys() const { return base.num_keys(); }
		void increment_num_keys() { base.increment_num_keys(); }
		void decrement_num_keys() { base.decrement_num_keys(); }
		size_t get_version() const { return base.get_version(); }
		void set_version(size_t new_version) { base.set_version(new_version); }
		void set_num_keys(size_t num_keys) { base.set_num_keys(num_keys); }
		size_t get_key(size_t index) const { return base.get_key(index); }
		void set_key(size_t index, size_t key) { base.set_key(index, key); }
	};

	//static_assert(std::is_trivially_copyable<InternalNode>::value, "InternalNode is trivially copyable");
	static_assert(std::is_standard_layout<InternalNode>::value, "InternalNode is standard layout");

	struct LeafNode {
		BaseNode base;
		std::array<BlobStore::index_type, Order> values;
		BlobStore::index_type next;

		LeafNode(BlobStore::index_type next, std::size_t n = 0)
			: base(NodeType::LEAF, n), next(next) {}

		LeafNode(const LeafNode& other) : base(other.base), values(other.values), next(other.next) {}

		bool is_full() const { return base.is_full(); }
		bool will_underflow() const { return base.will_underflow(); }
		size_t num_keys() const { return base.num_keys(); }
		void increment_num_keys() { base.increment_num_keys(); }
		void decrement_num_keys() { base.decrement_num_keys(); }
		size_t get_version() const { return base.get_version(); }
		void set_version(size_t new_version) { base.set_version(new_version); }
		void set_num_keys(size_t num_keys) { base.set_num_keys(num_keys); }
		size_t get_key(size_t index) const { return base.get_key(index); }
		void set_key(size_t index, size_t key) { base.set_key(index, key); }
	};

	//static_assert(std::is_trivially_copyable<LeafNode>::value, "LeafNode is trivially copyable");
	static_assert(std::is_standard_layout<LeafNode>::value, "LeafNode is standard layout");

	using InsertionBundle = InsertionBundle<KeyType, BaseNode>;

public:
	using offset_type = std::ptrdiff_t;

	// Iterator class for BPlusTree
	class Iterator {
	public:
		using key_type = KeyType;
		using value_type = ValueType;
		using difference_type = std::ptrdiff_t;
		using pointer = value_type*;
		using reference = value_type&;

		Iterator(BlobStore* store, BlobStoreObject<const LeafNode>&& leaf_node, size_t key_index) : store_(store), leaf_node_(std::move(leaf_node)), key_index_(key_index) {}

		Iterator& operator++() {
			++key_index_;
			if (key_index_ > leaf_node_->num_keys() - 1) {
				key_index_ = 0;
				leaf_node_ = BlobStoreObject<const LeafNode>(store_, leaf_node_->next);
			}
			return *this;
		}

		Iterator operator++(int) {
			Iterator temp = *this;
			++(*this);
			return temp;
		}

		bool operator==(const Iterator& other) const {
			return store_ == other.store_ && leaf_node_ == other.leaf_node_ && key_index_ == other.key_index_;
		}

		bool operator!=(const Iterator& other) const {
			return !(*this == other);
		}

		BlobStoreObject<const KeyType> GetKey() const {
			if (leaf_node_ != nullptr) {
				return BlobStoreObject<const KeyType>(store_, leaf_node_->get_key(key_index_));
			}
			return BlobStoreObject<const KeyType>();
		}

		BlobStoreObject<ValueType> GetValue() const {
			if (leaf_node_ != nullptr) {
				return BlobStoreObject<ValueType>(store_, leaf_node_->values[key_index_]);
			}
			return BlobStoreObject<ValueType>();
		}

	private:
		BlobStore* store_;
		BlobStoreObject<const LeafNode> leaf_node_;
		size_t key_index_;
	};

	BPlusTree(BlobStore& blob_store) : blob_store_(blob_store) {
		if (blob_store_.IsEmpty()) {
			CreateRoot();
		}
	}

	Iterator Search(const KeyType& key);

	bool Insert(const KeyType& key, const ValueType& value);
	bool Insert(BlobStoreObject<KeyType>&& key, BlobStoreObject<ValueType>&& value);

	KeyValuePair<KeyType, ValueType> Delete(const KeyType& key);

	// Prints a BlobStoreObject<BaseNode> in a human-readable format.
	void PrintNode(BlobStoreObject<const InternalNode> node) {
		if (node == nullptr) {
			std::cout << "NULL Node" << std::endl;
			return;
		}
		std::cout << "Internal node (n = " << node->num_keys() << ", version = " << node->get_version() << ") ";
		for (size_t i = 0; i < node->num_keys(); ++i) {
			std::cout << *GetKey(node, i) << " ";
		}
		std::cout << std::endl;
	}

	void PrintNode(BlobStoreObject<const LeafNode> node) {
		if (node == nullptr) {
			std::cout << "NULL Node" << std::endl;
			return;
		}
		std::cout << "Leaf node (n = " << node->num_keys() << ", version = " << node->get_version() << ") ";
		for (size_t i = 0; i < node->num_keys(); ++i) {
			std::cout << *GetKey(node, i) << " ";
		}
		std::cout << std::endl;
	}

	void PrintNode(BlobStoreObject<const BaseNode> node) {
		if (node == nullptr) {
			std::cout << "NULL Node" << std::endl;
			return;
		}
		if (node->type == NodeType::INTERNAL) {
			return PrintNode(node.To<InternalNode>());
		}
		return PrintNode(node.To<LeafNode>());
	}

	// Prints the tree in a human-readable format in breadth-first order.
	void PrintTree(size_t version) {
		struct NodeWithLevel {
			BlobStoreObject<const BaseNode> node;
			size_t level;
		};
		std::queue<NodeWithLevel> queue;
		BlobStoreObject<const BPlusTreeHeader> header = blob_store_.Get<BPlusTreeHeader>(1);
		while (header->previous_header != BlobStore::InvalidIndex && header->version > version) {
			header = blob_store_.Get<BPlusTreeHeader>(header->previous_header);
		}
		queue.push({ blob_store_.Get<BaseNode>(header->root_index), 0 });
		while (!queue.empty()) {
			NodeWithLevel node_with_level = queue.front();
			queue.pop();
			if (node_with_level.node->type == NodeType::INTERNAL) {
				BlobStoreObject<const InternalNode> internal_node = node_with_level.node.To<InternalNode>();
				for (size_t i = 0; i <= internal_node->num_keys(); ++i) {
					queue.push({ GetChild(internal_node, i), node_with_level.level + 1 });
				}
				std::cout << std::string(node_with_level.level, ' ');
				PrintNode(internal_node);
			}
			else {
				BlobStoreObject<const LeafNode> leaf_node = node_with_level.node.To<LeafNode>();
				std::cout << std::string(node_with_level.level, ' ');
				PrintNode(leaf_node);
			}
		}
	}

	// Prints the tree in a human-readable format in depth-first order.
	void PrintTreeDepthFirst() {
		struct NodeWithLevel {
			BlobStoreObject<BaseNode> node;
			size_t level;
		};
		std::vector<NodeWithLevel> v;
		v.push_back({ root_, 0 });

		while (!v.empty()) {
			NodeWithLevel node_with_level = v.back();
			v.pop_back();
			if (node_with_level.node->type == NodeType::INTERNAL) {
				BlobStoreObject<InternalNode> internal_node = node_with_level.node.To<InternalNode>();
				std::cout << std::string(node_with_level.level, ' ') << "Internal node (n = " << internal_node->num_keys() << ") ";
				for (size_t i = 0; i < internal_node->num_keys(); ++i) {
					std::cout << *BlobStoreObject<KeyType>(&blob_store_, internal_node->keys[i]) << " ";
				}
				for (int i = internal_node->num_keys(); i >= 0; --i) {
					v.push_back({ BlobStoreObject<BaseNode>(&blob_store_, internal_node->children[i]), node_with_level.level + 1 });
				}
				std::cout << std::endl;
			}
			else {
				BlobStoreObject<LeafNode> leaf_node = node_with_level.node.To<LeafNode>();
				std::cout << std::string(node_with_level.level, ' ') << "Leaf node (n = " << leaf_node->num_keys() << ") ";
				for (size_t i = 0; i < leaf_node->num_keys(); ++i) {
					std::cout << *BlobStoreObject<KeyType>(&blob_store_, leaf_node->keys[i]) << " ";
				}
				std::cout << std::endl;
			}
		}

	}

private:
	BlobStore& blob_store_;

	void CreateRoot() {
		auto header = blob_store_.New<BPlusTreeHeader>();
		auto root = blob_store_.New<LeafNode>(BlobStore::InvalidIndex);
		header->version = 0;
		header->root_index = root.Index();
		header->previous_header = BlobStore::InvalidIndex;
	}

	template<typename U, typename std::enable_if<
		std::is_same<typename std::remove_const<U>::type, InternalNode>::value
	>::type* = nullptr>
	auto GetChild(const BlobStoreObject<U>& node, size_t child_index) -> typename std::conditional <
		std::is_const<U>::value,
		BlobStoreObject<const BaseNode>,
		BlobStoreObject<BaseNode>
	>::type {
		using const_preserving_BaseNode = typename std::conditional <
			std::is_const<U>::value,
			const BaseNode,
			BaseNode
		>::type;
		if (node == nullptr || child_index > node->num_keys()) {
			return BlobStoreObject<const_preserving_BaseNode>();
		}
		return BlobStoreObject<const_preserving_BaseNode>(&blob_store_, node->children[child_index]);
	}

	template<typename U, typename std::enable_if<
		std::is_same<typename std::remove_const<U>::type, InternalNode>::value
	>::type* = nullptr>
	BlobStoreObject<const BaseNode> GetChildConst(const BlobStoreObject<U>& node, size_t child_index) {
		if (node == nullptr || child_index > node->num_keys()) {
			return BlobStoreObject<const BaseNode>();
		}
		return BlobStoreObject<const BaseNode>(&blob_store_, node->children[child_index]);
	}

	// Grab the key at the provided key_index. This method accepts all node types and works for
	// both const and non-const nodes.
	template<typename U, typename std::enable_if<
		std::is_same<typename std::remove_const<U>::type, BaseNode>::value ||
		std::is_same<typename std::remove_const<U>::type, InternalNode>::value ||
		std::is_same<typename std::remove_const<U>::type, LeafNode>::value
	>::type* = nullptr>
	BlobStoreObject<const KeyType> GetKey(const BlobStoreObject<U>&node, size_t key_index) {
		if (node == nullptr || key_index > node->num_keys() - 1) {
			return BlobStoreObject<const KeyType>();
		}
		return BlobStoreObject<const KeyType>(&blob_store_, node->get_key(key_index));
	}

	// Returns the value stored at position value_index in node. 
	// node can be a const or non-const LeafNode.
	template<typename U, typename std::enable_if<
		std::is_same<typename std::remove_const<U>::type, LeafNode>::value
	>::type* = nullptr>
	BlobStoreObject<const ValueType> GetValue(const BlobStoreObject<U>& node, size_t value_index) {
		if (node == nullptr || value_index > node->num_keys() - 1) {
			return BlobStoreObject<const ValueType>();
		}
		return BlobStoreObject<const ValueType>(&blob_store_, node->values[value_index]);
	}

	Iterator Search(BlobStoreObject<const BaseNode>&& node, const KeyType& key);

	// Split a leaf node into two leaf nodes and a middle key, all returned in InsertionBundle.
	// left_node is modified directly.
	InsertionBundle SplitLeafNode(BlobStoreObject<LeafNode> left_node);

	// Split an internal node into two internal nodes nodes and a middle key, all returned in InsertionBundle.
	// left_node is modified directly.
	InsertionBundle SplitInternalNode(BlobStoreObject<InternalNode> left_node);

	// Inserts key and value into the leaf node |node|. This method accepts both const and non-const leaves.
	// If the leaf is const (we're holding a read lock), we clone the node and insert into the clone.
	// If the leaf is non-const, we insert directly into the node.
	template<typename U>
	typename std::enable_if<
		std::is_same<typename std::remove_const<U>::type, typename BPlusTree<KeyType, ValueType, Order>::LeafNode>::value,
		InsertionBundle>::type InsertIntoLeaf(size_t version, BlobStoreObject<U> node,
			BlobStoreObject<KeyType> key,
			BlobStoreObject<ValueType> value);

	// Insert new_key and new_child into node with the assumption that node is not full.
	void InsertKeyChildIntoInternalNode(BlobStoreObject<InternalNode> node,
		BlobStoreObject<const KeyType> new_key,
		BlobStoreObject<BaseNode> new_child);

	InsertionBundle Insert(size_t version, BlobStoreObject<const BaseNode> node, BlobStoreObject<KeyType> key, BlobStoreObject<ValueType> value);

	KeyValuePair<KeyType, ValueType> Delete(BlobStoreObject<BaseNode>* parent_node, int child_index, const KeyType& key);
	KeyValuePair<KeyType, ValueType> DeleteFromLeafNode(BlobStoreObject<LeafNode> node, const KeyType& key);
	KeyValuePair<KeyType, ValueType> DeleteFromInternalNode(BlobStoreObject<InternalNode> node, const KeyType& key);

	bool TryToBorrowFromLeftSibling(BlobStoreObject<InternalNode> parent_node, BlobStoreObject<const BaseNode> left_sibling, BlobStoreObject<const BaseNode> right_sibling, BlobStoreObject<BaseNode>* out_right_sibling, int child_index);
	bool TryToBorrowFromRightSibling(BlobStoreObject<InternalNode> parent_node, BlobStoreObject<BaseNode>* out_left_sibling, BlobStoreObject<const BaseNode> left_sibling, BlobStoreObject<const BaseNode> right_sibling, int child_index);

	BlobStoreObject<const KeyType> GetPredecessorKey(BlobStoreObject<BaseNode> node);
	BlobStoreObject<const KeyType> GetSuccessorKey(BlobStoreObject<const BaseNode> node, const KeyType& key);

	void MergeInternalNodes(BlobStoreObject<InternalNode> left_child,
	                        BlobStoreObject<const InternalNode> right_child,
		                    size_t parent_key);
	void MergeLeafNodes(BlobStoreObject<LeafNode> left_child, BlobStoreObject<const LeafNode> right_child);
	void MergeChildWithLeftOrRightSibling(
		BlobStoreObject<InternalNode> parent,
		int child_index,
		BlobStoreObject<const BaseNode> child,
		BlobStoreObject<BaseNode>* out_child);
	void RebalanceChildWithLeftOrRightSibling(
		BlobStoreObject<InternalNode> parent,
		int child_index,
		BlobStoreObject<const BaseNode> child,
		BlobStoreObject<BaseNode>* new_child);

};

template<typename KeyType, typename ValueType, size_t Order>
bool BPlusTree<KeyType, ValueType, Order>::Insert(const KeyType& key, const ValueType& value) {
	BlobStoreObject<KeyType> key_ptr = blob_store_.New<KeyType>(key);
	BlobStoreObject<ValueType> value_ptr = blob_store_.New<ValueType>(value);

	return Insert(std::move(key_ptr), std::move(value_ptr));
}

template<typename KeyType, typename ValueType, size_t Order>
bool BPlusTree<KeyType, ValueType, Order>::Insert(BlobStoreObject<KeyType>&& key, BlobStoreObject<ValueType>&& value) {
	BlobStoreObject<const BPlusTreeHeader> old_header = blob_store_.Get<BPlusTreeHeader>(1);
	BlobStoreObject<BPlusTreeHeader> new_header = old_header.Clone();
	++new_header->version;
	new_header->previous_header = new_header.Index();
	BlobStoreObject<const BaseNode> root = blob_store_.Get<BaseNode>(old_header->root_index);
	InsertionBundle bundle = Insert(new_header->version, root, key, value);
	if (bundle.new_right_node != nullptr) {
		BlobStoreObject<InternalNode> new_root = blob_store_.New<InternalNode>(1);
		new_root->set_version(new_header->version);
		new_root->children[0] = bundle.new_left_node.Index();
		new_root->children[1] = bundle.new_right_node.Index();
		new_root->set_num_keys(1);
		new_root->set_key(0, bundle.new_key.Index());
		new_header->root_index = new_root.Index();
	}
	else {
		new_header->root_index = bundle.new_left_node.Index();
	}
	return old_header.CompareAndSwap(new_header);
}

template <typename KeyType, typename ValueType, size_t Order>
typename BPlusTree<KeyType, ValueType, Order>::Iterator BPlusTree<KeyType, ValueType, Order>::Search(const KeyType& key) {
	BlobStore::index_type root_index = BlobStore::InvalidIndex;
	{
		auto header = blob_store_.Get<BPlusTreeHeader>(1);
		root_index = header->root_index;
	}
	if (root_index == BlobStore::InvalidIndex) {
		return Iterator(&blob_store_, BlobStoreObject<const LeafNode>(), 0);
	}
	return Search(blob_store_.Get<BaseNode>(root_index), key);
}

template <typename KeyType, typename ValueType, size_t Order>
typename BPlusTree<KeyType, ValueType, Order>::Iterator BPlusTree<KeyType, ValueType, Order>::Search(BlobStoreObject<const BaseNode>&& node, const KeyType& key) {
	int i = 0;
	BlobStoreObject<const KeyType> current_key;
	while (i < node->num_keys()) {
		current_key = GetKey(node, i);
		if (key <= *current_key) {
			break;
		}
		++i;
	}
	if (node->type == NodeType::LEAF) {
		if (i < node->num_keys() && key == *current_key) {
			return Iterator(&blob_store_, std::move(node.To<LeafNode>()), i);
		}
		return Iterator(&blob_store_, BlobStoreObject<const LeafNode>(), 0);
	}
	else {
		if (i < node->num_keys() && key == *current_key) {
			return Search(GetChild(std::move(node.To<InternalNode>()), i + 1), key);
		}
		return Search(GetChild(std::move(node.To<InternalNode>()), i), key);
	}
}

template<typename KeyType, typename ValueType, size_t Order>
typename BPlusTree<KeyType, ValueType, Order>::InsertionBundle BPlusTree<KeyType, ValueType, Order>::SplitLeafNode(BlobStoreObject<LeafNode> left_node) {
	BlobStoreObject<LeafNode> new_right_node = blob_store_.New<LeafNode>(Order);
	new_right_node->set_version(left_node->get_version());

	int middle_key_index = (left_node->num_keys() - 1) / 2;
	BlobStoreObject<const KeyType> middle_key = GetKey(left_node, middle_key_index);

	new_right_node->set_num_keys(left_node->num_keys() - middle_key_index);
	for (int i = 0; i < new_right_node->num_keys(); ++i) {
		new_right_node->set_key(i, left_node->get_key(middle_key_index + i));
		left_node->set_key(middle_key_index + i, BlobStore::InvalidIndex);
	}
	auto new_leaf_node = new_right_node.To<LeafNode>();
	auto child_leaf_node = left_node.To<LeafNode>();
	for (int i = 0; i < new_right_node->num_keys(); ++i) {
		new_leaf_node->values[i] = child_leaf_node->values[middle_key_index + i];
		child_leaf_node->values[middle_key_index + i] = BlobStore::InvalidIndex;
	}
	new_leaf_node->next = child_leaf_node->next;
	child_leaf_node->next = new_right_node.Index();
	child_leaf_node->values[middle_key_index] = BlobStore::InvalidIndex;

	left_node->set_num_keys(middle_key_index);
	left_node->set_key(middle_key_index, BlobStore::InvalidIndex);
	return InsertionBundle(left_node.To<BaseNode>(), middle_key, new_right_node.To<BaseNode>());
}

template<typename KeyType, typename ValueType, size_t Order>
typename BPlusTree<KeyType, ValueType, Order>::InsertionBundle BPlusTree<KeyType, ValueType, Order>::SplitInternalNode(BlobStoreObject<InternalNode> left_node) {
	BlobStoreObject<InternalNode> new_right_node = blob_store_.New<InternalNode>(Order);
	new_right_node->set_version(left_node->get_version());

	int middle_key_index = (left_node->num_keys() - 1) / 2;
	BlobStoreObject<const KeyType> middle_key = GetKey(left_node, middle_key_index);

	new_right_node->set_num_keys(left_node->num_keys() - middle_key_index - 1);
	for (int i = 0; i < new_right_node->num_keys(); ++i) {
		new_right_node->set_key(i, left_node->get_key(middle_key_index + i + 1));
		left_node->set_key(middle_key_index + i + 1, BlobStore::InvalidIndex);
	}

	auto new_internal_node = new_right_node.To<InternalNode>();
	auto child_internal_node = left_node.To<InternalNode>();
	for (int i = 0; i <= new_right_node->num_keys(); ++i) {
		new_internal_node->children[i] = child_internal_node->children[middle_key_index + i + 1];
		child_internal_node->children[middle_key_index + i + 1] = BlobStore::InvalidIndex;
	}

	left_node->set_num_keys(middle_key_index);
	left_node->set_key(middle_key_index, BlobStore::InvalidIndex);
	return InsertionBundle(left_node.To<BaseNode>(), middle_key, new_right_node.To<BaseNode>());
}

template<typename KeyType, typename ValueType, size_t Order>
template<typename U>
typename std::enable_if<
	std::is_same<typename std::remove_const<U>::type, typename BPlusTree<KeyType, ValueType, Order>::LeafNode>::value,
	typename BPlusTree<KeyType, ValueType, Order>::InsertionBundle>::type
	BPlusTree<KeyType, ValueType, Order>::InsertIntoLeaf(size_t version, BlobStoreObject<U> node, BlobStoreObject<KeyType> key, BlobStoreObject <ValueType> value) {
	BlobStoreObject<LeafNode> new_left_node = node.GetMutableOrClone();
	new_left_node->set_version(version);
	if (new_left_node->is_full()) {
		InsertionBundle bundle = SplitLeafNode(new_left_node);
		// We pass the recursive InsertIntoLeaf a non-const LeafNode so we won't clone it.
		if (*key >= *bundle.new_key) {
			InsertIntoLeaf(version, bundle.new_right_node.To<LeafNode>(), std::move(key), std::move(value));
		}
		else {
			InsertIntoLeaf(version, bundle.new_left_node.To<LeafNode>(), std::move(key), std::move(value));
		}
		return bundle;
	}
	// Shift the keys and values right.
	int i = new_left_node->num_keys() - 1;
	while (i >= 0) {
		BlobStoreObject<const KeyType> key_ptr = GetKey(new_left_node, i);
		if (*key >= *key_ptr) {
			break;
		}
		new_left_node->set_key(i + 1, new_left_node->get_key(i));
		new_left_node->values[i + 1] = new_left_node->values[i];
		--i;
	}
	new_left_node->set_key(i + 1, key.Index());
	new_left_node->values[i + 1] = value.Index();
	new_left_node->increment_num_keys();
	return InsertionBundle(new_left_node.To<BaseNode>(), BlobStoreObject<const KeyType>(), BlobStoreObject<BaseNode>());
}

template<typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::InsertKeyChildIntoInternalNode(
	BlobStoreObject<InternalNode> node,
	BlobStoreObject<const KeyType> new_key,
	BlobStoreObject<BaseNode> new_child) {
	int i = node->num_keys() - 1;
	while (i >= 0) {
		BlobStoreObject<const KeyType> key_ptr = GetKey(node, i);
		if (*new_key >= *key_ptr) {
			break;
		}
		node->set_key(i + 1, node->get_key(i));
		node->children[i + 2] = node->children[i + 1];
		--i;
	}
	node->set_key(i + 1, new_key.Index());
	node->children[i + 2] = new_child.Index();
	node->increment_num_keys();
}

template<typename KeyType, typename ValueType, size_t Order>
typename BPlusTree<KeyType, ValueType, Order>::InsertionBundle BPlusTree<KeyType, ValueType, Order>::Insert(size_t version, BlobStoreObject<const BaseNode> node, BlobStoreObject<KeyType> key, BlobStoreObject <ValueType> value) {
	if (node->type == NodeType::LEAF) {
		return InsertIntoLeaf(version, node.To<LeafNode>(), std::move(key), std::move(value));
	}
	// Find the child node where the new key-value should be inserted.
	BlobStoreObject<const InternalNode> internal_node = node.To<InternalNode>();
	int i = internal_node->num_keys() - 1;
	while (i >= 0) {
		auto current_key = GetKey(internal_node, i);
		if (*key >= *current_key) {
			break;
		}
		--i;
	}
	i += 1;
	BlobStoreObject<const BaseNode> child_node = GetChild(internal_node, i);
	InsertionBundle child_node_bundle = Insert(version, child_node, key, value);
	BlobStoreObject<InternalNode> new_internal_node = internal_node.Clone();
	new_internal_node->set_version(version);
	new_internal_node->children[i] = child_node_bundle.new_left_node.Index();
	// If the child node bundle has a new right node, then that means that a split occurred to insert
	// the key/value pair. We need to find a place to insert the new middle key.
	if (child_node_bundle.new_right_node != nullptr) {
		// The internal node is full so we need to recursively split to find a  
		if (new_internal_node->is_full()) {
			// insert the new child node and its minimum key into the parent node recursively
			InsertionBundle node_bundle = SplitInternalNode(new_internal_node);

			// The parent node is full so we need to split it and insert the new node into the parent node or
			// its new sibling.
			if (*child_node_bundle.new_key < *node_bundle.new_key) {
				InsertKeyChildIntoInternalNode(
					node_bundle.new_left_node.To<InternalNode>(),
					std::move(child_node_bundle.new_key),
					std::move(child_node_bundle.new_right_node));
			}
			else {
				InsertKeyChildIntoInternalNode(
					node_bundle.new_right_node.To<InternalNode>(),
					std::move(child_node_bundle.new_key),
					std::move(child_node_bundle.new_right_node));
			}
			// return the new node and its middle key to be inserted into the parent node recursively
			return node_bundle;
		}
		// insert the new child node and its minimum key into the parent node
		int j = new_internal_node->num_keys() - 1;
		while (j >= static_cast<int>(i)) {
			new_internal_node->children[j + 2] = new_internal_node->children[j + 1];
			new_internal_node->set_key(j + 1, new_internal_node->get_key(j));
			--j;
		}
		new_internal_node->children[i + 1] = child_node_bundle.new_right_node.Index();
		new_internal_node->set_key(i, child_node_bundle.new_key.Index());
		new_internal_node->increment_num_keys();
	}
	// No split occurred so nothing to return.
	return InsertionBundle(new_internal_node.To<BaseNode>(), BlobStoreObject<const KeyType>(), BlobStoreObject<BaseNode>());
}

template <typename KeyType, typename ValueType, size_t Order>
KeyValuePair<KeyType, ValueType> BPlusTree<KeyType, ValueType, Order>::Delete(const KeyType& key) {
	BlobStoreObject<const BPlusTreeHeader> old_header = blob_store_.Get<BPlusTreeHeader>(1);
	if (old_header->root_index == BlobStore::InvalidIndex) {
		return std::make_pair(BlobStoreObject<const KeyType>(), BlobStoreObject<const ValueType>());
	}
	BlobStoreObject<BPlusTreeHeader> new_header = old_header.Clone();
	++new_header->version;
	new_header->previous_header = new_header.Index();

	BlobStoreObject<const BaseNode> root = blob_store_.Get<BaseNode>(new_header->root_index);
	if (root->type == NodeType::LEAF) {
		BlobStoreObject<LeafNode> new_root = root.To<const LeafNode>().Clone();
		auto kv = DeleteFromLeafNode(new_root, key);
		new_header->root_index = new_root.Index();
		if (old_header.CompareAndSwap(new_header)) {
			return kv;
		}
		// TODO(fsamuel): We need to distingush between a CAS fail and a key not found.
		return std::make_pair(BlobStoreObject<const KeyType>(), BlobStoreObject<const ValueType>());
	}
	BlobStoreObject<InternalNode> new_root = root.To<const InternalNode>().Clone();
	int i = 0;
	BlobStoreObject<const KeyType> current_key;
	while (i < new_root->num_keys()) {
		current_key = GetKey(new_root, i);
		if (key <= *current_key) {
			break;
		}
		i += 1;
	}
	std::pair<BlobStoreObject<const KeyType>, BlobStoreObject<const ValueType>> kv;
	BlobStoreObject<BaseNode> new_root_base = new_root.To<BaseNode>();
	if (i < new_root->num_keys() && key == *current_key) {
		kv = Delete(&new_root_base, i + 1, key);
	}
	else {
		kv = Delete(&new_root_base, i, key);
	}
	new_header->root_index = new_root_base.Index();
	if (old_header.CompareAndSwap(new_header)) {
		return kv;
	}
	// TODO(fsamuel): We need to distingush between a CAS fail and a key not found.
	return std::make_pair(BlobStoreObject<const KeyType>(), BlobStoreObject<const ValueType>());
}

template <typename KeyType, typename ValueType, size_t Order>
KeyValuePair<KeyType, ValueType> BPlusTree<KeyType, ValueType, Order>::DeleteFromLeafNode(BlobStoreObject<LeafNode> node, const KeyType& key) {
	std::cout << "Deleting " << key << " from leaf" << std::endl;
	PrintNode(node.To<const LeafNode>());
	int i = 0;
	while (i < node->num_keys() && key != *GetKey(node, i)) {
		i += 1;
	}

	if (i == node->num_keys()) {
		return std::make_pair(BlobStoreObject<const KeyType>(), BlobStoreObject<const ValueType>()); // Key not found
	}

	auto kv = std::make_pair(GetKey(node, i), GetValue(node, i));

	// Shift keys and values to fill the gap
	for (int j = i + 1; j < node->num_keys(); j++) {
		node->set_key(j - 1, node->get_key(j));
		node->values[j - 1] = node->values[j];
	}
	node->decrement_num_keys();

	return kv; // Key successfully removed
}

template <typename KeyType, typename ValueType, size_t Order>
KeyValuePair<KeyType, ValueType> BPlusTree<KeyType, ValueType, Order>::DeleteFromInternalNode(BlobStoreObject<InternalNode> node, const KeyType& key) {
	int i = 0;
	BlobStoreObject<const KeyType> current_key;
	while (i < node->num_keys()) {
		current_key = GetKey(node, i);
		if (key <= *current_key) {
			break;
		}
		i += 1;
	}
	BlobStoreObject<BaseNode> internal_node_base = node.To<BaseNode>();

	// We found the first key larger or equal to the node we're looking for.
	// Case 1: key == *current_key: we need to delete the key.
	if (i < node->num_keys() && key == *current_key) {
		// The key/value pair is in the right child. Recurse down
		// to delete the key/value pair first.
		auto kv = Delete(&internal_node_base, i + 1, key);
		// We need to update current key to a new successor since we just deleted the
		// successor to this node. We shouldn't refer to nodes that don't exist.
		BlobStoreObject<const KeyType> current_key;
		while (i < node->num_keys()) {
			current_key = GetKey(node, i);
			if (key <= *current_key) {
				break;
			}
			i += 1;
		}
		if (i < node->num_keys() && key == *current_key) {
			// Can there ever be a null successor? That means there is no successor at all.
			// That shouldn't happen I think.
			auto key_ptr = GetSuccessorKey(node.To<const BaseNode>(), key);
			node->set_key(i, key_ptr.Index());
		}
		return kv;
	}
	// Delete at the left child if the current key is larger.
	return Delete(&internal_node_base, i, key);
}

template <typename KeyType, typename ValueType, size_t Order>
bool BPlusTree<KeyType, ValueType, Order>::TryToBorrowFromLeftSibling(BlobStoreObject<InternalNode> parent_node, BlobStoreObject<const BaseNode> left_sibling, BlobStoreObject<const BaseNode> right_sibling, BlobStoreObject<BaseNode>* out_right_sibling, int child_index) {
	if (!left_sibling || left_sibling->will_underflow()) {
		return false;
	}

	BlobStoreObject<BaseNode> new_left_sibling;
	BlobStoreObject<BaseNode> new_right_sibling;
	if (left_sibling->type == NodeType::LEAF) {
		new_left_sibling = left_sibling.To<LeafNode>().Clone().To<BaseNode>();
		new_right_sibling = right_sibling.To<LeafNode>().Clone().To<BaseNode>();
	}
	else {
		new_left_sibling = left_sibling.To<InternalNode>().Clone().To<BaseNode>();
		new_right_sibling = right_sibling.To<InternalNode>().Clone().To<BaseNode>();
	}

	parent_node->children[child_index - 1] = new_left_sibling.Index();
	parent_node->children[child_index] = new_right_sibling.Index();

	// Move keys and children in the child node to make space for the borrowed key
	for (int i = new_right_sibling->num_keys() - 1; i >= 0; --i) {
		new_right_sibling->keys[i + 1] = new_right_sibling->keys[i];
	}

	if (new_right_sibling->type == NodeType::INTERNAL) {
		auto new_right_sibling_internal_node = new_right_sibling.To<InternalNode>();
		auto new_left_sibling_internal_node = new_left_sibling.To<InternalNode>();

		for (int i = new_right_sibling_internal_node->num_keys(); i >= 0; --i) {
			new_right_sibling_internal_node->children[i + 1] = new_right_sibling_internal_node->children[i];
		}
		new_right_sibling_internal_node->children[0] = new_left_sibling_internal_node->children[new_left_sibling_internal_node->num_keys()];
		new_left_sibling_internal_node->children[new_left_sibling_internal_node->num_keys()] = BlobStore::InvalidIndex;
	}
	else {
		auto new_right_sibling_leaf_node = new_right_sibling.To<LeafNode>();
		auto new_left_sibling_leaf_node = new_left_sibling.To<LeafNode>();

		// Move keys and children in the child node to make space for the borrowed key
		for (int i = new_right_sibling_leaf_node->num_keys() - 1; i >= 0; --i) {
			new_right_sibling_leaf_node->values[i + 1] = new_right_sibling_leaf_node->values[i];
		}
		new_right_sibling_leaf_node->values[0] = new_left_sibling_leaf_node->values[new_left_sibling_leaf_node->num_keys() - 1];
	}

	new_right_sibling->set_key(0, parent_node->get_key(child_index - 1));
	// We want to move this out so we need to return the last key of the left sibling that we're bumping up.
	parent_node->set_key(child_index - 1, new_left_sibling->get_key(new_left_sibling->num_keys() - 1));

	new_left_sibling->set_key(new_left_sibling->num_keys() - 1, BlobStore::InvalidIndex);
	new_right_sibling->increment_num_keys();

	new_left_sibling->decrement_num_keys();

	*out_right_sibling = new_right_sibling;

	return true;
}

template <typename KeyType, typename ValueType, size_t Order>
bool BPlusTree<KeyType, ValueType, Order>::TryToBorrowFromRightSibling(BlobStoreObject<InternalNode> parent_node, BlobStoreObject<BaseNode>* out_left_sibling, BlobStoreObject<const BaseNode> left_sibling, BlobStoreObject<const BaseNode> right_sibling, int child_index) {
	if (!right_sibling || right_sibling->will_underflow()) {
		return false;
	}

	BlobStoreObject<BaseNode> new_left_sibling;
	BlobStoreObject<BaseNode> new_right_sibling;
	if (right_sibling->type == NodeType::LEAF) {
		new_left_sibling = left_sibling.To<LeafNode>().Clone().To<BaseNode>();
		new_right_sibling = right_sibling.To<LeafNode>().Clone().To<BaseNode>();
	}
	else {
		new_left_sibling = left_sibling.To<InternalNode>().Clone().To<BaseNode>();
		new_right_sibling = right_sibling.To<InternalNode>().Clone().To<BaseNode>();
	}

	parent_node->children[child_index] = new_left_sibling.Index();
	parent_node->children[child_index + 1] = new_right_sibling.Index();

	size_t key_index;
	if (new_left_sibling->type == NodeType::INTERNAL) {
		auto new_left_internal_node = new_left_sibling.To<InternalNode>();
		auto new_right_internal_node = new_right_sibling.To<InternalNode>();

		new_left_internal_node->set_key(new_left_sibling->num_keys(), parent_node->get_key(child_index));
		new_left_internal_node->children[new_left_internal_node->num_keys() + 1] = new_right_internal_node->children[0];

		for (int i = 1; i <= new_right_internal_node->num_keys(); ++i) {
			new_right_internal_node->children[i - 1] = new_right_internal_node->children[i];
		}
		new_right_internal_node->children[new_right_internal_node->num_keys()] = BlobStore::InvalidIndex;
		key_index = new_right_sibling->get_key(0);
	}
	else {
		auto new_left_leaf_node = new_left_sibling.To<LeafNode>();
		auto new_right_leaf_node = new_right_sibling.To<LeafNode>();

		new_left_leaf_node->set_key(new_left_leaf_node->num_keys(), new_right_leaf_node->get_key(0));
		new_left_leaf_node->values[new_left_leaf_node->num_keys()] = new_right_leaf_node->values[0];

		for (int i = 1; i < new_right_leaf_node->num_keys(); ++i) {
			new_right_leaf_node->values[i - 1] = new_right_leaf_node->values[i];
		}
		new_right_leaf_node->values[new_right_leaf_node->num_keys() - 1] = BlobStore::InvalidIndex;

		key_index = new_right_sibling->get_key(1);
	}

	for (int i = 1; i < new_right_sibling->num_keys(); ++i) {
		new_right_sibling->keys[i - 1] = new_right_sibling->keys[i];
	}
	new_right_sibling->keys[new_right_sibling->num_keys() - 1] = BlobStore::InvalidIndex;

	new_left_sibling->increment_num_keys();
	new_right_sibling->decrement_num_keys();

	parent_node->set_key(child_index, key_index);

	*out_left_sibling = new_left_sibling;

	return true;
}

template <typename KeyType, typename ValueType, size_t Order>
KeyValuePair<KeyType, ValueType> BPlusTree<KeyType, ValueType, Order>::Delete(BlobStoreObject<BaseNode>* parent_node, int child_index, const KeyType& key) {
	BlobStoreObject<InternalNode> parent_internal_node = parent_node->To<InternalNode>();
	BlobStoreObject<const BaseNode> const_child = GetChildConst(parent_internal_node, child_index);
	BlobStoreObject<BaseNode> child;// = GetChild(parent_internal_node, child_index);
	// The current child where we want to delete a node is too small.
	if (const_child->will_underflow()) {
		// Rebalancing might involve one of three operations:
		//     1. Borrowing a key from the left sibling.
		//     2. Borrowing a key from the right sibling.
		//     3. Merging the left or right sibling with the current child.
		// Rebalancing might drop the current child and move all its keys to its left sibling if this child is the
		// rightmost child within the parent. This is why child is an output parameter.
		// If we end up merging nodes, we might remove a key from the parent which is why the child
		// must be rebalanced before the recursive calls below.
		RebalanceChildWithLeftOrRightSibling(parent_internal_node, child_index, const_child, &child);
		
		// TODO(fsamuel): This shouldn't happen here. This should happen at the first call.
		if (parent_internal_node->num_keys() == 0) {
			// The root node is empty, so make the left child the new root node
			// This is okay to drop since this is a new clone.
			blob_store_.Drop(parent_internal_node.Index());
			*parent_node = child;
		}
	}
	else {
		if (const_child->type == NodeType::LEAF) {
			child = const_child.To<LeafNode>().Clone().To<BaseNode>();
		}
		else {
			child = const_child.To<InternalNode>().Clone().To<BaseNode>();
		}
		parent_internal_node->children[child_index] = child.Index();
	}

	// The current child where we want to delete a node is a leaf node.
	if (child->type == NodeType::LEAF) {
		return DeleteFromLeafNode(child.To<LeafNode>(), key);
	}

	return DeleteFromInternalNode(child.To<InternalNode>(), key);
}

template <typename KeyType, typename ValueType, size_t Order>
BlobStoreObject<const KeyType> BPlusTree<KeyType, ValueType, Order>::GetPredecessorKey(BlobStoreObject<BaseNode> node) {
	while (node->type != NodeType::LEAF) {
		auto internal_node = node.To<InternalNode>();
		node = GetChild(internal_node, internal_node->num_keys()); // Get the rightmost child
	}
	return GetKey(node, node->num_keys() - 1); // Return the rightmost key in the leaf node
}

template <typename KeyType, typename ValueType, size_t Order>
BlobStoreObject<const KeyType> BPlusTree<KeyType, ValueType, Order>::GetSuccessorKey(BlobStoreObject<const BaseNode> node, const KeyType& key) {
	if (node->type == NodeType::LEAF) {
		for (int i = 0; i < node->num_keys(); ++i) {
			auto key_ptr = GetKey(node, i);
			if (*key_ptr > key)
				return key_ptr;
		}
		return BlobStoreObject<const KeyType>();

	}
	for (int i = 0; i <= node->num_keys(); ++i) {
		auto internal_node = node.To<InternalNode>();
		auto child = GetChild(internal_node, i); // Get the leftmost child
		auto key_ptr = GetSuccessorKey(child, std::move(key));
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
	left_node->set_key(left_node->num_keys(), /*parent_node->get_key(key_index_in_parent)*/parent_key);
	left_node->increment_num_keys();

	// Move all keys and child pointers from the right sibling node to the left sibling node
	for (size_t i = 0; i < right_node->num_keys(); ++i) {
		left_node->set_key(left_node->num_keys(), right_node->get_key(i));
		left_node->children[left_node->num_keys()] = right_node->children[i];

		// Update the parent of the moved children
		left_node->increment_num_keys();
	}
	left_node->children[left_node->num_keys()] = right_node->children[right_node->num_keys()];
}

template <typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::MergeLeafNodes(
	BlobStoreObject<LeafNode> left_node,
	BlobStoreObject<const LeafNode> right_node) {
	// Move the key from the parent node down to the left sibling node
	left_node->set_key(left_node->num_keys(), right_node->get_key(0));
	left_node->values[left_node->num_keys()] = right_node->values[0];
	left_node->increment_num_keys();

	// Move all keys and values from the right sibling node to the left sibling node
	for (size_t i = 1; i < right_node->num_keys(); ++i) {
		left_node->set_key(left_node->num_keys(), right_node->get_key(i));
		left_node->values[left_node->num_keys()] = right_node->values[i];
		left_node->increment_num_keys();
	}
	left_node->next = right_node->next;
}

template <typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::MergeChildWithLeftOrRightSibling(
	BlobStoreObject<InternalNode> parent, int child_index, BlobStoreObject<const BaseNode> child, BlobStoreObject<BaseNode>* out_child) {
	BlobStoreObject<BaseNode> left_child;
	BlobStoreObject<const BaseNode> right_child;
	int key_index_in_parent;  
	if (child_index < parent->num_keys()) {
		key_index_in_parent = child_index;
		//left_child = *child;
		right_child = GetChildConst(parent, child_index + 1);
		if (child->type == NodeType::LEAF) {
			left_child = child.To<LeafNode>().Clone().To<BaseNode>();
		}
		else {
			left_child = child.To<InternalNode>().Clone().To<BaseNode>();
		}
		*out_child = left_child;
		parent->children[child_index] = left_child.Index();
	}
	else {
		key_index_in_parent = child_index - 1;
		BlobStoreObject<const BaseNode> const_left_child = GetChildConst(parent, child_index - 1);
		right_child = child;
		if (const_left_child->type == NodeType::LEAF) {
			left_child = const_left_child.To<LeafNode>().Clone().To<BaseNode>();
		}
		else {
			left_child = const_left_child.To<InternalNode>().Clone().To<BaseNode>();
		}
		parent->children[child_index - 1] = left_child.Index();
		*out_child = left_child;
	}

	if (left_child->type == NodeType::LEAF) {
		MergeLeafNodes(
			left_child.To<LeafNode>(),
			right_child.To<const LeafNode>());
	}
	else {
		MergeInternalNodes(
			left_child.To<InternalNode>(), 
			right_child.To<const InternalNode>(), 
			parent->get_key(key_index_in_parent));
	}

	// Update the parent node by removing the key that was moved down and the pointer to the right sibling node
	for (int i = key_index_in_parent; i < parent->num_keys() - 1; ++i) {
		parent->set_key(i, parent->get_key(i + 1));
		parent->children[i + 1] = parent->children[i + 2];
	}
	// The parent could underflow if we don't do something before getting this far.
	parent->decrement_num_keys();
}

template <typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::RebalanceChildWithLeftOrRightSibling(
	BlobStoreObject<InternalNode> parent,
	int child_index,
	BlobStoreObject<const BaseNode> child,
	BlobStoreObject<BaseNode>* new_child) {
	{
		BlobStoreObject<const BaseNode> left_sibling = child_index > 0 ? GetChildConst(parent, child_index - 1) : BlobStoreObject<const BaseNode>();
		if (TryToBorrowFromLeftSibling(parent, left_sibling, child,new_child, child_index)) {
			return;
		}
	}
	
	{
		BlobStoreObject<const BaseNode> right_sibling = (child_index + 1) <= parent->num_keys() ? GetChildConst(parent, child_index + 1) : BlobStoreObject<const BaseNode>();
		if (TryToBorrowFromRightSibling(parent, new_child, child, right_sibling, child_index)) {
			return;
		}
	}
	
	MergeChildWithLeftOrRightSibling(parent, child_index, child, new_child);
}

#endif // B_PLUS_TREE_H_

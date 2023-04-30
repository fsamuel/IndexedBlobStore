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

template <typename KeyType, typename ValueType, std::size_t Order>
class BPlusTree {
private:
	enum class NodeType : uint8_t { INTERNAL, LEAF };

	struct BaseNode {
		NodeType type;
		std::size_t n;
		std::array<BlobStore::index_type, Order - 1> keys;

		BaseNode(NodeType type, std::size_t n) : type(type), n(n) {
			for (size_t i = 0; i < Order - 1; ++i) {
				keys[i] = BlobStore::InvalidIndex;
			}
		}

		bool IsFull() const { return n == Order - 1; }

		size_t num_keys() const { return n; }
		void increment_num_keys() { ++n; }
		void decrement_num_keys() { --n; }
		void set_num_keys(size_t num_keys) { n = num_keys; }
		size_t get_key(size_t index) const { return keys[index]; }
		void set_key(size_t index, size_t key) { keys[index] = key; }
	};

	static_assert(std::is_trivially_copyable<BaseNode>::value, "BaseNode is trivially copyable");
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

		size_t num_keys() const { return base.num_keys(); }
		void increment_num_keys() { base.increment_num_keys(); }
		void decrement_num_keys() { base.decrement_num_keys(); }
		void set_num_keys(size_t num_keys) { base.set_num_keys(num_keys); }
		size_t get_key(size_t index) const { return base.get_key(index); }
		void set_key(size_t index, size_t key) { base.set_key(index, key); }
	};

	static_assert(std::is_trivially_copyable<InternalNode>::value, "InternalNode is trivially copyable");
	static_assert(std::is_standard_layout<InternalNode>::value, "InternalNode is standard layout");

	struct LeafNode {
		BaseNode base;
		std::array<BlobStore::index_type, Order> values;
		BlobStore::index_type next;

		LeafNode(BlobStore::index_type next, std::size_t n = 0)
			: base(NodeType::LEAF, n), next(next) {}

		size_t num_keys() const { return base.num_keys();}
		void increment_num_keys() { base.increment_num_keys(); }
		void decrement_num_keys() { base.decrement_num_keys(); }
		void set_num_keys(size_t num_keys) { base.set_num_keys(num_keys); }
		size_t get_key(size_t index) const { return base.get_key(index); }
		void set_key(size_t index, size_t key) { base.set_key(index, key); }
	};

	static_assert(std::is_trivially_copyable<LeafNode>::value, "LeafNode is trivially copyable");
	static_assert(std::is_standard_layout<LeafNode>::value, "LeafNode is standard layout");

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
			return BlobStoreObject<const KeyType>::CreateNull();
		}

		BlobStoreObject<ValueType> GetValue() const {
			if (leaf_node_ != nullptr) {
				return BlobStoreObject<ValueType>(store_, leaf_node_->values[key_index_]);
			}
			return BlobStoreObject<ValueType>::CreateNull();
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
		else {
			root_index_ = blob_store_.begin().index();
		}
	}

	Iterator Search(const KeyType& key);

	void Insert(const KeyType& key, const ValueType& value);
	void Insert(BlobStoreObject<KeyType>&& key, BlobStoreObject<ValueType>&& value);

	KeyValuePair<KeyType, ValueType> Remove(const KeyType& key);

	// Prints a BlobStoreObject<BaseNode> in a human-readable format.
	void PrintNode(BlobStoreObject<BaseNode> node) {
		if (node == nullptr) {
			std::cout << "NULL Node" << std::endl;
			return;
		}
		if (node->type == NodeType::INTERNAL) {
			BlobStoreObject<InternalNode> internal_node = node.To<InternalNode>();
			std::cout << "Internal node (n = " << internal_node->num_keys() << ") ";
			for (size_t i = 0; i < internal_node->num_keys(); ++i) {
				std::cout << *BlobStoreObject<KeyType>(&blob_store_, internal_node->keys[i]) << " ";
			}
			std::cout << std::endl;
		}
		else {
			BlobStoreObject<LeafNode> leaf_node = node.To<LeafNode>();
			std::cout << "Leaf node (n = " << leaf_node->num_keys() << ") ";
			for (size_t i = 0; i < leaf_node->num_keys(); ++i) {
				std::cout << *BlobStoreObject<KeyType>(&blob_store_, leaf_node->keys[i]) << " ";
			}
			std::cout << std::endl;
		}
	}

	// Prints the tree in a human-readable format in breadth-first order.
	void PrintTree() {
		struct NodeWithLevel {
			BlobStoreObject<const BaseNode> node;
			size_t level;
		};
		std::queue<NodeWithLevel> queue;
		queue.push({ root_, 0 });
		while (!queue.empty()) {
			NodeWithLevel node_with_level = queue.front();
			queue.pop();
			if (node_with_level.node->type == NodeType::INTERNAL) {
				BlobStoreObject<const InternalNode> internal_node = node_with_level.node.To<InternalNode>();
				std::cout << std::string(node_with_level.level, ' ') << "Internal node (n = " << internal_node->num_keys() << ") ";
				for (size_t i = 0; i < internal_node->num_keys(); ++i) {
					std::cout << *BlobStoreObject<const KeyType>(&blob_store_, internal_node->keys[i]) << " ";
				}
				for (size_t i = 0; i <= internal_node->num_keys(); ++i) {
					queue.push({ GetChild(internal_node, i), node_with_level.level + 1 });
				}
				std::cout << std::endl;
			}
			else {
				BlobStoreObject<const LeafNode> leaf_node = node_with_level.node.To<LeafNode>();
				std::cout << std::string(node_with_level.level, ' ') << "Leaf node (n = " << leaf_node->num_keys() << ") ";
				for (size_t i = 0; i < leaf_node->num_keys(); ++i) {
					std::cout << *BlobStoreObject<const KeyType>(&blob_store_, leaf_node->keys[i]) << " ";
				}
				std::cout << std::endl;
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
	size_t root_index_ = BlobStore::InvalidIndex;

	void CreateRoot() {
		auto root = blob_store_.New<LeafNode>(BlobStore::InvalidIndex);
		root_index_ = root.Index();
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
			return BlobStoreObject<const_preserving_BaseNode>::CreateNull();
		}
		return BlobStoreObject<const_preserving_BaseNode>(&blob_store_, node->children[child_index]);
	}

	// Given a provided node with a held write lock, grab the key at the provided key_index.
	template<typename U, typename std::enable_if<
		std::is_same<typename std::remove_const<U>::type, BaseNode>::value ||
		std::is_same<typename std::remove_const<U>::type, InternalNode>::value ||
		std::is_same<typename std::remove_const<U>::type, LeafNode>::value
	>::type* = nullptr>
	BlobStoreObject<const KeyType> GetKey(const BlobStoreObject<U>& node, size_t key_index) {
		if (node == nullptr || key_index > node->num_keys() - 1) {
			return BlobStoreObject<const KeyType>::CreateNull();
		}
		return BlobStoreObject<const KeyType>(&blob_store_, node->get_key(key_index));
	}

	template<typename U, typename std::enable_if<
		std::is_same<typename std::remove_const<U>::type, LeafNode>::value
	>::type* = nullptr>
	BlobStoreObject<const ValueType> GetValue(const BlobStoreObject<U>& node, size_t value_index) {
		if (node == nullptr || value_index > node->num_keys() - 1) {
			return BlobStoreObject<const ValueType>::CreateNull();
		}
		return BlobStoreObject<const ValueType>(&blob_store_, node->values[value_index]);
	}

	Iterator Search(BlobStoreObject<const BaseNode>&& node, const KeyType& key);
	void SplitChild(BlobStoreObject<InternalNode> parentNode, BlobStoreObject<BaseNode> childNode, int child_index);
	void InsertNonFull(BlobStoreObject<BaseNode> node, BlobStoreObject<KeyType> key, BlobStoreObject<ValueType> value);

	KeyValuePair<KeyType, ValueType> Remove(BlobStoreObject<InternalNode> parent_node, int child_index, const KeyType& key);
	KeyValuePair<KeyType, ValueType> RemoveFromLeafNode(BlobStoreObject<LeafNode>&& node, const KeyType& key);
	bool BorrowFromLeftSibling(BlobStoreObject<InternalNode> parent_node, BlobStoreObject<BaseNode> child_node, int child_index);
	bool BorrowFromRightSibling(BlobStoreObject<InternalNode> parent_node, BlobStoreObject<BaseNode> child_node, int child_index);

	BlobStoreObject<const KeyType> GetPredecessorKey(BlobStoreObject<BaseNode> node);
	BlobStoreObject<const KeyType> GetSuccessorKey(BlobStoreObject<BaseNode> node, const KeyType& key);
	void MergeChildren(BlobStoreObject<InternalNode>&& parentNode, BlobStoreObject<BaseNode> left_child, int index);
};

template<typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::Insert(const KeyType& key, const ValueType& value) {
	BlobStoreObject<KeyType> key_ptr = blob_store_.New<KeyType>(key);
	BlobStoreObject<ValueType> value_ptr = blob_store_.New<ValueType>(value);

	Insert(std::move(key_ptr), std::move(value_ptr));
}

template<typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::Insert(BlobStoreObject<KeyType>&& key, BlobStoreObject<ValueType>&& value) {
	auto root = blob_store_.GetMutable<BaseNode>(root_index_);
	if (root->num_keys() == Order - 1) {
		// Root is full, create a new root
		BlobStoreObject<InternalNode> new_root = blob_store_.New<InternalNode>(1);
		new_root->children[0] = root.Index();
		new_root->set_num_keys(0);
		SplitChild(new_root, root, 0);
		InsertNonFull(new_root.To<BaseNode>(), key, value);
		root_index_ = new_root.Index();
	}
	else {
		InsertNonFull(root, key, value);
	}
}

template <typename KeyType, typename ValueType, size_t Order>
typename BPlusTree<KeyType, ValueType, Order>::Iterator BPlusTree<KeyType, ValueType, Order>::Search(const KeyType& key) {
	if (root_index_ == BlobStore::InvalidIndex) {
		return Iterator(&blob_store_, BlobStoreObject<const LeafNode>::CreateNull(), 0);
	}
	return Search(blob_store_.Get<BaseNode>(root_index_), key);
}

template <typename KeyType, typename ValueType, size_t Order>
typename BPlusTree<KeyType, ValueType, Order>::Iterator BPlusTree<KeyType, ValueType, Order>::Search(BlobStoreObject<const BaseNode>&& node, const KeyType& key) {
	if (node->type == NodeType::LEAF) {
		BlobStoreObject<const LeafNode> leaf_node = node.To<LeafNode>();
		for (int i = 0; i < leaf_node->num_keys(); i++) {
			auto current_key = GetKey(leaf_node.To<BaseNode>(), i);
			if (key == *current_key) {
				return Iterator(&blob_store_, std::move(leaf_node), i);
			}
		}
		return Iterator(&blob_store_, BlobStoreObject<const LeafNode>::CreateNull(), 0);
	}
	else {
		BlobStoreObject<const InternalNode> internal_node = node.To<InternalNode>();
		int i = 0;

		BlobStoreObject<const KeyType> current_key;
		while (i < internal_node->num_keys()) {
			current_key = GetKey(internal_node.To<BaseNode>(), i);
			if (key <= *current_key) {
				break;
			}
			i += 1;
		}
		if (i < internal_node->num_keys() && key == *current_key) {
			return Search(GetChild(std::move(internal_node), i + 1), key);
		}
		return Search(GetChild(std::move(internal_node), i), key);
	}
}

template<typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::SplitChild(BlobStoreObject<InternalNode> parent_node, BlobStoreObject<BaseNode> child_node, int child_index) {
	NodeType new_node_type = child_node->type;
	BlobStoreObject<BaseNode> new_node = (
		new_node_type == NodeType::INTERNAL ?
		blob_store_.New<InternalNode>(Order).To<BaseNode>() :
		blob_store_.New<LeafNode>(Order).To<BaseNode>());

	int middle_key_index = (child_node->num_keys() - 1) / 2;
	size_t middle_key = child_node->keys[middle_key_index];

	if (new_node_type == NodeType::INTERNAL) {
		new_node->set_num_keys(child_node->num_keys() - middle_key_index - 1);
		for (int i = 0; i < new_node->num_keys(); ++i) {
			new_node->keys[i] = child_node->keys[middle_key_index + i + 1];
			child_node->keys[middle_key_index + i + 1] = BlobStore::InvalidIndex;
		}

		auto new_internal_node = new_node.To<InternalNode>();
		auto child_internal_node = child_node.To<InternalNode>();
		for (int i = 0; i <= new_node->num_keys(); ++i) {
			new_internal_node->children[i] = child_internal_node->children[middle_key_index + i + 1];
			child_internal_node->children[middle_key_index + i + 1] = BlobStore::InvalidIndex;
		}
	}
	else {
		new_node->set_num_keys(child_node->num_keys() - middle_key_index);
		for (int i = 0; i < new_node->num_keys(); ++i) {
			new_node->keys[i] = child_node->keys[middle_key_index + i];
			child_node->keys[middle_key_index + i] = BlobStore::InvalidIndex;
		}
		auto new_leaf_node = new_node.To<LeafNode>();
		auto child_leaf_node = child_node.To<LeafNode>();
		for (int i = 0; i < new_node->num_keys(); ++i) {
			new_leaf_node->values[i] = child_leaf_node->values[middle_key_index + i];
			child_leaf_node->values[middle_key_index + i] = BlobStore::InvalidIndex;
		}
		new_leaf_node->next = child_leaf_node->next;
		child_leaf_node->next = new_node.Index();
		child_leaf_node->values[middle_key_index] = BlobStore::InvalidIndex;
	}

	child_node->set_num_keys(middle_key_index);
	child_node->keys[middle_key_index] = BlobStore::InvalidIndex;

	for (int i = parent_node->num_keys(); i >= static_cast<int>(child_index) + 1; --i) {
		parent_node->children[i + 1] = parent_node->children[i];
	}
	parent_node->children[child_index + 1] = new_node.Index();

	for (int i = parent_node->num_keys() - 1; i >= static_cast<int>(child_index); --i) {
		parent_node->set_key(i + 1, parent_node->get_key(i));
	}
	parent_node->set_key(child_index, middle_key);

	parent_node->set_num_keys(parent_node->num_keys() + 1);
}

template<typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::InsertNonFull(BlobStoreObject<BaseNode> node, BlobStoreObject<KeyType> key, BlobStoreObject <ValueType> value) {
	if (node->type == NodeType::LEAF) {
		BlobStoreObject<LeafNode> leaf_node = node.To<LeafNode>();
		// Shift the keys and values right.
		int i = leaf_node->num_keys() - 1;
		while (i >= 0) {
			BlobStoreObject<const KeyType> key_ptr = GetKey(leaf_node, i);
			if (*key >= *key_ptr) {
				break;
			}
			leaf_node->set_key(i + 1, leaf_node->get_key(i));
			leaf_node->values[i + 1] = leaf_node->values[i];
			--i;
		}
		leaf_node->set_key(i + 1, key.Index());
		leaf_node->values[i + 1] = value.Index();
		leaf_node->increment_num_keys();
	}
	else {
		BlobStoreObject<InternalNode> internal_node = node.To<InternalNode>();
		int i = internal_node->num_keys() - 1;
		while (i >= 0) {
			auto current_key = GetKey(internal_node, i);
			if (*key >= *current_key) {
				break;
			}
			--i;
		}
		i += 1;
		{
			BlobStoreObject<BaseNode> child_node = GetChild(internal_node, i);
			if (child_node->num_keys() == Order - 1) {
				SplitChild(internal_node, child_node, i);
				auto current_key = GetKey(internal_node, i);
				if (*key > *current_key) {
					i += 1;
				}
			}
		}
		BlobStoreObject<BaseNode> child_node = GetChild(internal_node, i);
		InsertNonFull(child_node, key, value);
	}
}

template <typename KeyType, typename ValueType, size_t Order>
KeyValuePair<KeyType, ValueType> BPlusTree<KeyType, ValueType, Order>::Remove(const KeyType& key) {
	if (root_index_ == BlobStore::InvalidIndex) {
		return std::make_pair(BlobStoreObject<const KeyType>::CreateNull(), BlobStoreObject<const ValueType>::CreateNull());
	}
	//return Remove(root_, key);
	auto root = blob_store_.GetMutable<BaseNode>(root_index_);
	if (root->type == NodeType::LEAF) {
		return RemoveFromLeafNode(root.To<LeafNode>(), key);
	}
	auto internal_node = root.To<InternalNode>();
	int i = 0;
	BlobStoreObject<const KeyType> current_key;
	while (i < internal_node->num_keys()) {
		current_key = GetKey(internal_node, i);
		if (key <= *current_key) {
			break;
		}
		i += 1;
	}
	if (i < internal_node->num_keys() && key == *current_key) {
		return Remove(std::move(internal_node), i + 1, key);
	}
	return Remove(std::move(internal_node), i, key);
}

template <typename KeyType, typename ValueType, size_t Order>
KeyValuePair<KeyType, ValueType> BPlusTree<KeyType, ValueType, Order>::RemoveFromLeafNode(BlobStoreObject<LeafNode>&& node, const KeyType& key) {
	int i = 0;
	while (i < node->num_keys() && key != *GetKey(node, i)) {
		i += 1;
	}

	if (i == node->num_keys()) {
		return std::make_pair(BlobStoreObject<const KeyType>::CreateNull(), BlobStoreObject<const ValueType>::CreateNull()); // Key not found
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
bool BPlusTree<KeyType, ValueType, Order>::BorrowFromLeftSibling(BlobStoreObject<InternalNode> parent_node, BlobStoreObject<BaseNode> child_node, int child_index) {
	if (child_index == 0) {
		return false;
	}

	BlobStoreObject<BaseNode> left_sibling(&blob_store_, parent_node->children[child_index - 1]);

	if (left_sibling->num_keys() <= (Order - 1) / 2) {
		return false;
	}

	// Move keys and children in the child node to make space for the borrowed key
	for (int i = child_node->num_keys() - 1; i >= 0; --i) {
		child_node->keys[i + 1] = child_node->keys[i];
	}

	if (child_node->type == NodeType::INTERNAL) {
		auto child_internal_node = child_node.To<InternalNode>();
		auto left_sibling_internal_node = left_sibling.To<InternalNode>();
		for (int i = child_internal_node->num_keys(); i >= 0; --i) {
			child_internal_node->children[i + 1] = child_internal_node->children[i];
		}
		child_internal_node->children[0] = left_sibling_internal_node->children[left_sibling_internal_node->num_keys()];
		left_sibling_internal_node->children[left_sibling_internal_node->num_keys()] = BlobStore::InvalidIndex;
	}
	else {
		auto child_leaf_node = child_node.To<LeafNode>();
		auto left_sibling_leaf_node = left_sibling.To<LeafNode>();

		// Move keys and children in the child node to make space for the borrowed key
		for (int i = child_leaf_node->num_keys() - 1; i >= 0; --i) {
			child_leaf_node->values[i + 1] = child_leaf_node->values[i];
		}
		child_leaf_node->values[0] = left_sibling_leaf_node->values[left_sibling_leaf_node->num_keys() - 1];
	}

	child_node->set_key(0, parent_node->get_key(child_index - 1));
	parent_node->set_key(child_index - 1, left_sibling->get_key(left_sibling->num_keys() - 1));
	left_sibling->set_key(left_sibling->num_keys() - 1, BlobStore::InvalidIndex);

	child_node->increment_num_keys();
	left_sibling->decrement_num_keys();

	return true;
}

template <typename KeyType, typename ValueType, size_t Order>
bool BPlusTree<KeyType, ValueType, Order>::BorrowFromRightSibling(BlobStoreObject<InternalNode> parent_node, BlobStoreObject<BaseNode> child_node, int child_index) {
	if (child_index == parent_node->num_keys()) {
		return false;
	}

	BlobStoreObject<BaseNode> right_sibling = GetChild(parent_node, child_index + 1);

	if (right_sibling->num_keys() <= (Order - 1) / 2) {
		return false;
	}

	child_node->set_key(child_node->num_keys(), parent_node->get_key(child_index));
	if (parent_node->get_key(child_index) != right_sibling->get_key(0)) {
		parent_node->set_key(child_index, right_sibling->get_key(0));
	}
	else {
		parent_node->set_key(child_index, right_sibling->get_key(1));
	}

	if (child_node->type == NodeType::INTERNAL) {
		auto child_internal_node = child_node.To<InternalNode>();
		auto right_sibling_internal_node = right_sibling.To<InternalNode>();
		child_internal_node->children[child_internal_node->num_keys() + 1] = right_sibling_internal_node->children[0];
		for (int i = 1; i <= right_sibling_internal_node->num_keys(); ++i) {
			right_sibling_internal_node->children[i - 1] = right_sibling_internal_node->children[i];
		}
		right_sibling_internal_node->children[right_sibling_internal_node->num_keys()] = BlobStore::InvalidIndex;
	}
	else {
		auto child_leaf_node = child_node.To<LeafNode>();
		auto right_sibling_leaf_node = right_sibling.To<LeafNode>();
		child_leaf_node->values[child_leaf_node->num_keys()] = right_sibling_leaf_node->values[0];
		for (int i = 1; i < right_sibling_leaf_node->num_keys(); ++i) {
			right_sibling_leaf_node->values[i - 1] = right_sibling_leaf_node->values[i];
		}
		right_sibling_leaf_node->values[right_sibling_leaf_node->num_keys() - 1] = BlobStore::InvalidIndex;
	}

	for (int i = 1; i < right_sibling->num_keys(); ++i) {
		right_sibling->keys[i - 1] = right_sibling->keys[i];
	}
	right_sibling->keys[right_sibling->num_keys() - 1] = BlobStore::InvalidIndex;

	child_node->increment_num_keys();
	right_sibling->decrement_num_keys();

	return true;
}

template <typename KeyType, typename ValueType, size_t Order>
KeyValuePair<KeyType, ValueType> BPlusTree<KeyType, ValueType, Order>::Remove(BlobStoreObject<InternalNode> parent_node, int child_index, const KeyType& key) {
	BlobStoreObject<BaseNode> child_node = GetChild(parent_node, child_index);
	if (child_node->num_keys() == (Order - 1) / 2) {
		if (!BorrowFromLeftSibling(parent_node, child_node, child_index) && !BorrowFromRightSibling(parent_node, child_node, child_index)) {
			if (child_index < parent_node->num_keys()) {
				MergeChildren(std::move(parent_node), child_node, child_index);
			}
			else {
				child_node = GetChild(parent_node, child_index - 1);
				MergeChildren(std::move(parent_node), child_node, child_index - 1);
			}
		}
	}
	if (child_node->type == NodeType::LEAF) {
		return RemoveFromLeafNode(child_node.To<LeafNode>(), key);
	}

	auto internal_node = child_node.To<InternalNode>();
	int i = 0;

	BlobStoreObject<const KeyType> current_key;
	while (i < internal_node->num_keys()) {
		current_key = GetKey(internal_node, i);
		if (key <= *current_key) {
			break;
		}
		i += 1;
	}
	if (i < internal_node->num_keys() && key == *current_key) {
		auto kv = Remove(internal_node, i + 1, key);
		BlobStoreObject<const KeyType> current_key;
		while (i < internal_node->num_keys()) {
			current_key = GetKey(internal_node, i);
			if (key <= *current_key) {
				break;
			}
			i += 1;
		}
		if (i < internal_node->num_keys() && key == *current_key) {
			// Can there ever be a null successor? That means there is no successor at all.
			// That shouldn't happen I think.
			auto key_ptr = GetSuccessorKey(internal_node.To<BaseNode>(), key);
			internal_node->set_key(i, key_ptr.Index());
		}
		return kv;
	}
	return Remove(internal_node, i, key);
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
BlobStoreObject<const KeyType> BPlusTree<KeyType, ValueType, Order>::GetSuccessorKey(BlobStoreObject<BaseNode> node, const KeyType& key) {
	if (node->type == NodeType::LEAF) {
		for (int i = 0; i < node->num_keys(); ++i) {
			auto key_ptr = GetKey(node, i);
			if (*key_ptr > key)
				return key_ptr;
		}
		return BlobStoreObject<const KeyType>::CreateNull();

	}
	for (int i = 0; i <= node->num_keys(); ++i) {
		auto internal_node = node.To<InternalNode>();
		auto child = GetChild(internal_node, i); // Get the leftmost child
		auto key_ptr = GetSuccessorKey(child, key);
		if (key_ptr != nullptr)
			return key_ptr;
	}
	// We should never get here unless the tree has a problem.
	return BlobStoreObject<const KeyType>::CreateNull();

}

template <typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::MergeChildren(BlobStoreObject<InternalNode>&& parent_node, BlobStoreObject<BaseNode> left_child, int key_index_in_parent) {
	BlobStoreObject<BaseNode> right_child = GetChild(parent_node, key_index_in_parent + 1);

	if (left_child->type == NodeType::LEAF) {
		// Merge leaf nodes
		auto left_node = left_child.To<LeafNode>();
		auto right_node = right_child.To<LeafNode>();

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
	else {
		// Merge internal nodes
		auto left_node = left_child.To<InternalNode>();
		auto right_node = right_child.To<InternalNode>();

		// Move the key from the parent node down to the left sibling node
		left_node->set_key(left_node->num_keys(), parent_node->get_key(key_index_in_parent));
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

	// Update the parent node by removing the key that was moved down and the pointer to the right sibling node
	for (int i = key_index_in_parent; i < parent_node->num_keys() - 1; ++i) {
		parent_node->set_key(i, parent_node->get_key(i + 1));
		parent_node->children[i + 1] = parent_node->children[i + 2];
	}
	parent_node->decrement_num_keys();

	// Free the right sibling node
	blob_store_.Drop(right_child.Index());

	if (parent_node.Index() == root_index_ && parent_node->num_keys() == 0) {
		// The root node is empty, so make the left child the new root node
		root_index_ = left_child.Index();
		blob_store_.Drop(parent_node.Index());
		parent_node = nullptr;
	}
}

#endif // B_PLUS_TREE_H_

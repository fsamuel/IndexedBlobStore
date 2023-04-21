#ifndef B_PLUS_TREE_H_
#define B_PLUS_TREE_H_

#include <assert.h>
#include <algorithm>
#include <array>
#include "blob_store.h"
#include <queue>

class BaseNode;

template <typename KeyType, typename ValueType>
using KeyValuePair = std::pair<BlobStoreObject<KeyType>, BlobStoreObject<ValueType>>;

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
	};

	struct InternalNode : BaseNode {
		std::array<BlobStore::index_type, Order> children;
		explicit InternalNode(std::size_t n)
			: BaseNode(NodeType::INTERNAL, n) {
			for (size_t i = 0; i < Order; ++i) {
				children[i] = BlobStore::InvalidIndex;
			}
		}
	};

	struct LeafNode : BaseNode {
		std::array<BlobStore::index_type, Order> values;
		BlobStore::index_type next;

		LeafNode(BlobStore::index_type next, std::size_t n = 0)
			: BaseNode(NodeType::LEAF, n), next(next) {}
	};

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

		Iterator(BlobStore* store, BlobStoreObject<LeafNode> leaf_node, size_t key_index) : store_(store), leaf_node_(leaf_node), key_index_(key_index) {}

		Iterator& operator++() {
			++key_index_;
			if (key_index_ > leaf_node_->n - 1) {
				key_index_ = 0;
				leaf_node_ = BlobStoreObject<LeafNode>(store_, leaf_node_->next);
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

		BlobStoreObject<KeyType> GetKey() const {
			if (leaf_node_ != nullptr) {
				return BlobStoreObject<KeyType>(store_, leaf_node_->keys[key_index_]);
			}
			return BlobStoreObject<KeyType>();
		}

		BlobStoreObject<ValueType> GetValue() const {
			if (leaf_node_ != nullptr) {
				return BlobStoreObject<ValueType>(store_, leaf_node_->values[key_index_]);
			}
			return BlobStoreObject<ValueType>();
		}

	private:
		BlobStore* store_;
		BlobStoreObject<LeafNode> leaf_node_;
		size_t key_index_;
	};

	BPlusTree(BlobStore& blob_store) : blob_store_(blob_store) {
		if (blob_store_.IsEmpty()) {
			CreateRoot();
		} else {
			root_ = BlobStoreObject<BaseNode>(&blob_store_, blob_store_.begin().index());
		}
	}

	Iterator Search(const KeyType& key);

	void Insert(const KeyType& key, const ValueType& value);
	void Insert(BlobStoreObject<KeyType> key, BlobStoreObject<ValueType> value);

	KeyValuePair<KeyType, ValueType> Remove(const KeyType& key);

	// Prints a BlobStoreObject<BaseNode> in a human-readable format.
	void PrintNode(BlobStoreObject<BaseNode> node) {
		if (node == nullptr) {
			std::cout << "NULL Node" << std::endl;
			return;
		}
		if (node->type == NodeType::INTERNAL) {
			BlobStoreObject<InternalNode> internal_node = node.To<InternalNode>();
			std::cout << "Internal node (n = " << internal_node->n << ") ";
			for (size_t i = 0; i < internal_node->n; ++i) {
				std::cout << *BlobStoreObject<KeyType>(&blob_store_, internal_node->keys[i]) << " ";
			}
			std::cout << std::endl;
		} else {
			BlobStoreObject<LeafNode> leaf_node = node.To<LeafNode>();
			std::cout << "Leaf node (n = " << leaf_node->n << ") ";
			for (size_t i = 0; i < leaf_node->n; ++i) {
				std::cout << *BlobStoreObject<KeyType>(&blob_store_, leaf_node->keys[i]) << " ";
			}
			std::cout << std::endl;
		}
	}

	// Prints the tree in a human-readable format in breadth-first order.
	void PrintTree() {
		struct NodeWithLevel {
			BlobStoreObject<BaseNode> node;
			size_t level;
		};
		std::queue<NodeWithLevel> queue;
		queue.push({ root_, 0 });
		while (!queue.empty()) {
			NodeWithLevel node_with_level = queue.front();
			queue.pop();
			if (node_with_level.node->type == NodeType::INTERNAL) {
				BlobStoreObject<InternalNode> internal_node = node_with_level.node.To<InternalNode>();
				std::cout << std::string(node_with_level.level, ' ') << "Internal node (n = " << internal_node->n << ") ";
				for (size_t i = 0; i < internal_node->n; ++i) {
					std::cout << *BlobStoreObject<KeyType>(&blob_store_, internal_node->keys[i]) << " ";
				}
				for (size_t i = 0; i <= internal_node->n; ++i) {
					queue.push({ GetChild(internal_node, i), node_with_level.level + 1 });
				}
				std::cout << std::endl;
			} else {
				BlobStoreObject<LeafNode> leaf_node = node_with_level.node.To<LeafNode>();
				std::cout << std::string(node_with_level.level, ' ') << "Leaf node (n = " << leaf_node->n << ") ";
				for (size_t i = 0; i < leaf_node->n; ++i) {
					std::cout << *BlobStoreObject<KeyType>(&blob_store_, leaf_node->keys[i]) << " ";
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
				std::cout << std::string(node_with_level.level, ' ') << "Internal node (n = " << internal_node->n << ") ";
				for (size_t i = 0; i < internal_node->n; ++i) {
					std::cout << *BlobStoreObject<KeyType>(&blob_store_, internal_node->keys[i]) << " ";
				}
				for (int i = internal_node->n; i >= 0; --i) {
					v.push_back({ BlobStoreObject<BaseNode>(&blob_store_, internal_node->children[i]), node_with_level.level + 1 });
				}
				std::cout << std::endl;
			} else {
				BlobStoreObject<LeafNode> leaf_node = node_with_level.node.To<LeafNode>();
				std::cout << std::string(node_with_level.level, ' ') << "Leaf node (n = " << leaf_node->n << ") ";
				for (size_t i = 0; i < leaf_node->n; ++i) {
					std::cout << *BlobStoreObject<KeyType>(&blob_store_, leaf_node->keys[i]) << " ";
				}
				std::cout << std::endl;
			}
		}

	}

private:
	BlobStore& blob_store_;
	BlobStoreObject<BaseNode> root_;

	void CreateRoot() {
		root_ = blob_store_.New<LeafNode>(BlobStore::InvalidIndex).To<BaseNode>();
	}

	BlobStoreObject<BaseNode> GetChild(BlobStoreObject<InternalNode> node, size_t child_index) {
		if (node == nullptr || child_index > node->n) {
			return BlobStoreObject<BaseNode>(&blob_store_, BlobStore::InvalidIndex);
		}
		return BlobStoreObject<BaseNode>(&blob_store_, node->children[child_index]);
	}

	BlobStoreObject<KeyType> GetKey(BlobStoreObject<BaseNode> node, size_t key_index) {
		if (node == nullptr || key_index > node->n - 1) {
			return BlobStoreObject<KeyType>(&blob_store_, BlobStore::InvalidIndex);
		}
		return BlobStoreObject<KeyType>(&blob_store_, node->keys[key_index]);
	}

	BlobStoreObject<ValueType> GetValue(BlobStoreObject<LeafNode> node, size_t value_index) {
		if (node == nullptr || value_index > node->n - 1) {
			return BlobStoreObject<ValueType>(&blob_store_, BlobStore::InvalidIndex);
		}
		return BlobStoreObject<ValueType>(&blob_store_, node->values[value_index]);
	}

	Iterator Search(BlobStoreObject<BaseNode> node, const KeyType& key);
	void SplitChild(BlobStoreObject<InternalNode> parentNode, size_t childIndex);
	void InsertNonFull(BlobStoreObject<BaseNode> node, BlobStoreObject<KeyType> key, BlobStoreObject<ValueType> value);

	KeyValuePair<KeyType, ValueType> Remove(BlobStoreObject<InternalNode> parent_node, int child_index, const KeyType& key);
	KeyValuePair<KeyType, ValueType> RemoveFromLeafNode(BlobStoreObject<LeafNode> node, const KeyType& key);
	bool BorrowFromLeftSibling(BlobStoreObject<InternalNode> parent_node, int child_index);
	bool BorrowFromRightSibling(BlobStoreObject<InternalNode> parent_node, int child_index);

	BlobStoreObject<KeyType> GetPredecessorKey(BlobStoreObject<BaseNode> node);
	BlobStoreObject<KeyType> GetSuccessorKey(BlobStoreObject<BaseNode> node, const KeyType& key);
	void MergeChildren(BlobStoreObject<InternalNode> parentNode, int index);
};

template<typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::Insert(const KeyType& key, const ValueType& value) {
	BlobStoreObject<KeyType> key_ptr = blob_store_.New<KeyType>(key);
	BlobStoreObject<ValueType> value_ptr = blob_store_.New<ValueType>(value);

	Insert(key_ptr, value_ptr);
}

template<typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::Insert(BlobStoreObject<KeyType> key, BlobStoreObject<ValueType> value) {
	if (root_->n == Order - 1) {
		// Root is full, create a new root
		BlobStoreObject<InternalNode> new_root = blob_store_.New<InternalNode>(1);
		new_root->children[0] = root_.Index();
		new_root->n = 0;
		SplitChild(new_root, 0);
		InsertNonFull(new_root.To<BaseNode>(), key, value);
		root_ = new_root.To<BaseNode>();
	} else {
		InsertNonFull(root_, key, value);
	}
}

template <typename KeyType, typename ValueType, size_t Order>
typename BPlusTree<KeyType, ValueType, Order>::Iterator BPlusTree<KeyType, ValueType, Order>::Search(const KeyType& key) {
	if (root_ == nullptr) {
		return Iterator(&blob_store_, BlobStoreObject<LeafNode>(), 0);
	}
	return Search(root_, key);
}

template <typename KeyType, typename ValueType, size_t Order>
typename BPlusTree<KeyType, ValueType, Order>::Iterator BPlusTree<KeyType, ValueType, Order>::Search(BlobStoreObject<BaseNode> node, const KeyType& key) {
	if (node->type == NodeType::LEAF) {
		auto leaf_node = node.To<LeafNode>();
		for (int i = 0; i < leaf_node->n; i++) {
			KeyType* current_key = &*GetKey(leaf_node.To<BaseNode>(), i);
			if (key == *current_key) {
				return Iterator(&blob_store_, leaf_node, i); 
			}
		}
		return Iterator(&blob_store_, BlobStoreObject<LeafNode>(), 0);
	} else {
		auto internal_node = node.To<InternalNode>();
		int i = 0;

		KeyType* current_key = nullptr;
		while (i < internal_node->n) {
			current_key = &*GetKey(internal_node.To<BaseNode>(), i);
			if (key <= *current_key) {
				break;
			}
			i += 1;
		}
		if (i < internal_node->n && key == *current_key) {
			return Search(GetChild(internal_node, i + 1), key);
		}
		return Search(GetChild(internal_node, i), key);
	}
}

template<typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::SplitChild(BlobStoreObject<InternalNode> parent_node, size_t child_index) {
	BlobStoreObject<BaseNode> child_node = GetChild(parent_node, child_index);
	NodeType new_node_type = child_node->type;
	BlobStoreObject<BaseNode> new_node = (
		new_node_type == NodeType::INTERNAL ?
		blob_store_.New<InternalNode>(Order).To<BaseNode>() :
		blob_store_.New<LeafNode>(Order).To<BaseNode>());

	int middle_key_index = (child_node->n - 1) / 2;
	size_t middle_key = child_node->keys[middle_key_index];

	if (new_node_type == NodeType::INTERNAL) {
		new_node->n = child_node->n - middle_key_index - 1;
		for (int i = 0; i < new_node->n; ++i) {
			new_node->keys[i] = child_node->keys[middle_key_index + i + 1];
			child_node->keys[middle_key_index + i + 1] = BlobStore::InvalidIndex;
		}

		auto new_internal_node = new_node.To<InternalNode>();
		auto child_internal_node = child_node.To<InternalNode>();
		for (int i = 0; i <= new_node->n; ++i) {
			new_internal_node->children[i] = child_internal_node->children[middle_key_index + i + 1];
			child_internal_node->children[middle_key_index + i + 1] = BlobStore::InvalidIndex;
		}
	} else {
		new_node->n = child_node->n - middle_key_index;
		for (int i = 0; i < new_node->n; ++i) {
			new_node->keys[i] = child_node->keys[middle_key_index + i];
			child_node->keys[middle_key_index + i] = BlobStore::InvalidIndex;
		}
		auto new_leaf_node = new_node.To<LeafNode>();
		auto child_leaf_node = child_node.To<LeafNode>();
		for (int i = 0; i < new_node->n; ++i) {
			new_leaf_node->values[i] = child_leaf_node->values[middle_key_index + i];
			child_leaf_node->values[middle_key_index + i] = BlobStore::InvalidIndex;
		}
		new_leaf_node->next = child_leaf_node->next;
		child_leaf_node->next = new_node.Index();
		child_leaf_node->values[middle_key_index] = BlobStore::InvalidIndex;
	}

	child_node->n = middle_key_index;
	child_node->keys[middle_key_index] = BlobStore::InvalidIndex;

	for (int i = parent_node->n; i >= static_cast<int>(child_index) + 1; --i) {
		parent_node->children[i + 1] = parent_node->children[i];
	}
	parent_node->children[child_index + 1] = new_node.Index();

	for (int i = parent_node->n - 1; i >= static_cast<int>(child_index); --i) {
		parent_node->keys[i + 1] = parent_node->keys[i];
	}
	parent_node->keys[child_index] = middle_key;

	parent_node->n += 1;
}

template<typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::InsertNonFull(BlobStoreObject<BaseNode> node, BlobStoreObject<KeyType> key, BlobStoreObject <ValueType> value) {
	if (node->type == NodeType::LEAF) {
		BlobStoreObject<LeafNode> leaf_node = node.To<LeafNode>();
		// Shift the keys and values right.
		int i = leaf_node->n - 1;
		while (i >= 0 && *key < *blob_store_.Get<KeyType>(leaf_node->keys[i])) {
			leaf_node->keys[i + 1] = leaf_node->keys[i];
			leaf_node->values[i + 1] = leaf_node->values[i];
			--i;
		}
		leaf_node->keys[i + 1] = key.Index();
		leaf_node->values[i + 1] = value.Index();
		leaf_node->n += 1;
	} else {
		BlobStoreObject<InternalNode> internal_node = node.To<InternalNode>();
		int i = internal_node->n - 1;
		while (i >= 0) {
			KeyType* current_key = &*GetKey(internal_node.To<BaseNode>(), i);
			if (*key >= *current_key) {
				break;
			}
			--i;
		}
		i += 1;
		BlobStoreObject<BaseNode> child_node = GetChild(internal_node, i);
		if (child_node->n == Order - 1) {
			SplitChild(internal_node, i);
			KeyType* current_key = &*GetKey(internal_node.To<BaseNode>(), i);
			if (*key > *current_key) {
				i += 1;
			}
		}
		InsertNonFull(GetChild(internal_node, i), key, value);
	}
}

template <typename KeyType, typename ValueType, size_t Order>
KeyValuePair<KeyType, ValueType> BPlusTree<KeyType, ValueType, Order>::Remove(const KeyType& key) {
	if (root_ == nullptr) {
		return std::make_pair(BlobStoreObject<KeyType>(&blob_store_, BlobStore::InvalidIndex),
			BlobStoreObject<ValueType>(&blob_store_, BlobStore::InvalidIndex));
	}
	//return Remove(root_, key);
	if (root_->type == NodeType::LEAF) {
		return RemoveFromLeafNode(root_.To<LeafNode>(), key);
	}
	auto internal_node = root_.To<InternalNode>();
	int i = 0;
	KeyType* current_key = nullptr;
	while (i < internal_node->n) {
		current_key = &*GetKey(internal_node.To<BaseNode>(), i);
		if (key <= *current_key) {
			break;
		}
		i += 1;
	}
	if (i < internal_node->n && key == *current_key) {
		return Remove(internal_node, i + 1, key);
	}
	return Remove(internal_node, i, key);
}

template <typename KeyType, typename ValueType, size_t Order>
KeyValuePair<KeyType, ValueType> BPlusTree<KeyType, ValueType, Order>::RemoveFromLeafNode(BlobStoreObject<LeafNode> node, const KeyType& key) {
	int i = 0;
	while (i < node->n && key != *GetKey(node.To<BaseNode>(), i)) {
		i += 1;
	}

	if (i == node->n) {
		return std::make_pair(BlobStoreObject<KeyType>(&blob_store_, BlobStore::InvalidIndex),
			BlobStoreObject<KeyType>(&blob_store_, BlobStore::InvalidIndex)); // Key not found
	}

	auto kv = std::make_pair(GetKey(node.To<BaseNode>(), i), GetValue(node, i));

	// Shift keys and values to fill the gap
	for (int j = i + 1; j < node->n; j++) {
		node->keys[j - 1] = node->keys[j];
		node->values[j - 1] = node->values[j];
	}
	node->n -= 1;

	return kv; // Key successfully removed
}

template <typename KeyType, typename ValueType, size_t Order>
bool BPlusTree<KeyType, ValueType, Order>::BorrowFromLeftSibling(BlobStoreObject<InternalNode> parent_node, int child_index) {
	if (child_index == 0) {
		return false;
	}

	BlobStoreObject<BaseNode> left_sibling(&blob_store_, parent_node->children[child_index - 1]);
	BlobStoreObject<BaseNode> child_node(&blob_store_, parent_node->children[child_index]);

	if (left_sibling->n <= (Order - 1) / 2) {
		return false;
	}

	// Move keys and children in the child node to make space for the borrowed key
	for (int i = child_node->n - 1; i >= 0; --i) {
		child_node->keys[i + 1] = child_node->keys[i];
	}

	if (child_node->type == NodeType::INTERNAL) {
		auto child_internal_node = child_node.To<InternalNode>();
		auto left_sibling_internal_node = left_sibling.To<InternalNode>();
		for (int i = child_internal_node->n; i >= 0; --i) {
			child_internal_node->children[i + 1] = child_internal_node->children[i];
		}
		child_internal_node->children[0] = left_sibling_internal_node->children[left_sibling_internal_node->n];
		left_sibling_internal_node->children[left_sibling_internal_node->n] = BlobStore::InvalidIndex;
	} else {
		auto child_leaf_node = child_node.To<LeafNode>();
		auto left_sibling_leaf_node = left_sibling.To<LeafNode>();

		// Move keys and children in the child node to make space for the borrowed key
		for (int i = child_leaf_node->n - 1; i >= 0; --i) {
			child_leaf_node->values[i + 1] = child_leaf_node->values[i];
		}
		child_leaf_node->values[0] = left_sibling_leaf_node->values[left_sibling_leaf_node->n - 1];
	}

	child_node->keys[0] = parent_node->keys[child_index - 1];
	parent_node->keys[child_index - 1] = left_sibling->keys[left_sibling->n - 1];
	left_sibling->keys[left_sibling->n - 1] = BlobStore::InvalidIndex;

	child_node->n += 1;
	left_sibling->n -= 1;

	return true;
}

template <typename KeyType, typename ValueType, size_t Order>
bool BPlusTree<KeyType, ValueType, Order>::BorrowFromRightSibling(BlobStoreObject<InternalNode> parent_node, int child_index) {
	if (child_index == parent_node->n) {
		return false;
	}

	BlobStoreObject<BaseNode> right_sibling = GetChild(parent_node, child_index + 1);
	BlobStoreObject<BaseNode> child_node = GetChild(parent_node, child_index);

	if (right_sibling->n <= (Order - 1) / 2) {
		return false;
	}

	child_node->keys[child_node->n] = parent_node->keys[child_index];
	if (parent_node->keys[child_index] != right_sibling->keys[0]) {
		parent_node->keys[child_index] = right_sibling->keys[0];
	} else {
		parent_node->keys[child_index] = right_sibling->keys[1];
	}

	if (child_node->type == NodeType::INTERNAL) {
		auto child_internal_node = child_node.To<InternalNode>();
		auto right_sibling_internal_node = right_sibling.To<InternalNode>();
		child_internal_node->children[child_internal_node->n + 1] = right_sibling_internal_node->children[0];
		for (int i = 1; i <= right_sibling_internal_node->n; ++i) {
			right_sibling_internal_node->children[i - 1] = right_sibling_internal_node->children[i];
		}
		right_sibling_internal_node->children[right_sibling_internal_node->n] = BlobStore::InvalidIndex;
	} else {
		auto child_leaf_node = child_node.To<LeafNode>();
		auto right_sibling_leaf_node = right_sibling.To<LeafNode>();
		child_leaf_node->values[child_leaf_node->n] = right_sibling_leaf_node->values[0];
		for (int i = 1; i < right_sibling_leaf_node->n; ++i) {
			right_sibling_leaf_node->values[i - 1] = right_sibling_leaf_node->values[i];
		}
		right_sibling_leaf_node->values[right_sibling_leaf_node->n - 1] = BlobStore::InvalidIndex;
	}

	for (int i = 1; i < right_sibling->n; ++i) {
		right_sibling->keys[i - 1] = right_sibling->keys[i];
	}
	right_sibling->keys[right_sibling->n - 1] = BlobStore::InvalidIndex;

	child_node->n += 1;
	right_sibling->n -= 1;

	return true;
}

template <typename KeyType, typename ValueType, size_t Order>
KeyValuePair<KeyType, ValueType> BPlusTree<KeyType, ValueType, Order>::Remove(BlobStoreObject<InternalNode> parent_node, int child_index, const KeyType& key) {
	BlobStoreObject<BaseNode> child_node = GetChild(parent_node, child_index);
	if (child_node->n == (Order - 1) / 2) {
		if (!BorrowFromLeftSibling(parent_node, child_index) && !BorrowFromRightSibling(parent_node, child_index)) {
			if (child_index < parent_node->n) {
				MergeChildren(parent_node, child_index);
			} else {
				child_node = GetChild(parent_node, child_index - 1); 
				MergeChildren(parent_node, child_index - 1);
			}
		}
	}
	if (child_node->type == NodeType::LEAF) {
		return RemoveFromLeafNode(child_node.To<LeafNode>(), key);
	}

	auto internal_node = child_node.To<InternalNode>();
	int i = 0;

	KeyType* current_key = nullptr;
	while (i < internal_node->n) {
		current_key = &*GetKey(internal_node.To<BaseNode>(), i);
		if (key <= *current_key) {
			break;
		}
		i += 1;
	}
	if (i < internal_node->n && key == *current_key) {
		auto kv = Remove(internal_node, i + 1, key);
		KeyType* current_key = nullptr;
		while (i < internal_node->n) {
			current_key = &*GetKey(internal_node.To<BaseNode>(), i);
			if (key <= *current_key) {
				break;
			}
			i += 1;
		}
		if (i < internal_node->n && key == *current_key) {
			// Can there ever be a null successor? That means there is no successor at all.
			// That shouldn't happen I think.
			auto key_ptr = GetSuccessorKey(internal_node.To<BaseNode>(), key);
			internal_node->keys[i] = key_ptr.Index();
		}
		return kv;
	}
	return Remove(internal_node, i, key);
}

template <typename KeyType, typename ValueType, size_t Order>
BlobStoreObject<KeyType> BPlusTree<KeyType, ValueType, Order>::GetPredecessorKey(BlobStoreObject<BaseNode> node) {
	while (node->type != NodeType::LEAF) {
		auto internal_node = node.To<InternalNode>();
		node = GetChild(internal_node, internal_node->n); // Get the rightmost child
	}
	return GetKey(node, node->n - 1); // Return the rightmost key in the leaf node
}

template <typename KeyType, typename ValueType, size_t Order>
BlobStoreObject<KeyType> BPlusTree<KeyType, ValueType, Order>::GetSuccessorKey(BlobStoreObject<BaseNode> node, const KeyType& key) {
	if (node->type == NodeType::LEAF) {
		for (int i = 0; i < node->n; ++i) {
			auto key_ptr = GetKey(node, i);
			if (*key_ptr > key)
				return key_ptr;
		}
		return BlobStoreObject<KeyType>(&blob_store_, BlobStore::InvalidIndex);

	}
	for (int i = 0; i <= node->n; ++i) {
		auto internal_node = node.To<InternalNode>();
		auto child = GetChild(internal_node, i); // Get the leftmost child
		auto key_ptr = GetSuccessorKey(child, key);
		if (key_ptr != nullptr)
			return key_ptr;
	}
	// We should never get here unless the tree has a problem.
	return BlobStoreObject<KeyType>(&blob_store_, BlobStore::InvalidIndex);

}

template <typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::MergeChildren(BlobStoreObject<InternalNode> parent_node, int key_index_in_parent) {
	BlobStoreObject<BaseNode> left_child = GetChild(parent_node, key_index_in_parent);
	BlobStoreObject<BaseNode> right_child = GetChild(parent_node, key_index_in_parent + 1);

	if (left_child->type == NodeType::LEAF) {
		// Merge leaf nodes
		auto left_node = left_child.To<LeafNode>();
		auto right_node = right_child.To<LeafNode>();

		// Move the key from the parent node down to the left sibling node
		left_node->keys[left_node->n] = right_node->keys[0];///parent_node->keys[key_index_in_parent];
		left_node->values[left_node->n] = right_node->values[0];
		++left_node->n;

		// Move all keys and values from the right sibling node to the left sibling node
		for (size_t i = 1; i < right_node->n; ++i) {
			left_node->keys[left_node->n] = right_node->keys[i];
			left_node->values[left_node->n] = right_node->values[i];
			++left_node->n;
		}
		left_node->next = right_node->next;
	} else {
		// Merge internal nodes
		auto left_node = left_child.To<InternalNode>();
		auto right_node = right_child.To<InternalNode>();

		// Move the key from the parent node down to the left sibling node
		left_node->keys[left_node->n] = parent_node->keys[key_index_in_parent];
		++left_node->n;

		// Move all keys and child pointers from the right sibling node to the left sibling node
		for (size_t i = 0; i < right_node->n; ++i) {
			left_node->keys[left_node->n] = right_node->keys[i];
			left_node->children[left_node->n] = right_node->children[i];

			// Update the parent of the moved children
			++left_node->n;
		}
		left_node->children[left_node->n] = right_node->children[right_node->n];
	}

	// Update the parent node by removing the key that was moved down and the pointer to the right sibling node
	for (int i = key_index_in_parent; i < parent_node->n - 1; ++i) {
		parent_node->keys[i] = parent_node->keys[i + 1];
		parent_node->children[i + 1] = parent_node->children[i + 2];
	}
	--parent_node->n;

	// Free the right sibling node
	blob_store_.Drop(right_child.Index());

	if (parent_node.To<BaseNode>() == root_ && parent_node->n == 0) {
		// The root node is empty, so make the left child the new root node
		root_ = left_child;
		blob_store_.Drop(parent_node.Index());
		parent_node = nullptr;
	}
}

#endif // B_PLUS_TREE_H_

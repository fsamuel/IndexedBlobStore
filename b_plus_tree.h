#ifndef B_PLUS_TREE_H_
#define B_PLUS_TREE_H_

#include <assert.h>
#include <algorithm>
#include <array>
#include "blob_store.h"

class BaseNode;

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

		bool isFull() const { return n == Order - 1; }

		size_t GetNumKeys() const {
			size_t count = 0;
			for (int i = 0; i < Order - 1; ++i) {
				if (keys[i] != BlobStore::InvalidIndex) {
					++count;
				}
			}
			return count;
		}
	};

	struct InternalNode : BaseNode {
		std::array<BlobStore::index_type, Order> children;

		InternalNode(BlobStore::index_type first_child, std::size_t n = 1)
			: BaseNode(NodeType::INTERNAL, n) {
			children[0] = first_child;
			for (size_t i = 1; i < Order; ++i) {
				children[i] = BlobStore::InvalidIndex;
			}
		}

		size_t GetNumChildren() const {
			size_t count = 0;
			for (int i = 0; i < Order; ++i) {
				if (children[i] != BlobStore::InvalidIndex) {
					++count;
				}
			}
			return count;
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

	BPlusTree(BlobStore& blob_store) : blob_store_(blob_store) {
		if (blob_store_.IsEmpty()) {
			CreateRoot();
		}
		else {
			root_ = BlobStoreObject<BaseNode>(&blob_store_, blob_store_.begin().index());
		}
	}

	ValueType* Search(const KeyType& key);

	void Insert(const KeyType& key, const ValueType& value);
	bool Remove(const KeyType& key);

private:
	BlobStore& blob_store_;
	BlobStoreObject<BaseNode> root_;

	void CreateRoot() {
		root_ = blob_store_.Put<LeafNode>(sizeof(LeafNode), BlobStore::InvalidIndex).To<BaseNode>();
	}

	ValueType* Search(BlobStoreObject<BaseNode> node, const KeyType& key);
	void SplitChild(BlobStoreObject<InternalNode> parentNode, size_t childIndex);
	void InsertNonFull(BlobStoreObject<BaseNode> node, const KeyType& key, const ValueType& value);

	bool Remove(BlobStoreObject<BaseNode> node, const KeyType & key);
	BlobStoreObject<KeyType> GetPredecessorKey(BlobStoreObject<BaseNode> node);
	BlobStoreObject<KeyType> GetSuccessorKey(BlobStoreObject<BaseNode> node);
	void MergeChildren(BlobStoreObject<InternalNode> parentNode, int index);
};

template<typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::Insert(const KeyType& key, const ValueType& value) {
	if (root_->n == Order - 1) {
		// Root is full, create a new root
		BlobStoreObject<InternalNode> new_root = blob_store_.Put<InternalNode>(sizeof(InternalNode), BlobStore::InvalidIndex);
		new_root->children[0] = root_.Index();
		new_root->n = 0;
		SplitChild(new_root, 0);
		InsertNonFull(new_root.To<BaseNode>(), key, value);
		root_ = new_root.To<BaseNode>();
	}
	else {
		InsertNonFull(root_, key, value);
	}
}

template <typename KeyType, typename ValueType, size_t Order>
ValueType* BPlusTree<KeyType, ValueType, Order>::Search(const KeyType& key) {
	if (root_ == nullptr) {
		return nullptr;
	}
	return Search(root_, key);
}

template <typename KeyType, typename ValueType, size_t Order>
ValueType* BPlusTree<KeyType, ValueType, Order>::Search(BlobStoreObject<BaseNode> node, const KeyType& key) {
	if (node->type == NodeType::LEAF) {
		auto leaf_node = node.To<LeafNode>();
		for (int i = 0; i < leaf_node->n; i++) {
			KeyType* current_key = reinterpret_cast<KeyType*>(blob_store_[leaf_node->keys[i]]);
			if (key == *current_key) {
				return reinterpret_cast<ValueType*>(blob_store_[leaf_node->values[i]]);
			}
		}
		return nullptr;
	}
	else {
		auto internal_node = node.To<InternalNode>();
		int i = 0;

		KeyType* current_key = nullptr;
		while (i < internal_node->n) {
			current_key = reinterpret_cast<KeyType*>(blob_store_[internal_node->keys[i]]);
			if (key <= *current_key) {
				break;
			}
			i += 1;
		}
		if (i < internal_node->n && key == *current_key) {
			return Search(BlobStoreObject<BaseNode>(&blob_store_, internal_node->children[i + 1]), key);
		}
		return Search(BlobStoreObject<BaseNode>(&blob_store_, internal_node->children[i]), key);
	}
}

template<typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::SplitChild(BlobStoreObject<InternalNode> parent_node, size_t child_index) {
	BlobStoreObject<BaseNode> child_node(&blob_store_, parent_node->children[child_index]);
	NodeType new_node_type = child_node->type;
	BlobStoreObject<BaseNode> new_node = (
		new_node_type == NodeType::INTERNAL ?
		blob_store_.Put<InternalNode>(sizeof(InternalNode), Order).To<BaseNode>() :
		blob_store_.Put<LeafNode>(sizeof(LeafNode), Order).To<BaseNode>());

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
	}
	else {
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
void BPlusTree<KeyType, ValueType, Order>::InsertNonFull(BlobStoreObject<BaseNode> node, const KeyType& key, const ValueType& value) {
	if (node->type == NodeType::LEAF) {
		BlobStoreObject<LeafNode> leaf_node = node.To<LeafNode>();
		// Shift the keys and values right.
		int i = leaf_node->n - 1;
		while (i >= 0 && key < *blob_store_[leaf_node->keys[i]]) {
			leaf_node->keys[i + 1] = leaf_node->keys[i];
			leaf_node->values[i + 1] = leaf_node->values[i];
			--i;
		}
		BlobStoreObject<KeyType> key_ptr = blob_store_.Put<KeyType>(sizeof(KeyType), key);
		BlobStoreObject<ValueType> value_ptr = blob_store_.Put<ValueType>(sizeof(ValueType), value);
		leaf_node->keys[i + 1] = key_ptr.Index();
		leaf_node->values[i + 1] = value_ptr.Index();
		leaf_node->n += 1;
	}
	else {
		BlobStoreObject<InternalNode> internal_node = node.To<InternalNode>();
		int i = internal_node->n - 1;
		while (i >= 0) {
			KeyType* current_key = reinterpret_cast<KeyType*>(blob_store_[internal_node->keys[i]]);
			if (key >= *current_key) {
				break;
			}
			--i;
		}
		i += 1;
		BlobStoreObject<BaseNode> child_node = BlobStoreObject<BaseNode>(&blob_store_, internal_node->children[i]);
		if (child_node->n == Order - 1) {
			SplitChild(internal_node, i);
			if (key > *blob_store_[internal_node->keys[i]]) {
				i += 1;
			}
		}
		InsertNonFull(BlobStoreObject<BaseNode>(&blob_store_, internal_node->children[i]), key, value);
	}
}

template <typename KeyType, typename ValueType, size_t Order>
bool BPlusTree<KeyType, ValueType, Order>::Remove(const KeyType& key) {
	if (root_ == nullptr) {
		return false;
	}
	return Remove(root_, key);
}

template <typename KeyType, typename ValueType, size_t Order>
bool BPlusTree<KeyType, ValueType, Order>::Remove(BlobStoreObject<BaseNode> node, const KeyType& key) {
	if (node->type == NodeType::LEAF) {
		BlobStoreObject<LeafNode> leaf_node = node.To<LeafNode>();
		int i = 0;
		while (i < leaf_node->n && key != *blob_store_[leaf_node->keys[i]]) {
			i += 1;
		}

		if (i == leaf_node->n) {
			return false; // Key not found
		}

		// Shift keys and values to fill the gap
		for (int j = i + 1; j < leaf_node->n; j++) {
			leaf_node->keys[j - 1] = leaf_node->keys[j];
			leaf_node->values[j - 1] = leaf_node->values[j];
		}
		leaf_node->n -= 1;

		return true; // Key successfully removed
	}
	else {
		BlobStoreObject<InternalNode> internal_node = node.To<InternalNode>();
		int i = 0;
		while (i < internal_node->n && key > *blob_store_[internal_node->keys[i]]) {
			i += 1;
		}
		// key <= *blob_store_[internal_node->keys[i]]
		if (i < internal_node->n && key == *blob_store_[internal_node->keys[i]]) {
			// Key found in the internal node
			BlobStoreObject<BaseNode> left_child = BlobStoreObject<BaseNode>(&blob_store_, internal_node->children[i]);
			BlobStoreObject<BaseNode> right_child = BlobStoreObject<BaseNode>(&blob_store_, internal_node->children[i + 1]);

			if (left_child->n >= Order) {
				// Case 1: left child has at least Order keys
				auto predecessor_key_node = GetPredecessorKey(left_child);
				internal_node->keys[i] = predecessor_key_node.Index();
				return Remove(left_child, *predecessor_key_node);
			}
			else if (right_child->n >= Order) {
				// Case 2: right child has at least Order keys
				auto successor_key_node = GetSuccessorKey(right_child);
				internal_node->keys[i] = successor_key_node.Index();
				return Remove(right_child, *successor_key_node);
			}
			else {
				// Case 3: both children have Order - 1 keys
				MergeChildren(internal_node, i);
				return Remove(left_child, key);
			}
		}

		return Remove(BlobStoreObject<BaseNode>(&blob_store_, internal_node->children[i]), key);
	}
}

template <typename KeyType, typename ValueType, size_t Order>
BlobStoreObject<KeyType> BPlusTree<KeyType, ValueType, Order>::GetPredecessorKey(BlobStoreObject<BaseNode> node) {
	while (node->type != NodeType::LEAF) {
		auto internal_node = node.To<InternalNode>();
		node = BlobStoreObject<BaseNode>(&blob_store_, internal_node->children[internal_node->n]); // Get the rightmost child
	}
	auto leaf_node = node.To<LeafNode>();
	return BlobStoreObject<KeyType>(&blob_store_, leaf_node->keys[leaf_node->n - 1]); // Return the rightmost key in the leaf node
}

template <typename KeyType, typename ValueType, size_t Order>
BlobStoreObject<KeyType> BPlusTree<KeyType, ValueType, Order>::GetSuccessorKey(BlobStoreObject<BaseNode> node) {
	while (node->type != NodeType::LEAF) {
		auto internal_node = node.To<InternalNode>();
		node = BlobStoreObject<BaseNode>(&blob_store_, internal_node->children[0]); // Get the leftmost child
	}
	auto leaf_node = node.To<LeafNode>();
	return BlobStoreObject<KeyType>(&blob_store_, leaf_node->keys[0]); // Return the leftmost key in the leaf node
}

template <typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::MergeChildren(BlobStoreObject<InternalNode> parent_node, int index) {
	BlobStoreObject<BaseNode> left_child(&blob_store_, parent_node->children[index]);
	BlobStoreObject<BaseNode> right_child(&blob_store_, parent_node->children[index + 1]);

	// Find the index of the key in the parent node that will be moved down.
	size_t key_index_in_parent = 0;
	for (size_t i = 0; i < parent_node->keys.size(); ++i) {
		if (parent_node->children[i + 1] == right_child.Index()) {
			key_index_in_parent = i;
			break;
		}
	}

	if (left_child->type == NodeType::LEAF) {
		// Merge leaf nodes
		auto left_node = left_child.To<LeafNode>();
		auto right_node = right_child.To<LeafNode>();

		// Move the key from the parent node down to the left sibling node
		left_node->keys[left_node->n] = parent_node->keys[key_index_in_parent];
		left_node->values[left_node->n] = right_node->values.front();
		++left_node->n;

		// Move all keys and values from the right sibling node to the left sibling node
		for (size_t i = 0; i < right_node->n; ++i) {
			left_node->keys[left_node->n] = right_node->keys[i];
			left_node->values[left_node->n] = right_node->values[i + 1];
			++left_node->n;
		}
	}
	else {
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
	parent_node->keys[key_index_in_parent] = parent_node->keys.back();
	parent_node->children[key_index_in_parent + 1] = parent_node->children.back();
	--parent_node->n;

	/*
	// If the parent node is now underflowing and is not the root node, call fixUnderflow
	if (parent_node->n < Order / 2 && parent_node->parent != BlobStore::InvalidIndex) {
		fixUnderflow(parentNodeIndex);
	}*/

	// Free the right sibling node
	blob_store_.Drop(right_child.Index());
}

#endif // B_PLUS_TREE_H_

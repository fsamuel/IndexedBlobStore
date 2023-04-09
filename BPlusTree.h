#ifndef B_PLUS_TREE_H
#define B_PLUS_TREE_H

#include <assert.h>
#include <algorithm>
#include <array>
#include "BlobStore.h"

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

		InternalNode(BlobStore::index_type firstChild, std::size_t n = 1)
			: BaseNode(NodeType::INTERNAL, n) {
			children[0] = firstChild;
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

	BPlusTree(BlobStore& blobStore) : m_blobStore(blobStore) {
		if (m_blobStore.IsEmpty()) {
			createRoot();
		}
		else {
			m_root = BlobStoreObject<BaseNode>(&m_blobStore, m_blobStore.begin().index());
		}
	}

	ValueType* search(const KeyType& key);

	void insert(const KeyType& key, const ValueType& value);
	bool remove(const KeyType& key);

private:
	BlobStore& m_blobStore;
	BlobStoreObject<BaseNode> m_root;

	void createRoot() {
		m_root = m_blobStore.Put<LeafNode>(sizeof(LeafNode), BlobStore::InvalidIndex).To<BaseNode>();
	}

	ValueType* search(BlobStoreObject<BaseNode> node, const KeyType& key);
	void splitChild(BlobStoreObject<InternalNode> parentNode, size_t childIndex);
	void insertNonFull(BlobStoreObject<BaseNode> node, const KeyType& key, const ValueType& value);

	bool remove(BlobStoreObject<BaseNode> node, const KeyType & key);
	BlobStoreObject<KeyType> getPredecessorKey(BlobStoreObject<BaseNode> node);
	BlobStoreObject<KeyType> getSuccessorKey(BlobStoreObject<BaseNode> node);
	void mergeChildren(BlobStoreObject<InternalNode> parentNode, int index);
};

template<typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::insert(const KeyType& key, const ValueType& value) {
	if (m_root->n == Order - 1) {
		// Root is full, create a new root
		BlobStoreObject<InternalNode> newRoot = m_blobStore.Put<InternalNode>(sizeof(InternalNode), BlobStore::InvalidIndex);
		newRoot->children[0] = m_root.Index();
		newRoot->n = 0;
		splitChild(newRoot, 0);
		insertNonFull(newRoot.To<BaseNode>(), key, value);
		m_root = newRoot.To<BaseNode>();
	}
	else {
		insertNonFull(m_root, key, value);
	}
}

template <typename KeyType, typename ValueType, size_t Order>
ValueType* BPlusTree<KeyType, ValueType, Order>::search(const KeyType& key) {
	if (m_root == nullptr) {
		return nullptr;
	}
	return search(m_root, key);
}

template <typename KeyType, typename ValueType, size_t Order>
ValueType* BPlusTree<KeyType, ValueType, Order>::search(BlobStoreObject<BaseNode> node, const KeyType& key) {
	if (node->type == NodeType::LEAF) {
		auto leafNode = node.To<LeafNode>();
		for (int i = 0; i < leafNode->n; i++) {
			KeyType* current_key = reinterpret_cast<KeyType*>(m_blobStore[leafNode->keys[i]]);
			if (key == *current_key) {
				return reinterpret_cast<ValueType*>(m_blobStore[leafNode->values[i]]);
			}
		}
		return nullptr;
	}
	else {
		auto internalNode = node.To<InternalNode>();
		int i = 0;

		KeyType* current_key = nullptr;
		while (i < internalNode->n) {
			current_key = reinterpret_cast<KeyType*>(m_blobStore[internalNode->keys[i]]);
			if (key <= *current_key) {
				break;
			}
			i += 1;
		}
		if (i < internalNode->n && key == *current_key) {
			return search(BlobStoreObject<BaseNode>(&m_blobStore, internalNode->children[i + 1]), key);
		}
		return search(BlobStoreObject<BaseNode>(&m_blobStore, internalNode->children[i]), key);
	}
}

template<typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::splitChild(BlobStoreObject<InternalNode> parentNode, size_t childIndex) {
	BlobStoreObject<BaseNode> childNode(&m_blobStore, parentNode->children[childIndex]);
	NodeType newNodeType = childNode->type;
	BlobStoreObject<BaseNode> newNode = (
		newNodeType == NodeType::INTERNAL ?
		m_blobStore.Put<InternalNode>(sizeof(InternalNode), Order).To<BaseNode>() :
		m_blobStore.Put<LeafNode>(sizeof(LeafNode), Order).To<BaseNode>());

	int middleKeyIndex = (childNode->n - 1) / 2;
	size_t middleKey = childNode->keys[middleKeyIndex];

	if (newNodeType == NodeType::INTERNAL) {
		newNode->n = childNode->n - middleKeyIndex - 1;
		for (int i = 0; i < newNode->n; ++i) {
			newNode->keys[i] = childNode->keys[middleKeyIndex + i + 1];
			childNode->keys[middleKeyIndex + i + 1] = BlobStore::InvalidIndex;
		}

		auto newInternalNode = newNode.To<InternalNode>();
		auto childInternalNode = childNode.To<InternalNode>();
		for (int i = 0; i <= newNode->n; ++i) {
			newInternalNode->children[i] = childInternalNode->children[middleKeyIndex + i + 1];
			childInternalNode->children[middleKeyIndex + i + 1] = BlobStore::InvalidIndex;
		}
	}
	else {
		newNode->n = childNode->n - middleKeyIndex;
		for (int i = 0; i < newNode->n; ++i) {
			newNode->keys[i] = childNode->keys[middleKeyIndex + i];
			childNode->keys[middleKeyIndex + i] = BlobStore::InvalidIndex;
		}
		auto newLeafNode = newNode.To<LeafNode>();
		auto childLeafNode = childNode.To<LeafNode>();
		for (int i = 0; i < newNode->n; ++i) {
			newLeafNode->values[i] = childLeafNode->values[middleKeyIndex + i];
			childLeafNode->values[middleKeyIndex + i] = BlobStore::InvalidIndex;
		}
		newLeafNode->next = childLeafNode->next;
		childLeafNode->next = newNode.Index();
		childLeafNode->values[middleKeyIndex] = BlobStore::InvalidIndex;
	}

	childNode->n = middleKeyIndex;
	childNode->keys[middleKeyIndex] = BlobStore::InvalidIndex;

	for (int i = parentNode->n; i >= static_cast<int>(childIndex) + 1; --i) {
		parentNode->children[i + 1] = parentNode->children[i];
	}
	parentNode->children[childIndex + 1] = newNode.Index();

	for (int i = parentNode->n - 1; i >= static_cast<int>(childIndex); --i) {
		parentNode->keys[i + 1] = parentNode->keys[i];
	}
	parentNode->keys[childIndex] = middleKey;

	parentNode->n += 1;
}

template<typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::insertNonFull(BlobStoreObject<BaseNode> node, const KeyType& key, const ValueType& value) {
	if (node->type == NodeType::LEAF) {
		BlobStoreObject<LeafNode> leafNode = node.To<LeafNode>();
		// Shift the keys and values right.
		int i = leafNode->n - 1;
		while (i >= 0 && key < *m_blobStore[leafNode->keys[i]]) {
			leafNode->keys[i + 1] = leafNode->keys[i];
			leafNode->values[i + 1] = leafNode->values[i];
			--i;
		}
		BlobStoreObject<KeyType> keyPtr = m_blobStore.Put<KeyType>(sizeof(KeyType), key);
		BlobStoreObject<ValueType> valuePtr = m_blobStore.Put<ValueType>(sizeof(ValueType), value);
		leafNode->keys[i + 1] = keyPtr.Index();
		leafNode->values[i + 1] = valuePtr.Index();
		leafNode->n += 1;
	}
	else {
		BlobStoreObject<InternalNode> internalNode = node.To<InternalNode>();
		int i = internalNode->n - 1;
		while (i >= 0) {
			KeyType* current_key = reinterpret_cast<KeyType*>(m_blobStore[internalNode->keys[i]]);
			if (key >= *current_key) {
				break;
			}
			--i;
		}
		i += 1;
		BlobStoreObject<BaseNode> childNode = BlobStoreObject<BaseNode>(&m_blobStore, internalNode->children[i]);
		if (childNode->n == Order - 1) {
			splitChild(internalNode, i);
			if (key > *m_blobStore[internalNode->keys[i]]) {
				i += 1;
			}
		}
		insertNonFull(BlobStoreObject<BaseNode>(&m_blobStore, internalNode->children[i]), key, value);
	}
}

template <typename KeyType, typename ValueType, size_t Order>
bool BPlusTree<KeyType, ValueType, Order>::remove(const KeyType& key) {
	if (m_root == nullptr) {
		return false;
	}
	return remove(m_root, key);
}

template <typename KeyType, typename ValueType, size_t Order>
bool BPlusTree<KeyType, ValueType, Order>::remove(BlobStoreObject<BaseNode> node, const KeyType& key) {
	if (node->type == NodeType::LEAF) {
		BlobStoreObject<LeafNode> leafNode = node.To<LeafNode>();
		int i = 0;
		while (i < leafNode->n && key != *m_blobStore[leafNode->keys[i]]) {
			i += 1;
		}

		if (i == leafNode->n) {
			return false; // Key not found
		}

		// Shift keys and values to fill the gap
		for (int j = i + 1; j < leafNode->n; j++) {
			leafNode->keys[j - 1] = leafNode->keys[j];
			leafNode->values[j - 1] = leafNode->values[j];
		}
		leafNode->n -= 1;

		return true; // Key successfully removed
	}
	else {
		BlobStoreObject<InternalNode> internalNode = node.To<InternalNode>();
		int i = 0;
		while (i < internalNode->n && key > *m_blobStore[internalNode->keys[i]]) {
			i += 1;
		}
		// key <= *m_blobStore[internalNode->keys[i]]
		if (i < internalNode->n && key == *m_blobStore[internalNode->keys[i]]) {
			// Key found in the internal node
			BlobStoreObject<BaseNode> leftChild = BlobStoreObject<BaseNode>(&m_blobStore, internalNode->children[i]);
			BlobStoreObject<BaseNode> rightChild = BlobStoreObject<BaseNode>(&m_blobStore, internalNode->children[i + 1]);

			if (leftChild->n >= Order) {
				// Case 1: left child has at least Order keys
				auto predecessorKeyNode = getPredecessorKey(leftChild);
				internalNode->keys[i] = predecessorKeyNode.Index();
				return remove(leftChild, *predecessorKeyNode);
			}
			else if (rightChild->n >= Order) {
				// Case 2: right child has at least Order keys
				auto successorKeyNode = getSuccessorKey(rightChild);
				internalNode->keys[i] = successorKeyNode.Index();
				return remove(rightChild, *successorKeyNode);
			}
			else {
				// Case 3: both children have Order - 1 keys
				mergeChildren(internalNode, i);
				return remove(leftChild, key);
			}
		}

		return remove(BlobStoreObject<BaseNode>(&m_blobStore, internalNode->children[i]), key);
	}
}

template <typename KeyType, typename ValueType, size_t Order>
BlobStoreObject<KeyType> BPlusTree<KeyType, ValueType, Order>::getPredecessorKey(BlobStoreObject<BaseNode> node) {
	while (node->type != NodeType::LEAF) {
		auto internalNode = node.To<InternalNode>();
		node = BlobStoreObject<BaseNode>(&m_blobStore, internalNode->children[internalNode->n]); // Get the rightmost child
	}
	auto leafNode = node.To<LeafNode>();
	return BlobStoreObject<KeyType>(&m_blobStore, leafNode->keys[leafNode->n - 1]); // Return the rightmost key in the leaf node
}

template <typename KeyType, typename ValueType, size_t Order>
BlobStoreObject<KeyType> BPlusTree<KeyType, ValueType, Order>::getSuccessorKey(BlobStoreObject<BaseNode> node) {
	while (node->type != NodeType::LEAF) {
		auto internalNode = node.To<InternalNode>();
		node = BlobStoreObject<BaseNode>(&m_blobStore, internalNode->children[0]); // Get the leftmost child
	}
	auto leafNode = node.To<LeafNode>();
	return BlobStoreObject<KeyType>(&m_blobStore, leafNode->keys[0]); // Return the leftmost key in the leaf node
}

template <typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::mergeChildren(BlobStoreObject<InternalNode> parentNode, int index) {
	BlobStoreObject<BaseNode> leftChild(&m_blobStore, parentNode->children[index]);
	BlobStoreObject<BaseNode> rightChild(&m_blobStore, parentNode->children[index + 1]);

	// Find the index of the key in the parent node that will be moved down.
	size_t keyIndexInParent = 0;
	for (size_t i = 0; i < parentNode->keys.size(); ++i) {
		if (parentNode->children[i + 1] == rightChild.Index()) {
			keyIndexInParent = i;
			break;
		}
	}

	if (leftChild->type == NodeType::LEAF) {
		// Merge leaf nodes
		auto leftNode = leftChild.To<LeafNode>();
		auto rightNode = rightChild.To<LeafNode>();

		// Move the key from the parent node down to the left sibling node
		leftNode->keys[leftNode->n] = parentNode->keys[keyIndexInParent];
		leftNode->values[leftNode->n] = rightNode->values.front();
		++leftNode->n;

		// Move all keys and values from the right sibling node to the left sibling node
		for (size_t i = 0; i < rightNode->n; ++i) {
			leftNode->keys[leftNode->n] = rightNode->keys[i];
			leftNode->values[leftNode->n] = rightNode->values[i + 1];
			++leftNode->n;
		}

	}
	else {
		// Merge internal nodes
		auto leftNode = leftChild.To<InternalNode>();
		auto rightNode = rightChild.To<InternalNode>();

		// Move the key from the parent node down to the left sibling node
		leftNode->keys[leftNode->n] = parentNode->keys[keyIndexInParent];
		++leftNode->n;

		// Move all keys and child pointers from the right sibling node to the left sibling node
		for (size_t i = 0; i < rightNode->n; ++i) {
			leftNode->keys[leftNode->n] = rightNode->keys[i];
			leftNode->children[leftNode->n] = rightNode->children[i];

			// Update the parent of the moved children
			++leftNode->n;
		}
		leftNode->children[leftNode->n] = rightNode->children[rightNode->n];
	}

	// Update the parent node by removing the key that was moved down and the pointer to the right sibling node
	parentNode->keys[keyIndexInParent] = parentNode->keys.back();
	parentNode->children[keyIndexInParent + 1] = parentNode->children.back();
	--parentNode->n;

	/*
	// If the parent node is now underflowing and is not the root node, call fixUnderflow
	if (parentNode->n < Order / 2 && parentNode->parent != BlobStore::InvalidIndex) {
		fixUnderflow(parentNodeIndex);
	}*/

	// Free the right sibling node
	m_blobStore.Drop(rightChild.Index());
}

#endif // B_PLUS_TREE_H

#ifndef B_PLUS_TREE_H
#define B_PLUS_TREE_H

#include <assert.h>
#include <algorithm>
#include <array>
#include "BlobStore.h"

template <typename KeyType, typename ValueType, std::size_t Order>
class BPlusTree {
public:
	using offset_type = std::ptrdiff_t;

	BPlusTree(BlobStore& blobStore) : m_blobStore(blobStore) {
		if (m_blobStore.IsEmpty()) {
			createRoot();
		}
		else {
			m_rootIndex = m_blobStore.begin().index();
		}
	}

	ValueType* search(const KeyType& key);
	ValueType* search(size_t nodeIndex, const KeyType& key);

	void insert(const KeyType& key, const ValueType& value);
	//void remove(const KeyType& key);

private:
	enum class NodeType : uint8_t { INTERNAL, LEAF };

	struct BaseNode {
		NodeType type;
		std::size_t n;
		std::array<BlobStore::index_type, Order -1> keys;

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

	BlobStore& m_blobStore;
	BlobStore::index_type m_rootIndex;

	void createRoot() {
		m_rootIndex = m_blobStore.Put<LeafNode>(sizeof(LeafNode), BlobStore::InvalidIndex);
	}
	void splitChild(size_t nodeIndex, size_t childIndex);
	void insertNonFull(size_t nodeIndex, const KeyType& key, const ValueType& value);


	BaseNode* getNode(size_t index);

	// Other functions

};

template<typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::insert(const KeyType& key, const ValueType& value) {
	BaseNode* root = getNode(m_rootIndex);
	if (root->n == Order - 1) {
		// Root is full, create a new root
		size_t newRootIndex = m_blobStore.Put<InternalNode>(sizeof(InternalNode), BlobStore::InvalidIndex);
		InternalNode* newRoot = reinterpret_cast<InternalNode*>(m_blobStore[newRootIndex]);
		newRoot->children[0] = m_rootIndex;
		newRoot->n = 0;
		splitChild(newRootIndex, 0);
		newRoot = reinterpret_cast<InternalNode*>(m_blobStore[newRootIndex]);
		root = getNode(m_rootIndex);
		insertNonFull(newRootIndex, key, value);
		m_rootIndex = newRootIndex;
	}
	else {
		insertNonFull(m_rootIndex, key, value);
	}
}

template <typename KeyType, typename ValueType, size_t Order>
ValueType* BPlusTree<KeyType, ValueType, Order>::search(const KeyType& key) {
	if (m_rootIndex == BlobStore::InvalidIndex) {
		return nullptr;
	}
	return search(m_rootIndex, key);
}

template <typename KeyType, typename ValueType, size_t Order>
ValueType* BPlusTree<KeyType, ValueType, Order>::search(size_t nodeIndex, const KeyType& key) {
	BaseNode* node = getNode(nodeIndex);
	if (node->type == NodeType::LEAF) {
		LeafNode* leafNode = static_cast<LeafNode*>(node);
		for (int i = 0; i < leafNode->n; i++) {
			KeyType* current_key = reinterpret_cast<KeyType*>(m_blobStore[leafNode->keys[i]]);
			if (key == *current_key) {
				return reinterpret_cast<ValueType*>(m_blobStore[leafNode->values[i]]);
			}
		}
		return nullptr;
	}
	else {
		InternalNode* internalNode = static_cast<InternalNode*>(node);
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
			return search(internalNode->children[i + 1], key);
		}
		return search(internalNode->children[i], key);
	}
}

template<typename KeyType, typename ValueType, size_t Order>
typename BPlusTree<KeyType, ValueType, Order>::BaseNode* BPlusTree<KeyType, ValueType, Order>::getNode(size_t index) {
	BaseNode* nodePtr = reinterpret_cast<BaseNode*>(m_blobStore[index]);

	if (nodePtr->type != NodeType::INTERNAL && nodePtr->type != NodeType::LEAF) {
		throw std::runtime_error("Invalid node type");
	}

	return nodePtr;
}

template<typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::splitChild(size_t nodeIndex, size_t childIndex) {
	size_t newNodeIndex = 0;
	NodeType newNodeType = NodeType::INTERNAL;
	size_t childNodeIndex = 0;
	// Putting a new node in the BlobStore can invalidate pointers to nodes, so we need to get the pointers again.
	{
		InternalNode* parentNode = reinterpret_cast<InternalNode*>(getNode(nodeIndex));
		childNodeIndex = parentNode->children[childIndex];
		BaseNode* childNode = getNode(childNodeIndex);
		newNodeType = childNode->type;
		newNodeIndex = newNodeType == NodeType::INTERNAL ? m_blobStore.Put<InternalNode>(sizeof(InternalNode), Order) : m_blobStore.Put<LeafNode>(sizeof(LeafNode), Order);
	}
	BaseNode* newNode = getNode(newNodeIndex);
	BaseNode* childNode = getNode(childNodeIndex);
	InternalNode* parentNode = static_cast<InternalNode*>(getNode(nodeIndex));

	int middleKeyIndex = (childNode->n - 1) / 2;
	size_t middleKey = childNode->keys[middleKeyIndex];



	if (newNodeType == NodeType::INTERNAL) {
		newNode->n = childNode->n - middleKeyIndex - 1;
		for (int i = 0; i < newNode->n; ++i) {
			newNode->keys[i] = childNode->keys[middleKeyIndex + i + 1];
			childNode->keys[middleKeyIndex + i + 1] = BlobStore::InvalidIndex;
		}

		InternalNode* newInternalNode = static_cast<InternalNode*>(newNode);
		InternalNode* childInternalNode = static_cast<InternalNode*>(childNode);
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
		LeafNode* newLeafNode = static_cast<LeafNode*>(newNode);
		LeafNode* childLeafNode = static_cast<LeafNode*>(childNode);
		for (int i = 0; i < newNode->n; ++i) {
			newLeafNode->values[i] = childLeafNode->values[middleKeyIndex + i];
			childLeafNode->values[middleKeyIndex + i] = BlobStore::InvalidIndex;
		}
		newLeafNode->next = childLeafNode->next;
		childLeafNode->next = newNodeIndex;
		childLeafNode->values[middleKeyIndex] = BlobStore::InvalidIndex;
	}

	childNode->n = middleKeyIndex;
	childNode->keys[middleKeyIndex] = BlobStore::InvalidIndex;

	for (int i = parentNode->n; i >= static_cast<int>(childIndex) + 1; --i) {
		parentNode->children[i + 1] = parentNode->children[i];
	}
	parentNode->children[childIndex + 1] = newNodeIndex;

	for (int i = parentNode->n - 1; i >= static_cast<int>(childIndex); --i) {
		parentNode->keys[i + 1] = parentNode->keys[i];
	}
	parentNode->keys[childIndex] = middleKey;

	parentNode->n += 1;
}

template<typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::insertNonFull(size_t nodeIndex, const KeyType& key, const ValueType& value) {
	BaseNode* node = getNode(nodeIndex);

	if (node->type == NodeType::LEAF) {
		LeafNode* leafNode = static_cast<LeafNode*>(node);
		// Shift the keys and values right.
		int i = leafNode->n - 1;
		while (i >= 0 && key < *m_blobStore[leafNode->keys[i]]) {
			leafNode->keys[i + 1] = leafNode->keys[i];
			leafNode->values[i + 1] = leafNode->values[i];
			--i;
		}
		size_t key_index = m_blobStore.Put<KeyType>(sizeof(KeyType), key);
		size_t value_index = m_blobStore.Put<ValueType>(sizeof(ValueType), value);
		// We might need to resize buffers on allocation which might invalidate pointers.
		leafNode = static_cast<LeafNode*>(getNode(nodeIndex));
		leafNode->keys[i + 1] = key_index;
		leafNode->values[i + 1] = value_index;
		leafNode->n += 1;
	}
	else {
		InternalNode* internalNode = static_cast<InternalNode*>(node);
		int i = internalNode->n - 1;
		while (i >= 0) {
			KeyType* current_key = reinterpret_cast<KeyType*>(m_blobStore[internalNode->keys[i]]);
			if (key >= *current_key) {
				break;
			}
			--i;
		}
		i += 1;
		BaseNode* childNode = getNode(internalNode->children[i]);
		if (childNode->n == Order - 1) {
			// splitChild can allocate nodes which can invalidate pointers.
			splitChild(nodeIndex, i);
			internalNode = static_cast<InternalNode*>(getNode(nodeIndex));
			if (key > *m_blobStore[internalNode->keys[i]]) {
				i += 1;
			}
		}
		insertNonFull(internalNode->children[i], key, value);
	}
}
/*
template <typename KeyType, typename ValueType, size_t Order>
bool BPlusTree<KeyType, ValueType, Order>::remove(const KeyType& key) {
	if (m_rootIndex == BlobStore::InvalidIndex) {
		return false;
	}
	return remove(m_rootIndex, key);
}

template <typename KeyType, typename ValueType, size_t Order>
bool BPlusTree<KeyType, ValueType, Order>::remove(size_t nodeIndex, const KeyType& key) {
	BaseNode* node = getNode(nodeIndex);
	if (node->type == NodeType::Leaf) {
		LeafNode* leafNode = static_cast<LeafNode*>(node);
		int i = 0;
		while (i < leafNode->n && key != leafNode->keys[i]) {
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
		m_blobStore.Update(nodeIndex, leafNode); // Update the node in the BlobStore

		return true; // Key successfully removed
	}
	else {
		InternalNode* internalNode = static_cast<InternalNode*>(node);
		int i = 0;
		while (i < internalNode->n && key > internalNode->keys[i]) {
			i += 1;
		}

		if (i < internalNode->n && key == internalNode->keys[i]) {
			// Key found in the internal node
			size_t leftChildIndex = internalNode->children[i];
			size_t rightChildIndex = internalNode->children[i + 1];
			BaseNode* leftChild = getNode(leftChildIndex);
			BaseNode* rightChild = getNode(rightChildIndex);

			if (leftChild->n >= Order) {
				// Case 1: left child has at least Order keys
				KeyType predecessorKey = getPredecessorKey(leftChildIndex);
				internalNode->keys[i] = predecessorKey;
				m_blobStore.Update(nodeIndex, internalNode); // Update the node in the BlobStore
				return remove(leftChildIndex, predecessorKey);
			}
			else if (rightChild->n >= Order) {
				// Case 2: right child has at least Order keys
				KeyType successorKey = getSuccessorKey(rightChildIndex);
				internalNode->keys[i] = successorKey;
				m_blobStore.Update(nodeIndex, internalNode); // Update the node in the BlobStore
				return remove(rightChildIndex, successorKey);
			}
			else {
				// Case 3: both children have Order - 1 keys
				mergeChildren(internalNode, i);
				m_blobStore.Update(nodeIndex, internalNode); // Update the node in the BlobStore
				return remove(leftChildIndex, key);
			}
		}

		return remove(internalNode->children[i], key);
	}
}

template <typename KeyType, typename ValueType, size_t Order>
KeyType BPlusTree<KeyType, ValueType, Order>::getPredecessorKey(size_t nodeIndex) {
	BaseNode* node = getNode(nodeIndex);
	while (node->type != NodeType::Leaf) {
		InternalNode* internalNode = static_cast<InternalNode*>(node);
		nodeIndex = internalNode->children[internalNode->n]; // Get the rightmost child
		node = getNode(nodeIndex);
	}
	LeafNode* leafNode = static_cast<LeafNode*>(node);
	return leafNode->keys[leafNode->n - 1]; // Return the rightmost key in the leaf node
}

template <typename KeyType, typename ValueType, size_t Order>
KeyType BPlusTree<KeyType, ValueType, Order>::getSuccessorKey(size_t nodeIndex) {
	BaseNode* node = getNode(nodeIndex);
	while (node->type != NodeType::Leaf) {
		InternalNode* internalNode = static_cast<InternalNode*>(node);
		nodeIndex = internalNode->children[0]; // Get the leftmost child
		node = getNode(nodeIndex);
	}
	LeafNode* leafNode = static_cast<LeafNode*>(node);
	return leafNode->keys[0]; // Return the leftmost key in the leaf node
}

template <typename KeyType, typename ValueType, size_t Order>
void BPlusTree<KeyType, ValueType, Order>::mergeChildren(InternalNode* parentNode, int index) {
	size_t leftChildIndex = parentNode->children[index];
	size_t rightChildIndex = parentNode->children[index + 1];
	InternalNode* leftChild = static_cast<InternalNode*>(getNode(leftChildIndex));
	InternalNode* rightChild = static_cast<InternalNode*>(getNode(rightChildIndex));
	// Move the key from the parent node to the left child
	leftChild->keys[leftChild->n] = parentNode->keys[index];
	leftChild->n += 1;

	// Move keys and children from the right child to the left child
	std::copy(rightChild->keys, rightChild->keys + rightChild->n, leftChild->keys + leftChild->n);
	std::copy(rightChild->children, rightChild->children + rightChild->n + 1, leftChild->children + leftChild->n);
	leftChild->n += rightChild->n;

	// Update the parent node
	std::copy(parentNode->keys + index + 1, parentNode->keys + parentNode->n, parentNode->keys + index);
	std::copy(parentNode->children + index + 2, parentNode->children + parentNode->n + 1, parentNode->children + index + 1);
	parentNode->n -= 1;

	// Update the nodes in the BlobStore
	m_blobStore.Update(leftChildIndex, leftChild);
	m_blobStore.Update(rightChildIndex, rightChild);
	m_blobStore.Delete(rightChildIndex);
}
*/
#endif // B_PLUS_TREE_H

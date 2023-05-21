#ifndef NODES_H_
#define NODES_H_

#include <array>
#include <cstddef>
#include <cstdint>

#include "blob_store.h"
#include "fixed_string.h"
#include "storage_traits.h"

enum class NodeType : uint8_t { HEAD, INTERNAL, LEAF };

struct Node {
	// The type of node this is: head, internal or leaf.
	NodeType type;
	// The version of this node.
	std::size_t version;
	
	Node(): type(NodeType::HEAD), version(0) {}

	Node(NodeType type, std::size_t version) : type(type), version(version) {}

	// Returns whether this node is a head node.
	bool is_head() const { return type == NodeType::HEAD; }

	// Returns whether this node is a leaf node.
	bool is_leaf() const { return type == NodeType::LEAF; }

	// Returns whether this node is an internal node.
	bool is_internal() const { return type == NodeType::INTERNAL; }

	// Returns the version of the node.
	size_t get_version() const {
		return version;
	}

	// Sets the version of the node.
	void set_version(size_t new_version) {
		version = new_version;
	}
};

static_assert(std::is_trivially_copyable<Node>::value, "Node is trivially copyable");
static_assert(std::is_standard_layout<Node>::value, "Node is standard layout");

struct HeadNode {
	Node node;
	// The index of the root node.
	BlobStore::index_type root_index;
	// The index of the previous head.
	BlobStore::index_type previous;

	HeadNode(std::size_t version) : node(NodeType::HEAD, version), previous(BlobStore::InvalidIndex) {}

	HeadNode() : node(NodeType::HEAD, 0), previous(BlobStore::InvalidIndex) {}

	bool is_head() const { return node.is_head(); }
	bool is_leaf() const { return node.is_leaf(); }
	bool is_internal() const { return node.is_internal(); }
	size_t get_version() const { return node.get_version(); }
	void set_version(size_t new_version) { node.set_version(new_version); }
};

static_assert(std::is_trivially_copyable<HeadNode>::value, "HeadNode is trivially copyable");
static_assert(std::is_standard_layout<HeadNode>::value, "HeadNode is standard layout");


template<std::size_t Order = 4>
struct BaseNode {
	Node node;
	// The number of keys in the node.
	std::size_t n;
	// The keys in the node.
	std::array<BlobStore::index_type, Order - 1> keys;

	BaseNode(NodeType type, std::size_t n) : node(type, 0), n(n) {
		// Initialize all keys to invalid index.
		for (size_t i = 0; i < Order - 1; ++i) {
			keys[i] = BlobStore::InvalidIndex;
		}
	}

	bool is_head() const { return node.is_head(); }
	bool is_leaf() const { return node.is_leaf(); }
	bool is_internal() const { return node.is_internal(); }
	size_t get_version() const { return node.get_version(); }
	void set_version(size_t new_version) { node.set_version(new_version); }

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

	// Sets the number of keys in the node.
	void set_num_keys(size_t num_keys) { n = num_keys; }

	// Returns the key at the given index.
	size_t get_key(size_t index) const { return keys[index]; }

	// Sets the key at the given index.
	void set_key(size_t index, size_t key) { keys[index] = key; }

	// Returns the first key in the node that is greater than or equal to the given key and its index in the node.
	template<typename KeyType>
	BlobStoreObject<const KeyType> Search(BlobStore* store, const KeyType& key, size_t* index) const {
		auto it = std::lower_bound(keys.begin(), keys.begin() + num_keys(), key,
			[store](size_t lhs, const KeyType& rhs) {
				return *store->Get<KeyType>(lhs) < rhs;
			});

		*index = std::distance(keys.begin(), it);
		if (*index < num_keys()) {
			return store->Get<KeyType>(*it);
		}
		return BlobStoreObject<const KeyType>();
	}
};

static_assert(std::is_trivially_copyable<BaseNode<>>::value, "BaseNode is trivially copyable");
static_assert(std::is_standard_layout<BaseNode<>>::value, "BaseNode is standard layout");

template<std::size_t Order = 4>
struct InternalNode {
	BaseNode<Order> base;
	std::array<BlobStore::index_type, Order> children;
	explicit InternalNode(std::size_t n)
		: base(NodeType::INTERNAL, n) {
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
	size_t get_version() const { return base.get_version(); }
	void set_version(size_t new_version) { base.set_version(new_version); }
	void set_num_keys(size_t num_keys) { base.set_num_keys(num_keys); }
	size_t get_key(size_t index) const { return base.get_key(index); }
	void set_key(size_t index, size_t key) { base.set_key(index, key); }

	template<typename KeyType>
	BlobStoreObject<const KeyType> Search(BlobStore* store, const KeyType& key, size_t* index) const {
		return base.Search(store, key, index);
	}
};

static_assert(std::is_trivially_copyable<InternalNode<>>::value, "InternalNode is trivially copyable");
static_assert(std::is_standard_layout<InternalNode<>>::value, "InternalNode is standard layout");

template<std::size_t Order = 4>
struct LeafNode {
	BaseNode<Order> base;
	std::array<BlobStore::index_type, Order - 1> values;

	LeafNode(std::size_t num_keys = 0)
		: base(NodeType::LEAF, num_keys) {
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
	size_t get_version() const { return base.get_version(); }
	void set_version(size_t new_version) { base.set_version(new_version); }
	void set_num_keys(size_t num_keys) { base.set_num_keys(num_keys); }
	size_t get_key(size_t index) const { return base.get_key(index); }
	void set_key(size_t index, size_t key) { base.set_key(index, key); }

	template<typename KeyType>
	BlobStoreObject<const KeyType> Search(BlobStore* store, const KeyType& key, size_t* index) const {
		return base.Search(store, key, index);
	}
};

static_assert(std::is_trivially_copyable<LeafNode<>>::value, "LeafNode is trivially copyable");
static_assert(std::is_standard_layout<LeafNode<>>::value, "LeafNode is standard layout");

#endif // NODES_H_
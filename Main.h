#include <iostream>
#include <vector>
#include <algorithm>
#include "SharedMemoryBuffer.h"
#include "SharedMemoryAllocator.h"
#include "SharedMemoryVector.h"


class BPTreeNode {
public:
    virtual bool is_leaf() const = 0;
};

class BPTreeInternalNode : public BPTreeNode {
public:
    std::vector<int> keys;
    std::vector<BPTreeNode*> children;

    BPTreeInternalNode() {}
    virtual bool is_leaf() const { return false; }
};

class BPTreeLeafNode : public BPTreeNode {
public:
    std::vector<int> keys;
    std::vector<int> values;
    BPTreeLeafNode* next;

    BPTreeLeafNode() : next(nullptr) {}
    virtual bool is_leaf() const { return true; }
};

class BPlusTree {
private:
    BPTreeNode* root = nullptr;
    int order;

    // Helper functions
    void insert_into_leaf(BPTreeLeafNode* leaf, int key, int value);
    void insert_into_internal(BPTreeInternalNode* internal, int key, BPTreeNode* child);
    BPTreeInternalNode* split_internal(BPTreeInternalNode* internal);
    BPTreeLeafNode* split_leaf(BPTreeLeafNode* leaf);


public:
    class iterator {
    public:
        typedef iterator self_type;
        typedef std::pair<int, int> value_type;
        typedef value_type& reference;
        typedef value_type* pointer;
        typedef std::forward_iterator_tag iterator_category;
        typedef int difference_type;

        iterator(BPTreeLeafNode* leaf = nullptr, size_t index = 0)
            : leaf_(leaf), index_(index) {
            update_current_pair();
        }

        self_type operator++() {
            self_type i = *this;
            advance();
            return i;
        }

        self_type operator++(int) {
            advance();
            return *this;
        }

        reference operator*() { return current_pair_; }
        pointer operator->() { return &current_pair_; }
        bool operator==(const self_type& rhs) { return leaf_ == rhs.leaf_ && index_ == rhs.index_; }
        bool operator!=(const self_type& rhs) { return !(*this == rhs); }

    private:
        void advance() {
            if (leaf_) {
                ++index_;
                if (index_ >= leaf_->keys.size()) {
                    leaf_ = leaf_->next;
                    index_ = 0;
                }
                update_current_pair();
            }
        }

        void update_current_pair() {
            if (leaf_ && index_ < leaf_->keys.size()) {
                current_pair_ = std::make_pair(leaf_->keys[index_], leaf_->values[index_]);
            }
            else {
                current_pair_ = std::make_pair(0, 0);
            }
        }

        BPTreeLeafNode* leaf_;
        size_t index_;
        value_type current_pair_;
    };


    BPlusTree(int order = 4) : root(nullptr), order(order) {}
    void insert(int key, int value);
    int find(int key);
    iterator begin() const {
        BPTreeNode* node = root;

        // Traverse to the leftmost leaf node.
        while (node && !node->is_leaf()) {
            BPTreeInternalNode* internal = static_cast<BPTreeInternalNode*>(node);
            node = internal->children[0];
        }

        return iterator(static_cast<BPTreeLeafNode*>(node), 0);
    }

    iterator end() const {
        return iterator(nullptr, 0);
    }


};

void BPlusTree::insert(int key, int value) {
    if (!root) {
        root = new BPTreeLeafNode();
    }

    BPTreeNode* node = root;
    BPTreeInternalNode* parent = nullptr;

    // Traverse the tree to find the leaf node where the key should be inserted.
    while (!node->is_leaf()) {
        parent = static_cast<BPTreeInternalNode*>(node);
        auto it = std::upper_bound(parent->keys.begin(), parent->keys.end(), key);
        int index = static_cast<int>(it - parent->keys.begin());
        node = parent->children[index];
    }

    BPTreeLeafNode* leaf = static_cast<BPTreeLeafNode*>(node);
    insert_into_leaf(leaf, key, value);

    // If the leaf node has been split, handle insertion into the parent internal node.
    if (leaf->keys.size() > order) {
        BPTreeLeafNode* new_leaf = split_leaf(leaf);

        if (!parent) {
            // Create a new root if there is no parent.
            parent = new BPTreeInternalNode();
            root = parent;
        }

        insert_into_internal(parent, new_leaf->keys[0], new_leaf);
    }
}

int BPlusTree::find(int key) {
    if (!root) {
        return -1;
    }

    BPTreeNode* node = root;
    while (!node->is_leaf()) {
        BPTreeInternalNode* internal = static_cast<BPTreeInternalNode*>(node);
        auto it = std::upper_bound(internal->keys.begin(), internal->keys.end(), key);
        int index = static_cast<int>(it - internal->keys.begin());
        node = internal->children[index];
    }

    BPTreeLeafNode* leaf = static_cast<BPTreeLeafNode*>(node);
    auto it = std::find(leaf->keys.begin(), leaf->keys.end(), key);
    if (it != leaf->keys.end()) {
        int index = static_cast<int>(it - leaf->keys.begin());
        return leaf->values[index];
    }

    return -1;
}

void BPlusTree::insert_into_leaf(BPTreeLeafNode* leaf, int key, int value) {
    auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
    int index = static_cast<int>(it - leaf->keys.begin());
    leaf->keys.insert(it, key);
    leaf->values.insert(leaf->values.begin() + index, value);

    if (leaf->keys.size() > order) {
        BPTreeLeafNode* new_leaf = split_leaf(leaf);
        if (leaf == root) {
            BPTreeInternalNode* new_root = new BPTreeInternalNode();
            new_root->keys.push_back(new_leaf->keys[0]);
            new_root->children.push_back(leaf);
            new_root->children.push_back(new_leaf);
            root = new_root;
        }
        else {
            insert_into_internal(static_cast<BPTreeInternalNode*>(root), new_leaf->keys[0], new_leaf);
        }
    }
}

void BPlusTree::insert_into_internal(BPTreeInternalNode* internal, int key, BPTreeNode* child) {
    auto it = std::upper_bound(internal->keys.begin(), internal->keys.end(), key);
    int index = static_cast<int>(it - internal->keys.begin());
    internal->keys.insert(it, key);
    internal->children.insert(internal->children.begin() + index + 1, child);

    if (internal->keys.size() > order) {
        BPTreeInternalNode* new_internal = split_internal(internal);
        int split_key = internal->keys.back();
        internal->keys.pop_back();

        if (internal == root) {
            BPTreeInternalNode* new_root = new BPTreeInternalNode();
            new_root->keys.push_back(split_key);
            new_root->children.push_back(internal);
            new_root->children.push_back(new_internal);
            root = new_root;
        }
        else {
            insert_into_internal(static_cast<BPTreeInternalNode*>(root), split_key, new_internal);
        }
    }
}

BPTreeLeafNode* BPlusTree::split_leaf(BPTreeLeafNode* leaf) {
    int mid = (order + 1) / 2;
    BPTreeLeafNode* new_leaf = new BPTreeLeafNode();

    new_leaf->keys.assign(leaf->keys.begin() + mid, leaf->keys.end());
    new_leaf->values.assign(leaf->values.begin() + mid, leaf->values.end());
    new_leaf->next = leaf->next;
    leaf->next = new_leaf;

    leaf->keys.erase(leaf->keys.begin() + mid, leaf->keys.end());
    leaf->values.erase(leaf->values.begin() + mid, leaf->values.end());

    return new_leaf;
}

BPTreeInternalNode* BPlusTree::split_internal(BPTreeInternalNode* internal) {
    int mid = (order + 1) / 2;
    BPTreeInternalNode* new_internal = new BPTreeInternalNode();

    new_internal->keys.assign(internal->keys.begin() + mid + 1, internal->keys.end());
    new_internal->children.assign(internal->children.begin() + mid + 1, internal->children.end());

    internal->keys.erase(internal->keys.begin() + mid, internal->keys.end());
    internal->children.erase(internal->children.begin() + mid + 1, internal->children.end());

    return new_internal;
}

// Include the B+ Tree implementation code here.

int main() {
    BPlusTree bptree(4);

    // Insert key-value pairs into the B+ Tree.
    bptree.insert(1, 10);
    bptree.insert(2, 20);
    bptree.insert(3, 30);
    bptree.insert(4, 40);
    bptree.insert(5, 50);
    bptree.insert(6, 60);
    bptree.insert(7, 70);
    bptree.insert(8, 80);
    bptree.insert(9, 90);
    bptree.insert(10, 100);

    // Search for keys in the B+ Tree.
    int search_key = 5;
    int value = bptree.find(search_key);
    if (value != -1) {
        std::cout << "Value associated with key " << search_key << ": " << value << std::endl;
    }
    else {
        std::cout << "Key " << search_key << " not found in the B+ Tree." << std::endl;
    }

    search_key = 11;
    value = bptree.find(search_key);
    if (value != -1) {
        std::cout << "Value associated with key " << search_key << ": " << value << std::endl;
    }
    else {
        std::cout << "Key " << search_key << " not found in the B+ Tree." << std::endl;
    }

    // Iterate through the B+ Tree using a range-based for loop.
    for (const auto& kv : bptree) {
        std::cout << "Key: " << kv.first << ", Value: " << kv.second << std::endl;
    }

    try {
        SharedMemoryBuffer buffer("C:\\Users\\fadys\\Documents\\SharedMemoryTest");
        std::cout << "Shared memory opened successfully. Size: " << buffer.size() << std::endl;

       // buffer.resize(2048);
        std::cout << "Shared memory resized successfully. New size: " << buffer.size() << std::endl;

        // Create a SharedMemoryAllocator for allocating objects of type int in the buffer
        SharedMemoryAllocator<int> allocator(buffer);
        SharedMemoryVector<int> v(allocator);
        v.push_back(10);
        v.push_back(20);
        v.push_back(30);
        v.push_back(40);
        v.push_back(50);
        v.push_back(60);
        v.push_back(70);
        v.push_back(80);
        v.push_back(90);
        v.push_back(100);
        v.resize(5);
        
        // Allocate 10 ints in the shared memory buffer
        int* ptr1 = allocator.allocate(sizeof(int));
        int* ptr2 = allocator.allocate(sizeof(int));
        int* ptr3 = allocator.allocate(sizeof(int));
        int* ptr4 = allocator.allocate(sizeof(int));
        int* ptr5 = allocator.allocate(sizeof(int));
        *ptr1 = 1;
        *ptr2 = 2;
        *ptr3 = 3;
        *ptr4 = 4;
        *ptr5 = 5;
        
        for (auto i = 0; i < v.size(); ++i) {
            std::cout << "v[" << i << "] = " << v[i] << std::endl;
        }
        std::cout << "v[2] " << v[2] << " v[1] " << v[1] << " v[0] " << v[0] << std::endl;
        // Print the allocated ints
        for (auto it = allocator.begin(); it != allocator.end(); ++it) {
            std::cout << *it << std::endl;
        }

        // Deallocate the memory
        //allocator.deallocate(ptr1);
        ///allocator.deallocate(ptr2);
        //allocator.deallocate(ptr3);
        allocator.deallocate(ptr4);
        //allocator.deallocate(ptr5);

    }
    catch (const std::runtime_error& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    return 0;
}

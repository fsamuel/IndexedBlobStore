#include "b_plus_tree.h"
#include "gtest/gtest.h"

class BPlusTreeTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        std::remove("metadataBuffer");
        std::remove("dataBuffer");
        SharedMemoryBuffer metadataBuffer("metadataBuffer", 8192);
        SharedMemoryBuffer dataBuffer("dataBuffer", 8192);
        blob_store = new BlobStore(std::move(metadataBuffer), std::move(dataBuffer));
    }

    virtual void TearDown() {
        // cleanup the BlobStore
        delete blob_store;
    }

    BlobStore* blob_store;
};

TEST_F(BPlusTreeTest, BasicTree) {
	BPlusTree<int, int, 4> tree(*blob_store);
    for (int i = 0; i < 100; i++) {
		tree.Insert(i, i * 100);
	}
    for (int i = 0; i < 100; i++) {
        auto it = tree.Search(i);
		auto value_ptr = it.GetValue();
        int value = value_ptr == nullptr ? 0 : *value_ptr;
        EXPECT_NE(value_ptr, nullptr);
		EXPECT_EQ(value, i * 100);
	}
}

TEST_F(BPlusTreeTest, BasicTreeWithDelete) {
	BPlusTree<int, int, 4> tree(*blob_store);
    for (int i = 0; i < 100; i++) {
		tree.Insert(i, i * 100);
	}
    for (int i = 0; i < 100; i++) {
		auto it = tree.Search(i);
		auto value_ptr = it.GetValue();
		int value = value_ptr == nullptr ? 0 : *value_ptr;
		EXPECT_NE(value_ptr, nullptr);
		EXPECT_EQ(value, i * 100);
	}
    for (int i = 0; i < 100; i++) {
		tree.Delete(i);
		auto it = tree.Search(i);
		auto value_ptr = it.GetValue();
		EXPECT_EQ(value_ptr, nullptr);
	}
}

// Builds a B+ tree with 100 elements, randomly deletes some of them, and then
// checks that the remaining elements are still in the tree.
TEST_F(BPlusTreeTest, DeleteAndVerify) {
	BPlusTree<int, int, 4> tree(*blob_store);
    for (int i = 0; i < 100; i++) {
		tree.Insert(i, i * 100);
	}
	for (int i = 0; i < 100; i++) {
		auto it = tree.Search(i);
		auto value_ptr = it.GetValue();
		int value = value_ptr == nullptr ? 0 : *value_ptr;
		EXPECT_NE(value_ptr, nullptr);
		EXPECT_EQ(value, i * 100);
	}

	// Delete a random half the elements in random order and store 
	// the deleted keys in a set so we can check that they're not
	// in the tree later.
	std::set<int> deleted;
	for (int i = 0; i < 50; ++i) {
		int val = rand() % 100;
		while (deleted.count(val) > 0) {
			val = rand() % 100;
			KeyValuePair<int, int> kv = tree.Delete(i);
			std::cout << "Deleted " << i << " key: " << *kv.first << ", value: " << *kv.second << std::endl;
			deleted.insert(i);
		}
	}
	
	for (int i = 0; i < 100; i++) {
		auto it = tree.Search(i);
		auto value_ptr = it.GetValue();
		int value = value_ptr == nullptr ? 0 : *value_ptr;
		if (deleted.count(i) > 0) {
			EXPECT_EQ(value_ptr, nullptr);
		}
		else {
			EXPECT_NE(value_ptr, nullptr);
			EXPECT_EQ(value, i * 100);
		}
	}
}

// Populate a B+ tree with 100 elements. Search for an element in the middle
// of the tree, and iterate over the rest until the end.
TEST_F(BPlusTreeTest, BPlusTreeIteration) {
	BPlusTree<int, int, 4> tree(*blob_store);
	for (int i = 0; i < 100; i++) {
		tree.Insert(i, i * 100);
	}
	auto it = tree.Search(50);
	int last_key = 49;
	while (it.GetKey() != nullptr) {
		int key = *it.GetKey();
		int value = *it.GetValue();
		EXPECT_EQ(value, key * 100);
		EXPECT_GT(key, last_key);
		last_key = key;
		std::cout << "Key: " << *it.GetKey() << ", Value: " << *it.GetValue() << std::endl;
		++it;
	}
}

// Popualte a B+ tree with 100 elements in random order. Verify that the elements are
// in the tree. Delete the elements also in random order. Verify that the elements
// are no longer in the tree.
TEST_F(BPlusTreeTest, BPlusTreeInsertionDeletion) {
	BPlusTree<int, int, 4> tree(*blob_store);
	std::set<int> inserted;
	for (int i = 0; i < 100; i++) {
		int val = rand() % 100;
		while (inserted.count(val) > 0) {
			val = rand() % 100;
		}
		tree.Insert(val, val * 100);
		inserted.insert(val);
	}
	std::cout << std::endl << std::endl << "Tree Version 100" << std::endl;
	tree.PrintTree(100);
	std::cout << std::endl << std::endl << "Tree Version 50" << std::endl;
	tree.PrintTree(50);
	for (int i = 0; i < 100; i++) {
		auto it = tree.Search(i);
		auto value_ptr = it.GetValue();
		int value = value_ptr == nullptr ? 0 : *value_ptr;
		EXPECT_NE(value_ptr, nullptr);
		EXPECT_EQ(value, i * 100);
	}
	// Delete a random half the elements in random order and store 
	// the deleted keys in a set so we can check that they're not
	// in the tree later.
	std::set<int> deleted;
	for (int i = 0; i < 50; ++i) {
		int val = rand() % 100;
		while (deleted.count(val) > 0) {
			val = rand() % 100;
			KeyValuePair<int, int> kv = tree.Delete(i);
			std::cout << "Deleted " << i << " key: " << *kv.first << ", value: " << *kv.second << std::endl;
			deleted.insert(i);
		}
	}
	for (int i = 0; i < 100; i++) {
		auto it = tree.Search(i);
		auto value_ptr = it.GetValue();
		int value = value_ptr == nullptr ? 0 : *value_ptr;
		if (deleted.count(i) > 0) {
			EXPECT_EQ(value_ptr, nullptr);
		}
		else {
			EXPECT_NE(value_ptr, nullptr);
			EXPECT_EQ(value, i * 100);
		}
	}
}
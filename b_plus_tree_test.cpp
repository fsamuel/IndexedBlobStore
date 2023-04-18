#include "b_plus_tree.h"
#include "gtest/gtest.h"

class BPlusTreeTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        std::remove("metadataBuffer");
        std::remove("dataBuffer");
        SharedMemoryBuffer metadataBuffer("metadataBuffer", 2048);
        SharedMemoryBuffer dataBuffer("dataBuffer", 2048);
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
		tree.Remove(i);
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
			KeyValuePair<int, int> kv = tree.Remove(i);
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
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
        int* value_ptr = tree.Search(i);
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
		int* value_ptr = tree.Search(i);
		int value = value_ptr == nullptr ? 0 : *value_ptr;
		EXPECT_NE(value_ptr, nullptr);
		EXPECT_EQ(value, i * 100);
	}
    for (int i = 0; i < 100; i++) {
		tree.Remove(i);
		int* value_ptr = tree.Search(i);
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
		int* value_ptr = tree.Search(i);
		int value = value_ptr == nullptr ? 0 : *value_ptr;
		EXPECT_NE(value_ptr, nullptr);
		EXPECT_EQ(value, i * 100);
	}
	std::set<int> deleted;
	// Store the deleted elements in a set so we can check that they're not
	// in the tree later.
	
	for (int i = 0; i < 100; i++) {
		if (rand() % 2 == 0) {
			KeyValuePair<int, int> kv = tree.Remove(i);

			deleted.insert(i);
			std::cout << "Deleting " << i << " key: " << *kv.first << ", value: " << *kv.second << std::endl;
		}
	}
	
	for (int i = 0; i < 100; i++) {
		int* value_ptr = tree.Search(i);
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
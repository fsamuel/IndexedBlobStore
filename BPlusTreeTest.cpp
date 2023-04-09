#include "BPlusTree.h"
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
		tree.insert(i, i * 100);
	}
    for (int i = 0; i < 100; i++) {
        int* value_ptr = tree.search(i);
        int value = value_ptr == nullptr ? 0 : *value_ptr;
        EXPECT_NE(value_ptr, nullptr);
		EXPECT_EQ(value, i * 100);
	}
}

TEST_F(BPlusTreeTest, BasicTreeWithDelete) {
	BPlusTree<int, int, 4> tree(*blob_store);
    for (int i = 0; i < 100; i++) {
		tree.insert(i, i * 100);
	}
    for (int i = 0; i < 100; i++) {
		int* value_ptr = tree.search(i);
		int value = value_ptr == nullptr ? 0 : *value_ptr;
		EXPECT_NE(value_ptr, nullptr);
		EXPECT_EQ(value, i * 100);
	}
    for (int i = 0; i < 100; i++) {
		tree.remove(i);
		int* value_ptr = tree.search(i);
		EXPECT_EQ(value_ptr, nullptr);
	}
}
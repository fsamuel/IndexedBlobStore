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
		BlobStoreObject<const int> deleted;
		bool success = tree.Delete(i, &deleted);
		EXPECT_TRUE(success);
		EXPECT_EQ(*deleted, i*100);
		auto it = tree.Search(i);
		if (it.GetKey() != nullptr) {
			EXPECT_GT(*it.GetKey(), i);
		}
	}
}

// Builds a B+ tree with 100 elements, randomly deletes some of them, and then
// checks that the remaining elements are still in the tree.
TEST_F(BPlusTreeTest, DeleteAndVerify) {
	BPlusTree<int, int, 4> tree(*blob_store);
	std::set<int> inserted;
    for (int i = 0; i < 20; i++) {
		tree.Insert(i, i * 100);
		inserted.insert(i);
	}
	for (int i = 0; i < 10; i++) {
		auto it = tree.Search(i);
		auto value_ptr = it.GetValue();
		int value = value_ptr == nullptr ? 0 : *value_ptr;
		EXPECT_NE(value_ptr, nullptr);
		EXPECT_EQ(value, i * 100);
	}

	tree.PrintTree(20);
	// Delete a random half the elements in random order and store 
	// the deleted keys in a set so we can check that they're not
	// in the tree later.
	for (int i = 0; i < 10; ++i) {
		int val = rand() % 20;
		while (inserted.count(val) == 0) {
			val = rand() %20;
		}
		BlobStoreObject<const int> deleted;
		bool success = tree.Delete(val, &deleted);
		EXPECT_TRUE(success);
		EXPECT_EQ(*deleted, val*100);
		inserted.erase(val);
	}
	tree.PrintTree(21);
	for (int i = 0; i < 20; i++) {
		auto it = tree.Search(i);
		auto key_ptr = it.GetKey();
		int key = key_ptr == nullptr ? 0 : *key_ptr;
		if (inserted.count(i) == 0) {
			EXPECT_NE(key, i);
		}
		else {
			EXPECT_EQ(key, i);
			EXPECT_EQ(*it.GetValue(), i * 100);
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
		ASSERT_EQ(value, key * 100);
		ASSERT_EQ(key, last_key+1);
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
			BlobStoreObject<const int> deleted_value;
			bool success = tree.Delete(i, &deleted_value);
			EXPECT_TRUE(success);
			EXPECT_EQ(*deleted_value, i * 100);
			std::cout << "Deleted key: " << i << ", value: " << *deleted_value << std::endl;
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

// Verifies that if we attempt to delete a key that is not in the tree, the
// delete operation succeeds but will return a nullptr for the value.
TEST_F(BPlusTreeTest, DeleteNonExistentKey) {
	BPlusTree<int, int, 4> tree(*blob_store);
	for (int i = 0; i < 100; i++) {
		tree.Insert(i, i * 100);
	}
	BlobStoreObject<const int> deleted;
	bool success = tree.Delete(1000, &deleted);
	EXPECT_TRUE(success);
	EXPECT_EQ(deleted, nullptr);
}

// If a key isn't in the tree, the search operation should return an iterator
// with the first key greater than the key we're searching for.
TEST_F(BPlusTreeTest, SearchNonExistentKey) {
	BPlusTree<int, int, 4> tree(*blob_store);
	// Insert 100 elements with even keys.
	for (int i = 0; i < 100; i++) {
		tree.Insert(2 * i, i * 200);
	}
	auto it = tree.Search(1000);
	EXPECT_EQ(it.GetKey(), nullptr);
	EXPECT_EQ(it.GetValue(), nullptr);
	// Search for all the odd keys from 1 to 199. Verify that the iterator
	// returns the next even key.
	for (int i = 1; i < 200; i += 2) {
		auto it = tree.Search(i);
		if (it.GetKey() != nullptr) {
			int key = *it.GetKey();
			int value = *it.GetValue();
			EXPECT_EQ(key, i + 1);
			EXPECT_EQ(value, (i + 1) * 100);
		}
	}
}

// Inserts 50 elements into a B+ tree. Creates a transaction and inserts
// 50 more elements into the tree through the transaction. Verifies that
// all 100 elements are in the tree.
TEST_F(BPlusTreeTest, TransactionInsertion) {
	BPlusTree<int, int, 8> tree(*blob_store);
	for (int i = 0; i < 50; i++) {
		tree.Insert(i, i * 100);
	}
	auto txn = tree.CreateTransaction();
	for (int i = 50; i < 100; i++) {
		txn.Insert(i, i * 100);
	}
	tree.PrintTree(1000);
	// Search for 75 inside the transaction to find it, see it's the correct value and outside to
	// verify that it's not there.
	auto it = txn.Search(75);
	EXPECT_NE(it.GetKey(), nullptr);
	EXPECT_EQ(*it.GetKey(), 75);
	EXPECT_EQ(*it.GetValue(), 7500);
	it = tree.Search(75);
	EXPECT_EQ(it.GetKey(), nullptr);
	EXPECT_EQ(it.GetValue(), nullptr);
	// Commit the transaction and verify that all 100 elements are in the tree.
	std::move(txn).Commit();
	tree.PrintTree(1000);
	for (int i = 0; i < 100; i++) {
		auto it = tree.Search(i);
		auto value_ptr = it.GetValue();
		int value = value_ptr == nullptr ? 0 : *value_ptr;
		EXPECT_NE(value_ptr, nullptr);
		EXPECT_EQ(value, i * 100);
	}
	{
		auto txn = tree.CreateTransaction();
		for (int i = 0; i < 20; ++i) {
			auto value = txn.Delete(50 + i);
		}
		auto it = tree.Search(51);
		EXPECT_EQ(*it.GetKey(), 51);
		EXPECT_EQ(*it.GetValue(), 5100);
		it = txn.Search(51);
		EXPECT_EQ(*it.GetKey(), 70);
		EXPECT_EQ(*it.GetValue(), 7000);
		std::move(txn).Commit();
		tree.PrintTree(1000);
	}

}
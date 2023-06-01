#include "b_plus_tree.h"
#include "chunk_manager.h"
#include "gtest/gtest.h"
#include "utils.h"

class BPlusTreeTest : public ::testing::Test {
protected:
    virtual void SetUp() {
		RemoveChunkFiles();
        SharedMemoryBuffer metadataBuffer("MetadataBuffer", utils::GetPageSize());
        ChunkManager dataBuffer("DataBuffer", 4 * utils::GetPageSize());
        blob_store = new BlobStore(std::move(metadataBuffer), std::move(dataBuffer));
    }

    virtual void TearDown() {
        // cleanup the BlobStore
        delete blob_store;
		RemoveChunkFiles();
    }

	void RemoveChunkFiles() {
		// Delete all files with the prefix "test_chunk"
		// Do this in case the previous test failed and left some files behind
		for (int i = 0; i < 20; ++i) {
			std::string filename = "DataBuffer_" + std::to_string(i);
			std::remove(filename.c_str());
			filename = "MetadataBuffer_" + std::to_string(i);
			std::remove(filename.c_str());
		}
		std::remove("MetadataBuffer");
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

// Sanity check concurrent version of BasicTree.  Spawns 10 threads, each of which
// inserts 10 elements into the tree.  Then, verifies that all 100 elements are in the tree.
TEST_F(BPlusTreeTest, DISABLED_BasicTreeConcurrent) {
	BPlusTree<int, int, 4> tree(*blob_store);
	std::vector<std::thread> threads;
	for (int i = 0; i < 10; ++i) {
		threads.push_back(std::thread([&tree, i]() {
			for (int j = 0; j < 10; ++j) {
				tree.Insert(i * 10 + j, (i * 10 + j) * 100);
			}
		}));
	}
	for (auto& thread : threads) {
		thread.join();
	}
	for (int i = 0; i < 100; i++) {
		auto it = tree.Search(i);
		auto value_ptr = it.GetValue();
		int value = value_ptr == nullptr ? 0 : *value_ptr;
		EXPECT_NE(value_ptr, nullptr);
		EXPECT_EQ(value, i * 100);
	}
}


// Similar to BasicTree, but inserts all 100 elements in a transaction,
// and then verifies they exist outside the transaction.
TEST_F(BPlusTreeTest, BasicTreeWithTransaction) {
	BPlusTree<int, int, 4> tree(*blob_store);
	{
		auto txn(tree.CreateTransaction());
		for (int i = 0; i < 100; i++) {
			txn.Insert(i, i * 100);
		}
		std::move(txn).Commit();
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
	BPlusTree<int, int, 16> tree(*blob_store);
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
	BPlusTree<int, int, 16> tree(*blob_store);
	std::set<int> inserted;
    for (int i = 0; i < 100; i++) {
		tree.Insert(i, i * 100);
		inserted.insert(i);
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
	for (int i = 0; i < 50; ++i) {
		int val = rand() % 50;
		while (inserted.count(val) == 0) {
			val = rand() %50;
		}
		BlobStoreObject<const int> deleted;
		bool success = tree.Delete(val, &deleted);

		EXPECT_TRUE(success);
		EXPECT_EQ(*deleted, val*100);
		inserted.erase(val);
	}
	for (int i = 0; i < 100; i++) {
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
		++it;
	}
}

// Popualte a B+ tree with 100 elements in random order. Verify that the elements are
// in the tree. Delete the elements also in random order. Verify that the elements
// are no longer in the tree.
TEST_F(BPlusTreeTest, BPlusTreeInsertionDeletion) {
	BPlusTree<int, int, 8> tree(*blob_store);
	std::set<int> inserted;
	constexpr int kNumElements = 100;
	for (int i = 0; i < kNumElements; i++) {
		int val = rand() % kNumElements;
		while (inserted.count(val) > 0) {
			val = rand() % kNumElements;
		}
		tree.Insert(val, val * 100);
		inserted.insert(val);
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
	for (int i = 0; i < 2* kNumElements / 3; ++i) {
		int val = rand() % kNumElements;
		while (deleted.count(val) > 0) {
			val = rand() % kNumElements;
			BlobStoreObject<const int> deleted_value;
			bool success = tree.Delete(i, &deleted_value);
			EXPECT_TRUE(success);
			EXPECT_EQ(*deleted_value, i * 100);
			std::cout << "Deleted key: " << i << ", value: " << *deleted_value << std::endl;
			deleted.insert(i);
		}
	}
	for (int i = 0; i < kNumElements; i++) {
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

// Similar to SearchNonExistentKey but with strings.
TEST_F(BPlusTreeTest, SearchNonExistentStringKey) {
	BPlusTree<std::string, std::string, 4> tree(*blob_store);
	// Insert 100 elements with even keys.
	for (int i = 0; i < 100; i++) {
		std::string key = std::to_string(2 * i);
		std::string value = std::to_string(i * 200);
		tree.Insert(key, value);
	}
	auto it = tree.Search("1000");
	// Verify that the key doesn't exist by verifying that the key doesn't
	// match what we looked up.
	EXPECT_NE(*it.GetKey(), "1000");

	// Search or all the odd keys from 1 to 199. Verify that the iterator
	// doesn't return the key we searched for.
	for (int i = 1; i < 200; i += 2) {
		std::string key = std::to_string(i);
		auto it = tree.Search(key);
		if (it.GetKey() != nullptr) {
			std::string key = *it.GetKey();
			EXPECT_NE(key, std::to_string(i));
		}
	}
}

// Inserts 50 elements into a B+ tree. Creates a transaction and inserts
// 50 more elements into the tree through the transaction. Verifies that
// all 100 elements are in the tree.
TEST_F(BPlusTreeTest, TransactionInsertion) {
	BPlusTree<int, int, 32> tree(*blob_store);
	for (int i = 0; i < 50; i++) {
		tree.Insert(i, i * 100);
	}
	auto txn = tree.CreateTransaction();
	for (int i = 50; i < 100; i++) {
		txn.Insert(i, i * 100);
	}
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
		tree.Print();
	}
}

// Insert 50 elements into the B+ tree. Create a transaction, insert a few more through the transaction.
// Verify that the first 50  appear outside the transaction and all 55 appear inside the transaction.
// Abort the transaction and verify that the first 50 elements are still in the tree and the last 5 are not.
TEST_F(BPlusTreeTest, InsertTransactionAbort) {
	BPlusTree<int, int, 32> tree(*blob_store);
	for (int i = 0; i < 50; i++) {
		tree.Insert(i, i * 100);
	}
	auto txn = tree.CreateTransaction();
	for (int i = 50; i < 55; i++) {
		txn.Insert(i, i * 100);
	}
	// Verify that all 55 elements are in the tree in the transaction.
	for (int i = 0; i < 55; i++) {
		auto it = txn.Search(i);
		auto value_ptr = it.GetValue();
		int value = value_ptr == nullptr ? 0 : *value_ptr;
		EXPECT_NE(value_ptr, nullptr);
		EXPECT_EQ(value, i * 100);
	}
	// Verify that only the first 50 elements are in the tree outside the transaction.
	for (int i = 0; i < 55; i++) {
		auto it = tree.Search(i);
		auto value_ptr = it.GetValue();
		int value = value_ptr == nullptr ? 0 : *value_ptr;
		if (i < 50) {
			EXPECT_NE(value_ptr, nullptr);
			EXPECT_EQ(value, i * 100);
		}
		else {
			EXPECT_EQ(value_ptr, nullptr);
		}
	}
	// Abort the transaction and verify that the first 50 elements are still in
	// the tree and the last 5 are not.
	std::move(txn).Abort();
	for (int i = 0; i < 50; i++) {
		auto it = tree.Search(i);
		auto value_ptr = it.GetValue();
		int value = value_ptr == nullptr ? 0 : *value_ptr;
		EXPECT_NE(value_ptr, nullptr);
		EXPECT_EQ(value, i * 100);
	}
	for (int i = 50; i < 55; i++) {
		auto it = tree.Search(i);
		auto value_ptr = it.GetValue();
		EXPECT_EQ(value_ptr, nullptr);
	}
	// Insert some more elements into the tree and verify that they are there.
	for (int i = 50; i < 100; i++) {
		tree.Insert(i, i * 100);
	}
	// Verify that all 100 elements are in the tree.
	for (int i = 0; i < 100; i++) {
		auto it = tree.Search(i);
		auto value_ptr = it.GetValue();
		int value = value_ptr == nullptr ? 0 : *value_ptr;
		EXPECT_NE(value_ptr, nullptr);
		EXPECT_EQ(value, i * 100);
	}
}

// Inserts a bunch of strings as keys and ints as values into a B+ tree.
// Verifies that all the elements are in the tree.
TEST_F(BPlusTreeTest, StringInsertion) {
	BPlusTree<std::string, std::string, 32> tree(*blob_store);
	for (int i = 0; i < 100; i++) {
		tree.Insert("K" + std::to_string(i), "V" + std::to_string(i * 100));
	}
	for (int i = 0; i < 100; i++) {
		std::string key = "K" + std::to_string(i);
		std::string value = "V" + std::to_string(i * 100);
		auto it = tree.Search(key);
		auto value_ptr = it.GetValue();
		std::string value_output = value_ptr == nullptr ? std::string() : *value_ptr;
		EXPECT_NE(value_ptr, nullptr);
		EXPECT_EQ(value_output, value);
	}
	for (int i = 0; i < 100; i++) {
		BlobStoreObject<const std::string> deleted;
		std::string key = "K" + std::to_string(i);
		std::string value = "V" + std::to_string(i*100);

		bool success = tree.Delete(key, &deleted);
		EXPECT_TRUE(success);
		EXPECT_EQ(*deleted, value);
	}
}
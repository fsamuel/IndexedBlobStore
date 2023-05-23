#include "blob_store.h"
#include "gtest/gtest.h"

#include "fixed_string.h"
#include "nodes.h"

class BlobStoreTest : public ::testing::Test {
protected:
	virtual void SetUp() {
		std::remove("DataBuffer");
		std::remove("MetadataBuffer");
		// create a shared memory allocator
		dataBuffer = new SharedMemoryBuffer("DataBuffer", 4096);
		metadataBuffer = new SharedMemoryBuffer("MetadataBuffer", 4096);
	}

	virtual void TearDown() {
		delete dataBuffer;
		delete metadataBuffer;
	}

	SharedMemoryBuffer *dataBuffer = nullptr;
	SharedMemoryBuffer *metadataBuffer = nullptr;
};

TEST_F(BlobStoreTest, CreateEmptyBlobStore) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));
	EXPECT_EQ(store.GetSize(), 0);
}

TEST_F(BlobStoreTest, CreateBlobStoreWithTwoBlobs) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));
	BlobStoreObject<char> ptr1 = store.New<char>(100);
	strcpy(&*ptr1, "This is a test.");
	BlobStoreObject<char> ptr2 = store.New<char>(100);
	strcpy(&*ptr2, "Hello World!");
	EXPECT_EQ(store.GetSize(), 2);
}

// Creates two blobs with the templatized Put method, and then gets them back.
TEST_F(BlobStoreTest, CreateBlobStoreWithTwoBlobsUsingTemplatizedPut) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));
	BlobStoreObject<int> ptr1 = store.New<int>(100);
	BlobStoreObject<int> ptr2 = store.New<int>(1337);

	EXPECT_EQ(store.GetSize(), 2);
	EXPECT_EQ(*ptr1, 100);
	EXPECT_EQ(*ptr2, 1337);
}

// Creates a few blobs of ints, iterates, deletes a few then iterates again.
TEST_F(BlobStoreTest, BlobIteration) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));

	BlobStoreObject<const int> ptr1 = std::move(store.New<int>(100)).Downgrade();
	BlobStoreObject<const int> ptr2 = std::move(store.New<int>(200)).Downgrade();
	BlobStoreObject<const int> ptr3 = std::move(store.New<int>(300)).Downgrade();
	auto it = store.begin();
	EXPECT_EQ(it.index(), ptr1.Index());
	EXPECT_EQ(*it.Get<int>(), 100);
	EXPECT_EQ(it.size(), sizeof(int));
	++it;
	EXPECT_EQ(it.index(), ptr2.Index());
	EXPECT_EQ(*it.Get<int>(), 200);
	EXPECT_EQ(it.size(), sizeof(int));
	++it;
	EXPECT_EQ(it.index(), ptr3.Index());
	EXPECT_EQ(*it.Get<int>(), 300);
	EXPECT_EQ(it.size(), sizeof(int));
	store.Drop(ptr2.Index());

	it = store.begin();
	EXPECT_EQ(it.index(), ptr1.Index());
	EXPECT_EQ(*it.Get<int>(), 100);
	EXPECT_EQ(it.size(), sizeof(int));
	++it;

	EXPECT_EQ(it.index(), ptr3.Index());
	EXPECT_EQ(*it.Get<int>(), 300);
	EXPECT_EQ(it.size(), sizeof(int));
}

// Creates a couple of blobs, deletes one and insures that the BlobStoreObject is no longer valid.
TEST_F(BlobStoreTest, BlobStoreObjectInvalid) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));

	BlobStoreObject<const std::string> ptr1; 
	{
		BlobStoreObject<std::string> newPtr = store.New<std::string>("This is a test.");
		ptr1 = std::move(newPtr).Downgrade();
	}

	EXPECT_EQ(&*store.Get<std::string>(ptr1.Index()), &*ptr1);

	BlobStoreObject<const std::string> ptr2;
	{
		BlobStoreObject<std::string> newPtr = store.New<std::string>("Hello World!");
		ptr2 = std::move(newPtr).Downgrade();
	}

	EXPECT_EQ(&*store.Get<const std::string>(ptr2.Index()), &*ptr2);

	EXPECT_EQ(store.GetSize(), 2);

	store.Drop(std::move(ptr2));
	EXPECT_EQ(store.GetSize(), 1);

	EXPECT_EQ(&*store.Get<char[64]>(ptr1.Index()), &*ptr1);
	EXPECT_EQ(ptr1->size, 15);
	// Fixed-size arrays are treated as one element.
	EXPECT_EQ(ptr2, nullptr);
}

// Tests BlobStoreObject::Clone.
TEST_F(BlobStoreTest, BlobStoreObjectClone) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));
	// TODO(fsamuel): I should probably implement an array-based version of Clone.
	BlobStoreObject<int> ptr = store.New<int>(64);
	EXPECT_EQ(ptr.GetSize(), sizeof(int));
	EXPECT_EQ(*ptr, 64);
	BlobStoreObject<int> ptr2 = ptr.Clone();
	EXPECT_NE(ptr2.Index(), ptr.Index());
	EXPECT_EQ(*ptr, *ptr2);
	*ptr2 = 1337;
	EXPECT_EQ(*ptr2, 1337);
	EXPECT_NE(*ptr, *ptr2);
}

// Verify that if we have multiple non-const pointers to the same blob and we downgrade one of them,
// the newly downgraded pointer will be invalidated.
TEST_F(BlobStoreTest, BlobStoreObjectDowngradeInvalidates) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));
	BlobStoreObject<int> ptr = store.New<int>(64);
	EXPECT_EQ(ptr.GetSize(), sizeof(int));
	EXPECT_EQ(*ptr, 64);
	BlobStoreObject<int> ptr2 = ptr;
	EXPECT_EQ(ptr2.Index(), ptr.Index());
	EXPECT_EQ(*ptr, *ptr2);
	BlobStoreObject<const int> ptr3 = std::move(ptr2).Downgrade();
	EXPECT_EQ(ptr2, nullptr);
	EXPECT_EQ(ptr3, nullptr);
}

// Tests upgrading a const BlobStoreObject to non-const. Also tests that if we have multiple
// const pointers to the same blob and we upgrade one of them, the newly upgraded pointer will
// be invalidated.
TEST_F(BlobStoreTest, BlobStoreObjectUpgradeInvalidates) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));
	BlobStoreObject<const int> ptr = store.New<int>(64).Downgrade();
	EXPECT_EQ(ptr.GetSize(), sizeof(int));
	EXPECT_EQ(*ptr, 64);
	BlobStoreObject<const int> ptr2 = ptr;
	EXPECT_EQ(ptr2.Index(), ptr.Index());
	EXPECT_EQ(*ptr, *ptr2);
	BlobStoreObject<int> ptr3 = std::move(ptr2).Upgrade();
	EXPECT_EQ(ptr2, nullptr);
	EXPECT_EQ(ptr3, nullptr);
}

// Create a blob, get a BlobStoreObject from it, and then drop the blob. Try to get another
// BlobStoreObject from the index of the dropped blob. This should fail.
TEST_F(BlobStoreTest, BlobStoreObjectDrop) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));
	BlobStoreObject<int> ptr = store.New<int>(64);
	size_t index = ptr.Index();
	EXPECT_EQ(ptr.GetSize(), sizeof(int));
	EXPECT_EQ(*ptr, 64);
	store.Drop(std::move(ptr));
	EXPECT_EQ(ptr, nullptr);
	BlobStoreObject<const int> ptr2 = store.Get<int>(index);
	EXPECT_EQ(ptr2, nullptr);
}

// Create a blob, get a BlobStoreObject from it, store it in another BlobStoreObject. Drop the first
// BlobStoreObject. The second BlobStoreObject should no longer be valid. Try to get another
// BlobStoreObject from the index of the dropped blob. This should fail.
TEST_F(BlobStoreTest, BlobStoreObjectDrop2) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));
	BlobStoreObject<int> ptr = store.New<int>(64);
	size_t index = ptr.Index();
	EXPECT_EQ(ptr.GetSize(), sizeof(int));
	EXPECT_EQ(*ptr, 64);
	BlobStoreObject<int> ptr2 = ptr;
	EXPECT_EQ(ptr, ptr2);
	EXPECT_EQ(ptr2.GetSize(), sizeof(int));
	EXPECT_EQ(*ptr2, 64);
	store.Drop(std::move(ptr2));
	// Right now we don't invalidate the pointer, but we should.
	//EXPECT_EQ(ptr, nullptr);
	EXPECT_EQ(ptr2, nullptr);
	BlobStoreObject<const int> ptr3 = store.Get<int>(index);
	EXPECT_EQ(ptr3, nullptr);
}

// Create blobs using FixedString, try to access them using BlobStoreObject. Convert back
// to std::string or StringSlice and verify that the contents are the same.
TEST_F(BlobStoreTest, FixedString) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));
	BlobStoreObject<const std::string> ptr = std::move(store.New<std::string>("Hello, world!")).Downgrade();
	BlobStoreObject<const std::string> ptr2 = std::move(store.New<std::string>("New World")).Downgrade();
	// 13 bytes for the string, 8 bytes for the size, 8 bytes for the hash.
	EXPECT_EQ(ptr.GetSize(), 29);
	EXPECT_EQ(ptr->size, 13);
	StringSlice slice = *ptr;
	EXPECT_EQ(slice.size(), 13);
	std::string str = *ptr;
	EXPECT_EQ(str.size(), 13);
	std::cout << *ptr << std::endl;
	std::cout << *ptr2 << std::endl;
	EXPECT_NE(*ptr, *ptr2);
	auto  ptr3 = store.Get<std::string>(ptr.Index());
	EXPECT_EQ(*ptr, *ptr3);
	// As long as we're holding onto a BlobStoreObject, it's safe to hold onto
	// StringSlices of the same blob.
	slice = ptr->substring(0, 5);
	EXPECT_EQ(slice.size(), 5);
	std::cout << slice << std::endl;
	slice = ptr->substring(7, 5);
	EXPECT_EQ(slice.size(), 5);
	std::cout << slice << std::endl;
}

// Create a bunch of blobs of std::string. Create a blob of LeafNode.
// Populate the keys of Leaf node with the blobs. Test the search function
// of LeafNode.
TEST_F(BlobStoreTest, LeafNode) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));
	BlobStoreObject<const std::string> ptr =
		std::move(store.New<std::string>("S1")).Downgrade();
	BlobStoreObject<const std::string> ptr2 =
		std::move(store.New<std::string>("S2")).Downgrade();
   BlobStoreObject<const std::string> ptr3 =
        std::move(store.New<std::string>("S3")).Downgrade();
   // Create a LeafNode Blob and populate it with the keys above.
	BlobStoreObject<LeafNode<4>> leaf_node =  store.New<LeafNode<4>>();
	leaf_node->set_key(0, ptr.Index());
	leaf_node->set_key(1, ptr2.Index());
	leaf_node->set_key(2, ptr3.Index());
	leaf_node->set_num_keys(3);
	// Search for the key.
	BlobStoreObject<const std::string> key;
	size_t index = leaf_node->Search(&store, "S2", &key);
		
	EXPECT_EQ(index, 1);
	EXPECT_EQ(*key, "S2");
}

// Create a blob that's a char array with a string in it. Verify that the string is
// the same as the original string.
TEST_F(BlobStoreTest, CharArray) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));
	BlobStoreObject<const char[14]> ptr = std::move(store.New<const char[14]>("Hello, world!")).Downgrade();
	EXPECT_EQ(ptr->size, 13);
	EXPECT_EQ(*ptr, "Hello, world!");
}

// Create a blob that's an int array, and populate it with some values. Verify that the values
// are the same as the original values.
TEST_F(BlobStoreTest, IntArray) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));

	std::array<int, 4> arr = { 1, 2, 3, 4 };
	BlobStoreObject<int[4]> ptr = store.New<int[4]>(arr);
	EXPECT_EQ(ptr->size(), 4);
	EXPECT_EQ(ptr[0], 1);
	EXPECT_EQ(ptr[1], 2);
	EXPECT_EQ(ptr[2], 3);
	EXPECT_EQ(ptr[3], 4);

	BlobStoreObject<int[4]> ptr2 = store.New<int[4]>({1, 2, 3, 4});
	EXPECT_EQ(ptr2->size(), 4);
	EXPECT_EQ(ptr2[0], 1);
	EXPECT_EQ(ptr2[1], 2);
	EXPECT_EQ(ptr2[2], 3);
	EXPECT_EQ(ptr2[3], 4);
}
#include "blob_store.h"
#include "gtest/gtest.h"

class BlobStoreTest : public ::testing::Test {
protected:
	virtual void SetUp() {
		std::remove("DataBuffer");
		std::remove("MetadataBuffer");
		// create a shared memory allocator
		dataBuffer = new SharedMemoryBuffer("DataBuffer");
		metadataBuffer = new SharedMemoryBuffer("MetadataBuffer");
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
	EXPECT_EQ(store.Get<char>(ptr1.Index()), &*ptr1);
	EXPECT_EQ(store.Get<char>(ptr2.Index()), &*ptr2);
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

// Creates a few blobs, deletes some in the middle, and the compacts the store.
TEST_F(BlobStoreTest, CreateBlobStoreWithBlobsAndDeleteTwo) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));
	BlobStoreObject<char> ptr1 = store.New<char>(100);
	strcpy(&*ptr1, "This is a test.");
	BlobStoreObject<char> ptr2 = store.New<char>(100);
	strcpy(&*ptr2, "Hello World!");
	BlobStoreObject<char> ptr3 = store.New<char>(100);
	strcpy(&*ptr3, "This is a test.");
	BlobStoreObject<char> ptr4 = store.New<char>(100);
	strcpy(&*ptr4, "Hello World!");
	BlobStoreObject<char> ptr5 = store.New<char>(100);
	strcpy(&*ptr5, "This is a test.");
	BlobStoreObject<char> ptr6 = store.New<char>(100);
	strcpy(&*ptr6, "Hello World!");
	EXPECT_EQ(store.GetSize(), 6);
	EXPECT_EQ(store.Get<char>(ptr1.Index()), &*ptr1);
	EXPECT_EQ(store.Get<char>(ptr2.Index()), &*ptr2);
	EXPECT_EQ(store.Get<char>(ptr3.Index()), &*ptr3);
	EXPECT_EQ(store.Get<char>(ptr4.Index()), &*ptr4);
	EXPECT_EQ(store.Get<char>(ptr5.Index()), &*ptr5);
	EXPECT_EQ(store.Get<char>(ptr6.Index()), &*ptr6);
	store.Drop(ptr2.Index());
	store.Drop(ptr4.Index());
	EXPECT_EQ(store.GetSize(), 4);
	EXPECT_EQ(store.Get<char>(ptr1.Index()), &*ptr1);
	EXPECT_EQ(store.Get<char>(ptr3.Index()), &*ptr3);
	EXPECT_EQ(store.Get<char>(ptr5.Index()), &*ptr5);
	EXPECT_EQ(store.Get<char>(ptr6.Index()), &*ptr6);
	store.Compact();
	EXPECT_EQ(store.GetSize(), 4);
	EXPECT_EQ(store.Get<char>(ptr1.Index()), &*ptr1);
	EXPECT_EQ(store.Get<char>(ptr3.Index()), &*ptr3);
	EXPECT_EQ(store.Get<char>(ptr5.Index()), &*ptr5);
	EXPECT_EQ(store.Get<char>(ptr6.Index()), &*ptr6);
}

// Creates a few blobs, deletes some in the middle, and the compacts the store.
TEST_F(BlobStoreTest, CreateBlobStoreWithTwoBlobsAndDeleteSomeAndAddMore) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));
	BlobStoreObject<char> ptr1 = store.New<char>(100);
	strcpy(&*ptr1, "This is a test.");
	BlobStoreObject<char> ptr2 = store.New<char>(100);
	strcpy(&*ptr2, "Hello World!");
	BlobStoreObject<char> ptr3 = store.New<char>(100);
	strcpy(&*ptr3, "This is a test.");
	BlobStoreObject<char> ptr4 = store.New<char>(100);
	strcpy(&*ptr4, "Hello World!");
	BlobStoreObject<char> ptr5 = store.New<char>(100);
	strcpy(&*ptr5, "This is a test.");
	BlobStoreObject<char> ptr6 = store.New<char>(100);
	strcpy(&*ptr6, "Hello World!");
	EXPECT_EQ(store.GetSize(), 6);
	EXPECT_EQ(store.Get<char>(ptr1.Index()), &*ptr1);
	EXPECT_EQ(store.Get<char>(ptr2.Index()), &*ptr2);
	EXPECT_EQ(store.Get<char>(ptr3.Index()), &*ptr3);
	EXPECT_EQ(store.Get<char>(ptr4.Index()), &*ptr4);
	EXPECT_EQ(store.Get<char>(ptr5.Index()), &*ptr5);
	EXPECT_EQ(store.Get<char>(ptr6.Index()), &*ptr6);
	store.Drop(ptr2.Index());
	store.Drop(ptr4.Index());
	EXPECT_EQ(store.GetSize(), 4);
	EXPECT_EQ(store.Get<char>(ptr1.Index()), &*ptr1);
	EXPECT_EQ(store.Get<char>(ptr3.Index()), &*ptr3);
	EXPECT_EQ(store.Get<char>(ptr5.Index()), &*ptr5);
	EXPECT_EQ(store.Get<char>(ptr6.Index()), &*ptr6);
	store.Compact();
	EXPECT_EQ(store.GetSize(), 4);
	EXPECT_EQ(store.Get<char>(ptr1.Index()), &*ptr1);
	EXPECT_EQ(store.Get<char>(ptr3.Index()), &*ptr3);
	EXPECT_EQ(store.Get<char>(ptr5.Index()), &*ptr5);
	EXPECT_EQ(store.Get<char>(ptr6.Index()), &*ptr6);
	BlobStoreObject<char> ptr7 = store.New<char>(100);
	strcpy(&*ptr7, "This is a test.");
	BlobStoreObject<char> ptr8 = store.New<char>(100);
	strcpy(&*ptr8, "Hello World!");
	EXPECT_EQ(store.GetSize(), 6);
	EXPECT_EQ(store.Get<char>(ptr1.Index()), &*ptr1);
	EXPECT_EQ(store.Get<char>(ptr3.Index()), &*ptr3);
	EXPECT_EQ(store.Get<char>(ptr5.Index()), &*ptr5);
	EXPECT_EQ(store.Get<char>(ptr6.Index()), &*ptr6);
	EXPECT_EQ(store.Get<char>(ptr7.Index()), &*ptr7);
	EXPECT_EQ(store.Get<char>(ptr8.Index()), &*ptr8);
}

// Creates a few blobs of ints, iterates, deletes a few then iterates again.
TEST_F(BlobStoreTest, BlobIteration) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));
	BlobStoreObject<int> ptr1 = store.New<int>(100);
	BlobStoreObject<int> ptr2 = store.New<int>(200);
	BlobStoreObject<int> ptr3 = store.New<int>(300);
	auto it = store.begin();
	EXPECT_EQ(it.index(), ptr1.Index());
	EXPECT_EQ(*reinterpret_cast<const int*>(&*it), 100);
	EXPECT_EQ(it.size(), sizeof(int));
	++it;
	EXPECT_EQ(it.index(), ptr2.Index());
	EXPECT_EQ(*reinterpret_cast<const int*>(&*it), 200);
	EXPECT_EQ(it.size(), sizeof(int));
	++it;
	EXPECT_EQ(it.index(), ptr3.Index());
	EXPECT_EQ(*reinterpret_cast<const int*>(&*it), 300);
	EXPECT_EQ(it.size(), sizeof(int));
	store.Drop(ptr2.Index());

	it = store.begin();
	EXPECT_EQ(it.index(), ptr1.Index());
	EXPECT_EQ(*reinterpret_cast<const int*>(&*it), 100);
	EXPECT_EQ(it.size(), sizeof(int));
	++it;

	EXPECT_EQ(it.index(), ptr3.Index());
	EXPECT_EQ(*reinterpret_cast<const int*>(&*it), 300);
	EXPECT_EQ(it.size(), sizeof(int));
}

// Creates a couple of blobs, deletes one and insures that the BlobStoreObject is no longer valid.
TEST_F(BlobStoreTest, BlobStoreObjectInvalid) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));
	BlobStoreObject<char> ptr1 = store.New<char>(100);
	strcpy(&*ptr1, "This is a test.");
	BlobStoreObject<char> ptr2 = store.New<char>(100);
	strcpy(&*ptr2, "Hello World!");
	EXPECT_EQ(store.GetSize(), 2);
	EXPECT_EQ(store.Get<char>(ptr1.Index()), &*ptr1);
	EXPECT_EQ(store.Get<char>(ptr2.Index()), &*ptr2);
	store.Drop(ptr2.Index());
	EXPECT_EQ(store.GetSize(), 1);
	EXPECT_EQ(store.Get<char>(ptr1.Index()), &*ptr1);
	EXPECT_EQ(ptr2, nullptr);
}
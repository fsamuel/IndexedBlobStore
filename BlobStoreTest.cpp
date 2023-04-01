#include "BlobStore.h"
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
	char* ptr1 = nullptr;
	size_t token1 = store.Put(100, ptr1);
	strcpy(ptr1, "This is a test.");
	char* ptr2 = nullptr;
	size_t token2 = store.Put(100, ptr2);
	strcpy(ptr2, "Hello World!");
	EXPECT_EQ(store.GetSize(), 2);
	EXPECT_EQ(store[token1], ptr1);
	EXPECT_EQ(store[token2], ptr2);
}

// Creates two blobs with the templatized Put method, and then gets them back.
TEST_F(BlobStoreTest, CreateBlobStoreWithTwoBlobsUsingTemplatizedPut) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));
	size_t token1 = store.Put<int>(sizeof(int), 100);
	size_t token2 = store.Put<int>(sizeof(int), 1337);

	EXPECT_EQ(store.GetSize(), 2);
	EXPECT_EQ(*store.Get<int>(token1), 100);
	EXPECT_EQ(*store.Get<int>(token2), 1337);
}

// Creates a few blobs, deletes some in the middle, and the compacts the store.
TEST_F(BlobStoreTest, CreateBlobStoreWithTwoBlobsAndDeleteSome) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));
	char* ptr1 = nullptr;
	size_t token1 = store.Put(100, ptr1);
	strcpy(ptr1, "This is a test.");
	char* ptr2 = nullptr;
	size_t token2 = store.Put(100, ptr2);
	strcpy(ptr2, "Hello World!");
	char* ptr3 = nullptr;
	size_t token3 = store.Put(100, ptr3);
	strcpy(ptr3, "This is a test.");
	char* ptr4 = nullptr;
	size_t token4 = store.Put(100, ptr4);
	strcpy(ptr4, "Hello World!");
	char* ptr5 = nullptr;
	size_t token5 = store.Put(100, ptr5);
	strcpy(ptr5, "This is a test.");
	char* ptr6 = nullptr;
	size_t token6 = store.Put(100, ptr6);
	strcpy(ptr6, "Hello World!");
	EXPECT_EQ(store.GetSize(), 6);
	EXPECT_EQ(store.Get<char>(token1), ptr1);
	EXPECT_EQ(store.Get<char>(token2), ptr2);
	EXPECT_EQ(store.Get<char>(token3), ptr3);
	EXPECT_EQ(store.Get<char>(token4), ptr4);
	EXPECT_EQ(store.Get<char>(token5), ptr5);
	EXPECT_EQ(store.Get<char>(token6), ptr6);
	store.Drop(token2);
	EXPECT_EQ(store.GetSize(), 5);
	store.Drop(token4);
	EXPECT_EQ(store.GetSize(), 4);
	store.Drop(token6);
	EXPECT_EQ(store.GetSize(), 3);
	EXPECT_EQ(store.Get<char>(token1), ptr1);
	EXPECT_EQ(store.Get<char>(token3), ptr3);
	EXPECT_EQ(store.Get<char>(token5), ptr5);
	store.Compact();
	ptr1 = store.Get<char>(token1);
	EXPECT_EQ(strcmp(ptr1, "This is a test."), 0);
	ptr3 = store.Get<char>(token3);
	EXPECT_EQ(strcmp(ptr3, "This is a test."), 0);
	ptr5 = store.Get<char>(token5);
	EXPECT_EQ(strcmp(ptr5, "This is a test."), 0);
}

// Creates a few blobs, deletes some in the middle, and the compacts the store.
TEST_F(BlobStoreTest, CreateBlobStoreWithTwoBlobsAndDeleteSomeAndAddMore) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));
	char* ptr1 = nullptr;
	size_t token1 = store.Put(100, ptr1);
	strcpy(ptr1, "This is a test.");
	char* ptr2 = nullptr;
	size_t token2 = store.Put(100, ptr2);
	strcpy(ptr2, "Hello World!");
	char* ptr3 = nullptr;
	size_t token3 = store.Put(100, ptr3);
	strcpy(ptr3, "This is a test.");
	char* ptr4 = nullptr;
	size_t token4 = store.Put(100, ptr4);
	strcpy(ptr4, "Hello World!");
	char* ptr5 = nullptr;
	size_t token5 = store.Put(100, ptr5);
	strcpy(ptr5, "This is a test.");
	char* ptr6 = nullptr;
	size_t token6 = store.Put(100, ptr6);
	strcpy(ptr6, "Hello World!");
	EXPECT_EQ(store.GetSize(), 6);
	EXPECT_EQ(store.Get<char>(token1), ptr1);
	EXPECT_EQ(store.Get<char>(token2), ptr2);
	EXPECT_EQ(store.Get<char>(token3), ptr3);
	EXPECT_EQ(store.Get<char>(token4), ptr4);
	EXPECT_EQ(store.Get<char>(token5), ptr5);
	EXPECT_EQ(store.Get<char>(token6), ptr6);
	store.Drop(token2);
	store.Drop(token4);
	store.Drop(token6);
	EXPECT_EQ(store.Get<char>(token1), ptr1);
	EXPECT_EQ(store.Get<char>(token3), ptr3);
	EXPECT_EQ(store.Get<char>(token5), ptr5);
	store.Compact();
	ptr1 = store.Get<char>(token1);
	EXPECT_EQ(strcmp(ptr1, "This is a test."), 0);
	ptr3 = store.Get<char>(token3);
	EXPECT_EQ(strcmp(ptr3, "This is a test."), 0);
	ptr5 = store.Get<char>(token5);
	EXPECT_EQ(strcmp(ptr5, "This is a test."), 0);
	char* ptr7 = nullptr;
	size_t token7 = store.Put(100, ptr7);
	strcpy(ptr7, "This is a test.");
	char* ptr8 = nullptr;
	size_t token8 = store.Put(100, ptr8);
	strcpy(ptr8, "Hello World!");
	char* ptr9 = nullptr;
	size_t token9 = store.Put(100, ptr9);
	strcpy(ptr9, "This is a test.");
	char* ptr10 = nullptr;
	size_t token10 = store.Put(100, ptr10);
}
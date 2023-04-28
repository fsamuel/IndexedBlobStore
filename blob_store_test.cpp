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
	const char* test_str1 = "This is a test.";
	const char* test_str2 = "Hello World!";
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));
	BlobStoreObject<char[]> ptr1 = store.NewArray<char>(100);

	strcpy(&ptr1[0], test_str1);
	EXPECT_EQ(strcmp(*ptr1, test_str1), 0);

	BlobStoreObject<char[]> ptr2 = store.NewArray<char>(100);
	strcpy(*ptr2, "Hello World!");
	BlobStoreObject<char[]> ptr3 = store.NewArray<char>(100);
	strcpy(*ptr3, "This is a test.");
	BlobStoreObject<char[]> ptr4 = store.NewArray<char>(100);
	strcpy(*ptr4, "Hello World!");
	BlobStoreObject<char[]> ptr5 = store.NewArray<char>(100);
	strcpy(*ptr5, "This is a test.");
	BlobStoreObject<char[]> ptr6 = store.NewArray<char>(100);
	strcpy(*ptr6, "Hello World!");
	EXPECT_EQ(store.GetSize(), 6);
	store.Drop(std::move(ptr2));
	store.Drop(std::move(ptr4));
	EXPECT_EQ(store.GetSize(), 4);
	store.Compact();
	EXPECT_EQ(store.GetSize(), 4);
	// Compare the contents of the blobs with the expected strings.
	EXPECT_EQ(strcmp(*ptr1, "This is a test."), 0);
	EXPECT_EQ(strcmp(*ptr3, "This is a test."), 0);
	EXPECT_EQ(strcmp(*ptr5, "This is a test."), 0);
	EXPECT_EQ(strcmp(*ptr6, "Hello World!"), 0);
}

// Creates a few blobs, deletes some in the middle, and the compacts the store.
TEST_F(BlobStoreTest, CreateBlobStoreWithTwoBlobsAndDeleteSomeAndAddMore) {
	const char* test_str1 = "This is a test.";
	const char* test_str2 = "Hello World!";
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));
	BlobStoreObject<char[]> ptr1 = store.NewArray<char>(100);

	strcpy(&ptr1[0], test_str1);
	EXPECT_EQ(strcmp(*ptr1, test_str1), 0);

	BlobStoreObject<char[]> ptr2 = store.NewArray<char>(100);
	strcpy(*ptr2, "Hello World!");
	BlobStoreObject<char[]> ptr3 = store.NewArray<char>(100);
	strcpy(*ptr3, "This is a test.");
	BlobStoreObject<char[]> ptr4 = store.NewArray<char>(100);
	strcpy(*ptr4, "Hello World!");
	BlobStoreObject<char[]> ptr5 = store.NewArray<char>(100);
	strcpy(*ptr5, "This is a test.");
	BlobStoreObject<char[]> ptr6 = store.NewArray<char>(100);
	strcpy(*ptr6, "Hello World!");
	EXPECT_EQ(store.GetSize(), 6);
	store.Drop(std::move(ptr2));
	store.Drop(std::move(ptr4));
	EXPECT_EQ(store.GetSize(), 4);
	store.Compact();
	EXPECT_EQ(store.GetSize(), 4);
	// Compare the contents of the blobs with the expected strings.
	EXPECT_EQ(strcmp(*ptr1, "This is a test."), 0);
	EXPECT_EQ(strcmp(*ptr3, "This is a test."), 0);
	EXPECT_EQ(strcmp(*ptr5, "This is a test."), 0);
	EXPECT_EQ(strcmp(*ptr6, "Hello World!"), 0);

	BlobStoreObject<char[]> ptr7 = store.NewArray<char>(100);
	strcpy(*ptr7, test_str1);
	BlobStoreObject<char[]> ptr8 = store.NewArray<char>(100);
	strcpy(*ptr8, test_str2);
	EXPECT_EQ(store.GetSize(), 6);

	EXPECT_EQ(strcmp(*ptr1, test_str1), 0);
	EXPECT_EQ(strcmp(*ptr3, test_str1), 0);
	EXPECT_EQ(strcmp(*ptr5, test_str1), 0);
	EXPECT_EQ(strcmp(*ptr6, test_str2), 0);
	EXPECT_EQ(strcmp(*ptr7, test_str1), 0);
	EXPECT_EQ(strcmp(*ptr8, test_str2), 0);

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

	BlobStoreObject<const char[64]> ptr1; 
	{
		BlobStoreObject<char[64]> newPtr = store.New<char[64]>();
		strcpy(&newPtr[0], "This is a test.");
		ptr1 = std::move(newPtr).Downgrade();
	}

	EXPECT_EQ(&*store.Get<char[64]>(ptr1.Index()), &*ptr1);

	BlobStoreObject<const char[64]> ptr2;
	{
		BlobStoreObject<char[64]> newPtr = store.New<char[64]>();
		strcpy(&newPtr[0], "Hello World!");
		ptr2 = std::move(newPtr).Downgrade();
	}

	EXPECT_EQ(&*store.Get<char[64]>(ptr2.Index()), &*ptr2);

	EXPECT_EQ(store.GetSize(), 2);

	store.Drop(std::move(ptr2));
	EXPECT_EQ(store.GetSize(), 1);

	EXPECT_EQ(&*store.Get<char[64]>(ptr1.Index()), &*ptr1);
	EXPECT_EQ(ptr1.GetSize(), 64);
	// Fixed-size arrays are treated as one element.
	EXPECT_EQ(ptr1.GetElementCount(), 1);
	EXPECT_EQ(ptr2, nullptr);

	BlobStoreObject<char[]> ptr3 = store.NewArray<char>(64);
	ptr3[0] = 'a';
	ptr3[1] = 'b';
	ptr3[2] = 'c';
	ptr3[3] = '\0';

	BlobStoreObject<char[]> ptr4 = ptr3;
	EXPECT_EQ(*ptr4, *ptr3);
	EXPECT_EQ(ptr4, ptr3);
	EXPECT_EQ(ptr4.GetSize(), ptr3.GetSize());
	EXPECT_EQ(ptr4.GetElementCount(), ptr3.GetElementCount());

	EXPECT_EQ(ptr3.GetSize(), 64);
	EXPECT_EQ(ptr3.GetElementCount(), 64);

	store.Drop(std::move(ptr3));
	EXPECT_EQ(ptr3, nullptr);
	EXPECT_EQ(ptr4, nullptr);
}

// Tests BlobStoreObject: creates a blob, gets a BlobStoreObject from it,
// copies the BlobStoreObject, moves the BlobStoreObject, assigns the BlobStoreObject,
// and uses other BlobStoreObject accessors
TEST_F(BlobStoreTest, BlobStoreObject) {
	BlobStore store(std::move(*metadataBuffer), std::move(*dataBuffer));

	BlobStoreObject<int[]> ptr = store.NewArray<int>(64);
	for (int i = 0; i < ptr.GetElementCount(); ++i) {
		ptr[i] = i*100;
	}
	EXPECT_EQ(ptr.GetSize(), 64 * sizeof(int));
	// Verify that the elements of ptr have been set.
	for (int i = 0; i < 64; ++i) {
		EXPECT_EQ(ptr[i], i * 100);
	}

	BlobStoreObject<int[]> ptr2 = ptr;
	EXPECT_EQ(ptr2.GetSize(), 64 * sizeof(int));
	EXPECT_EQ(ptr2, ptr);
	for (int i = 0; i < 64; ++i) {
		EXPECT_EQ(ptr2[i], i * 100);
	}
	ptr2[2] = 1337;
	EXPECT_EQ(ptr[2], 1337);

	BlobStoreObject<int[]> ptr3 = std::move(ptr2);
	EXPECT_EQ(ptr3.GetSize(), 64 * sizeof(int));
	EXPECT_EQ(ptr3, ptr);
	EXPECT_EQ(ptr2, nullptr);
}
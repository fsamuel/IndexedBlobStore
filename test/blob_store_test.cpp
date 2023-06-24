#include "blob_store.h"
#include "gtest/gtest.h"

#include "chunk_manager.h"
#include "fixed_string.h"
#include "nodes.h"
#include "test_memory_buffer_factory.h"

class BlobStoreTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    RemoveChunkFiles();
    // create a shared memory allocator
    dataBuffer =
        new ChunkManager(TestMemoryBufferFactory::Get(), "DataBuffer", 4096);
    metadataBuffer = new SharedMemoryBuffer("MetadataBuffer", 4096);
  }

  virtual void TearDown() {
    delete dataBuffer;
    delete metadataBuffer;
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

  ChunkManager* dataBuffer = nullptr;
  SharedMemoryBuffer* metadataBuffer = nullptr;
};

TEST_F(BlobStoreTest, CreateEmptyBlobStore) {
  BlobStore store(TestMemoryBufferFactory::Get(), "MetadataBuffer", 4096,
                  std::move(*dataBuffer));
  EXPECT_EQ(store.GetSize(), 0);
}

TEST_F(BlobStoreTest, CreateBlobStoreWithTwoBlobs) {
  BlobStore store(TestMemoryBufferFactory::Get(), "MetadataBuffer", 4096,
                  std::move(*dataBuffer));
  BlobStoreObject<std::string> ptr1 = store.New<std::string>("This is a test.");
  // strcpy(&*ptr1, "This is a test.");
  BlobStoreObject<std::string> ptr2 = store.New<std::string>("Hello World!");
  // strcpy(&*ptr2, "Hello World!");
  EXPECT_EQ(store.GetSize(), 2);
}

// Creates two blobs with the templatized Put method, and then gets them back.
TEST_F(BlobStoreTest, CreateBlobStoreWithTwoBlobsUsingTemplatizedPut) {
  BlobStore store(TestMemoryBufferFactory::Get(), "MetadataBuffer", 4096,
                  std::move(*dataBuffer));
  BlobStoreObject<int> ptr1 = store.New<int>(100);
  BlobStoreObject<int> ptr2 = store.New<int>(1337);

  EXPECT_EQ(store.GetSize(), 2);
  EXPECT_EQ(*ptr1, 100);
  EXPECT_EQ(*ptr2, 1337);
}

// Creates a few blobs of ints, iterates, deletes a few then iterates again.
TEST_F(BlobStoreTest, BlobIteration) {
  BlobStore store(TestMemoryBufferFactory::Get(), "MetadataBuffer", 4096,
                  std::move(*dataBuffer));

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

// Creates a couple of blobs, deletes one and insures that the BlobStoreObject
// is no longer valid.
TEST_F(BlobStoreTest, BlobStoreObjectInvalid) {
  BlobStore store(TestMemoryBufferFactory::Get(), "MetadataBuffer", 4096,
                  std::move(*dataBuffer));

  BlobStoreObject<const std::string> ptr1;
  {
    BlobStoreObject<std::string> newPtr =
        store.New<std::string>("This is a test.");
    ptr1 = std::move(newPtr).Downgrade();
  }

  EXPECT_EQ(&*store.Get<std::string>(ptr1.Index()), &*ptr1);

  BlobStoreObject<const std::string> ptr2;
  {
    BlobStoreObject<std::string> newPtr =
        store.New<std::string>("Hello World!");
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
  BlobStore store(TestMemoryBufferFactory::Get(), "MetadataBuffer", 4096,
                  std::move(*dataBuffer));
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

// Verify that if we have multiple non-const pointers to the same blob and we
// downgrade one of them, the newly downgraded pointer will be invalidated.
TEST_F(BlobStoreTest, BlobStoreObjectDowngradeInvalidates) {
  BlobStore store(TestMemoryBufferFactory::Get(), "MetadataBuffer", 4096,
                  std::move(*dataBuffer));
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

// Tests upgrading a const BlobStoreObject to non-const. Also tests that if we
// have multiple const pointers to the same blob and we upgrade one of them, the
// newly upgraded pointer will be invalidated.
TEST_F(BlobStoreTest, BlobStoreObjectUpgradeInvalidates) {
  BlobStore store(TestMemoryBufferFactory::Get(), "MetadataBuffer", 4096,
                  std::move(*dataBuffer));
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

// Create a blob, get a BlobStoreObject from it, and then drop the blob. Try to
// get another BlobStoreObject from the index of the dropped blob. This should
// fail.
TEST_F(BlobStoreTest, BlobStoreObjectDrop) {
  BlobStore store(TestMemoryBufferFactory::Get(), "MetadataBuffer", 4096,
                  std::move(*dataBuffer));
  BlobStoreObject<int> ptr = store.New<int>(64);
  size_t index = ptr.Index();
  EXPECT_EQ(ptr.GetSize(), sizeof(int));
  EXPECT_EQ(*ptr, 64);
  store.Drop(std::move(ptr));
  EXPECT_EQ(ptr, nullptr);
  BlobStoreObject<const int> ptr2 = store.Get<int>(index);
  EXPECT_EQ(ptr2, nullptr);
}

// Create a blob, get a BlobStoreObject from it, store it in another
// BlobStoreObject. Drop the first BlobStoreObject. The second BlobStoreObject
// should no longer be valid. Try to get another BlobStoreObject from the index
// of the dropped blob. This should fail.
TEST_F(BlobStoreTest, BlobStoreObjectDrop2) {
  BlobStore store(TestMemoryBufferFactory::Get(), "MetadataBuffer", 4096,
                  std::move(*dataBuffer));
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
  // EXPECT_EQ(ptr, nullptr);
  EXPECT_EQ(ptr2, nullptr);
  BlobStoreObject<const int> ptr3 = store.Get<int>(index);
  EXPECT_EQ(ptr3, nullptr);
}

// Create blobs using FixedString, try to access them using BlobStoreObject.
// Convert back to std::string or StringSlice and verify that the contents are
// the same.
TEST_F(BlobStoreTest, FixedString) {
  BlobStore store(TestMemoryBufferFactory::Get(), "MetadataBuffer", 4096,
                  std::move(*dataBuffer));
  BlobStoreObject<const std::string> ptr =
      std::move(store.New<std::string>("Hello, world!")).Downgrade();
  BlobStoreObject<const std::string> ptr2 =
      std::move(store.New<std::string>("New World")).Downgrade();
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
  auto ptr3 = store.Get<std::string>(ptr.Index());
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
  BlobStore store(TestMemoryBufferFactory::Get(), "MetadataBuffer", 4096,
                  std::move(*dataBuffer));
  BlobStoreObject<const std::string> ptr =
      std::move(store.New<std::string>("S1")).Downgrade();
  BlobStoreObject<const std::string> ptr2 =
      std::move(store.New<std::string>("S2")).Downgrade();
  BlobStoreObject<const std::string> ptr3 =
      std::move(store.New<std::string>("S3")).Downgrade();
  // Create a LeafNode Blob and populate it with the keys above.
  BlobStoreObject<LeafNode<4>> leaf_node = store.New<LeafNode<4>>();
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

// Create a blob that's a char array with a string in it. Verify that the string
// is the same as the original string.
TEST_F(BlobStoreTest, CharArray) {
  BlobStore store(TestMemoryBufferFactory::Get(), "MetadataBuffer", 4096,
                  std::move(*dataBuffer));
  BlobStoreObject<const char[14]> ptr =
      std::move(store.New<const char[14]>("Hello, world!")).Downgrade();
  EXPECT_EQ(ptr->size, 13);
  EXPECT_EQ(*ptr, "Hello, world!");
}

// Create a blob that's an int array, and populate it with some values. Verify
// that the values are the same as the original values.
TEST_F(BlobStoreTest, IntArray) {
  BlobStore store(TestMemoryBufferFactory::Get(), "MetadataBuffer", 4096,
                  std::move(*dataBuffer));

  std::array<int, 4> arr = {1, 2, 3, 4};
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

// Similar to IntArray creates 8 concurrent threads and drops each blob after
// writing to it and verifying.
TEST_F(BlobStoreTest, IntArrayConcurrent) {
  BlobStore store(TestMemoryBufferFactory::Get(), "MetadataBuffer", 4096,
                  std::move(*dataBuffer));
  std::array<BlobStoreObject<int[4]>, 8> results;
  std::vector<std::thread> threads;
  for (int i = 0; i < 8; i++) {
    threads.push_back(std::thread([&, i]() {
      BlobStoreObject<int[4]> ptr =
          store.New<int[4]>({i * 8 + 1, i * 8 + 2, i * 8 + 3, i * 8 + 4});
      results[i] = ptr;
      EXPECT_EQ(ptr->size(), 4);
      EXPECT_EQ(ptr[0], i * 8 + 1);
      EXPECT_EQ(ptr[1], i * 8 + 2);
      EXPECT_EQ(ptr[2], i * 8 + 3);
      EXPECT_EQ(ptr[3], i * 8 + 4);
    }));
  }
  for (auto& thread : threads) {
    thread.join();
  }
  // Iterate over the results and verify their contents.
  for (int i = 0; i < 8; i++) {
    EXPECT_EQ(results[i]->size(), 4);
    EXPECT_EQ(results[i][0], i * 8 + 1);
    EXPECT_EQ(results[i][1], i * 8 + 2);
    EXPECT_EQ(results[i][2], i * 8 + 3);
    EXPECT_EQ(results[i][3], i * 8 + 4);
    EXPECT_EQ(results[i].GetSize(), 16);
  }
}

TEST_F(BlobStoreTest, IntArrayConcurrentDrop) {
  BlobStore store(TestMemoryBufferFactory::Get(), "MetadataBuffer", 4096,
                  std::move(*dataBuffer));
  std::array<BlobStoreObject<int[4]>, 8> results;
  std::vector<std::thread> threads;
  for (int i = 0; i < 8; i++) {
    threads.push_back(std::thread([&, i]() {
      BlobStoreObject<int[4]> ptr =
          store.New<int[4]>({i * 8 + 1, i * 8 + 2, i * 8 + 3, i * 8 + 4});
      results[i] = ptr;
      EXPECT_EQ(ptr->size(), 4);
      EXPECT_EQ(ptr[0], i * 8 + 1);
      EXPECT_EQ(ptr[1], i * 8 + 2);
      EXPECT_EQ(ptr[2], i * 8 + 3);
      EXPECT_EQ(ptr[3], i * 8 + 4);
      store.Drop(std::move(ptr));
    }));
  }
  for (auto& thread : threads) {
    thread.join();
  }
  // Verify the contents of the results vector.
  // Even though the blobs have been dropped, the contents should still be
  // valid. This is because the blobs are tombstoned, and the data is still in
  // the data buffer. The data is only deallocated once all BlobStoreObjects
  // release their locks on the data.
  for (int i = 0; i < 8; i++) {
    EXPECT_EQ(results[i]->size(), 4);
    EXPECT_EQ(results[i][0], i * 8 + 1);
    EXPECT_EQ(results[i][1], i * 8 + 2);
    EXPECT_EQ(results[i][2], i * 8 + 3);
    EXPECT_EQ(results[i][3], i * 8 + 4);
    // The size of the blob should be 0, since it's been dropped.
    EXPECT_EQ(results[i].GetSize(), 0);
  }
}

// Allocate some blobs, pass them to 8 threads, and verify that the contents are
// the same as the original contents.
TEST_F(BlobStoreTest, IntArrayConcurrentDropVerify) {
  BlobStore store(TestMemoryBufferFactory::Get(), "MetadataBuffer", 4096,
                  std::move(*dataBuffer));
  std::array<BlobStoreObject<int[4]>, 8> inputs;
  // Allocate 8 blobs.
  for (int i = 0; i < 8; i++) {
    inputs[i] = store.New<int[4]>({i * 8 + 1, i * 8 + 2, i * 8 + 3, i * 8 + 4});
    EXPECT_EQ(inputs[i].GetSize(), 16);
  }
  std::vector<std::thread> threads;
  for (int i = 0; i < 8; i++) {
    threads.push_back(std::thread([&]() {
      // Iterate over the blobs and verify their contents.
      for (int i = 0; i < 8; i++) {
        BlobStoreObject<int[4]> ptr = inputs[i];
        EXPECT_EQ(ptr->size(), 4);
        EXPECT_EQ(ptr[0], i * 8 + 1);
        EXPECT_EQ(ptr[1], i * 8 + 2);
        EXPECT_EQ(ptr[2], i * 8 + 3);
        EXPECT_EQ(ptr[3], i * 8 + 4);
        store.Drop(std::move(ptr));
      }
    }));
  }
  for (auto& thread : threads) {
    thread.join();
  }
  for (int i = 0; i < 8; i++) {
    EXPECT_EQ(inputs[i]->size(), 4);
    EXPECT_EQ(inputs[i][0], i * 8 + 1);
    EXPECT_EQ(inputs[i][1], i * 8 + 2);
    EXPECT_EQ(inputs[i][2], i * 8 + 3);
    EXPECT_EQ(inputs[i][3], i * 8 + 4);
    // The size of the blob should be 0, since it's been dropped.
    EXPECT_EQ(inputs[i].GetSize(), 0);
  }
}

// Allocate a blob, pass it to 8 threads, have them all clone it, and store the
// clones in a vector. After the threads are done, verify that the contents of
// the clones are the same as the original.
// TODO(fsamuel): This vector occasionally crashes trying to access the results
// vector. In one case, in seems like the lhs expression string is -1. Is this a
// gtest bug? Is it some kind of buffer overrun clobbering other data?
// In another case, it seems like the control_block_ of BlobStoreObject is being
// double freed on operator=.
TEST_F(BlobStoreTest, IntArrayConcurrentClone) {
  BlobStore store(TestMemoryBufferFactory::Get(), "MetadataBuffer", 4096,
                  std::move(*dataBuffer));
  BlobStoreObject<int[4]> ptr = store.New<int[4]>({1, 2, 3, 4});
  std::array<BlobStoreObject<int[4]>, 8> inputs;
  std::array<BlobStoreObject<int[4]>, 8> results;
  // Populate inputs with copies of ptr.
  for (int i = 0; i < 8; i++) {
    inputs[i] = ptr;
  }
  std::vector<std::thread> threads;
  for (int i = 0; i < 8; i++) {
    threads.push_back(std::thread([&, i]() {
      // Clone the blob 8 times.
      for (int i = 0; i < 8; i++) {
        results[i] = inputs[i].Clone();
        EXPECT_EQ(results[i]->size(), 4);
        EXPECT_EQ(results[i][0], 1);
        EXPECT_EQ(results[i][1], 2);
        EXPECT_EQ(results[i][2], 3);
        EXPECT_EQ(results[i][3], 4);
      }
    }));
  }
  for (auto& thread : threads) {
    thread.join();
  }
  // Verify the contents of the results vector.
  for (int i = 0; i < 8; i++) {
    EXPECT_EQ(results[i]->size(), 4);
    EXPECT_EQ(results[i][0], 1);
    EXPECT_EQ(results[i][1], 2);
    EXPECT_EQ(results[i][2], 3);
    EXPECT_EQ(results[i][3], 4);
    // The size of the blob should be 16, since it's been cloned.
    EXPECT_EQ(results[i].GetSize(), 16);
    // Verify that no two clones refer to the same index.
    for (int j = 0; j < 8; j++) {
      if (i != j) {
        EXPECT_NE(results[i].Index(), results[j].Index());
      }
    }
  }
}
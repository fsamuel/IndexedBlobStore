#include <array>

#include "chunk_manager.h"
#include "gtest/gtest.h"
#include "shm_allocator.h"

class ShmAllocatorTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    // create a shared memory allocator
    RemoveChunkFiles();
    // The initial chunk size must be at least the size of the allocator header.
    ChunkManager manager("test_buffer", 32);
    shared_mem_allocator = new ShmAllocator(std::move(manager));
  }

  virtual void TearDown() {
    // cleanup the shared memory allocator
    delete shared_mem_allocator;
    RemoveChunkFiles();
  }

  void RemoveChunkFiles() {
    // Delete all files with the prefix "test_chunk"
    // Do this in case the previous test failed and left some files behind
    for (int i = 0; i < 20; ++i) {
      std::string filename = "test_buffer_" + std::to_string(i);
      std::remove(filename.c_str());
    }
  }

  ShmAllocator* shared_mem_allocator;
};

TEST_F(ShmAllocatorTest, AllocateMemory) {
  void* ptr = shared_mem_allocator->Allocate(128);
  EXPECT_NE(ptr, nullptr);
}

TEST_F(ShmAllocatorTest, FreeMemory) {
  uint8_t* ptr = shared_mem_allocator->Allocate(128);
  EXPECT_NE(ptr, nullptr);

  shared_mem_allocator->Deallocate(ptr);

  // attempt to free the same pointer again should fail
  EXPECT_FALSE(shared_mem_allocator->Deallocate(ptr));
}

TEST_F(ShmAllocatorTest, FreeNullPointer) {
  EXPECT_DEATH(shared_mem_allocator->Deallocate(nullptr), "");
}

TEST_F(ShmAllocatorTest, MultipleAllocations) {
  uint8_t* ptr1 = shared_mem_allocator->Allocate(128);
  EXPECT_NE(ptr1, nullptr);
  uint8_t* ptr2 = shared_mem_allocator->Allocate(256);
  EXPECT_NE(ptr2, nullptr);
  EXPECT_NE(ptr1, ptr2);
  EXPECT_GE(shared_mem_allocator->GetCapacity(ptr1), 128);
  EXPECT_GE(shared_mem_allocator->GetCapacity(ptr2), 256);
}

// Similar to MultipleAllocations but across 8 threads.
TEST_F(ShmAllocatorTest, MultipleAllocationsMultithreaded) {
  std::vector<std::thread> threads;
  for (int thread_index = 0; thread_index < 8; ++thread_index) {
    threads.push_back(std::thread([this]() {
      uint8_t* ptr1 = shared_mem_allocator->Allocate(128);
      EXPECT_NE(ptr1, nullptr);
      uint8_t* ptr2 = shared_mem_allocator->Allocate(256);
      EXPECT_NE(ptr2, nullptr);
      EXPECT_NE(ptr1, ptr2);
      EXPECT_GE(shared_mem_allocator->GetCapacity(ptr1), 128);
      EXPECT_GE(shared_mem_allocator->GetCapacity(ptr2), 256);
      shared_mem_allocator->Deallocate(ptr1);
      shared_mem_allocator->Deallocate(ptr2);
    }));
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

// Request several allocations per thread, then deallocate them.
TEST_F(ShmAllocatorTest, MultipleAllocationsMultithreaded2) {
  std::vector<std::thread> threads;
  for (int thread_index = 0; thread_index < 8; ++thread_index) {
    threads.push_back(std::thread([this, thread_index]() {
      for (int i = 0; i < 100; ++i) {
        uint8_t* ptr = shared_mem_allocator->Allocate(128);
        EXPECT_NE(ptr, nullptr);
        shared_mem_allocator->Deallocate(ptr);
      }
    }));
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST_F(ShmAllocatorTest, MultipleAllocationsAndDeallocations) {
  uint8_t* ptr1 = shared_mem_allocator->Allocate(128);
  EXPECT_NE(ptr1, nullptr);
  uint8_t* ptr2 = shared_mem_allocator->Allocate(256);
  EXPECT_NE(ptr2, nullptr);
  EXPECT_NE(ptr1, ptr2);
  shared_mem_allocator->Deallocate(ptr1);
  shared_mem_allocator->Deallocate(ptr2);
}

TEST_F(ShmAllocatorTest, AllocateMoreThanAvailable) {
  uint8_t* ptr1 = shared_mem_allocator->Allocate(1024);
  EXPECT_NE(ptr1, nullptr);
  // This should cause the buffer to resize.
  uint8_t* ptr2 = shared_mem_allocator->Allocate(1);
  EXPECT_NE(ptr2, nullptr);
}

TEST_F(ShmAllocatorTest, AllocateMoreThanAvailableAndDeallocate) {
  uint8_t* ptr1 = shared_mem_allocator->Allocate(1024);
  EXPECT_NE(ptr1, nullptr);
  // This should cause the buffer to resize.
  uint8_t* ptr2 = shared_mem_allocator->Allocate(1);
  EXPECT_NE(ptr2, nullptr);
  shared_mem_allocator->Deallocate(ptr1);
  shared_mem_allocator->Deallocate(ptr2);
}

TEST_F(ShmAllocatorTest, MemoryRecycling) {
  uint8_t* ptr1 = shared_mem_allocator->Allocate(1024);
  EXPECT_NE(ptr1, nullptr);
  for (int i = 0; i < 1024; i++) {
    ptr1[i] = 'a';
  }
  shared_mem_allocator->Deallocate(ptr1);

  uint8_t* ptr2 = shared_mem_allocator->Allocate(256);
  EXPECT_NE(ptr2, nullptr);
  for (int i = 0; i < 256; i++) {
    ptr2[i] = 'b';
  }

  uint8_t* ptr3 = shared_mem_allocator->Allocate(256);
  EXPECT_NE(ptr3, nullptr);
  for (int i = 0; i < 256; i++) {
    ptr3[i] = 'c';
  }
  EXPECT_TRUE(ptr3 > ptr2 + 256 || ptr3 < ptr2 - 256);

  uint8_t* ptr4 = shared_mem_allocator->Allocate(256);
  EXPECT_NE(ptr4, nullptr);
  for (int i = 0; i < 256; i++) {
    ptr4[i] = 'd';
  }
  EXPECT_TRUE(ptr4 > ptr3 + 256 || ptr4 < ptr3 - 256);
  EXPECT_TRUE(ptr4 > ptr2 + 256 || ptr4 < ptr2 - 256);

  // Verify that memory isn't being clobbered.
  for (int i = 0; i < 256; i++) {
    EXPECT_EQ(ptr2[i], 'b');
  }
  for (int i = 0; i < 256; i++) {
    EXPECT_EQ(ptr3[i], 'c');
  }
  for (int i = 0; i < 256; i++) {
    EXPECT_EQ(ptr4[i], 'd');
  }

  shared_mem_allocator->Deallocate(ptr2);
  shared_mem_allocator->Deallocate(ptr3);
  shared_mem_allocator->Deallocate(ptr4);
}

// Similar to MemoryRecycling but with multiple concurrent threads.
TEST_F(ShmAllocatorTest, MemoryRecyclingMultithreaded) {
  std::vector<std::thread> threads;
  for (int thread_index = 0; thread_index < 10; ++thread_index) {
    threads.push_back(std::thread([this, thread_index]() {
      uint8_t* ptr1 = shared_mem_allocator->Allocate(256);
      EXPECT_NE(ptr1, nullptr);
      for (int i = 0; i < 256; i++) {
        ptr1[i] = 'a';
      }
      shared_mem_allocator->Deallocate(ptr1);

      uint8_t* ptr2 = shared_mem_allocator->Allocate(256);
      EXPECT_NE(ptr2, nullptr);
      for (int i = 0; i < 256; i++) {
        ptr2[i] = 'b';
      }

      uint8_t* ptr3 = shared_mem_allocator->Allocate(256);
      EXPECT_NE(ptr3, nullptr);
      for (int i = 0; i < 256; i++) {
        ptr3[i] = 'c';
      }
      EXPECT_TRUE(ptr3 > ptr2 + 256 || ptr3 < ptr2 - 256);

      uint8_t* ptr4 = shared_mem_allocator->Allocate(256);
      EXPECT_NE(ptr4, nullptr);
      for (int i = 0; i < 256; i++) {
        ptr4[i] = 'd';
      }
      EXPECT_TRUE(ptr4 > ptr3 + 256 || ptr4 < ptr3 - 256);
      EXPECT_TRUE(ptr4 > ptr2 + 256 || ptr4 < ptr2 - 256);

      // Verify that memory isn't being clobbered.
      for (int i = 0; i < 256; i++) {
        EXPECT_EQ(ptr2[i], 'b');
      }
      for (int i = 0; i < 256; i++) {
        EXPECT_EQ(ptr3[i], 'c');
      }
      for (int i = 0; i < 256; i++) {
        EXPECT_EQ(ptr4[i], 'd');
      }

      shared_mem_allocator->Deallocate(ptr2);
      shared_mem_allocator->Deallocate(ptr3);
      shared_mem_allocator->Deallocate(ptr4);
    }));
  }
  for (auto& thread : threads) {
    thread.join();
  }
}
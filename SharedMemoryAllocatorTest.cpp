#include "SharedMemoryAllocator.h"
#include "gtest/gtest.h"

class SharedMemoryAllocatorTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        // create a shared memory allocator
        std::remove("test_buffer");
        SharedMemoryBuffer buffer("test_buffer", 1024);
        shared_mem_allocator = new SharedMemoryAllocator<char>(std::move(buffer));
    }

    virtual void TearDown() {
        // cleanup the shared memory allocator
        delete shared_mem_allocator;
    }

    SharedMemoryAllocator<char>* shared_mem_allocator;
};

TEST_F(SharedMemoryAllocatorTest, AllocateMemory) {
    void* ptr = shared_mem_allocator->allocate(128);
    EXPECT_NE(ptr, nullptr);
}

TEST_F(SharedMemoryAllocatorTest, FreeMemory) {
    char* ptr = shared_mem_allocator->allocate(128);
    EXPECT_NE(ptr, nullptr);

    shared_mem_allocator->deallocate(ptr);

    // attempt to free the same pointer again should fail
    EXPECT_DEATH(shared_mem_allocator->deallocate(ptr), "");
}

TEST_F(SharedMemoryAllocatorTest, FreeNullPointer) {
    EXPECT_DEATH(shared_mem_allocator->deallocate(nullptr), "");
}

TEST_F(SharedMemoryAllocatorTest, MultipleAllocations) {
    char* ptr1 = shared_mem_allocator->allocate(128);
    EXPECT_NE(ptr1, nullptr);
    char* ptr2 = shared_mem_allocator->allocate(256);
    EXPECT_NE(ptr2, nullptr);
    EXPECT_NE(ptr1, ptr2);
    EXPECT_EQ(shared_mem_allocator->capacity(ptr1), 128);
    EXPECT_EQ(shared_mem_allocator->capacity(ptr2), 256);
}

TEST_F(SharedMemoryAllocatorTest, MultipleAllocationsAndDeallocations) {
    char* ptr1 = shared_mem_allocator->allocate(128);
    EXPECT_NE(ptr1, nullptr);
    char* ptr2 = shared_mem_allocator->allocate(256);
    EXPECT_NE(ptr2, nullptr);
    EXPECT_NE(ptr1, ptr2);
    shared_mem_allocator->deallocate(ptr1);
    shared_mem_allocator->deallocate(ptr2);
}

TEST_F(SharedMemoryAllocatorTest, AllocateMoreThanAvailable) {
    char* ptr1 = shared_mem_allocator->allocate(1024);
    EXPECT_NE(ptr1, nullptr);
    // This should cause the buffer to resize.
    char* ptr2 = shared_mem_allocator->allocate(1);
    EXPECT_NE(ptr2, nullptr);
}

TEST_F(SharedMemoryAllocatorTest, AllocateMoreThanAvailableAndDeallocate) {
	char* ptr1 = shared_mem_allocator->allocate(1024);
	EXPECT_NE(ptr1, nullptr);
	// This should cause the buffer to resize.
	char* ptr2 = shared_mem_allocator->allocate(1);
	EXPECT_NE(ptr2, nullptr);
	shared_mem_allocator->deallocate(ptr1);
	shared_mem_allocator->deallocate(ptr2);
}

TEST_F(SharedMemoryAllocatorTest, MemoryRecycling) {
    char* ptr1 = shared_mem_allocator->allocate(1024);
    EXPECT_NE(ptr1, nullptr);
    for (int i = 0; i < 1024; i++) {
        ptr1[i] = 'a';
    }
    shared_mem_allocator->deallocate(ptr1);
    
    char* ptr2 = shared_mem_allocator->allocate(256);
    EXPECT_NE(ptr2, nullptr);
    for (int i = 0; i < 256; i++) {
		ptr2[i] = 'b';
	}

    char* ptr3 = shared_mem_allocator->allocate(256);
    EXPECT_NE(ptr3, nullptr);
    for (int i = 0; i < 256; i++) {
        ptr3[i] = 'c';
    }
    EXPECT_TRUE(ptr3 > ptr2 + 256 || ptr3 < ptr2 - 256);

    char* ptr4 = shared_mem_allocator->allocate(256);
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

    shared_mem_allocator->deallocate(ptr2);
    shared_mem_allocator->deallocate(ptr3);
    shared_mem_allocator->deallocate(ptr4);
}
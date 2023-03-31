#include "pch.h"
#include "gtest/gtest.h"
#include "SharedMemoryAllocator.h"
#include "SharedMemoryVector.h"

class SharedMemoryVectorTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        // create a shared memory allocator
        std::remove("test_buffer");
        SharedMemoryBuffer buffer("test_buffer", 1024);
        shared_mem_allocator = new SharedMemoryAllocator<int>(std::move(buffer));
    }

    virtual void TearDown() {
        // cleanup the shared memory allocator
        delete shared_mem_allocator;
    }

    SharedMemoryAllocator<int>* shared_mem_allocator;
};

TEST_F(SharedMemoryVectorTest, CreateEmptyVector) {
	SharedMemoryVector<int> vec(*shared_mem_allocator);
	EXPECT_EQ(vec.size(), 0);
	EXPECT_EQ(vec.capacity(), 0);
	EXPECT_EQ(vec.data(), nullptr);
}

TEST_F(SharedMemoryVectorTest, PushBackAndPop) {
    SharedMemoryVector<int> vec(*shared_mem_allocator);
    vec.push_back(1);
    EXPECT_EQ(vec.size(), 1);
    vec.pop_back();
    EXPECT_EQ(vec.size(), 0);
}
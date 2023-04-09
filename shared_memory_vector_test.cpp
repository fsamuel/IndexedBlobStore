#include "gtest/gtest.h"
#include "shared_memory_allocator.h"
#include "shared_memory_vector.h"

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
	EXPECT_EQ(vec.GetCapacity(), 0);
	EXPECT_EQ(vec.data(), nullptr);
}

TEST_F(SharedMemoryVectorTest, PushBackAndPop) {
    SharedMemoryVector<int> vec(*shared_mem_allocator);
    vec.push_back(1);
    EXPECT_EQ(vec.size(), 1);
    vec.pop_back();
    EXPECT_EQ(vec.size(), 0);
}

TEST_F(SharedMemoryVectorTest, PushBackAndPopMultiple) {
	SharedMemoryVector<int> vec(*shared_mem_allocator);
	vec.push_back(1);
	vec.push_back(2);
	vec.push_back(3);
	EXPECT_EQ(vec.size(), 3);
	vec.pop_back();
	EXPECT_EQ(vec.size(), 2);
	vec.pop_back();
	EXPECT_EQ(vec.size(), 1);
	vec.pop_back();
	EXPECT_EQ(vec.size(), 0);
}

TEST_F(SharedMemoryVectorTest, PushBackAndPopMultipleWithCapacity) {
	SharedMemoryVector<int> vec(*shared_mem_allocator);
	vec.push_back(1);
	vec.push_back(2);
	vec.push_back(3);
	EXPECT_EQ(vec.size(), 3);
	size_t GetCapacity = vec.GetCapacity();
	vec.pop_back();
	EXPECT_EQ(vec.size(), 2);
	EXPECT_EQ(vec.GetCapacity(), GetCapacity);
	vec.pop_back();
	EXPECT_EQ(vec.size(), 1);
	EXPECT_EQ(vec.GetCapacity(), GetCapacity);
	vec.pop_back();
	EXPECT_EQ(vec.size(), 0);
	EXPECT_EQ(vec.GetCapacity(), GetCapacity);
}

// Creates a vector, fills it with 1000 elements, then clears it.
TEST_F(SharedMemoryVectorTest, PushBackAndClear) {
	SharedMemoryVector<int> vec(*shared_mem_allocator);
	for (int i = 0; i < 1000; i++) {
		vec.push_back(i);
	}
	EXPECT_EQ(vec.size(), 1000);
	// Iterates over the vector to make sure the elements are there.
	for (int i = 0; i < 1000; i++) {
		EXPECT_EQ(vec[i], i);
	}
	vec.clear();
	EXPECT_EQ(vec.size(), 0);
}

// Creates a vector, fills it with 1000 elements and then resizes it smaller.
TEST_F(SharedMemoryVectorTest, FillAndResize) {
	SharedMemoryVector<int> vec(*shared_mem_allocator);
	for (int i = 0; i < 1000; i++) {
		vec.push_back(i);
	}
	EXPECT_EQ(vec.size(), 1000);
	// Iterates over the vector to make sure the elements are there.
	for (int i = 0; i < 1000; i++) {
		EXPECT_EQ(vec[i], i);
	}
	vec.Resize(500);
	EXPECT_EQ(vec.size(), 500);
	// Iterates over the vector to make sure the elements are there.
	for (int i = 0; i < 500; i++) {
		EXPECT_EQ(vec[i], i);
	}
}

// Creates a vector, fills it with 100 elements, then resizes it larger.
TEST_F(SharedMemoryVectorTest, FillAndResizeLarger) {
	SharedMemoryVector<int> vec(*shared_mem_allocator);
	for (int i = 0; i < 100; i++) {
		vec.push_back(i);
	}
	EXPECT_EQ(vec.size(), 100);
	// Iterates over the vector to make sure the elements are there.
	for (int i = 0; i < 100; i++) {
		EXPECT_EQ(vec[i], i);
	}
	vec.Resize(500);
	EXPECT_EQ(vec.size(), 500);
	// Iterates over the vector to make sure the elements are there.
	for (int i = 0; i < 100; i++) {
		EXPECT_EQ(vec[i], i);
	}
}
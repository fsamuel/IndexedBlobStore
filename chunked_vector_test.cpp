#include "gtest/gtest.h"
#include "shared_memory_allocator.h"
#include "chunked_vector.h"

class ChunkedVectorTest : public ::testing::Test {
protected:
    virtual void SetUp() {
		// Delete all files with the prefix "chunked_vector_test"
		// Do this in case the previous test failed and left some files behind
		for (int i = 0; i < 10; ++i) {
			std::string filename = "chunked_vector_test_" + std::to_string(i);
			std::remove(filename.c_str());
		}
    }

    virtual void TearDown() {

    }

};

TEST_F(ChunkedVectorTest, SanityCheck) {
	ChunkedVector<int, 4> vector("chunked_vector_test");
	EXPECT_EQ(vector.size(), 0);
	vector.push_back(0);
	EXPECT_EQ(vector.size(), 1);
	EXPECT_EQ(vector[0], 0);
	vector.push_back(1);
	EXPECT_EQ(vector.size(), 2);
	EXPECT_EQ(vector[1], 1);
	vector.push_back(2);
	EXPECT_EQ(vector.size(), 3);	
	EXPECT_EQ(vector[2], 2);
	vector.push_back(3);
	EXPECT_EQ(vector.size(), 4);
	EXPECT_EQ(vector[3], 3);
	vector.push_back(4);
	EXPECT_EQ(vector.size(), 5);
	EXPECT_EQ(vector[4], 4);
}

// Basic test for ChunkedVector that populates it with 100 elements and
// verifies that they are all there.
TEST_F(ChunkedVectorTest, BasicTest) {
	ChunkedVector<int, 4> vector("chunked_vector_test");
	for (int i = 0; i < 10; i++) {
		vector.push_back(i);
	}
	EXPECT_EQ(vector.capacity(), 15);
	EXPECT_EQ(vector.size(), 10);
	for (int i = 0; i < 10; i++) {
		EXPECT_EQ(vector[i], i);
	}
}

// Push back one element and pop it off and make sure size is updated appropriately
// each time.
TEST_F(ChunkedVectorTest, PushbackAndPop) {
	ChunkedVector<int, 4> vec("chunked_vector_test");
	EXPECT_EQ(vec.size(), 0);
	vec.push_back(1);
	EXPECT_EQ(vec.size(), 1);
	vec.pop_back();
	EXPECT_EQ(vec.size(), 0);
}

// Tests reserve, capacity, and resize operations on ChunkedVector.
TEST_F(ChunkedVectorTest, ReserveCapacityResize) {
	ChunkedVector<int, 4> vec("chunked_vector_test");
	EXPECT_EQ(vec.capacity(), 1);
	vec.reserve(10);
	EXPECT_EQ(vec.capacity(), 15);
	vec.resize(10);
	EXPECT_EQ(vec.size(), 10);
	EXPECT_EQ(vec.capacity(), 15);
	vec.resize(20);
	EXPECT_EQ(vec.size(), 20);
	EXPECT_EQ(vec.capacity(), 31);
}
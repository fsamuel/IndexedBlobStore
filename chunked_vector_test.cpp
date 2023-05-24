#include "gtest/gtest.h"
#include "shared_memory_allocator.h"
#include "chunked_vector.h"
#include "utils.h"

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
	ChunkedVector<int> vector("chunked_vector_test", 4);
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
	ChunkedVector<int> vector("chunked_vector_test", 4);
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
	ChunkedVector<int> vec("chunked_vector_test", 4);
	EXPECT_EQ(vec.size(), 0);
	vec.push_back(1);
	EXPECT_EQ(vec.size(), 1);
	vec.pop_back();
	EXPECT_EQ(vec.size(), 0);
}

// Tests reserve, capacity, and resize operations on ChunkedVector.
TEST_F(ChunkedVectorTest, ReserveCapacityResize) {
	ChunkedVector<int> vec("chunked_vector_test", 4);
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

// Write 1000 elements to the ChunkedVector and then read them back and make sure
// they are all there. Modify a few elements and make sure they are updated. Also
// make sure that the other elements are still there and unchanged.
TEST_F(ChunkedVectorTest, WriteAndRead) {
	ChunkedVector<int> vec("chunked_vector_test", 4);
	for (int i = 0; i < 1000; i++) {
		vec.push_back(i);
	}
	EXPECT_EQ(vec.size(), 1000);
	for (int i = 0; i < 1000; i++) {
		EXPECT_EQ(vec[i], i);
	}
	EXPECT_EQ(vec.capacity(), 1023);
	vec[0] = 1000;
	vec[1] = 1001;
	vec[2] = 1002;
	EXPECT_EQ(vec[0], 1000);
	EXPECT_EQ(vec[1], 1001);
	EXPECT_EQ(vec[2], 1002);
	for (int i = 3; i < 1000; i++) {
		EXPECT_EQ(vec[i], i);
	}
}

// Try a ChunkedVector with a different chunk size and a different type.
TEST_F(ChunkedVectorTest, LargeChunkWithStruct) {
	struct TestStruct {
		int a;
		int b;
		int c;
		int d;
	};
	ChunkedVector<TestStruct> vec("chunked_vector_test", 16);
	EXPECT_EQ(vec.size(), 0);
	TestStruct obj;
	obj.a = 1;
	obj.b = 2;
	obj.c = 3;
	obj.d = 4;
	vec.push_back(obj);
	EXPECT_EQ(vec.size(), 1);
	EXPECT_EQ(vec[0].a, 1);
	EXPECT_EQ(vec[0].b, 2);
	EXPECT_EQ(vec[0].c, 3);
	EXPECT_EQ(vec[0].d, 4);
}

// Insert 1000 elements into a ChunkedVector and then erase them all and make sure
// the size is updated appropriately.
TEST_F(ChunkedVectorTest, Erase) {
	ChunkedVector<int> vec("chunked_vector_test", 4);
	for (int i = 0; i < 1000; i++) {
		vec.push_back(i);
	}
	EXPECT_EQ(vec.size(), 1000);
	for (int i = 0; i < 1000; i++) {
		vec.pop_back();
	}
	EXPECT_EQ(vec.size(), 0);
}

// Insert 1000 structs into a ChunkedVector and modify a few of them making
// sure that the modifications are reflected in the ChunkedVector and other elements
// are unchanged.
TEST_F(ChunkedVectorTest, EraseStruct) {
	struct TestStruct {
		int a;
		int b;
		int c;
		int d;
	};
	ChunkedVector<TestStruct> vec("chunked_vector_test", 16);
	for (int i = 0; i < 1000; i++) {
		TestStruct obj;
		obj.a = i;
		obj.b = i + 1;
		obj.c = i + 2;
		obj.d = i + 3;
		vec.push_back(obj);
	}
	EXPECT_EQ(vec.size(), 1000);
	for (int i = 1; i < 1000; i++) {
		vec[i].a = 1000;
		vec[i].b = 1001;
		vec[i].c = 1002;
		vec[i].d = 1003;
	}
	for (int i = 1; i < 1000; i++) {
		EXPECT_EQ(vec[i].a, 1000);
		EXPECT_EQ(vec[i].b, 1001);
		EXPECT_EQ(vec[i].c, 1002);
		EXPECT_EQ(vec[i].d, 1003);
	}
	EXPECT_EQ(vec[0].a, 0);
	EXPECT_EQ(vec[0].b, 1);
	EXPECT_EQ(vec[0].c, 2);
	EXPECT_EQ(vec[0].d, 3);
}

// Tests a ChunkVector as a byte array.
TEST_F(ChunkedVectorTest, ByteArray) {
	ChunkedVector<uint8_t> vec("chunked_vector_test", 4);
	for (int i = 0; i < 256; i++) {
		vec.push_back(i);
	}
	EXPECT_EQ(vec.size(), 256);
	for (int i = 0; i < 256; i++) {
		EXPECT_EQ(vec[i], i);
	}
	EXPECT_EQ(vec.capacity(), 508);
	vec[0] = 3;
	vec[1] = 4;
	vec[2] = 5;
	EXPECT_EQ(vec[0], 3);
	EXPECT_EQ(vec[1], 4);
	EXPECT_EQ(vec[2], 5);
	for (int i = 3; i < 256; i++) {
		EXPECT_EQ(vec[i], i);
	}
}

// Tests a ChunkVector as a byte array with a different chunk size.
TEST_F(ChunkedVectorTest, ByteArrayLargeChunk) {
	std::size_t page_size = utils::GetPageSize();
	ChunkedVector<uint8_t> vec("chunked_vector_test", page_size);
	for (int i = 0; i < 256; i++) {
		vec.push_back(i);
	}
	EXPECT_EQ(vec.size(), 256);
	for (int i = 0; i < 256; i++) {
		EXPECT_EQ(vec[i], i);
	}
	EXPECT_EQ(vec.capacity(), page_size);
	vec[0] = 3;
	vec[1] = 4;
	vec[2] = 5;
	EXPECT_EQ(vec[0], 3);
	EXPECT_EQ(vec[1], 4);
	EXPECT_EQ(vec[2], 5);
	for (int i = 3; i < 256; i++) {
		EXPECT_EQ(vec[i], i);
	}
}
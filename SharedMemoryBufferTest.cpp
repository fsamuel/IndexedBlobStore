#include "gtest/gtest.h"
#include "SharedMemoryBuffer.h"
#include <fstream>
#include <fstream>
#include <cstring>
#include <cstdio>

class SharedMemoryBufferTest : public ::testing::Test {
public:

protected:
	SharedMemoryBufferTest() {
	}

	virtual ~SharedMemoryBufferTest() {
	}

	virtual void SetUp() override {}

	virtual void TearDown() override {
		std::remove(existing_file.c_str());
		std::remove(file_name.c_str());
		std::remove(test_file.c_str());
	}

	std::string existing_file = "existing_file.bin";
	std::string file_name = "resize_test_file.bin";
	std::string test_file = "testfile";

};

TEST_F(SharedMemoryBufferTest, CreateEmptyBuffer) {
	SharedMemoryBuffer buffer("testfile");
	EXPECT_EQ(buffer.name(), "testfile");
	EXPECT_EQ(buffer.size(), 0);
	EXPECT_EQ(buffer.data(), nullptr);
}

TEST_F(SharedMemoryBufferTest, CreateBufferWithSize) {
	SharedMemoryBuffer buffer("testfile", 1024);
	EXPECT_EQ(buffer.name(), "testfile");
	EXPECT_EQ(buffer.size(), 1024);
	EXPECT_NE(buffer.data(), nullptr);
}

TEST_F(SharedMemoryBufferTest, Constructor) {
	// Test constructing a SharedMemoryBuffer object with an existing file.
	{
		std::ofstream file(existing_file);
		file << "Testing";
	}
	SharedMemoryBuffer buf_existing(existing_file);
	ASSERT_EQ(buf_existing.size(), 7);

	// Test constructing a SharedMemoryBuffer object with a non-existent file.
	std::string non_existent_file = "non_existent_file.bin";
	SharedMemoryBuffer buf_non_existent(non_existent_file);
	ASSERT_EQ(buf_non_existent.size(), 0);

	// Test constructing a SharedMemoryBuffer object with a given size.
	std::string sized_file = "sized_file.bin";
	SharedMemoryBuffer buf_sized(sized_file, 1024);
	ASSERT_EQ(buf_sized.size(), 1024);
}

TEST_F(SharedMemoryBufferTest, Resize) {
	// Create a file with initial content.
	{
		std::ofstream file(file_name);
		file << "InitialContent";
	}

	// Test constructing a SharedMemoryBuffer object with the created file.
	SharedMemoryBuffer buffer(file_name);

	// Test resizing to a larger size.
	buffer.resize(64);
	ASSERT_EQ(buffer.size(), 64);
	ASSERT_EQ(std::memcmp(buffer.data(), "InitialContent", 14), 0);

	// Test resizing to a smaller size.
	buffer.resize(8);
	ASSERT_EQ(buffer.size(), 8);
	ASSERT_EQ(std::memcmp(buffer.data(), "InitialC", 8), 0);

	// Test resizing to the same size.
	buffer.resize(8);
	ASSERT_EQ(buffer.size(), 8);
	ASSERT_EQ(std::memcmp(buffer.data(), "InitialC", 8), 0);

	// Test resizing to a size smaller than the initial file size.
	buffer.resize(12);
	ASSERT_EQ(buffer.size(), 12);
	ASSERT_EQ(std::memcmp(buffer.data(), "InitialC", 12), 0);
}

TEST_F(SharedMemoryBufferTest, MoveConstructor) {
	// Create a SharedMemoryBuffer object with a new name and size
	SharedMemoryBuffer buffer1("testfile", 100);

	// Move the buffer into a new buffer
	SharedMemoryBuffer buffer2(std::move(buffer1));

	// Check that the original buffer is empty
	EXPECT_EQ(buffer1.name(), "");
	EXPECT_EQ(buffer1.size(), 0);
	EXPECT_EQ(buffer1.data(), nullptr);

	// Check that the new buffer has the correct name and size
	EXPECT_EQ(buffer2.name(), "testfile");
	EXPECT_EQ(buffer2.size(), 100);

	// Check that the new buffer contains zeros
	char* data = (char*)buffer2.data();
	for (std::size_t i = 0; i < buffer2.size(); i++) {
		EXPECT_EQ(data[i], 0);
	}
}

TEST_F(SharedMemoryBufferTest, MoveAssignmentOperator) {
	// Create two SharedMemoryBuffer objects with different names and sizes
	SharedMemoryBuffer buffer1("testfile1", 100);
	SharedMemoryBuffer buffer2("testfile2", 200);

	// Move buffer1 into buffer2
	buffer2 = std::move(buffer1);

	// Check that buffer1 is empty
	EXPECT_EQ(buffer1.name(), "");
	EXPECT_EQ(buffer1.size(), 0);
	EXPECT_EQ(buffer1.data(), nullptr);

	// Check that buffer2 has the correct name and size
	EXPECT_EQ(buffer2.name(), "testfile1");
	EXPECT_EQ(buffer2.size(), 100);

	// Check that buffer2 contains zeros
	char* data = (char*)buffer2.data();
	for (std::size_t i = 0; i < buffer2.size(); i++) {
		EXPECT_EQ(data[i], 0);
	}
}

TEST_F(SharedMemoryBufferTest, ResizeToZero) {
	// Create a SharedMemoryBuffer object with a new name and size
	SharedMemoryBuffer buffer("testfile", 100);

	// Resize the buffer to zero
	buffer.resize(0);

	// Check that the buffer has the correct size
	EXPECT_EQ(buffer.size(), 0);

	// Check that the buffer contains no data
	EXPECT_EQ(buffer.data(), nullptr);
}

TEST_F(SharedMemoryBufferTest, MultipleInstances) {
	// Create two SharedMemoryBuffer objects with the same name and size
	SharedMemoryBuffer buffer1("testfile", 100);
	SharedMemoryBuffer buffer2("testfile", 100);

	// Check that the two buffers have the same name and size
	EXPECT_EQ(buffer1.name(), "testfile");
	EXPECT_EQ(buffer1.size(), 100);
	EXPECT_EQ(buffer2.name(), "testfile");
	EXPECT_EQ(buffer2.size(), 100);

	// Check that the two buffers share the same data
	char* data1 = (char*)buffer1.data();
	char* data2 = (char*)buffer2.data();
	for (std::size_t i = 0; i < buffer1.size(); i++) {
		EXPECT_EQ(data1[i], data2[i]);
	}

	// Change the data in one of the buffers
	data1[0] = 'A';

	// Check that the data in both buffers has changed
	for (std::size_t i = 0; i < buffer1.size(); i++) {
		EXPECT_EQ(data1[i], data2[i]);
	}
}

TEST_F(SharedMemoryBufferTest, MultipleInstancesDifferentSizes) {
	// Create two SharedMemoryBuffer objects with the same name but different sizes
	SharedMemoryBuffer buffer1("testfile", 100);
	SharedMemoryBuffer buffer2("testfile", 200);
	// Check that the two buffers have the same name
	EXPECT_EQ(buffer1.name(), "testfile");
	EXPECT_EQ(buffer2.name(), "testfile");
	// Check that the two buffers have different sizes
	EXPECT_EQ(buffer1.size(), 100);
	EXPECT_EQ(buffer2.size(), 200);
	// Check that the two buffers share the same data
	char* data1 = (char*)buffer1.data();
	char* data2 = (char*)buffer2.data();
	for (std::size_t i = 0; i < buffer1.size(); i++) {
		EXPECT_EQ(data1[i], data2[i]);
	}
	// Change the data in one of the buffers
	data1[0] = 'A';
	// Check that the data in both buffers has changed
	for (std::size_t i = 0; i < buffer1.size(); i++) {
		EXPECT_EQ(data1[i], data2[i]);
	}
}
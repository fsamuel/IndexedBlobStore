#include "shared_memory_buffer.h"
#include <cstdio>
#include <cstring>
#include <fstream>
#include "gtest/gtest.h"

class SharedMemoryBufferTest : public ::testing::Test {
 public:
 protected:
  SharedMemoryBufferTest() {}

  virtual ~SharedMemoryBufferTest() {}

  virtual void SetUp() override {
    std::remove(existing_file.c_str());
    std::remove(file_name.c_str());
    std::remove(test_file.c_str());
  }

  virtual void TearDown() override {}

  void remoteTestFiles() {
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
  EXPECT_EQ(buffer.GetName(), "testfile");
  EXPECT_EQ(buffer.GetSize(), 0);
  EXPECT_EQ(buffer.GetData(), nullptr);
}

TEST_F(SharedMemoryBufferTest, CreateBufferWithSize) {
  SharedMemoryBuffer buffer("testfile", 1024);
  EXPECT_EQ(buffer.GetName(), "testfile");
  EXPECT_EQ(buffer.GetSize(), 1024);
  EXPECT_NE(buffer.GetData(), nullptr);
}

TEST_F(SharedMemoryBufferTest, Constructor) {
  // Test constructing a SharedMemoryBuffer object with an existing file.
  {
    std::ofstream file(existing_file);
    file << "Testing";
  }
  SharedMemoryBuffer buf_existing(existing_file);
  ASSERT_EQ(buf_existing.GetSize(), 7);

  // Test constructing a SharedMemoryBuffer object with a non-existent file.
  std::string non_existent_file = "non_existent_file.bin";
  SharedMemoryBuffer buf_non_existent(non_existent_file);
  ASSERT_EQ(buf_non_existent.GetSize(), 0);

  // Test constructing a SharedMemoryBuffer object with a given size.
  std::string sized_file = "sized_file.bin";
  SharedMemoryBuffer buf_sized(sized_file, 1024);
  ASSERT_EQ(buf_sized.GetSize(), 1024);
}

TEST_F(SharedMemoryBufferTest, MoveConstructor) {
  // Create a SharedMemoryBuffer object with a new name and size
  SharedMemoryBuffer buffer1("testfile", 100);

  // Move the buffer into a new buffer
  SharedMemoryBuffer buffer2(std::move(buffer1));

  // Check that the original buffer is empty
  EXPECT_EQ(buffer1.GetName(), "");
  EXPECT_EQ(buffer1.GetSize(), 0);
  EXPECT_EQ(buffer1.GetData(), nullptr);

  // Check that the new buffer has the correct name and size
  EXPECT_EQ(buffer2.GetName(), "testfile");
  EXPECT_EQ(buffer2.GetSize(), 100);

  // Check that the new buffer contains zeros
  char* data = (char*)buffer2.GetData();
  for (std::size_t i = 0; i < buffer2.GetSize(); i++) {
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
  EXPECT_EQ(buffer1.GetName(), "");
  EXPECT_EQ(buffer1.GetSize(), 0);
  EXPECT_EQ(buffer1.GetData(), nullptr);

  // Check that buffer2 has the correct name and size
  EXPECT_EQ(buffer2.GetName(), "testfile1");
  EXPECT_EQ(buffer2.GetSize(), 100);

  // Check that buffer2 contains zeros
  char* data = (char*)buffer2.GetData();
  for (std::size_t i = 0; i < buffer2.GetSize(); i++) {
    EXPECT_EQ(data[i], 0);
  }
}

TEST_F(SharedMemoryBufferTest, ResizeToZero) {
  // Create a SharedMemoryBuffer object with a new name and size
  SharedMemoryBuffer buffer("testfile", 100);

  // Resize the buffer to zero
  buffer.Resize(0);

  // Check that the buffer has the correct size
  EXPECT_EQ(buffer.GetSize(), 0);

  // Check that the buffer contains no data
  EXPECT_EQ(buffer.GetData(), nullptr);
}

TEST_F(SharedMemoryBufferTest, MultipleInstances) {
  // Create two SharedMemoryBuffer objects with the same name and size
  SharedMemoryBuffer buffer1("testfile", 100);
  SharedMemoryBuffer buffer2("testfile", 100);

  // Check that the two buffers have the same name and size
  EXPECT_EQ(buffer1.GetName(), "testfile");
  EXPECT_EQ(buffer1.GetSize(), 100);
  EXPECT_EQ(buffer2.GetName(), "testfile");
  EXPECT_EQ(buffer2.GetSize(), 100);

  // Check that the two buffers share the same data
  char* data1 = (char*)buffer1.GetData();
  char* data2 = (char*)buffer2.GetData();
  for (std::size_t i = 0; i < buffer1.GetSize(); i++) {
    EXPECT_EQ(data1[i], data2[i]);
  }

  // Change the data in one of the buffers
  data1[0] = 'A';

  // Check that the data in both buffers has changed
  for (std::size_t i = 0; i < buffer1.GetSize(); i++) {
    EXPECT_EQ(data1[i], data2[i]);
  }
}

TEST_F(SharedMemoryBufferTest, MultipleInstancesDifferentSizes) {
  // Create two SharedMemoryBuffer objects with the same name but different
  // sizes
  SharedMemoryBuffer buffer1("testfile", 100);
  SharedMemoryBuffer buffer2("testfile", 200);
  // Check that the two buffers have the same name
  EXPECT_EQ(buffer1.GetName(), "testfile");
  EXPECT_EQ(buffer2.GetName(), "testfile");
  // Check that the two buffers have different sizes
  EXPECT_EQ(buffer1.GetSize(), 100);
  EXPECT_EQ(buffer2.GetSize(), 200);
  // Check that the two buffers share the same data
  char* data1 = (char*)buffer1.GetData();
  char* data2 = (char*)buffer2.GetData();
  for (std::size_t i = 0; i < buffer1.GetSize(); i++) {
    EXPECT_EQ(data1[i], data2[i]);
  }
  // Change the data in one of the buffers
  data1[0] = 'A';
  // Check that the data in both buffers has changed
  for (std::size_t i = 0; i < buffer1.GetSize(); i++) {
    EXPECT_EQ(data1[i], data2[i]);
  }
}

// Tests SharedMemoryBuffer by writing to the buffer, flushing it to disk,
// closing the buffer, and then reading the file to check that the data was
// written.
TEST_F(SharedMemoryBufferTest, WriteToFile) {
  // Define test data and memory-mapped file name
  const std::string test_data =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  const std::string test_file_name = "testfile";
  {
    // Create a SharedMemoryBuffer object with a new name and size
    SharedMemoryBuffer buffer(test_file_name, test_data.size());
    std::memcpy(buffer.GetData(), test_data.data(), test_data.size());

    // Flush the buffer to ensure the data is written to disk
    buffer.flush();
  }
  // Read the file and check that the data was written
  std::ifstream file(test_file_name);
  std::string content((std::istreambuf_iterator<char>(file)),
                      (std::istreambuf_iterator<char>()));
  EXPECT_EQ(content, test_data);
}
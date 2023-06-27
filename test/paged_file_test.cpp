#include "blob_store.h"
#include "gtest/gtest.h"

#include "chunk_manager.h"
#include "paged_file.h"
#include "paged_file_nodes.h"
#include "test_memory_buffer_factory.h"

class PagedFileTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    // create a shared memory allocator
    dataBuffer =
        new ChunkManager(TestMemoryBufferFactory::Get(), "DataBuffer", 4096);
  }

  virtual void TearDown() { delete dataBuffer; }

  ChunkManager* dataBuffer = nullptr;
};

// Verifies that the paged file can be created and writin to it will resize the
// file and seek to the correct location.
TEST_F(PagedFileTest, SanityCheck) {
  BlobStore store(TestMemoryBufferFactory::Get(), "MetadataBuffer", 4096,
                  std::move(*dataBuffer));
  EXPECT_EQ(store.GetSize(), 0);
  PagedFile file = PagedFile<6, 32>::Create(&store);
  constexpr std::size_t BUFFER_SIZE = 256;
  char write_buffer[BUFFER_SIZE];
  // Fill the write buffer with random bits.
  for (std::size_t i = 0; i < BUFFER_SIZE; ++i) {
    write_buffer[i] = i;
  }
  file.Write(&write_buffer[0], sizeof(write_buffer));
  EXPECT_EQ(file.Tell(), BUFFER_SIZE);
  char read_buffer[BUFFER_SIZE];
  file.Seek(0);
  file.Read(&read_buffer[0], sizeof(read_buffer));
  EXPECT_EQ(file.Tell(), BUFFER_SIZE);
  for (std::size_t i = 0; i < BUFFER_SIZE; ++i) {
    EXPECT_EQ(static_cast<uint8_t>(read_buffer[i]), i);
  }
}
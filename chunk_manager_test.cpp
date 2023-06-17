#include "chunk_manager.h"
#include <gtest/gtest.h>
#include <cstddef>

#include "shared_memory_buffer_factory.h"

class ChunkManagerTest : public ::testing::Test {
 protected:
  virtual void SetUp() { RemoveChunkFiles(); }

  virtual void TearDown() { RemoveChunkFiles(); }

 private:
  void RemoveChunkFiles() {
    // Delete all files with the prefix "test_chunk"
    // Do this in case the previous test failed and left some files behind
    for (int i = 0; i < 20; ++i) {
      std::string filename = "test_chunk_" + std::to_string(i);
      std::remove(filename.c_str());
    }
  }
};

TEST_F(ChunkManagerTest, AddChunkAndRemoveChunk) {
  ChunkManager manager(SharedMemoryBufferFactory::Get(), "test_chunk", 64);

  // Initially, there should be 1 chunk
  EXPECT_EQ(manager.num_chunks(), 1);

  // Add 3 more chunks
  uint8_t* chunk;
  std::size_t chunk_size;
  std::size_t chunks_created =
      manager.get_or_create_chunk(0, &chunk, &chunk_size);
  EXPECT_EQ(chunks_created, 0);
  chunks_created = manager.get_or_create_chunk(1, &chunk, &chunk_size);
  EXPECT_EQ(chunks_created, 1);
  chunks_created = manager.get_or_create_chunk(2, &chunk, &chunk_size);
  EXPECT_EQ(chunks_created, 1);

  // Now, there should be 3 chunks
  EXPECT_EQ(manager.num_chunks(), 3);

  // Remove 2 chunks
  manager.remove_chunk();
  manager.remove_chunk();

  // Now, there should be 2 chunks
  EXPECT_EQ(manager.num_chunks(), 1);
}

TEST_F(ChunkManagerTest, AccessChunkAndOffset) {
  ChunkManager manager(SharedMemoryBufferFactory::Get(), "test_chunk", 64);

  // Add 3 more chunks
  uint8_t* chunk;
  std::size_t chunk_size;
  manager.get_or_create_chunk(1, &chunk, &chunk_size);
  manager.get_or_create_chunk(2, &chunk, &chunk_size);
  manager.get_or_create_chunk(3, &chunk, &chunk_size);

  // Access a specific offset within a chunk
  uint8_t* data1 = manager.at(1, 16);
  uint8_t* data2 = manager.at(2, 32);

  // Verify the accessed data is not null
  EXPECT_TRUE(data1 != nullptr);
  EXPECT_TRUE(data2 != nullptr);

  // Modify the accessed memory
  *data1 = 10;
  *data2 = 20;

  // Verify the modifications
  EXPECT_EQ(*data1, 10);
  EXPECT_EQ(*data2, 20);
}

TEST_F(ChunkManagerTest, TotalCapacity) {
  std::size_t chunk_size = 64;
  ChunkManager manager(SharedMemoryBufferFactory::Get(), "test_chunk",
                       chunk_size);

  // The total capacity should match the chunk size initially
  EXPECT_EQ(manager.capacity(), chunk_size);

  // Add 3 more chunks
  {
    uint8_t* chunk;
    std::size_t chunk_size;
    manager.get_or_create_chunk(3, &chunk, &chunk_size);
  }

  // The total capacity should be the sum of capacities of all chunks
  std::size_t total_capacity = chunk_size;
  chunk_size *= 2;
  for (std::size_t i = 1; i < 4; ++i) {
    total_capacity += chunk_size;
    chunk_size *= 2;
  }
  EXPECT_EQ(manager.capacity(), total_capacity);
}

// Tests get_or_create_chunk() verifying that it will not create a new chunk
// if the chunk already exists and that it can create multiple chunks to get
// to the desired chunk.
TEST_F(ChunkManagerTest, GetOrCreateChunk) {
  ChunkManager manager(SharedMemoryBufferFactory::Get(), "test_chunk", 64);

  // Initially, there should be 1 chunk
  EXPECT_EQ(manager.num_chunks(), 1);

  // Add 3 more chunks
  uint8_t* chunk;
  std::size_t chunk_size;
  std::size_t num_created = manager.get_or_create_chunk(5, &chunk, &chunk_size);
  EXPECT_EQ(num_created, 5);
  EXPECT_EQ(manager.num_chunks(), 6);
  num_created = manager.get_or_create_chunk(2, &chunk, &chunk_size);
  EXPECT_EQ(num_created, 0);
  EXPECT_EQ(manager.num_chunks(), 6);
}
TEST_F(ChunkManagerTest, ConcurrentAccess) {
  const std::size_t chunk_size = 64;
  ChunkManager manager(SharedMemoryBufferFactory::Get(), "test_chunk",
                       chunk_size);
  const std::size_t num_threads = 8;
  const std::size_t iterations = chunk_size;
  std::atomic<std::size_t> num_created(0);
  std::vector<std::thread> threads(num_threads);

  // Concurrently access and modify the ChunkManager
  for (std::size_t i = 0; i < num_threads; ++i) {
    threads[i] = std::thread([&manager, &num_created, i, iterations]() {
      std::size_t chunk_size;
      uint8_t* chunk_start;
      std::size_t num_chunks_created =
          manager.get_or_create_chunk(i, &chunk_start, &chunk_size);
      num_created.fetch_add(num_chunks_created);

      for (std::size_t j = 0; j < iterations; ++j) {
        uint8_t* chunk_offset_data = manager.at(i, j % iterations);

        // Verify the chunk start and offset access
        EXPECT_TRUE(chunk_offset_data != nullptr);
        // Verify the chunk offset is within the chunk.
        EXPECT_GE(chunk_offset_data, chunk_start);
        EXPECT_LE(chunk_offset_data, chunk_start + chunk_size);

        num_chunks_created =
            manager.get_or_create_chunk(i, &chunk_start, &chunk_size);
        EXPECT_EQ(num_chunks_created, 0);

        // Modify the accessed memory
        *chunk_offset_data = static_cast<uint8_t>(i + j);
      }
    });
  }

  // Wait for all threads to complete
  for (std::size_t i = 0; i < num_threads; ++i) {
    threads[i].join();
  }

  EXPECT_EQ(manager.num_chunks(), num_threads);
  EXPECT_EQ(num_created.load(), num_threads - 1);
  // Perform additional verification after thread completion
  for (std::size_t i = 0; i < num_threads; ++i) {
    for (std::size_t j = 0; j < iterations; ++j) {
      // Access the same chunk and offset
      uint8_t* chunk_offset_data = manager.at(i, j % iterations);

      // Verify the correctness of the modifications
      EXPECT_EQ(*chunk_offset_data, static_cast<uint8_t>(i + j));
    }
  }
}
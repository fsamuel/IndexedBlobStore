#ifndef CHUNK_MANAGER_H_
#define CHUNK_MANAGER_H_

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "buffer_factory.h"

// ChunkManager is a data structure that manages chunks of shared memory.
// Each chunk is double the size of the previous chunk.
// The number of chunks is stored in the first chunk for persistence.
// It supports basic operations like adding a chunk at the end, removing a chunk
// at the end, and allocating a contiguous space of a certain size.
class ChunkManager {
 public:
  // Constructs a ChunkManager with the specified name_prefix for the shared
  // memory buffers. Each SharedMemoryBuffer will be named as name_prefix_i,
  // where i is the chunk index. Reads the number of chunks from the first chunk
  // and adds any necessary chunks.
  ChunkManager(BufferFactory* buffer_factory,
               const std::string& name_prefix,
               std::size_t initial_chunk_size);

  ChunkManager(ChunkManager&& other);
  ChunkManager& operator=(ChunkManager&& other);

  // Ensures that a chunk exists up to the specified chunk index, and returns
  // a pointer to the chunk at that index. If the chunk already existed, then
  // the function returns 0. If one or more chunks were added to get to the
  // provided chunk index, then the function returns the number of chunks that
  // were added.
  std::size_t get_or_create_chunk(size_t chunk_index,
                                  uint8_t** data,
                                  std::size_t* chunk_size);

  // Removes the last chunk from the ChunkManager and updates the number of
  // chunks in the first chunk.
  void remove_chunk();

  // Returns the number of chunks that the ChunkManager is managing.
  std::uint64_t num_chunks() const;

  // Returns a pointer to the specified index in the ChunkManager.
  const uint8_t* at(std::uint64_t index) const;
  uint8_t* at(std::uint64_t index);

  // Returns a pointer to the specified index in the ChunkManager.
  // This version of the function takes a chunk index and an offset in the
  // chunk.
  const uint8_t* at(std::size_t chunk_index, std::size_t offset_in_chunk) const;
  uint8_t* at(std::size_t chunk_index, std::size_t offset_in_chunk);

  // Returns the capacity of the ChunkManager.
  std::size_t capacity() const;

  // Returns an encoded index that can be used to retrieve the chunk index
  // and offset in the chunk. The chunk index is stored in the upper 32 bits,
  // and the offset in the chunk is stored in the lower 32 bits.
  std::uint64_t encode_index(std::size_t chunk_index,
                             std::size_t offset_in_chunk);

  // Returns the chunk index from the encoded index.
  static std::size_t chunk_index(std::uint64_t encoded_index);

  // Returns the offset in the chunk from the encoded index.
  static std::size_t offset_in_chunk(std::uint64_t encoded_index);

  // Given a chunk_index, returns the size of the chunk at that index.
  std::size_t chunk_size_at_index(std::size_t chunk_index) const {
    return chunk_size_ * (1ull << chunk_index);
  }

 private:
  // Loads the number of chunks from the first chunk and adds any necessary
  // chunks. Returns the number of chunks that were added.
  std::size_t load_chunks_if_necessary();

  // num_chunks consists of two 32-bit quantities: the number of increments and
  // the number of decrements. 32-bit quantities into a single number that
  // represents the number of chunks.
  static std::uint64_t decode_num_chunks(uint64_t num_chunks_encoded);

  // Increments the first quantity of the encoded num_chunks.
  static std::uint64_t increment_num_chunks(std::uint64_t num_chunks_encoded,
                                            std::uint64_t value = 1ull);

  // Increments the second quanity of the encoded num_chunks.
  static std::uint64_t decrement_num_chunks(std::uint64_t num_chunks_encoded,
                                            std::uint64_t value = 1ull);

  // Sets the num_chunks to the specified value. If the specified value is less
  // than the current value, then the function increments the second quantity of
  // the encoded num_chunks. If the specified value is greater than the current
  // value, then the function increments the first quantity of the encoded
  // num_chunks.
  static std::uint64_t set_num_chunks(std::uint64_t num_chunks_encoded,
                                      std::uint64_t num_chunks);

  // Prefix for the names of the shared memory buffers.
  std::string name_prefix_;

  // The size of the first chunk in bytes.
  std::size_t chunk_size_;

  // Vector of buffers that store the chunks of the ChunkManager.
  // This is mutable because the vector is modified when a new chunk is added or
  // removed which can happen even when just reading from the ChunkManager.
  std::vector<std::unique_ptr<Buffer>> chunks_;

  // The number of chunks in the ChunkManager, cached from the first chunk for
  // performance.
  std::atomic<std::uint64_t>* num_chunks_encoded_;

  // Mutex for protecting the chunks vector. This is needed because the vector
  // is modified when a new chunk is added or removed.
  mutable std::shared_mutex chunks_rw_mutex_;

  // BufferFactory creates either private or shared memory buffers.
  BufferFactory* buffer_factory_;
};

#endif  // CHUNK_MANAGER_H_

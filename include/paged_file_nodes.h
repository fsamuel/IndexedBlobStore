#ifndef PAGED_FILE_NODES_H_
#define PAGED_FILE_NODES_H_

#include <cstddef>

#include "blob_store_object.h"

constexpr bool IsPowerOfTwo(std::size_t x) {
  return (x != 0u) && ((x & (x - 1u)) == 0u);
}

template <std::size_t BlockSize>
struct DirectBlock {
  // 4KB
  char data[BlockSize];
  static_assert(IsPowerOfTwo(BlockSize), "BlockSize must be a power of two.");
};

template <std::size_t BlockSize>
struct IndirectBlock {
  static constexpr std::size_t MAX_CHILDREN = BlockSize / sizeof(std::size_t);

  std::size_t children[MAX_CHILDREN];

  static_assert(IsPowerOfTwo(BlockSize), "BlockSize must be a power of two.");
};

template <std::size_t NumBlocks, std::size_t BlockSize>
struct INode {
  static constexpr std::size_t NUM_BLOCKS = NumBlocks;
  static constexpr std::size_t NUM_DIRECT_BLOCKS = NUM_BLOCKS / 3;
  static constexpr std::size_t NUM_INDIRECT_BLOCKS = NUM_BLOCKS / 3;
  static constexpr std::size_t NUM_DIRECT_BLOCKS_PER_INDIRECT_BLOCK =
      IndirectBlock<BlockSize>::MAX_CHILDREN;
  static constexpr std::size_t NUM_DOUBLY_INDIRECT_BLOCKS = NUM_BLOCKS / 3;

  // Size of the file
  std::size_t size;

  // Capacity of the file
  std::size_t capacity;

  // Block ID of the root block
  std::size_t direct_block_ids[NUM_DIRECT_BLOCKS];
  std::size_t indirect_block_ids[NUM_INDIRECT_BLOCKS];
  std::size_t doubly_indirect_block_ids[NUM_DOUBLY_INDIRECT_BLOCKS];

  static_assert(NUM_BLOCKS % 3 == 0, "NUM_BLOCKS Must be divisible by 3.");
};

#endif  // PAGED_FILE_NODES_H_
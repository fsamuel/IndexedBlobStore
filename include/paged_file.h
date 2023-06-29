#ifndef PAGED_FILE_H_
#define PAGED_FILE_H_

#include <algorithm>
#include <cstddef>
#include <queue>
#include <stack>

#include "blob_store.h"
#include "blob_store_object.h"
#include "paged_file_nodes.h"

template <std::size_t NumBlocks, std::size_t BlockSize>
class PagedFile {
 public:
  using INode = INode<NumBlocks, BlockSize>;
  using DirectBlock = DirectBlock<BlockSize>;
  using IndirectBlock = IndirectBlock<BlockSize>;

  PagedFile(BlobStore* blob_store, BlobStoreObject<INode> inode)
      : blob_store_(blob_store), inode_(std::move(inode)) {}

  static PagedFile Create(BlobStore* blob_store) {
    BlobStoreObject<INode> inode = blob_store->New<INode>();
    inode->size = 0;
    return PagedFile(blob_store, std::move(inode));
  }

  static PagedFile Open(BlobStore* blob_store, std::size_t index) {
    BlobStoreObject<INode> inode = blob_store->GetMutable<INode>(index);
    return PagedFile(blob_store, std::move(inode));
  }

  template <class T>
  BlobStoreObject<T> GetOrCreate(std::size_t* blob_id) {
    BlobStoreObject<T> obj = blob_store_->GetMutable<T>(*blob_id);
    if (obj == nullptr) {
      obj = blob_store_->New<T>();
      *blob_id = obj.Index();
    }
    return obj;
  }

  // Given a block_index, find a corresponding direct block through potentially
  // multiple layers of indirection.
  BlobStoreObject<DirectBlock> GetOrCreateDirectBlock(std::size_t block_index) {
    constexpr std::size_t NumDirectBlocks = INode::NUM_DIRECT_BLOCKS;
    constexpr std::size_t NumIndirectBlocks = INode::NUM_INDIRECT_BLOCKS;
    constexpr std::size_t NumDoublyIndirectBlocks =
        INode::NUM_DOUBLY_INDIRECT_BLOCKS;

    if (block_index < NumDirectBlocks) {
      return GetOrCreate<DirectBlock>(&inode_->direct_block_ids[block_index]);
    }

    block_index -= NumDirectBlocks;
    constexpr std::size_t IndirectBlockCapacity =
        BlockSize / sizeof(std::size_t);
    if (block_index < IndirectBlockCapacity * NumIndirectBlocks) {
      std::size_t indirect_block_index = block_index / IndirectBlockCapacity;
      std::size_t indirect_block_offset = block_index % IndirectBlockCapacity;
      BlobStoreObject<IndirectBlock> indirect_block =
          GetOrCreate<IndirectBlock>(
              &inode_->indirect_block_ids[indirect_block_index]);
      return GetOrCreate<DirectBlock>(
          &indirect_block->children[indirect_block_offset]);
    }

    block_index -= IndirectBlockCapacity * NumIndirectBlocks;
    constexpr std::size_t DoublyIndirectBlockCapacity =
        IndirectBlockCapacity * IndirectBlockCapacity;
    if (block_index < DoublyIndirectBlockCapacity * NumDoublyIndirectBlocks) {
      // This indicates which doubly indirect block we're in.
      std::size_t doubly_indirect_block_index =
          block_index / DoublyIndirectBlockCapacity;
      // Within the doubly indirect block, this indicates which indirect block
      // we're in.
      std::size_t indirect_block_index =
          (block_index / IndirectBlockCapacity) % IndirectBlockCapacity;
      // Within the indirect block, this indicates which direct block we're in.
      std::size_t indirect_block_offset = block_index % IndirectBlockCapacity;
      BlobStoreObject<IndirectBlock> doubly_indirect_block =
          GetOrCreate<IndirectBlock>(
              &inode_->doubly_indirect_block_ids[doubly_indirect_block_index]);

      BlobStoreObject<IndirectBlock> indirect_block =
          GetOrCreate<IndirectBlock>(
              &doubly_indirect_block->children[indirect_block_index]);

      return GetOrCreate<DirectBlock>(
          &indirect_block->children[indirect_block_offset]);
    }

    return BlobStoreObject<DirectBlock>();
  }

  // TODO(fsamuel): Any write operation should increment the version
  // counter associated with the node. Suppose the version of a 
  // BlobStoreObject was stored as part of the ID to it. We can then
  // very quickly identify whether the current node needs to be updated
  // or not.
  void Write(const char* data, std::size_t data_size) {
    std::size_t remaining_data_size = data_size;
    while (remaining_data_size > 0) {
      std::size_t block_index = pos_ / BlockSize;
      BlobStoreObject<DirectBlock> direct_block =
          GetOrCreateDirectBlock(block_index);
      if (direct_block == nullptr) {
        // We ran out of space.
        // TODO(fsamuel): Handle this case.
        return;
      }
      std::size_t data_offset = pos_ % BlockSize;
      std::size_t bytes_to_write =
          std::min(remaining_data_size, BlockSize - data_offset);
      std::memcpy(direct_block->data + data_offset, data, bytes_to_write);
      pos_ += bytes_to_write;
      data += bytes_to_write;
      remaining_data_size -= bytes_to_write;
    }
    inode_->size = std::max(inode_->size, pos_);
  }

  // We should have a pure GetDirectBlock for reads. We should never
  // be creating any blocks on reads. 
  void Read(char* data, std::size_t size) {
    std::size_t remaining_data_size = size;
    while (remaining_data_size > 0) {
      std::size_t block_index = pos_ / BlockSize;
      BlobStoreObject<DirectBlock> direct_block =
          GetOrCreateDirectBlock(block_index);

      std::size_t data_offset = pos_ % BlockSize;
      std::size_t bytes_to_read =
          std::min(remaining_data_size, BlockSize - data_offset);
      if (direct_block != nullptr) {
          // We support sparse files so a missing block isn't necessarily a problem.
          std::memcpy(data, direct_block->data + data_offset, bytes_to_read);
      } else {
		  std::memset(data, 0, bytes_to_read);
	  }
      pos_ += bytes_to_read;
      data += bytes_to_read;
      remaining_data_size -= bytes_to_read;
    }
  }

  void Seek(std::size_t offset) { pos_ = offset; }

  std::size_t Tell() const { return pos_; }

  std::size_t GetSize() const { return inode_->size;  }

 private:
  // Position within the file
  std::size_t pos_ = 0;
  BlobStore* blob_store_;
  BlobStoreObject<INode> inode_;
};

#endif  // PAGED_FILE_H_
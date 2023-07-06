#ifndef PAGED_FILE_H_
#define PAGED_FILE_H_

#include <algorithm>
#include <cstddef>
#include <queue>
#include <stack>

#include "blob_store.h"
#include "blob_store_object.h"
#include "blob_store_transaction.h"
#include "paged_file_base.h"
#include "paged_file_nodes.h"
#include "paged_file_transaction.h"

namespace paged_file {

using blob_store::BlobStore;
using blob_store::BlobStoreObject;
using blob_store::HeadNode;

// A file that is stored in a blob store. The file is divided into blocks of
// size BlockSize. The file is represented by an inode, which contains the
// size of the file and the block ids of the blocks that make up the file.
template <std::size_t NumBlocks, std::size_t BlockSize>
class PagedFile : public PagedFileBase<NumBlocks, BlockSize> {
 public:
  using Transaction = PagedFileBase<NumBlocks, BlockSize>::Transaction;

  // Create a new file with no data. Only the inode is allocated.
  static PagedFile Create(BlobStore* blob_store);

  // Open an existing file with the given inode_index.
  static PagedFile Open(BlobStore* blob_store, std::size_t head_index);

  Transaction CreateTransaction();

  // Write data with the provided size to the file at the current position.
  // If the file is not large enough to hold the data, it will be extended.
  // The file position will be updated to the end of the written data.
  void Write(const char* data, std::size_t data_size);

  // Read data with the provided size from the file at the current position.
  // Returns the number of bytes actually read. The file position will be
  // updated to the end of the read data.
  std::size_t Read(char* data, std::size_t size);

  // Seek to the given offset in the file.
  void Seek(std::size_t offset) { pos_ = offset; }

  // Return the current position in the file.
  std::size_t Tell() const { return pos_; }

  // Return the size of the file.
  std::size_t GetSize() const {
    BlobStoreObject<const HeadNode> head_node =
        blob_store_->Get<HeadNode>(head_index_);
    BlobStoreObject<const INode> inode =
        blob_store_->Get<INode>(head_node->root_index);
    return inode->size;
  }

 private:
  using INode = INode<NumBlocks, BlockSize>;
  using DirectBlock = DirectBlock<BlockSize>;
  using IndirectBlock = IndirectBlock<BlockSize>;

  // PagedFileBase implementation:
  void Write(Transaction* transaction,
             const char* data,
             std::size_t data_size) override;
  std::size_t Read(Transaction* transaction,
                   char* data,
                   std::size_t size) override;

  PagedFile(BlobStore* blob_store, std::size_t head_index)
      : blob_store_(blob_store), head_index_(head_index) {}

  template <class T>
  BlobStoreObject<const T> GetBlock(std::size_t block_id) {
    return blob_store_->Get<T>(block_id);
  }

  template <class T>
  BlobStoreObject<T> GetOrCreateBlock(Transaction* transaction,
                                      std::size_t* block_id) {
    BlobStoreObject<const T> obj = blob_store_->Get<T>(*block_id);
    BlobStoreObject<T> new_obj;
    if (obj == nullptr) {
      new_obj = transaction->New<T>();
    } else {
      new_obj = transaction->GetMutable<T>(std::move(obj));
    }
    // Even if we're not calling New, GetMutable may create a
    // clone of the object, so we need to update the block_id.
    *block_id = new_obj.Index();
    return new_obj;
  }

  std::size_t GetIndirectBlockChildID(std::size_t indirect_block_id,
                                      std::size_t block_index) {
    BlobStoreObject<const IndirectBlock> indirect_block =
        GetBlock<IndirectBlock>(indirect_block_id);
    return indirect_block != nullptr ? indirect_block->children[block_index]
                                     : 0;
  }

  template <typename T, typename ChildType>
  BlobStoreObject<T> GetOrCreateBlockWithChild(Transaction* transaction,
                                               std::size_t* block_id,
                                               std::size_t child_index) {
    BlobStoreObject<T> block = GetOrCreateBlock<T>(transaction, block_id);
    if (block->children[child_index] == 0) {
      BlobStoreObject<ChildType> child_block = transaction->New<ChildType>();
      block->children[child_index] = child_block.Index();
    }
    return block;
  }

  // Given a block_index, find a corresponding direct block through potentially
  // multiple layers of indirection. If the block does not exist, return a
  // nullptr.
  BlobStoreObject<const DirectBlock> GetDirectBlock(
      BlobStoreObject<const INode> inode,
      std::size_t block_index);

  // Given a block_index, find a corresponding direct block through potentially
  // multiple layers of indirection. If the block does not exist, create it.
  BlobStoreObject<DirectBlock> GetOrCreateDirectBlock(
      Transaction* transaction,
      BlobStoreObject<INode> inode,
      std::size_t block_index);

  // Position within the file
  std::size_t pos_ = 0;
  BlobStore* blob_store_;
  std::size_t head_index_;
};

template <std::size_t NumBlocks, std::size_t BlockSize>
PagedFile<NumBlocks, BlockSize> PagedFile<NumBlocks, BlockSize>::Create(
    BlobStore* blob_store) {
  BlobStoreObject<HeadNode> head = blob_store->New<HeadNode>();
  auto root = blob_store->New<INode>();
  head->root_index = root.Index();
  head->previous = BlobStore::InvalidIndex;
  root->size = 0;
  return PagedFile(blob_store, head.Index());
}

template <std::size_t NumBlocks, std::size_t BlockSize>
PagedFile<NumBlocks, BlockSize> PagedFile<NumBlocks, BlockSize>::Open(
    BlobStore* blob_store,
    std::size_t head_index) {
  return PagedFile(blob_store, head_index);
}

template <std::size_t NumBlocks, std::size_t BlockSize>
typename PagedFileBase<NumBlocks, BlockSize>::Transaction
PagedFile<NumBlocks, BlockSize>::CreateTransaction() {
  Transaction transaction(this, blob_store_, head_index_);
  return transaction;
}

template <std::size_t NumBlocks, std::size_t BlockSize>
void PagedFile<NumBlocks, BlockSize>::Write(Transaction* transaction,
                                            const char* data,
                                            std::size_t data_size) {
  BlobStoreObject<const INode> inode = transaction->GetRootNode<INode>();
  BlobStoreObject<INode> new_inode =
      transaction->GetMutable<INode>(std::move(inode));
  transaction->SetRootNode(new_inode.Index());

  std::size_t remaining_data_size = data_size;
  while (remaining_data_size > 0) {
    std::size_t block_index = transaction->Tell() / BlockSize;
    BlobStoreObject<DirectBlock> direct_block =
        GetOrCreateDirectBlock(transaction, new_inode, block_index);
    if (direct_block == nullptr) {
      // We ran out of space.
      // TODO(fsamuel): Handle this case.
      break;
    }
    std::size_t data_offset = transaction->Tell() % BlockSize;
    std::size_t bytes_to_write =
        std::min(remaining_data_size, BlockSize - data_offset);
    std::memcpy(direct_block->data + data_offset, data, bytes_to_write);
    transaction->Seek(transaction->Tell() + bytes_to_write);
    data += bytes_to_write;
    remaining_data_size -= bytes_to_write;
  }
  new_inode->size = std::max(new_inode->size, transaction->Tell());
}

template <std::size_t NumBlocks, std::size_t BlockSize>
std::size_t PagedFile<NumBlocks, BlockSize>::Read(Transaction* transaction,
                                                  char* data,
                                                  std::size_t size) {
  BlobStoreObject<const INode> inode = transaction->GetRootNode<INode>();

  std::size_t remaining_data_size = size;
  std::size_t file_size = inode->size;
  while (remaining_data_size > 0 && transaction->Tell() < file_size) {
    std::size_t block_index = transaction->Tell() / BlockSize;
    BlobStoreObject<const DirectBlock> direct_block =
        GetDirectBlock(inode, block_index);

    std::size_t data_offset = transaction->Tell() % BlockSize;
    std::size_t bytes_to_read =
        std::min(remaining_data_size, BlockSize - data_offset);
    if (direct_block != nullptr) {
      // We support sparse files so a missing block isn't necessarily a
      // problem.
      std::memcpy(data, direct_block->data + data_offset, bytes_to_read);
    } else {
      std::memset(data, 0, bytes_to_read);
    }
    transaction->Seek(transaction->Tell() + bytes_to_read);
    data += bytes_to_read;
    remaining_data_size -= bytes_to_read;
  }
  return size - remaining_data_size;
}

template <std::size_t NumBlocks, std::size_t BlockSize>
BlobStoreObject<const typename PagedFile<NumBlocks, BlockSize>::DirectBlock>
PagedFile<NumBlocks, BlockSize>::GetDirectBlock(
    BlobStoreObject<const INode> inode,
    std::size_t block_index) {
  constexpr std::size_t IndirectBlockCapacity = BlockSize / sizeof(std::size_t);

  if (block_index < INode::NUM_DIRECT_BLOCKS) {
    return GetBlock<DirectBlock>(inode->direct_block_ids[block_index]);
  }

  block_index -= INode::NUM_DIRECT_BLOCKS;

  if (block_index < IndirectBlockCapacity * INode::NUM_INDIRECT_BLOCKS) {
    std::size_t indirect_block_index = block_index / IndirectBlockCapacity;
    std::size_t block_id =
        GetIndirectBlockChildID(inode->indirect_block_ids[indirect_block_index],
                                block_index % IndirectBlockCapacity);
    return GetBlock<DirectBlock>(block_id);
  }

  block_index -= IndirectBlockCapacity * INode::NUM_INDIRECT_BLOCKS;

  if (block_index < IndirectBlockCapacity * IndirectBlockCapacity *
                        INode::NUM_DOUBLY_INDIRECT_BLOCKS) {
    std::size_t doubly_indirect_block_index =
        block_index / (IndirectBlockCapacity * IndirectBlockCapacity);
    std::size_t indirect_block_index =
        (block_index / IndirectBlockCapacity) % IndirectBlockCapacity;
    std::size_t indirect_block_id = GetIndirectBlockChildID(
        inode->doubly_indirect_block_ids[doubly_indirect_block_index],
        indirect_block_index);
    std::size_t block_id = GetIndirectBlockChildID(
        indirect_block_id, block_index % IndirectBlockCapacity);
    return GetBlock<DirectBlock>(block_id);
  }

  return BlobStoreObject<const DirectBlock>();
}

template <std::size_t NumBlocks, std::size_t BlockSize>
BlobStoreObject<typename PagedFile<NumBlocks, BlockSize>::DirectBlock>
PagedFile<NumBlocks, BlockSize>::GetOrCreateDirectBlock(
    Transaction* transaction,
    BlobStoreObject<INode> inode,
    std::size_t block_index) {
  constexpr std::size_t IndirectBlockCapacity = BlockSize / sizeof(std::size_t);

  if (block_index < INode::NUM_DIRECT_BLOCKS) {
    return GetOrCreateBlock<DirectBlock>(transaction,
                                         &inode->direct_block_ids[block_index]);
  }

  block_index -= INode::NUM_DIRECT_BLOCKS;

  if (block_index < IndirectBlockCapacity * INode::NUM_INDIRECT_BLOCKS) {
    std::size_t indirect_block_index = block_index / IndirectBlockCapacity;
    BlobStoreObject<IndirectBlock> indirect_block =
        GetOrCreateBlockWithChild<IndirectBlock, DirectBlock>(
            transaction, &inode->indirect_block_ids[indirect_block_index],
            block_index % IndirectBlockCapacity);
    BlobStoreObject<const DirectBlock> direct_block =
        blob_store_->Get<DirectBlock>(
            indirect_block->children[block_index % IndirectBlockCapacity]);
    return transaction->GetMutable<DirectBlock>(std::move(direct_block));
  }

  block_index -= IndirectBlockCapacity * INode::NUM_INDIRECT_BLOCKS;

  if (block_index < IndirectBlockCapacity * IndirectBlockCapacity *
                        INode::NUM_DOUBLY_INDIRECT_BLOCKS) {
    std::size_t doubly_indirect_block_index =
        block_index / (IndirectBlockCapacity * IndirectBlockCapacity);
    std::size_t indirect_block_index =
        (block_index / IndirectBlockCapacity) % IndirectBlockCapacity;
    BlobStoreObject<IndirectBlock> doubly_indirect_block =
        GetOrCreateBlockWithChild<IndirectBlock, IndirectBlock>(
            transaction,
            &inode->doubly_indirect_block_ids[doubly_indirect_block_index],
            indirect_block_index);
    BlobStoreObject<IndirectBlock> indirect_block =
        GetOrCreateBlockWithChild<IndirectBlock, DirectBlock>(
            transaction, &doubly_indirect_block->children[indirect_block_index],
            block_index % IndirectBlockCapacity);
    BlobStoreObject<const DirectBlock> direct_block =
        blob_store_->Get<DirectBlock>(
            indirect_block->children[block_index % IndirectBlockCapacity]);
    return transaction->GetMutable<DirectBlock>(std::move(direct_block));
  }

  return BlobStoreObject<DirectBlock>();
}

template <std::size_t NumBlocks, std::size_t BlockSize>
void PagedFile<NumBlocks, BlockSize>::Write(const char* data,
                                            std::size_t data_size) {
  std::size_t new_pos;
  while (true) {
    Transaction transaction = CreateTransaction();
    transaction.Seek(pos_);
    Write(&transaction, data, data_size);
    new_pos = transaction.Tell();
    if (std::move(transaction).Commit()) {
      break;
    }
  }
  pos_ = new_pos;
}

template <std::size_t NumBlocks, std::size_t BlockSize>
std::size_t PagedFile<NumBlocks, BlockSize>::Read(char* data,
                                                  std::size_t size) {
  std::size_t new_pos;
  std::size_t bytes_read;
  while (true) {
    Transaction transaction = CreateTransaction();
    transaction.Seek(pos_);
    bytes_read = Read(&transaction, data, size);
    new_pos = transaction.Tell();
    if (std::move(transaction).Commit()) {
      break;
    }
  }
  pos_ = new_pos;
  return bytes_read;
}

}  // namespace paged_file

#endif  // PAGED_FILE_H_
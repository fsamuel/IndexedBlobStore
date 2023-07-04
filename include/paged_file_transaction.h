#ifndef PAGED_FILE_TRANSACTION_H_
#define PAGED_FILE_TRANSACTION_H_

#include "blob_store_object.h"
#include "blob_store_transaction.h"
#include "paged_file_base.h"
#include "paged_file_nodes.h"

namespace paged_file {
using blob_store::BlobStore;
using blob_store::BlobStoreObject;

template <std::size_t NumBlocks, std::size_t BlockSize>
class Transaction : public blob_store::Transaction {
 public:
  using PagedFileBase = PagedFileBase<NumBlocks, BlockSize>;
  using INode = INode<NumBlocks, BlockSize>;

  Transaction(PagedFileBase* paged_file, BlobStore* store, size_t head_index)
      : blob_store::Transaction(store, head_index), paged_file_(paged_file) {}

  void Write(const char* data, std::size_t data_size) {
    paged_file_->Write(this, data, data_size);
  }

  std::size_t Read(char* data, std::size_t size) {
    return paged_file_->Read(this, data, size);
  }

  // Seek to the given offset in the file.
  void Seek(std::size_t offset) { pos_ = offset; }

  // Return the current position in the file.
  std::size_t Tell() const { return pos_; }

  // Return the size of the file.
  std::size_t GetSize() const {
    BlobStoreObject<const INode> inode = GetRootNode<INode>();
    return inode->size;
  }

 private:
  std::size_t pos_ = 0;
  PagedFileBase* paged_file_;
};

}  // namespace paged_file

#endif  // PAGED_FILE_TRANSACTION_H_
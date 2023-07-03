#ifndef PAGED_FILE_TRANSACTION_H_
#define PAGED_FILE_TRANSACTION_H_

#include "blob_store_object.h"
#include "blob_store_transaction.h"
#include "paged_file_nodes.h"
#include "paged_file.h"

namespace paged_file {

    template <std::size_t NumBlocks, std::size_t BlockSize>
	class Transaction : public blob_store::Transaction {
    public:
        using PagedFile = PagedFile<NumBlocks, BlockSize>;
        using INode = INode<NumBlocks, BlockSize>;

        Transaction(PagedFile* paged_file, BlobStore* store, size_t head_index)
            : blob_store::Transaction(store, head_index), paged_file_(paged_file) {}


    private:
        std::size_t pos_ = 0;
        BlobStoreObject<INode> inode_;
        PagedFile* paged_file_;
    };

}  // namespace paged_file

#endif  // PAGED_FILE_TRANSACTION_H_
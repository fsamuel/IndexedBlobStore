#ifndef PAGED_FILE_BASE_H_
#define PAGED_FILE_BASE_H_

#include <cstddef>

namespace paged_file {
template <std::size_t NumBlocks, std::size_t BlockSize>
class Transaction;

template <std::size_t NumBlocks, std::size_t BlockSize>
class PagedFileBase {
 public:
  using Transaction = Transaction<NumBlocks, BlockSize>;

  // Write data with the provided size to the file at the current position.
  // If the file is not large enough to hold the data, it will be extended.
  // The file position will be updated to the end of the written data.
  virtual void Write(Transaction* transaction,
                     const char* data,
                     std::size_t data_size) = 0;

  // Read data with the provided size from the file at the current position.
  // Returns the number of bytes actually read. The file position will be
  // updated to the end of the read data.
  virtual std::size_t Read(Transaction* transaction,
                           char* data,
                           std::size_t size) = 0;
};

}  // namespace paged_file

#endif  // PAGED_FILE_BASE_H_
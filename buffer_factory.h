#ifndef BUFFER_FACTORY_H_
#define BUFFER_FACTORY_H_

#include <memory>
#include "buffer.h"

// A BufferFactory is a factory class that creates buffers used by
// ChunkManager, and ChunkVector. It is used to allow testing of these
// classes without having to create on-disk buffers.
class BufferFactory {
 public:
  virtual std::unique_ptr<Buffer> CreateBuffer(const std::string& name,
                                               std::size_t size) = 0;
};

#endif
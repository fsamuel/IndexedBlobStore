#ifndef SHARED_MEMORY_BUFFER_FACTORY_
#define SHARED_MEMORY_BUFFER_FACTORY_

#include "buffer_factory.h"
#include "shared_memory_buffer.h"

class SharedMemoryBufferFactory : public BufferFactory {
 public:
  static BufferFactory* Get() {
    static SharedMemoryBufferFactory instance;
    return &instance;
  }

  std::unique_ptr<Buffer> CreateBuffer(const std::string& name,
                                       size_t size) override {
    return std::unique_ptr<Buffer>(new SharedMemoryBuffer(name, size));
  }
};

#endif  // SHARED_MEMORY_BUFFER_FACTORY_
#ifndef TEST_MEMORY_BUFFER_FACTORY_H_
#define TEST_MEMORY_BUFFER_FACTORY_H_

#include <memory>

#include "buffer_factory.h"
#include "test_memory_buffer.h"

// Constructs in-memory-only buffers for testing.
class TestMemoryBufferFactory : public BufferFactory {
 public:
  static BufferFactory* Get() {
    static TestMemoryBufferFactory instance;
    return &instance;
  }

  std::unique_ptr<Buffer> CreateBuffer(const std::string& name,
                                       size_t size) override {
    return std::unique_ptr<Buffer>(new TestMemoryBuffer(name, size));
  }
};

#endif  // TEST_MEMORY_BUFFER_FACTORY_H_
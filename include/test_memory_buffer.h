#ifndef TEST_MEMORY_BUFFER_H_
#define TEST_MEMORY_BUFFER_H_

#include <memory>
#include "buffer.h"

// A buffer that is backed by a memory buffer.
class TestMemoryBuffer : public Buffer {
 public:
  TestMemoryBuffer(const std::string& name, size_t size)
      : name_(name), size_(size), buffer_(new char[size]) {
    // TODO(fsamuel): The fact that we need to do this suggests that the
    // code that uses this class is brittle. We should fix that.
    std::memset(buffer_.get(), 0, size);
  }

  ~TestMemoryBuffer() override {}

  // Return the name of the memory-mapped file
  const std::string& GetName() const override { return name_; }

  // Return the size of the memory-mapped file
  std::size_t GetSize() const override { return size_; }

  // Return a pointer to the start of the memory-mapped file
  void* GetData() override { return reinterpret_cast<void*>(buffer_.get()); }

  // Return a const pointer to the start of the memory-mapped file
  const void* GetData() const override {
    return const_cast<TestMemoryBuffer*>(this)->GetData();
  }

 private:
  std::string name_;
  size_t size_;
  std::unique_ptr<char[]> buffer_;
};

#endif  // TEST_MEMORY_BUFFER_H_
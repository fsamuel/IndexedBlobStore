#ifndef BUFFER_H_
#define BUFFER_H_

#include <string>

class Buffer {
 public:
  virtual ~Buffer() = default;

  // Return the name of the memory-mapped file
  virtual const std::string& GetName() const = 0;

  // Return the size of the memory-mapped file
  virtual std::size_t GetSize() const = 0;

  // Return a pointer to the start of the memory-mapped file
  virtual void* GetData() = 0;

  // Return a const pointer to the start of the memory-mapped file
  virtual const void* GetData() const = 0;
};

#endif  // BUFFER_H_
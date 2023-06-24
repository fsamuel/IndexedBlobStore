#ifndef BUFFER_H_
#define BUFFER_H_

#include <string>

// An abstract class representing a buffer of data.That buffer may be in-memory
// only or it may be backed by a file or other storage medium.
class Buffer {
 public:
  virtual ~Buffer() = default;

  // Return the name of the buffer.
  virtual const std::string& GetName() const = 0;

  // Return the size of buffer.
  virtual std::size_t GetSize() const = 0;

  // Return a pointer to the start of the buffer.
  virtual void* GetData() = 0;

  // Return a const pointer to the start of the buffer.
  virtual const void* GetData() const = 0;
};

#endif  // BUFFER_H_
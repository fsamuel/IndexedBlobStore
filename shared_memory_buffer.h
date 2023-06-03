#ifndef SHARED_MEMORY_BUFFER_H_
#define SHARED_MEMORY_BUFFER_H_

#include <cstddef>
#include <string>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <Windows.h>
#else
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

class SharedMemoryBuffer {
 public:
  // Move constructor
  SharedMemoryBuffer(SharedMemoryBuffer&& other) noexcept;

  // Constructor that opens an existing memory-mapped file with the given name
  SharedMemoryBuffer(const std::string& name);

  // Constructor that creates a new memory-mapped file with the given name and
  // size
  SharedMemoryBuffer(const std::string& name, std::size_t size);

  // Disable copy constructor
  SharedMemoryBuffer(const SharedMemoryBuffer&) = delete;

  // Disable copy assignment operator
  SharedMemoryBuffer& operator=(const SharedMemoryBuffer&) = delete;

  // Destructor
  ~SharedMemoryBuffer() noexcept;

  // Resize the memory-mapped file to the given size
  void Resize(std::size_t new_size);

  // Return the name of the memory-mapped file
  const std::string& name() const { return name_; }

  // Return the size of the memory-mapped file
  std::size_t size() const { return size_; }

  // Return a pointer to the start of the memory-mapped file
  void* data() { return data_; }

  // Return a const pointer to the start of the memory-mapped file
  const void* data() const { return data_; }

  // Flush the memory-mapped buffer to disk
  void flush();

  // Move assignment operator
  SharedMemoryBuffer& operator=(SharedMemoryBuffer&& other) noexcept {
    UnmapMemory();
    CloseFile();
    name_ = std::move(other.name_);
    size_ = other.size_;
    data_ = other.data_;
    other.size_ = 0;
    other.data_ = nullptr;
    other.name_.clear();
#ifdef _WIN32
    file_handle_ = other.file_handle_;
    file_mapping_ = other.file_mapping_;
    other.file_handle_ = 0;
    other.file_mapping_ = 0;
#else
    file_descriptor_ = other.file_descriptor_;
    other.file_descriptor_ = -1;
#endif
    return *this;
  }

 private:
  // Open the memory-mapped file
  void OpenFile();

  // Close the memory-mapped file
  void CloseFile();

  // Map the memory-mapped file into memory
  void MapMemory(std::size_t size);

  // Unmap the memory-mapped file from memory
  void UnmapMemory();

  std::string name_;  // The name of the memory-mapped file
  std::size_t size_;  // The size of the memory-mapped file
  void* data_;        // A pointer to the start of the memory-mapped file
#ifdef _WIN32
  HANDLE file_handle_;   // The Windows file handle
  HANDLE file_mapping_;  // The Windows file mapping handle
#else
  int file_descriptor_;  // The Linux file descriptor
#endif
};

#endif  // SHARED_MEMORY_BUFFER_H_

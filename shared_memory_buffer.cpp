#include "shared_memory_buffer.h"
#include <algorithm>
#include <fstream>
#include <stdexcept>

SharedMemoryBuffer::SharedMemoryBuffer(SharedMemoryBuffer&& other) noexcept
    : name_(std::move(other.name_)),
      size_(other.size_),
      data_(other.data_)
#ifdef _WIN32
      ,
      file_handle_(other.file_handle_),
      file_mapping_(other.file_mapping_)
#else
      ,
      file_descriptor_(other.file_descriptor_)
#endif
{
  other.size_ = 0;
  other.data_ = nullptr;
#ifdef _WIN32
  other.file_handle_ = nullptr;
  other.file_mapping_ = nullptr;
#else
  other.file_descriptor_ = -1;
#endif
}

SharedMemoryBuffer::SharedMemoryBuffer(const std::string& name)
    : name_(name),
      size_(0),
      data_(nullptr)
#ifdef _WIN32
      ,
      file_handle_(nullptr),
      file_mapping_(nullptr)
#else
      ,
      file_descriptor_(-1)
#endif
{
  // Get the size of the file on disk
  std::ifstream file(name_, std::ios::binary | std::ios::ate);
  std::streampos size_on_disk = file.tellg();
  file.close();

  // If the file doesn't exist, set the size to 0
  if (size_on_disk == -1) {
    size_on_disk = 0;
  }

  OpenFile();
  MapMemory(size_on_disk);
}

SharedMemoryBuffer::SharedMemoryBuffer(const std::string& name,
                                       std::size_t size)
    : name_(name),
      size_(size),
      data_(nullptr)
#ifdef _WIN32
      ,
      file_handle_(nullptr),
      file_mapping_(nullptr)
#else
      ,
      file_descriptor_(-1)
#endif
{
  OpenFile();
  Resize(size);
}

SharedMemoryBuffer::~SharedMemoryBuffer() {
  UnmapMemory();
  CloseFile();
}

void SharedMemoryBuffer::Resize(std::size_t new_size) {
  // Unmap the memory-mapped file
  UnmapMemory();

#ifdef _WIN32
  // Windows implementation
  // Resize the file
  LARGE_INTEGER file_size{};
  file_size.QuadPart = new_size;
  if (!SetFilePointerEx(file_handle_, file_size, nullptr, FILE_BEGIN)) {
    throw std::runtime_error(
        "Failed to resize memory-mapped file: SetFilePointerEx");
  }
  if (!SetEndOfFile(file_handle_)) {
    throw std::runtime_error(
        "Failed to resize memory-mapped file: SetEndOfFile");
  }

  // Update the size
  size_ = new_size;

#else
  // Linux implementation
  // Resize the file
  if (ftruncate(file_descriptor_, new_size) == -1) {
    throw std::runtime_error("Failed to resize memory-mapped file");
  }
#endif
  // Map the memory-mapped file into memory
  MapMemory(new_size);
}

void SharedMemoryBuffer::flush() {
  if (data_ == nullptr || size_ == 0) {
    return;
  }

#ifdef _WIN32
  // Windows implementation
  if (!FlushViewOfFile(data_, size_)) {
    throw std::runtime_error(
        "Failed to flush memory-mapped file: FlushViewOfFile");
  }
  if (!FlushFileBuffers(file_handle_)) {
    throw std::runtime_error(
        "Failed to flush memory-mapped file: FlushFileBuffers");
  }
#else
  // Linux implementation
  if (msync(m_data, m_size, MS_SYNC) == -1) {
    throw std::runtime_error("Failed to flush memory-mapped file: msync");
  }
  if (fsync(file_descriptor_) == -1) {
    throw std::runtime_error("Failed to flush memory-mapped file: fsync");
  }
#endif
}

void SharedMemoryBuffer::OpenFile() {
#ifdef _WIN32
  // Windows implementation
  // Open the file for reading and writing
  file_handle_ = CreateFileA(name_.c_str(), GENERIC_READ | GENERIC_WRITE,
                             FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr,
                             OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
  if (file_handle_ == INVALID_HANDLE_VALUE) {
    throw std::runtime_error("Failed to open memory-mapped file");
  }

  // Get the file size
  size_ = GetFileSize(file_handle_, nullptr);

#else
  // Linux implementation
  // Open the file for reading and writing
  file_descriptor_ = open(m_name.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (file_descriptor_ == -1) {
    throw std::runtime_error("Failed to open memory-mapped file");
  }

  // Get the file size
  struct stat fileInfo;
  if (fstat(file_descriptor_, &fileInfo) == -1) {
    close(file_descriptor_);
    throw std::runtime_error("Failed to get memory-mapped file size");
  }
  m_file_size = fileInfo.st_size;

  // Map the file into memory
  MapMemory(m_file_size);
#endif
}

void SharedMemoryBuffer::CloseFile() {
#ifdef _WIN32
  // Windows implementation
  // Close the file mapping handle and file handle
  CloseHandle(file_mapping_);
  CloseHandle(file_handle_);
#else
  // Linux implementation
  // Close the file descriptor
  close(file_descriptor_);
#endif
}

void SharedMemoryBuffer::MapMemory(std::size_t size) {
#ifdef _WIN32
  // Windows implementation
  // Create or open the file mapping
  file_mapping_ = CreateFileMappingA(file_handle_, nullptr, PAGE_READWRITE, 0u,
                                     static_cast<DWORD>(size), nullptr);
  if (file_mapping_ == nullptr) {
    data_ = nullptr;
    return;
  }
  // Map the file into memory
  data_ = MapViewOfFile(file_mapping_, FILE_MAP_ALL_ACCESS, 0, 0,
                        static_cast<DWORD>(size));
  if (data_ == nullptr) {
    CloseHandle(file_mapping_);
    CloseHandle(file_handle_);
    data_ = nullptr;
    file_mapping_ = nullptr;
    file_handle_ = nullptr;
    return;
  }
#else
  // Linux implementation
  // Map the file into memory
  m_data = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED,
                file_descriptor_, 0);
  if (m_data == MAP_FAILED) {
    close(file_descriptor_);
    throw std::runtime_error("Failed to memory-map file");
  }
#endif
}

void SharedMemoryBuffer::UnmapMemory() {
#ifdef _WIN32
  // Windows implementation
  // Unmap the memory-mapped file
  if (data_ != nullptr) {
    UnmapViewOfFile(data_);
    data_ = nullptr;
  }
  if (file_mapping_ != nullptr) {
    CloseHandle(file_mapping_);
    file_mapping_ = nullptr;
  }
#else
  // Linux implementation
  // Unmap the memory-mapped file
  munmap(m_data, m_size);
#endif
}
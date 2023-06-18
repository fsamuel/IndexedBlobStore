#ifndef CHUNKED_VECTOR_H_
#define CHUNKED_VECTOR_H_

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "buffer_factory.h"
#include "shared_memory_buffer.h"

// ChunkedVector is a dynamic array-like data structure that uses
// SharedMemoryBuffer to allocate its memory in chunks. Each chunk is double the
// size of the previous chunk. It supports basic operations like push_back,
// pop_back, access at a particular index, and checking the size of the vector.
template <typename T>
class ChunkedVector {
 public:
  static constexpr std::size_t ElementSize = sizeof(T);

  // Constructs a ChunkedVector with the specified name_prefix for the shared
  // memory buffers. Each SharedMemoryBuffer will be named as name_prefix_i,
  // where i is the chunk index.
  ChunkedVector(BufferFactory* buffer_factory,
                const std::string& name_prefix,
                std::size_t requested_chunk_size);

  ChunkedVector(ChunkedVector&& other)
      : name_prefix_(std::move(other.name_prefix_)),
        size_(std::move(other.size_)),
        chunks_(std::move(other.chunks_)),
        chunk_size_(std::move(other.chunk_size_)),
        buffer_factory_(other.buffer_factory_) {}

  ChunkedVector& operator=(ChunkedVector&& other) {
    name_prefix_ = std::move(other.name_prefix_);
    size_ = std::move(other.size_);
    chunks_ = std::move(other.chunks_);
    chunk_size_ = std::move(other.chunk_size_);
  }

  // Returns the number of elements in the ChunkedVector.
  std::size_t size() const;

  // Returns true if the ChunkedVector is empty, false otherwise.
  bool empty() const;

  // Returns the capacity of the ChunkedVector.
  std::size_t capacity() const;

  template <typename... Args>
  size_t emplace_back(Args&&... args);

  // Adds an element to the end of the ChunkedVector.
  size_t push_back(const T& value);

  // Removes the last element of the ChunkedVector.
  void pop_back();

  // Returns the chunk index and byte offset of the element at the specified
  // index. The chunk index is the index of the chunk that contains the element.
  // The byte offset is the offset of the element from the start of the chunk.
  void chunk_index_and_offset(std::size_t index,
                              std::size_t* chunk_index,
                              std::size_t* byte_offset) const;

  // Returns the number of chunks that the ChunkVector is composed of.
  std::size_t num_chunks() const { return chunks_.size(); }

  // Accesses the element at the specified index in the ChunkedVector.
  T* at(std::size_t index);

  // Accesses the element at the specified index in the ChunkedVector (const
  // version).
  const T* at(std::size_t index) const;

  // Accesses the element at the specified index in the ChunkedVector.
  T& operator[](std::size_t index);

  // Accesses the element at the specified index in the ChunkedVector (const
  // version).
  const T& operator[](std::size_t index) const;

  // Ensures that the ChunkedVector has enough capacity to store the specified
  // number of elements.
  void reserve(std::size_t new_cap);

  // Resizes the ChunkedVector to the specified size, either truncating or
  // adding elements as necessary.
  void resize(std::size_t new_size);

 private:
  // Prefix for the names of the shared memory buffers.
  std::string name_prefix_;
  // The size of each chunk in bytes. This is always a multiple of ElementSize.
  const std::size_t chunk_size_;
  // Pointer to the size of the vector. This is stored at the start of the first
  // chunk.
  std::atomic_size_t* size_ = nullptr;
  // Vector of SharedMemoryBuffers that store the elements of the ChunkedVector.
  std::vector<std::unique_ptr<Buffer>> chunks_;
  // Mutex for protecting the chunks vector. This is needed because the vector
  // is modified when a new chunk is added.
  mutable std::shared_mutex chunks_rw_mutex_;

  BufferFactory* buffer_factory_;

  // Loads the existing shared memory buffers based on the current size of the
  // vector.
  void load_chunks();

  // Adds a new chunk to the vector with double the size of the previous chunk.
  void expand();
};

template <typename T>
ChunkedVector<T>::ChunkedVector(BufferFactory* buffer_factory,
                                const std::string& name_prefix,
                                std::size_t requested_chunk_size)
    : name_prefix_(name_prefix),
      chunk_size_(std::max(requested_chunk_size, ElementSize) / ElementSize *
                  ElementSize),
      buffer_factory_(buffer_factory) {
  // Load the first chunk and add it to the vector. The first chunk also stores
  // the size of the vector.
  chunks_.emplace_back(buffer_factory_->CreateBuffer(
      name_prefix_ + "_0", chunk_size_ + sizeof(std::size_t)));

  // Read the size from the first chunk
  size_ = reinterpret_cast<std::atomic_size_t*>(chunks_[0]->GetData());

  load_chunks();
}

template <typename T>
std::size_t ChunkedVector<T>::size() const {
  // The size is stored at the start of the first chunk.
  return *size_;
}

template <typename T>
bool ChunkedVector<T>::empty() const {
  return size() == 0;
}

template <typename T>
std::size_t ChunkedVector<T>::capacity() const {
  std::shared_lock<std::shared_mutex> lock(chunks_rw_mutex_);
  return (chunk_size_ * ((static_cast<std::size_t>(1) << chunks_.size()) - 1)) /
         ElementSize;
}

template <typename T>
void ChunkedVector<T>::load_chunks() {
  size_t size = *size_ / ElementSize;

  // Calculate the number of chunks needed
  size_t chunk_index;
  size_t byte_offset;
  chunk_index_and_offset(size, &chunk_index, &byte_offset);
  size_t num_chunks = chunk_index + 1;

  // Load the additional chunks
  for (std::size_t i = 1; i < num_chunks; ++i) {
    chunks_.emplace_back(buffer_factory_->CreateBuffer(
        name_prefix_ + "_" + std::to_string(i),
        chunk_size_ * (static_cast<std::size_t>(1) << i)));
  }
}

template <typename T>
void ChunkedVector<T>::expand() {
  std::unique_lock<std::shared_mutex> lock(chunks_rw_mutex_);
  chunks_.emplace_back(buffer_factory_->CreateBuffer(
      name_prefix_ + "_" + std::to_string(chunks_.size()),
      chunk_size_ * (static_cast<std::size_t>(1) << chunks_.size())));
}

template <typename T>
void ChunkedVector<T>::chunk_index_and_offset(std::size_t index,
                                              std::size_t* chunk_index,
                                              std::size_t* byte_offset) const {
  // Calculate the total byte offset for the desired index
  *byte_offset = index * ElementSize;

  // Calculate the number of chunks needed to reach the byte offset.
  // We start with chunk_index 0, which has a capacity of chunk_size_.
  *chunk_index = 0;
  std::size_t chunk_capacity = chunk_size_;

  // Increase the chunk_index and double the chunk_capacity until
  // we have enough capacity to reach the byte_offset.
  while (*byte_offset >= chunk_capacity) {
    *byte_offset -= chunk_capacity;
    chunk_capacity *= 2;
    ++(*chunk_index);
  }
  *byte_offset += (*chunk_index == 0 ? sizeof(size_t) : 0);
}

template <typename T>
template <typename... Args>
std::size_t ChunkedVector<T>::emplace_back(Args&&... args) {
  std::size_t old_size = size_->fetch_add(1);

  std::size_t chunk_index;
  std::size_t byte_offset;
  chunk_index_and_offset(old_size, &chunk_index, &byte_offset);
  while (true) {
    std::size_t num_chunks = 0;
    {
      std::shared_lock<std::shared_mutex> lock(chunks_rw_mutex_);
      num_chunks = chunks_.size();
    }
    if (chunk_index > num_chunks - 1) {
      expand();
      continue;
    }
    break;
  }
  T* element_ptr = reinterpret_cast<T*>(
      reinterpret_cast<char*>(chunks_[chunk_index]->GetData()) + byte_offset);
  new (element_ptr) T(std::forward<Args>(args)...);

  return old_size;
}

template <typename T>
size_t ChunkedVector<T>::push_back(const T& value) {
  return emplace_back(value);
}

template <typename T>
void ChunkedVector<T>::pop_back() {
  std::size_t old_size = size_->fetch_sub(1);
  if (old_size == 0) {
    throw std::out_of_range("Cannot pop_back from an empty ChunkedVector");
  }
}

template <typename T>
T* ChunkedVector<T>::at(std::size_t index) {
  while (true) {
    if (index >= size()) {
      // It's possible that even if this index existed earlier, it's no
      // longer there due to a pop on another thread.
      return nullptr;
    }
    std::size_t chunk_index, byte_offset;
    chunk_index_and_offset(index, &chunk_index, &byte_offset);
    std::unique_lock<std::shared_mutex> lock(chunks_rw_mutex_);
    if (chunk_index >= chunks_.size()) {
      // It can take time for the chunks vector to catch up with the size
      // in shared memory.
      continue;
    }
    return reinterpret_cast<T*>(
        reinterpret_cast<char*>(chunks_[chunk_index]->GetData()) + byte_offset);
  }
}

template <typename T>
const T* ChunkedVector<T>::at(std::size_t index) const {
  return const_cast<ChunkedVector*>(this)->at(index);
}

template <typename T>
T& ChunkedVector<T>::operator[](std::size_t index) {
  T* ptr = at(index);
  if (ptr == nullptr) {
    throw std::out_of_range("Index out of range");
  }
  return *ptr;
}

template <typename T>
const T& ChunkedVector<T>::operator[](std::size_t index) const {
  const T* ptr = at(index);
  if (ptr == nullptr) {
    throw std::out_of_range("Index out of range");
  }
  return *ptr;
}

template <typename T>
void ChunkedVector<T>::reserve(std::size_t new_cap) {
  std::size_t current_cap = capacity();
  if (new_cap <= current_cap) {
    // current capacity is enough, nothing to do
    return;
  }
  while (new_cap > current_cap) {
    expand();
    current_cap = capacity();
  }
}

template <typename T>
void ChunkedVector<T>::resize(std::size_t new_size) {
  std::size_t expected_size;
  do {
    expected_size = *size_;
    if (new_size > expected_size) {
      // If the new size is larger than the expected size, reserve space.
      reserve(new_size);
    }
  } while (
      !std::atomic_compare_exchange_strong(size_, &expected_size, new_size));
}

#endif  // CHUNKED_VECTOR_H_

#ifndef CHUNKED_VECTOR_H_
#define CHUNKED_VECTOR_H_

#include "shared_memory_buffer.h"
#include <cstddef>
#include <vector>
#include <memory>
#include <atomic>

// ChunkedVector is a dynamic array-like data structure that uses SharedMemoryBuffer
// to allocate its memory in chunks. Each chunk is double the size of the previous chunk.
// It supports basic operations like push_back, pop_back, access at a particular index,
// and checking the size of the vector.
template <typename T, std::size_t RequestedChunkSize>
class ChunkedVector {
public:
    static constexpr std::size_t max_size(std::size_t a, std::size_t b) {
        return a < b ? b : a;
    }

    static constexpr std::size_t ElementSize = sizeof(T);

    static constexpr std::size_t ChunkSize =
        max_size(ElementSize, RequestedChunkSize) / ElementSize * ElementSize;


    // Constructs a ChunkedVector with the specified name_prefix for the shared memory buffers.
    // Each SharedMemoryBuffer will be named as name_prefix_i, where i is the chunk index.
    explicit ChunkedVector(const std::string& name_prefix);

    ChunkedVector(ChunkedVector&& other):
        name_prefix_(std::move(other.name_prefix_)), chunks_(std::move(other.chunks_)) {}

    explicit ChunkedVector(SharedMemoryBuffer&& first_buffer):
        name_prefix_(first_buffer.name()) {
        chunks_.emplace_back(std::move(first_buffer));
    }

    ChunkedVector& operator=(ChunkedVector&& other) {
        name_prefix_ = std::move(other.name_prefix_);
        chunks_ = std::move(other.chunks_);
    }

    // Returns the number of elements in the ChunkedVector.
    std::size_t size() const;

    // Returns true if the ChunkedVector is empty, false otherwise.
    bool empty() const;

    // Returns the capacity of the ChunkedVector.
    std::size_t capacity() const;

    template <typename... Args>
    void emplace_back(Args&&... args);

    // Adds an element to the end of the ChunkedVector.
    void push_back(const T& value);

    // Removes the last element of the ChunkedVector.
    void pop_back();

    // Accesses the element at the specified index in the ChunkedVector.
    T& operator[](std::size_t index);

    // Accesses the element at the specified index in the ChunkedVector (const version).
    const T& operator[](std::size_t index) const;

    // Ensures that the ChunkedVector has enough capacity to store the specified number of elements.
    void reserve(std::size_t new_cap);

    // Resizes the ChunkedVector to the specified size, either truncating or adding elements as necessary.
    void resize(std::size_t new_size);

private:
    std::string name_prefix_;
    std::vector<SharedMemoryBuffer> chunks_;

    // Loads the existing shared memory buffers based on the current size of the vector.
    void load_chunks();

    // Adds a new chunk to the vector with double the size of the previous chunk.
    void expand();

    // Calculates the index of the chunk that contains the element at the specified index.
    void chunk_index_and_offset(std::size_t index, std::size_t* chunk_index, std::size_t* byte_offset) const;
};

template <typename T, std::size_t RequestedChunkSize>
ChunkedVector<T, RequestedChunkSize>::ChunkedVector(const std::string& name_prefix)
    : name_prefix_(name_prefix) {
    load_chunks();
}

template <typename T, std::size_t RequestedChunkSize>
std::size_t ChunkedVector<T, RequestedChunkSize>::size() const {
    // The size is stored at the start of the first chunk.
    const std::size_t* size_ptr = static_cast<const std::size_t*>(chunks_[0].data());
    return *size_ptr;
}

template <typename T, std::size_t RequestedChunkSize>
bool ChunkedVector<T, RequestedChunkSize>::empty() const {
    return size() == 0;
}

template <typename T, std::size_t RequestedChunkSize>
std::size_t ChunkedVector<T, RequestedChunkSize>::capacity() const {
    return (ChunkSize * ((1 << chunks_.size()) - 1)) / ElementSize;
}

template <typename T, std::size_t RequestedChunkSize>
void ChunkedVector<T, RequestedChunkSize>::load_chunks() {
    // Load the first chunk and add it to the vector. The first chunk also stores the size of the vector.
    chunks_.emplace_back(name_prefix_ + "_0", ChunkSize + sizeof(std::size_t));

    // Read the size from the first chunk
    size_t size = *reinterpret_cast<std::size_t*>(chunks_[0].data()) / ElementSize;

    // Calculate the number of chunks needed
    size_t chunk_index;
    size_t byte_offset;
    chunk_index_and_offset(size, &chunk_index, &byte_offset);
    size_t num_chunks = chunk_index + 1;

    // Load the additional chunks
    for (std::size_t i = 1; i < num_chunks; ++i) {
        chunks_.emplace_back(name_prefix_ + "_" + std::to_string(i), ChunkSize * (1 << i));
    }
}

template <typename T, std::size_t RequestedChunkSize>
void ChunkedVector<T, RequestedChunkSize>::expand() {
    chunks_.emplace_back(name_prefix_ + "_" + std::to_string(chunks_.size()), ChunkSize * (1 << chunks_.size()));
}

template <typename T, std::size_t RequestedChunkSize>
void ChunkedVector<T, RequestedChunkSize>::chunk_index_and_offset(std::size_t index, std::size_t* chunk_index, std::size_t* byte_offset) const {
    // Calculate the total byte offset for the desired index
    *byte_offset = index * ElementSize;

    // Calculate the number of chunks needed to reach the byte offset.
    // We start with chunk_index 0, which has a capacity of ChunkSize.
    *chunk_index = 0;
    std::size_t chunk_capacity = ChunkSize;

    // Increase the chunk_index and double the chunk_capacity until
    // we have enough capacity to reach the byte_offset.
    while (*byte_offset >= chunk_capacity) {
        *byte_offset -= chunk_capacity;
        chunk_capacity *= 2;
        ++(*chunk_index);
    }
    *byte_offset += (*chunk_index == 0 ? sizeof(size_t) : 0);
}

template <typename T, std::size_t RequestedChunkSize>
template <typename... Args>
void ChunkedVector<T, RequestedChunkSize>::emplace_back(Args&&... args) {
    std::atomic_size_t* size_ptr = reinterpret_cast<std::atomic_size_t*>(chunks_[0].data());
	std::size_t old_size = size_ptr->fetch_add(1);
	std::size_t new_size = old_size + 1;

    std::size_t chunk_index;
    std::size_t byte_offset;
	chunk_index_and_offset(old_size, &chunk_index, &byte_offset);
    while (chunk_index > (chunks_.size() - 1)) {
        expand();
    }
	T* element_ptr = reinterpret_cast<T*>(reinterpret_cast<char*>(chunks_[chunk_index].data()) + byte_offset);
	new (element_ptr) T(std::forward<Args>(args)...);
}

template <typename T, std::size_t RequestedChunkSize>
void ChunkedVector<T, RequestedChunkSize>::push_back(const T& value) {
    emplace_back(value);
}

template <typename T, std::size_t RequestedChunkSize>
void ChunkedVector<T, RequestedChunkSize>::pop_back() {
    std::atomic_size_t* size_ptr = reinterpret_cast<std::atomic_size_t*>(chunks_[0].data());
    std::size_t old_size = size_ptr->fetch_sub(1);
    if (old_size == 0) {
        throw std::out_of_range("Cannot pop_back from an empty ChunkedVector");
    }
}

template <typename T, std::size_t RequestedChunkSize>
T& ChunkedVector<T, RequestedChunkSize>::operator[](std::size_t index) {
    if (index >= size()) {
        std::cout << "Index: " << index << ", Size: " << size() << std::endl;
        throw std::out_of_range("Index out of range");
    }
    std::size_t chunk_index;
    std::size_t byte_offset;
    chunk_index_and_offset(index, &chunk_index, &byte_offset);  
    return *(reinterpret_cast<T*>(reinterpret_cast<char*>(chunks_[chunk_index].data()) + byte_offset));
}

template <typename T, std::size_t RequestedChunkSize>
const T& ChunkedVector<T, RequestedChunkSize>::operator[](std::size_t index) const {
    if (index >= size()) {
        throw std::out_of_range("Index out of range");
    }
    std::size_t chunk_index;
    std::size_t byte_offset;
    chunk_index_and_offset(index, &chunk_index, &byte_offset);
    return *(reinterpret_cast<const T*>(reinterpret_cast<const char*>(chunks_[chunk_index].data()) + byte_offset));
}

template <typename T, std::size_t RequestedChunkSize>
void ChunkedVector<T, RequestedChunkSize>::reserve(std::size_t new_cap) {
    std::atomic_size_t* size_ptr = reinterpret_cast<std::atomic_size_t*>(chunks_[0].data());
    std::size_t old_size = *size_ptr;
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

template <typename T, std::size_t RequestedChunkSize>
void ChunkedVector<T, RequestedChunkSize>::resize(std::size_t new_size) {
    std::atomic_size_t* size_ptr = reinterpret_cast<std::atomic_size_t*>(chunks_[0].data());
    std::size_t expected_size;
    do {
        expected_size = *size_ptr;
        if (new_size > expected_size) {
            // If the new size is larger than the expected size, reserve space.
            reserve(new_size);
        }
    } while (!std::atomic_compare_exchange_strong(size_ptr, &expected_size, new_size));
}


#endif // CHUNKED_VECTOR_H_

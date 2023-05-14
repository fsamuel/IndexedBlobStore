#ifndef CHUNKED_VECTOR_H_
#define CHUNKED_VECTOR_H_

#include "shared_memory_buffer.h"
#include <cstddef>
#include <vector>
#include <memory>
#include <atomic>

template <size_t N>
struct HighestOnePosition {
    static constexpr size_t value = 1 + HighestOnePosition<N / 2>::value;
};

template <>
struct HighestOnePosition<0> {
    static constexpr size_t value = 0;
};

template <>
struct HighestOnePosition<1> {
    static constexpr size_t value = 0;
};

// ChunkedVector is a dynamic array-like data structure that uses SharedMemoryBuffer
// to allocate its memory in chunks. Each chunk is double the size of the previous chunk.
// It supports basic operations like push_back, pop_back, access at a particular index,
// and checking the size of the vector.
template <typename T, std::size_t ChunkSize>
class ChunkedVector {
public:
    // Constructs a ChunkedVector with the specified name_prefix for the shared memory buffers.
    // Each SharedMemoryBuffer will be named as name_prefix_i, where i is the chunk index.
    explicit ChunkedVector(const std::string& name_prefix);

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

    // Calculates the log base 2 of the specified number.
    std::size_t log2(size_t n) const;

    // Calculates the index of the chunk that contains the element at the specified index.
    std::size_t chunk_index(std::size_t index) const;

    // Calculates the position of the element within its chunk for the specified index.
    std::size_t position_in_chunk(std::size_t index) const;
};

template <typename T, std::size_t ChunkSize>
ChunkedVector<T, ChunkSize>::ChunkedVector(const std::string& name_prefix)
    : name_prefix_(name_prefix) {
    load_chunks();
}

template <typename T, std::size_t ChunkSize>
std::size_t ChunkedVector<T, ChunkSize>::size() const {
    // The size is stored at the start of the first chunk.
    const std::size_t* size_ptr = static_cast<const std::size_t*>(chunks_[0].data());
    return *size_ptr;
}

template <typename T, std::size_t ChunkSize>
bool ChunkedVector<T, ChunkSize>::empty() const {
    return size() == 0;
}

template <typename T, std::size_t ChunkSize>
std::size_t ChunkedVector<T, ChunkSize>::capacity() const {
    return (ChunkSize * ((1 << chunks_.size()) - 1)) / sizeof(T);
}

template <typename T, std::size_t ChunkSize>
void ChunkedVector<T, ChunkSize>::load_chunks() {
    // Load the first chunk and add it to the vector
    chunks_.emplace_back(name_prefix_ + "_0", ChunkSize + sizeof(std::size_t));

    // Read the size from the first chunk
    size_t size = *reinterpret_cast<std::size_t*>(chunks_[0].data()) / sizeof(T);

    // Calculate the number of chunks needed
    size_t num_chunks = log2(size * sizeof(T) / ChunkSize) + 1;

    // Load the additional chunks
    for (std::size_t i = 1; i < num_chunks; ++i) {
        chunks_.emplace_back(name_prefix_ + "_" + std::to_string(i), ChunkSize * (1 << i));
    }
}


template <typename T, std::size_t ChunkSize>
void ChunkedVector<T, ChunkSize>::expand() {
    chunks_.emplace_back(name_prefix_ + "_" + std::to_string(chunks_.size()), ChunkSize * (1 << chunks_.size()));
}

template <typename T, std::size_t ChunkSize>
std::size_t ChunkedVector<T, ChunkSize>::log2(size_t n) const {
    std::size_t log = 0;
    while (n >>= 1) ++log;
    return log;
}

template <typename T, std::size_t ChunkSize>
std::size_t ChunkedVector<T, ChunkSize>::chunk_index(std::size_t index) const {
    std::size_t num_chunks = ((index + 1) * sizeof(T)) / ChunkSize;

    // Initialize the chunk index to 0.
    std::size_t chunk_index = 0;

    // Use a bit shift to check each bit position in shifted_index.
    while (num_chunks >>= 1) {
        ++chunk_index;
    }

    // Return the calculated chunk index.
    return chunk_index;
}

template <typename T, std::size_t ChunkSize>
std::size_t ChunkedVector<T, ChunkSize>::position_in_chunk(std::size_t index) const {
    std::size_t postion_in_bytes = index * sizeof(T);
    std::size_t chunk = chunk_index(index);
    std::size_t chunk_size = (std::size_t(1) << chunk) * ChunkSize;

    // Return the index within the chunk. This is equivalent to the adjusted
    // index modulo the size of the chunk. Note that since chunk_size is a power
    // of 2, we can use a bitwise AND operation to compute the modulo efficiently.
    return (postion_in_bytes & (chunk_size - 1)) + (chunk == 0 ? sizeof(size_t) : 0);
}

template <typename T, std::size_t ChunkSize>
template <typename... Args>
void ChunkedVector<T, ChunkSize>::emplace_back(Args&&... args) {
    std::atomic_size_t* size_ptr = reinterpret_cast<std::atomic_size_t*>(chunks_[0].data());
	std::size_t old_size = size_ptr->fetch_add(1);
	std::size_t new_size = old_size + 1;
    if ((new_size * sizeof(T)) > ChunkSize * ((1 << chunks_.size()) - 1)) {
		expand();
	}
	size_t cindex = chunk_index(old_size);
	size_t pos_in_chunk = position_in_chunk(old_size);
	T* element_ptr = reinterpret_cast<T*>(reinterpret_cast<char*>(chunks_[cindex].data()) + pos_in_chunk);
	new (element_ptr) T(std::forward<Args>(args)...);
}

template <typename T, std::size_t ChunkSize>
void ChunkedVector<T, ChunkSize>::push_back(const T& value) {
    emplace_back(value);
}

template <typename T, std::size_t ChunkSize>
void ChunkedVector<T, ChunkSize>::pop_back() {
    std::atomic_size_t* size_ptr = reinterpret_cast<std::atomic_size_t*>(chunks_[0].data());
    std::size_t old_size = size_ptr->fetch_sub(1);
    if (old_size == 0) {
        throw std::out_of_range("Cannot pop_back from an empty ChunkedVector");
    }
}

template <typename T, std::size_t ChunkSize>
T& ChunkedVector<T, ChunkSize>::operator[](std::size_t index) {
    if (index >= size()) {
        throw std::out_of_range("Index out of range");
    }
    std::size_t cindex = chunk_index(index);
    std::size_t pos_in_chunk = position_in_chunk(index);
    return *(reinterpret_cast<T*>(reinterpret_cast<char*>(chunks_[cindex].data()) + pos_in_chunk));
}

template <typename T, std::size_t ChunkSize>
const T& ChunkedVector<T, ChunkSize>::operator[](std::size_t index) const {
    if (index >= size()) {
        throw std::out_of_range("Index out of range");
    }
    std::size_t cindex = chunk_index(index);
    std::size_t pos_in_chunk = position_in_chunk(index);
    return *(reinterpret_cast<T*>(reinterpret_cast<char*>(chunks_[cindex].data()) + pos_in_chunk));
}

template <typename T, std::size_t ChunkSize>
void ChunkedVector<T, ChunkSize>::reserve(std::size_t new_cap) {
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

template <typename T, std::size_t ChunkSize>
void ChunkedVector<T, ChunkSize>::resize(std::size_t new_size) {
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

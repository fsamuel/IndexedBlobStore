#include "chunk_manager.h"

static std::size_t next_power_of_two(std::size_t v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    v++;
    return v;
}

ChunkManager::ChunkManager(const std::string& name_prefix, std::size_t initial_chunk_size)
    : name_prefix_(name_prefix), chunk_size_(next_power_of_two(initial_chunk_size)) {
    chunks_.push_back(SharedMemoryBuffer(name_prefix_ + "_0", chunk_size_ + sizeof(std::uint32_t)));
    num_chunks_ = reinterpret_cast<std::atomic<std::uint32_t>*>(chunks_[0].data());
    if (*num_chunks_ == 0) {
        *num_chunks_ = 1;
    }
    else {
        load_chunks();
    }
}

ChunkManager::ChunkManager(ChunkManager&& other)
    : name_prefix_(std::move(other.name_prefix_)), chunk_size_(other.chunk_size_), chunks_(std::move(other.chunks_)), num_chunks_(other.num_chunks_) {}

ChunkManager& ChunkManager::operator=(ChunkManager&& other) {
    name_prefix_ = std::move(other.name_prefix_);
	chunk_size_ = std::move(other.chunk_size_);
	chunks_ = std::move(other.chunks_);
	num_chunks_ = other.num_chunks_;
	return *this;
}

void ChunkManager::add_chunk(size_t* chunk_index, size_t* chunk_size) {
    std::unique_lock<std::shared_mutex> lock(chunks_rw_mutex_);
    *chunk_index = chunks_.size();
    *chunk_size = chunk_size_ * (1 << *chunk_index);
    chunks_.emplace_back(name_prefix_ + "_" + std::to_string(*chunk_index), *chunk_size);
    ++(*num_chunks_);
}

void ChunkManager::remove_chunk() {
    std::unique_lock<std::shared_mutex> lock(chunks_rw_mutex_);
    if (chunks_.size() <= 1) {
        return;
    }
    chunks_.pop_back();
    --(*num_chunks_);
}

std::size_t ChunkManager::num_chunks() const {
    return *num_chunks_;
}

uint8_t* ChunkManager::at(std::uint64_t index) {
    std::size_t chunk_index = index >> 56;
    std::size_t offset = index & ((1ull << 56) - 1);
    return at(chunk_index, offset);
}

uint8_t* ChunkManager::at(std::size_t chunk_index, std::size_t offset_in_chunk) {
    std::shared_lock<std::shared_mutex> lock(chunks_rw_mutex_);
    if (chunk_index == 0) {
        offset_in_chunk += sizeof(std::uint32_t);
    }
    return reinterpret_cast<uint8_t*>(chunks_[chunk_index].data()) + offset_in_chunk;
}

uint8_t* ChunkManager::get_chunk_start(std::size_t chunk_index) {
    std::shared_lock<std::shared_mutex> lock(chunks_rw_mutex_);
    if (chunk_index == 0) {
        return reinterpret_cast<uint8_t*>(chunks_[chunk_index].data()) + sizeof(std::uint32_t);
    }
    else {
        return reinterpret_cast<uint8_t*>(chunks_[chunk_index].data());
    }
}

std::size_t ChunkManager::capacity() const {
    std::size_t num_chunks = *num_chunks_;
    std::size_t chunk_size = chunk_size_;
    std::size_t total_capacity = 0;
    for (std::size_t i = 0; i < num_chunks; ++i) {
        total_capacity += chunk_size;
        chunk_size *= 2;
    }
    return total_capacity;
}

void ChunkManager::load_chunks() {
    while (chunks_.size() < *num_chunks_) {
        chunks_.push_back(SharedMemoryBuffer(name_prefix_ + "_" + std::to_string(chunks_.size()), chunk_size_ << chunks_.size()));
    }
}
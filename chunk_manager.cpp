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
    chunks_.push_back(SharedMemoryBuffer(name_prefix_ + "_0", chunk_size_ + sizeof(std::size_t)));
    num_chunks_ = reinterpret_cast<std::atomic<std::size_t>*>(chunks_[0].data());
    load_chunks_if_necessary();
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

std::size_t ChunkManager::get_or_create_chunk(size_t chunk_index, uint8_t** data, std::size_t* chunk_size) {
    while (true) {
        std::size_t num_chunks = num_chunks_->load();
        if (chunk_index < num_chunks) {
            std::shared_lock<std::shared_mutex> lock(chunks_rw_mutex_);
            if (chunk_index < chunks_.size()) {
                std::size_t offset = chunk_index == 0 ? sizeof(std::size_t) : 0;
                *data = reinterpret_cast<uint8_t*>(chunks_[chunk_index].data()) + offset;
                *chunk_size = chunk_size_ * (static_cast<std::size_t>(1) << chunk_index);
                return 0;
            }
            // Another thread or process has resized the chunks vector. Try again.
            continue;
        }
        if (num_chunks_->compare_exchange_strong(num_chunks, chunk_index + 1)) {
            std::size_t num_chunks_loaded = load_chunks_if_necessary();
            std::unique_lock<std::shared_mutex> lock(chunks_rw_mutex_);
            if (chunk_index < chunks_.size()) {
                *data = reinterpret_cast<uint8_t*>(chunks_[chunk_index].data());
                *chunk_size = chunk_size_ * (static_cast<std::size_t>(1) << chunk_index);
                return num_chunks_loaded;
            }
        }
        // Another thread or process has resized the chunks vector. Try again.
    }
}

void ChunkManager::remove_chunk() {
    while (true) {
        std::size_t num_chunks = *num_chunks_;
        if (num_chunks <= 1) {
            return;
        }
        if (num_chunks_->compare_exchange_strong(num_chunks, num_chunks - 1)) {
            // TODO(fsamuel): It's not safe to delete files corresponding to chunks because another
            // thread might increment the chunk count and start using the chunk again.
            // In order to be safe, we need a way to know when a chunk is no longer in use.
            // For now, we just keep the files around.
            // Idea: perhaps we can keep a separate increment and decrement count for each chunk.
            // When a new chunk is added, the state of the counts is stored in the previous chunk.
            // When a chunk is removed, the state of the counts is stored in the previous chunk.
            // The state in the previous chunk is used to determine the file name of the next chunk.
            std::unique_lock<std::shared_mutex> lock(chunks_rw_mutex_);
            while (chunks_.size() >= num_chunks) {
                chunks_.pop_back();
            }
			break;
		}
    }
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
        offset_in_chunk += sizeof(std::size_t);
    }
    return reinterpret_cast<uint8_t*>(chunks_[chunk_index].data()) + offset_in_chunk;
}

std::size_t ChunkManager::capacity() const {
    std::size_t num_chunks = num_chunks_->load();
    std::size_t chunk_size = chunk_size_;
    std::size_t total_capacity = 0;
    for (std::size_t i = 0; i < num_chunks; ++i) {
        total_capacity += chunk_size;
        chunk_size *= 2;
    }
    return total_capacity;
}

std::size_t ChunkManager::load_chunks_if_necessary() {
    std::size_t num_chunks = num_chunks_->load();
    if (num_chunks == 0) {
        if (num_chunks_->compare_exchange_strong(num_chunks, 1)) {
            return 0;
		}
    }
    std::unique_lock<std::shared_mutex> lock(chunks_rw_mutex_);
    std::size_t num_chunks_loaded = 0;
    while (chunks_.size() < num_chunks) {
        chunks_.push_back(SharedMemoryBuffer(name_prefix_ + "_" + std::to_string(chunks_.size()), chunk_size_ << chunks_.size()));
        ++num_chunks_loaded;
    }
    return num_chunks_loaded;
}
#include "chunk_manager.h"
#include "buffer_factory.h"

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

ChunkManager::ChunkManager(BufferFactory* buffer_factory,
                           const std::string& name_prefix,
                           std::size_t initial_chunk_size)
    : name_prefix_(name_prefix),
      chunk_size_(next_power_of_two(initial_chunk_size)),
      buffer_factory_(buffer_factory) {
  chunks_.emplace_back(buffer_factory_->CreateBuffer(
      name_prefix_ + "_0", chunk_size_ + sizeof(std::uint64_t)));
  num_chunks_encoded_ =
      reinterpret_cast<std::atomic<std::uint64_t>*>(chunks_[0]->GetData());
  load_chunks_if_necessary();
}

ChunkManager::ChunkManager(ChunkManager&& other)
    : name_prefix_(std::move(other.name_prefix_)),
      chunk_size_(other.chunk_size_),
      chunks_(std::move(other.chunks_)),
      num_chunks_encoded_(other.num_chunks_encoded_),
      buffer_factory_(other.buffer_factory_) {}

ChunkManager& ChunkManager::operator=(ChunkManager&& other) {
  name_prefix_ = std::move(other.name_prefix_);
  chunk_size_ = std::move(other.chunk_size_);
  chunks_ = std::move(other.chunks_);
  num_chunks_encoded_ = other.num_chunks_encoded_;
  buffer_factory_ = other.buffer_factory_;
  return *this;
}

std::size_t ChunkManager::get_or_create_chunk(size_t chunk_index,
                                              uint8_t** data,
                                              std::size_t* chunk_size) {
  while (true) {
    std::uint64_t num_chunks_encoded = num_chunks_encoded_->load();
    std::uint64_t num_chunks = decode_num_chunks(num_chunks_encoded);
    if (chunk_index < num_chunks) {
      std::shared_lock<std::shared_mutex> lock(chunks_rw_mutex_);
      if (chunk_index < chunks_.size()) {
        std::size_t offset = chunk_index == 0 ? sizeof(std::uint64_t) : 0;
        *data = reinterpret_cast<uint8_t*>(chunks_[chunk_index]->GetData()) +
                offset;
        *chunk_size =
            chunk_size_ * (static_cast<std::size_t>(1) << chunk_index);
        return 0;
      }
      // Another thread or process has resized the chunks vector. Try again.
      continue;
    }
    if (num_chunks_encoded_->compare_exchange_strong(
            num_chunks_encoded,
            set_num_chunks(num_chunks_encoded, chunk_index + 1))) {
      std::size_t num_chunks_loaded = load_chunks_if_necessary();
      std::unique_lock<std::shared_mutex> lock(chunks_rw_mutex_);
      if (chunk_index < chunks_.size()) {
        *data = reinterpret_cast<uint8_t*>(chunks_[chunk_index]->GetData());
        *chunk_size =
            chunk_size_ * (static_cast<std::size_t>(1) << chunk_index);
        return num_chunks_loaded;
      }
    }
    // Another thread or process has resized the chunks vector. Try again.
  }
}

const uint8_t* ChunkManager::get_chunk_start(std::size_t chunk_index) const {
  return const_cast<ChunkManager*>(this)->get_chunk_start(chunk_index);
}

uint8_t* ChunkManager::get_chunk_start(std::size_t chunk_index) {
  std::shared_lock<std::shared_mutex> lock(chunks_rw_mutex_);
  if (chunk_index >= chunks_.size()) {
    return nullptr;
  }
  std::size_t offset = chunk_index == 0 ? sizeof(std::uint64_t) : 0;
  return reinterpret_cast<uint8_t*>(chunks_[chunk_index]->GetData()) + offset;
}

void ChunkManager::remove_chunk() {
  while (true) {
    std::uint64_t num_chunks_encoded = num_chunks_encoded_->load();
    std::uint64_t num_chunks = decode_num_chunks(num_chunks_encoded);
    if (num_chunks <= 1) {
      return;
    }
    if (num_chunks_encoded_->compare_exchange_strong(
            num_chunks_encoded, decrement_num_chunks(num_chunks_encoded))) {
      // TODO(fsamuel): It's not safe to delete files corresponding to chunks
      // because another thread might increment the chunk count and start using
      // the chunk again. In order to be safe, we need a way to know when a
      // chunk is no longer in use. For now, we just keep the files around.
      // Idea:
      // When a new chunk is added, the state of the counts is stored in the
      // previous chunk. When a chunk is removed, the state of the counts is
      // stored in the previous chunk. The state in the previous chunk is used
      // to determine the file name of the next chunk.
      std::unique_lock<std::shared_mutex> lock(chunks_rw_mutex_);
      while (chunks_.size() >= num_chunks) {
        chunks_.pop_back();
      }
      break;
    }
  }
}

std::uint64_t ChunkManager::num_chunks() const {
  std::uint64_t num_chunks_encoded = num_chunks_encoded_->load();
  return decode_num_chunks(num_chunks_encoded);
}

const uint8_t* ChunkManager::at(std::uint64_t index) const {
  return const_cast<ChunkManager*>(this)->at(index);
}

uint8_t* ChunkManager::at(std::uint64_t index) {
  std::size_t chunk_index = this->chunk_index(index);
  std::size_t offset = index & ((1ull << 56) - 1);
  return at(chunk_index, offset);
}

const uint8_t* ChunkManager::at(std::size_t chunk_index,
                                std::size_t offset_in_chunk) const {
  return const_cast<ChunkManager*>(this)->at(chunk_index, offset_in_chunk);
}

uint8_t* ChunkManager::at(std::size_t chunk_index,
                          std::size_t offset_in_chunk) {
  std::shared_lock<std::shared_mutex> lock(chunks_rw_mutex_);
  if (chunk_index == 0) {
    offset_in_chunk += sizeof(std::uint64_t);
  }
  if (chunk_index >= chunks_.size() ||
      offset_in_chunk >= chunks_[chunk_index]->GetSize()) {
    return nullptr;
  }
  return reinterpret_cast<uint8_t*>(chunks_[chunk_index]->GetData()) +
         offset_in_chunk;
}

std::size_t ChunkManager::capacity() const {
  std::uint64_t num_chunks_encoded = num_chunks_encoded_->load();
  std::uint64_t num_chunks = decode_num_chunks(num_chunks_encoded);
  std::size_t chunk_size = chunk_size_;
  std::size_t total_capacity = 0;
  for (std::uint64_t i = 0; i < num_chunks; ++i) {
    total_capacity += chunk_size;
    chunk_size *= 2;
  }
  return total_capacity;
}

std::uint64_t ChunkManager::encode_index(std::size_t chunk_index,
                                         std::size_t offset_in_chunk) {
  return (static_cast<std::uint64_t>(chunk_index & 0x7F) << 56) |
         offset_in_chunk;
}

std::size_t ChunkManager::chunk_index(std::uint64_t encoded_index) {
  // Clear the topmost bit as it's reserved for other uses.
  // We can never have more than 128 chunks. That's a massive amount of
  // memory/storage.
  return (encoded_index & 0x7FFFFFFFFFFFFFFF) >> 56;
}

std::size_t ChunkManager::offset_in_chunk(std::uint64_t encoded_index) {
  return encoded_index & ((1ull << 56) - 1);
}

std::size_t ChunkManager::load_chunks_if_necessary() {
  std::uint64_t num_chunks_encoded = num_chunks_encoded_->load();
  if (num_chunks_encoded == 0) {
    std::uint64_t new_num_chunks_encoded =
        increment_num_chunks(num_chunks_encoded);
    if (num_chunks_encoded_->compare_exchange_strong(num_chunks_encoded,
                                                     new_num_chunks_encoded)) {
      return 0;
    }
  }
  std::unique_lock<std::shared_mutex> lock(chunks_rw_mutex_);
  std::uint64_t num_chunks_loaded = 0;
  uint64_t num_chunks = decode_num_chunks(num_chunks_encoded);
  while (chunks_.size() < num_chunks) {
    chunks_.emplace_back(buffer_factory_->CreateBuffer(
        name_prefix_ + "_" + std::to_string(chunks_.size()),
        chunk_size_ << chunks_.size()));
    ++num_chunks_loaded;
  }
  return num_chunks_loaded;
}

std::uint64_t ChunkManager::decode_num_chunks(uint64_t num_chunks_encoded) {
  // The first 32-bits of num_chunks_encoded are the number of increments.
  // The next 32-bits of num_chunks_encoded are the number of decrements.
  // The number of chunks is the number of increments minus the number of
  // decrements.
  return (num_chunks_encoded >> 32) - (num_chunks_encoded & ((1ull << 32) - 1));
}

std::uint64_t ChunkManager::increment_num_chunks(
    std::uint64_t num_chunks_encoded,
    std::uint64_t value) {
  return ((num_chunks_encoded + (value << 32)) & 0xFFFFFFFF00000000) |
         (num_chunks_encoded & ((1ull << 32) - 1));
}

std::uint64_t ChunkManager::decrement_num_chunks(
    std::uint64_t num_chunks_encoded,
    std::uint64_t value) {
  return (num_chunks_encoded & 0xFFFFFFFF00000000) |
         ((num_chunks_encoded + value) & ((1ull << 32) - 1));
}

std::uint64_t ChunkManager::set_num_chunks(std::uint64_t num_chunks_encoded,
                                           std::uint64_t num_chunks) {
  uint64_t num_chunks_decoded = decode_num_chunks(num_chunks_encoded);
  if (num_chunks_decoded > num_chunks) {
    uint64_t decrement_count = num_chunks_decoded - num_chunks;
    return decrement_num_chunks(num_chunks_encoded, decrement_count);
  } else if (num_chunks_decoded < num_chunks) {
    uint64_t increment_count = num_chunks - num_chunks_decoded;
    return increment_num_chunks(num_chunks_encoded, increment_count);
  } else {
    return num_chunks_encoded;
  }
}

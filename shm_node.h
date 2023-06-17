#ifndef SHM_NODE_H_
#define SHM_NODE_H_

#include <atomic>

// Header for a free/allocated node in the allocator
struct ShmNode {
  // Reference count for the node. This is used to determine when the node can
  // be coalesced.
  std::atomic<std::uint32_t> ref_count;
  // Version number for detecting state changes in the node.
  std::atomic<std::uint32_t> version;
  // The index of the chunk in the chunk manager. This is necessary to convert
  // the pointer to an index relative to the start of the chunk.
  std::size_t index;
  // Size of the block, including the header
  std::atomic<std::size_t> size;
  // index of the next free block in the free list
  std::atomic<std::size_t> next_index;

  bool is_allocated() const { return !is_free(); }

  bool is_free() const { return (version.load() & 0x1) == 0; }
};

class ShmNodePtr {
 public:
  explicit ShmNodePtr(ShmNode* ptr = nullptr) : ptr_(ptr) {
    if (ptr_) {
      std::size_t ref_count = ptr_->ref_count.fetch_add(1) + 1;
      if (ref_count == 0) {
        AllocationLogger::Get()->PrintLastOperations();
      }
    }
  }

  explicit ShmNodePtr(const ShmNodePtr& other) : ptr_(other.ptr_) {
    if (ptr_) {
      ptr_->ref_count.fetch_add(1);
    }
  }

  // Move constructor: saves the cost of a refcount increment.
  ShmNodePtr(ShmNodePtr&& other) noexcept : ptr_(other.ptr_) {
    other.ptr_ = nullptr;
  }

  ShmNodePtr& operator=(const ShmNodePtr& other) {
    if (ptr_ != other.ptr_) {
      reset();
      ptr_ = other.ptr_;
      if (ptr_) {
        ptr_->ref_count.fetch_add(1);
      }
    }
    return *this;
  }

  ShmNodePtr& operator=(ShmNodePtr&& other) {
    if (ptr_ != other.ptr_) {
      reset();
      ptr_ = other.ptr_;
      other.ptr_ = nullptr;
    }
    return *this;
  }

  ~ShmNodePtr() { reset(); }

  void reset() {
    if (ptr_) {
      std::size_t last_ref_count = ptr_->ref_count.fetch_sub(1);
      if (last_ref_count == 0) {
        AllocationLogger::Get()->PrintLastOperations();
      }
      ptr_ = nullptr;
    }
  }

  ShmNode* get() const { return ptr_; }

  ShmNode& operator*() const { return *ptr_; }

  ShmNode* operator->() const { return ptr_; }

  operator bool() const { return ptr_ != nullptr; }

  bool operator==(std::nullptr_t) const { return ptr_ == nullptr; }

  bool operator!=(std::nullptr_t) const { return ptr_ != nullptr; }

  bool operator==(const ShmNodePtr& other) const { return ptr_ == other.ptr_; }

  bool operator!=(const ShmNodePtr& other) const { return ptr_ != other.ptr_; }

 private:
  ShmNode* ptr_;
};

#endif  // SHM_NODE_H_
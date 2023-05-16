#include "blob_store.h"

BlobStore::BlobStore(SharedMemoryBuffer&& metadataBuffer, SharedMemoryBuffer&& dataBuffer) :
    allocator_(std::move(dataBuffer)),
    metadata_(std::move(metadataBuffer))
{
    if (metadata_.empty()) {
        metadata_.emplace_back();
    }
    allocator_.AddObserver(this);
}

BlobStore::~BlobStore() {
    allocator_.RemoveObserver(this);
}

void BlobStore::Drop(size_t index) {
    if (index == BlobStore::InvalidIndex) {
		return;
	}

    BlobMetadata& free_list_head = metadata_[0];
    while (true) {
        BlobMetadata* metadata = metadata_.at(index);
        if (metadata == nullptr) {
			return;
		}
        
        // Set a tombstone on the blob to ensure that no new locks are acquired on it.
        metadata->tombstone = true;

        // Check if a lock is held on the blob, and if so, return. It will be dropped when the lock is released.
        if (metadata->lock_state != 0) {
			return;
		}
        size_t allocated_offset = metadata->offset;
        ssize_t next_free_index = metadata->next_free_index;
        if (next_free_index != -1) {
            return;
        }
        ssize_t first_free_index = free_list_head.next_free_index.load();
        if (!metadata->next_free_index.compare_exchange_weak(next_free_index, first_free_index)) {
			continue;
		}
        // If the head of the free list has changed, undo the change we made if possible and try again.

        if (!free_list_head.next_free_index.compare_exchange_weak(first_free_index, index)) {
            metadata->next_free_index.compare_exchange_weak(first_free_index, next_free_index);
            continue;
        }

        allocator_.Deallocate(allocator_.ToPtr<char>(allocated_offset));
        NotifyObserversOnDroppedBlob(index);
        return;
    }
}

void BlobStore::Compact() {
    std::string buffer_name = allocator_.buffer_name();
    SharedMemoryBuffer temp_buffer(buffer_name + "_data_compact");
    Allocator new_allocator(std::move(temp_buffer));
    for (size_t i = 0; i < metadata_.size(); ++i) {
        BlobMetadata& metadata_entry = metadata_[i];

        // Skip free slots
        if (metadata_entry.next_free_index != -1) {
            continue;
        }

        // Allocate memory in the new allocator and copy the data
        char* new_ptr = new_allocator.Allocate(metadata_entry.size * metadata_entry.count);
        std::memcpy(new_ptr, allocator_.ToPtr<char>(metadata_entry.offset), metadata_entry.size * metadata_entry.count);

        // Update metadata with the new pointer
        metadata_entry.offset = new_allocator.ToOffset(new_ptr);;
    }

    allocator_ = std::move(new_allocator);
    std::remove(buffer_name.c_str());

    // Rename the new shared memory file to the original name
    std::rename((buffer_name + "_data_compact").c_str(), buffer_name.c_str());

    NotifyObserversOnMemoryReallocated();
}

void BlobStore::AddObserver(BlobStoreObserver* observer) {
    observers_.push_back(observer);
}

void BlobStore::RemoveObserver(BlobStoreObserver* observer) {
    observers_.erase(std::remove(observers_.begin(), observers_.end(), observer), observers_.end());
}

void BlobStore::NotifyObserversOnMemoryReallocated() {
    for (BlobStoreObserver* observer : observers_) {
        observer->OnMemoryReallocated();
    }
}

void BlobStore::NotifyObserversOnDroppedBlob(size_t index) {
    for (BlobStoreObserver* observer : observers_) {
        observer->OnDroppedBlob(index);
    }
}

size_t BlobStore::FindFreeSlot() {
    while (true) {
        BlobMetadata& free_list_head = metadata_[0];
		ssize_t free_index = free_list_head.next_free_index.load();
        if (free_index == 0) {
            return metadata_.emplace_back();
        }
        ssize_t next_free_index = metadata_[free_index].next_free_index.load();
        if (free_list_head.next_free_index.compare_exchange_weak(free_index, next_free_index)) {
            // Make sure the tombstone bit is not set for the recycled metadata.
            BlobMetadata& metadata = metadata_[free_index];
            metadata.tombstone = false;
			return free_index;
		}
    }
}

// Returns the number of free slots in the metadata vector
size_t BlobStore::GetFreeSlotCount() const {
	size_t count = 0;
	size_t index = metadata_[0].next_free_index;
    // The free list is a circular linked list with a dummy node at the head.
    while (index !=0) {
		++count;
		index = metadata_[index].next_free_index;
	}
	return count;
}

void BlobStore::OnBufferResize() {
    NotifyObserversOnMemoryReallocated();
}

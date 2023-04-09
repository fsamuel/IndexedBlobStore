#include <iostream>
#include <vector>
#include <algorithm>
#include "BlobStore.h"
#include "shared_memory_buffer.h"
#include "SharedMemoryAllocator.h"
#include "SharedMemoryVector.h"

int main() {
    try {
        SharedMemoryBuffer dataBuffer("C:\\Users\\fadys\\Documents\\DataTest");
        SharedMemoryBuffer metadataBuffer("C:\\Users\\fadys\\Documents\\MetadataTest");
        BlobStore blobStore(std::move(metadataBuffer), std::move(dataBuffer));
        blobStore.Drop(2);
        blobStore.Drop(3);
        char* last = blobStore.Get<char>(1);
        if (last != nullptr) {
            std::cout << "Last Blob: " << *reinterpret_cast<int*>(last) << std::endl;
        }
        BlobStoreObject<int> ptr = blobStore.Put<int>(sizeof(int), 1337);
        std::cout << "Blob: " << *ptr << std::endl;
    }
    catch (const std::runtime_error& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    return 0;
}

#ifndef __SHARED_MEMORY_BUFFER_H
#define __SHARED_MEMORY_BUFFER_H

#include <cstddef>
#include <string>

#ifdef _WIN32
#include <Windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#endif

class SharedMemoryBuffer {
public:
    // Move constructor
    SharedMemoryBuffer(SharedMemoryBuffer&& other) noexcept;

    // Constructor that opens an existing memory-mapped file with the given name
    SharedMemoryBuffer(const std::string& name);

    // Constructor that creates a new memory-mapped file with the given name and size
    SharedMemoryBuffer(const std::string& name, std::size_t size);

    // Disable copy constructor
    SharedMemoryBuffer(const SharedMemoryBuffer&) = delete;

    // Disable copy assignment operator
    SharedMemoryBuffer& operator=(const SharedMemoryBuffer&) = delete;

    // Destructor
    ~SharedMemoryBuffer() noexcept;

    // Resize the memory-mapped file to the given size
    void resize(std::size_t new_size);

    // Return the name of the memory-mapped file
    const std::string& name() const {
        return m_name;
    }

    // Return the size of the memory-mapped file
    std::size_t size() const {
        return m_size;
    }

    // Return a pointer to the start of the memory-mapped file
    void* data() {
        return m_data;
    }

    // Return a const pointer to the start of the memory-mapped file
    const void* data() const {
        return m_data;
    }

    // Flush the memory-mapped buffer to disk
    void flush();

    // Move assignment operator
    SharedMemoryBuffer& operator=(SharedMemoryBuffer&& other) noexcept {
        unmap_memory();
        close_file();
        m_name = std::move(other.m_name);
        m_size = other.m_size;
        m_data = other.m_data;
        other.m_size = 0;
        other.m_data = nullptr;
        other.m_name.clear();
#ifdef _WIN32
        m_file_handle = other.m_file_handle;
        m_file_mapping = other.m_file_mapping;
        other.m_file_handle = 0;
        other.m_file_mapping = 0;
#else
        m_file_descriptor = other.m_file_descriptor;
        other.m_file_descriptor = -1;
#endif
        return *this;
    }

private:
    // Open the memory-mapped file
    void open_file();

    // Close the memory-mapped file
    void close_file();

    // Map the memory-mapped file into memory
    void map_memory(std::size_t size);

    // Unmap the memory-mapped file from memory
    void unmap_memory();

    std::string m_name;     // The name of the memory-mapped file
    std::size_t m_size;     // The size of the memory-mapped file
    void* m_data;           // A pointer to the start of the memory-mapped file
#ifdef _WIN32
    HANDLE m_file_handle;   // The Windows file handle
    HANDLE m_file_mapping;  // The Windows file mapping handle
#else
    int m_file_descriptor;  // The Linux file descriptor
#endif
};

#endif // __SHARED_MEMORY_BUFFER_H

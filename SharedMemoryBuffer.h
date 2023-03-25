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
    SharedMemoryBuffer(const std::string& name);
    SharedMemoryBuffer(const std::string& name, std::size_t size);
    ~SharedMemoryBuffer();

    const std::string& name() const;
    std::size_t size() const;
    std::size_t size_on_disk() const;
    void resize(std::size_t new_size);

    void* data();
    const void* data() const;

private:
    void open_file();
    void close_file();
    void map_memory(std::size_t size);
    void unmap_memory();

    std::string m_name;
    std::size_t m_size;
    void* m_data;
#ifdef _WIN32
    HANDLE m_file_handle;
    HANDLE m_file_mapping;
#else
    int m_file_descriptor;
#endif
    std::size_t m_file_size;
};

#endif // __SHARED_MEMORY_BUFFER_H

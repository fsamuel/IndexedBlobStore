#include "SharedMemoryBuffer.h"
#include <algorithm>
#include <stdexcept>
#include <fstream>

SharedMemoryBuffer::SharedMemoryBuffer(const std::string& name)
    : m_name(name)
    , m_size(0)
    , m_data(nullptr)
#ifdef _WIN32
    , m_file_handle(nullptr)
    , m_file_mapping(nullptr)
#else
    , m_file_descriptor(-1)
#endif
    , m_file_size(0)
{
    // Get the size of the file on disk
    std::ifstream file(m_name, std::ios::binary | std::ios::ate);
    m_file_size = file.tellg();
    file.close();

    // If the file doesn't exist, set the size to 0
    if (m_file_size == -1) {
        m_file_size = 0;
    }

    open_file();
    map_memory(m_file_size);
}

SharedMemoryBuffer::SharedMemoryBuffer(const std::string& name, std::size_t size)
    : m_name(name)
    , m_size(size)
    , m_data(nullptr)
#ifdef _WIN32
    , m_file_handle(nullptr)
    , m_file_mapping(nullptr)
#else
    , m_file_descriptor(-1)
#endif
    , m_file_size(0)
{
    open_file();
    map_memory(size);
}

SharedMemoryBuffer::~SharedMemoryBuffer() {
    unmap_memory();
    close_file();
}

const std::string& SharedMemoryBuffer::name() const {
    return m_name;
}

std::size_t SharedMemoryBuffer::size() const {
    return m_size;
}

std::size_t SharedMemoryBuffer::size_on_disk() const {
    return m_file_size;
}

void SharedMemoryBuffer::resize(std::size_t new_size) {
    new_size = max(new_size, size_on_disk());

    // Unmap the memory-mapped file
    unmap_memory();

#ifdef _WIN32
    // Windows implementation
    // Resize the file
    LARGE_INTEGER file_size{};
    file_size.QuadPart = new_size;
    if (!SetFilePointerEx(m_file_handle, file_size, nullptr, FILE_BEGIN)) {
        throw std::runtime_error("Failed to resize memory-mapped file: SetFilePointerEx");
    }
    if (!SetEndOfFile(m_file_handle)) {
        throw std::runtime_error("Failed to resize memory-mapped file: SetEndOfFile");
    }

    // Update the size
    m_size = new_size;
    m_file_size = new_size;
#else
    // Linux implementation
    // Resize the file
    if (ftruncate(m_file_descriptor, new_size) == -1) {
        throw std::runtime_error("Failed to resize memory-mapped file");
    }
#endif
    // Map the memory-mapped file into memory
    map_memory(new_size);


}

void* SharedMemoryBuffer::data() {
    return m_data;
}

const void* SharedMemoryBuffer::data() const {
    return m_data;
}

void SharedMemoryBuffer::open_file() {
#ifdef _WIN32
    // Windows implementation
    // Open the file for reading and writing
    m_file_handle = CreateFileA(m_name.c_str(), GENERIC_READ | GENERIC_WRITE, 0, nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (m_file_handle == INVALID_HANDLE_VALUE) {
        throw std::runtime_error("Failed to open memory-mapped file");
    }

    // Get the file size
    m_file_size = GetFileSize(m_file_handle, nullptr);

    // Set the file size
    if (m_file_size == 0) {
        LARGE_INTEGER file_size;
        file_size.QuadPart = 1;
        if (!SetFilePointerEx(m_file_handle, file_size, nullptr, FILE_BEGIN) || !SetEndOfFile(m_file_handle)) {
            CloseHandle(m_file_handle);
            throw std::runtime_error("Error: Unable to set file size (error code " + std::to_string(GetLastError()) + ")");
        }
    }

#else
    // Linux implementation
    // Open the file for reading and writing
    m_file_descriptor = open(m_name.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (m_file_descriptor == -1) {
        throw std::runtime_error("Failed to open memory-mapped file");
    }

    // Get the file size
    struct stat fileInfo;
    if (fstat(m_file_descriptor, &fileInfo) == -1) {
        close(m_file_descriptor);
        throw std::runtime_error("Failed to get memory-mapped file size");
    }
    m_file_size = fileInfo.st_size;

    // Map the file into memory
    map_memory(m_file_size);
#endif
}

void SharedMemoryBuffer::close_file() {
#ifdef _WIN32
    // Windows implementation
    // Close the file mapping handle and file handle
    CloseHandle(m_file_mapping);
    CloseHandle(m_file_handle);
#else
    // Linux implementation
    // Close the file descriptor
    close(m_file_descriptor);
#endif
}

void SharedMemoryBuffer::map_memory(std::size_t size) {
#ifdef _WIN32
    // Windows implementation
    // Create or open the file mapping
    m_file_mapping = CreateFileMappingA(m_file_handle, nullptr, PAGE_READWRITE, 0, size, nullptr);
    if (m_file_mapping == nullptr) {
        CloseHandle(m_file_handle);
        DWORD error_code = GetLastError();
        throw std::runtime_error("Failed to create file mapping. Error code: " + std::to_string(error_code));
    }
    // Map the file into memory
    m_data = MapViewOfFile(m_file_mapping, FILE_MAP_ALL_ACCESS, 0, 0, size);
    if (m_data == nullptr) {
        CloseHandle(m_file_mapping);
        CloseHandle(m_file_handle);
        m_file_mapping = nullptr;
        m_file_handle = nullptr;
        DWORD error_code = GetLastError();
        throw std::runtime_error("Failed to create view of mapping. Error code: " + std::to_string(error_code));

    }
    // Store the file handle
    //m_file_handle = m_file_mapping;
#else
    // Linux implementation
    // Map the file into memory
    m_data = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, m_file_descriptor, 0);
    if (m_data == MAP_FAILED) {
        close(m_file_descriptor);
        throw std::runtime_error("Failed to memory-map file");
    }
#endif
}

void SharedMemoryBuffer::unmap_memory() {
#ifdef _WIN32
    // Windows implementation
    // Unmap the memory-mapped file
    if (m_data != nullptr) {
        UnmapViewOfFile(m_data);
        m_data = nullptr;
    }
    if (m_file_mapping != nullptr) {
        CloseHandle(m_file_mapping);
        m_file_mapping = nullptr;
    }
#else
    // Linux implementation
    // Unmap the memory-mapped file
    munmap(m_data, m_size);
#endif
}
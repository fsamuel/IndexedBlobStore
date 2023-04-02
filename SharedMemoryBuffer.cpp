#include "SharedMemoryBuffer.h"
#include <algorithm>
#include <stdexcept>
#include <fstream>

SharedMemoryBuffer::SharedMemoryBuffer(SharedMemoryBuffer&& other) noexcept
	: m_name(std::move(other.m_name))
	, m_size(other.m_size)
	, m_data(other.m_data)
#ifdef _WIN32
	, m_file_handle(other.m_file_handle)
	, m_file_mapping(other.m_file_mapping)
#else
	, m_file_descriptor(other.m_file_descriptor)
#endif 
{
	other.m_size = 0;
	other.m_data = nullptr;
#ifdef _WIN32
	other.m_file_handle = nullptr;
	other.m_file_mapping = nullptr;
#else
	other.m_file_descriptor = -1;
#endif
}

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
{
	// Get the size of the file on disk
	std::ifstream file(m_name, std::ios::binary | std::ios::ate);
	std::streampos size_on_disk = file.tellg();
	file.close();

	// If the file doesn't exist, set the size to 0
	if (size_on_disk == -1) {
		size_on_disk = 0;
	}

	open_file();
	map_memory(size_on_disk);
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
{
	open_file();
	resize(size);
}

SharedMemoryBuffer::~SharedMemoryBuffer() {
	unmap_memory();
	close_file();
}

void SharedMemoryBuffer::resize(std::size_t new_size) {
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

void SharedMemoryBuffer::flush() {
	if (m_data == nullptr || m_size == 0) {
		return;
	}

#ifdef _WIN32
	// Windows implementation
	if (!FlushViewOfFile(m_data, m_size)) {
		throw std::runtime_error("Failed to flush memory-mapped file: FlushViewOfFile");
	}
	if (!FlushFileBuffers(m_file_handle)) {
		throw std::runtime_error("Failed to flush memory-mapped file: FlushFileBuffers");
	}
#else
	// Linux implementation
	if (msync(m_data, m_size, MS_SYNC) == -1) {
		throw std::runtime_error("Failed to flush memory-mapped file: msync");
	}
	if (fsync(m_file_descriptor) == -1) {
		throw std::runtime_error("Failed to flush memory-mapped file: fsync");
	}
#endif
}

void SharedMemoryBuffer::open_file() {
#ifdef _WIN32
	// Windows implementation
	// Open the file for reading and writing
	m_file_handle = CreateFileA(m_name.c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
	if (m_file_handle == INVALID_HANDLE_VALUE) {
		throw std::runtime_error("Failed to open memory-mapped file");
	}

	// Get the file size
	m_size = GetFileSize(m_file_handle, nullptr);

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
		m_data = nullptr;
		return;
	}
	// Map the file into memory
	m_data = MapViewOfFile(m_file_mapping, FILE_MAP_ALL_ACCESS, 0, 0, size);
	if (m_data == nullptr) {
		CloseHandle(m_file_mapping);
		CloseHandle(m_file_handle);
		m_data = nullptr;
		m_file_mapping = nullptr;
		m_file_handle = nullptr;
		return;
	}
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
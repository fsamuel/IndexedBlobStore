#ifndef FIXED_STRING_H_
#define FIXED_STRING_H_

#include <cstring>
#include <string>
#include <algorithm>
#include <functional>
#include "storage_traits.h"
#include "string_slice.h"

#ifdef _WIN32
#undef max
#undef min
#endif

#if defined(_MSC_VER)
#pragma pack(push, 1)
#endif
class FixedString {
public:
    // The size of the data is stored at the start of the memory block
    size_t size;
    // Hash of the string data
    std::size_t hash;
    // Flexible array member for the string data
    char data[1];

    // Regular constructor from std::string
    // This isn't safe unless memory was preallocated for the FixedString.
    explicit FixedString(const std::string& str) : size(str.size()), hash(std::hash<std::string>{}(str)) {
        std::memcpy(data, str.data(), size);
    }

    // Regular constructor from StringSlice.
    // This isn't safe unless memory was preallocated for the FixedString.
    explicit FixedString(const StringSlice& slice) : size(slice.size()), hash(std::hash<StringSlice>{}(slice)) {
        std::memcpy(data, slice.data(), size);
    }

    // Constructor from a C string
    // This isn't safe unless memory was preallocated for the FixedString.
    explicit FixedString(const char* str) : size(std::strlen(str)), hash(std::hash<std::string>{}(str)) {
		std::memcpy(data, str, size);
	}

    StringSlice substring(size_t start, size_t length) const {
        // Check if start is out of bounds
        if (start >= size) {
            throw std::out_of_range("Start index out of bounds");
        }

        // Adjust length if it goes beyond the end of the string
        if (start + length > size) {
            length = size - start;
        }

        // Create a StringSlice representing the substring
        return StringSlice(data + start, 0, length);
    }


    // Equality comparison with other FixedString
    bool operator==(const FixedString& other) const {
        return hash == other.hash && size == other.size && std::memcmp(data, other.data, size) == 0;
    }

    bool operator!=(const FixedString& other) const {
        return !(*this == other);
    }
    // Equality comparison with std::string
    bool operator==(const std::string& str) const {
        return size == str.size() && hash == std::hash<std::string>{}(str) && std::memcmp(data, str.data(), size) == 0;
    }

    // Inequality comparison with std::string
    bool operator!=(const std::string& str) const {
        return !(*this == str);
    }

    // Equality comparison with StringSlice
    bool operator==(const StringSlice& slice) const {
        return size == slice.size() && hash == std::hash<StringSlice>{}(slice) && std::memcmp(data, slice.data(), size) == 0;
    }

    // Inequality comparison with StringSlice
    bool operator!=(const StringSlice& slice) const {
        return !(*this == slice);
    }

    // Allow for comparison with std::string on the LHS
    friend bool operator==(const std::string& str, const FixedString& fs) {
        return fs == str;
    }

    friend bool operator!=(const std::string& str, const FixedString& fs) {
        return fs != str;
    }

    // Allow for comparison with StringSlice on the LHS
    friend bool operator==(const StringSlice& slice, const FixedString& fs) {
        return fs == slice;
    }

    friend bool operator!=(const StringSlice& slice, const FixedString& fs) {
        return fs != slice;
    }

    // Less than and greater than comparisons
    bool operator<(const FixedString& other) const {
        int cmp = std::memcmp(data, other.data, std::min(size, other.size));
        return cmp < 0 || (cmp == 0 && size < other.size);
    }

    bool operator>(const FixedString& other) const {
        return other < *this;
    }

    bool operator<=(const FixedString& other) const {
        return !(other < *this);
    }

    bool operator>=(const FixedString& other) const {
        return !(*this < other);
    }

    // Less than comparison with std::string
    bool operator<(const std::string& str) const {
        int cmp = std::memcmp(data, str.data(), std::min(size, str.size()));
        return cmp < 0 || (cmp == 0 && size < str.size());
    }

    // Greater than comparison with std::string
    bool operator>(const std::string& str) const {
        return str < *this;
    }

    // Less than comparison with StringSlice
    bool operator<(const StringSlice& slice) const {
        int cmp = std::memcmp(data, slice.data(), std::min(size, slice.size()));
        return cmp < 0 || (cmp == 0 && size < slice.size());
    }

    // Greater than comparison with StringSlice
    bool operator>(const StringSlice& slice) const {
        return slice < *this;
    }

    // Allow for comparison with std::string on the LHS
    friend bool operator<(const std::string& str, const FixedString& fs) {
        return fs > str;
    }

    friend bool operator>(const std::string& str, const FixedString& fs) {
        return fs < str;
    }

    // Allow for comparison with StringSlice on the LHS
    friend bool operator<(const StringSlice& slice, const FixedString& fs) {
        return fs > slice;
    }

    friend bool operator>(const StringSlice& slice, const FixedString& fs) {
        return fs < slice;
    }

    // Conversion to std::string
    operator std::string() const {
        return std::string(data, size);
    }

    // Conversion to StringSlice
    operator StringSlice() const {
        return StringSlice(reinterpret_cast<const char*>(&data), 0, size);
    }

    // Stream insertion operator to print the FixedString to an output stream.
    friend std::ostream& operator<<(std::ostream& os, const FixedString& str);
}
#if defined(__GNUC__) || defined(__clang__)
__attribute__((packed))
#endif
;

#if defined(_MSC_VER)
#pragma pack(pop)
#endif

template <>
struct StorageTraits<std::string> {
    using StorageType = FixedString;

    template <typename... Args>
    static size_t size(const std::string& str) {
        return sizeof(FixedString) + str.size() - 1;
    }
};

template <>
struct StorageTraits<const std::string> {
    using StorageType = const FixedString;

    template <typename... Args>
    static size_t size(const std::string& str) {
        return sizeof(FixedString) + str.size() - 1;
    }
};

template <>
struct StorageTraits<StringSlice> {
    using StorageType = FixedString;

    template<typename... Args>
    static size_t size(const StringSlice& str) {
        return sizeof(FixedString) + str.size() - 1;
    }
};

template <std::size_t N>
struct StorageTraits<const char(&)[N]> {
    using StorageType = FixedString;

    static size_t size(const char(&)[N]) {
        return sizeof(FixedString) + N - 2; // subtract 2 because N includes the null terminator
    }
};

#endif // FIXED_STRING_H_
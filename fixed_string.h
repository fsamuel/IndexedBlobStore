#ifndef FIXED_STRING_H_
#define FIXED_STRING_H_

#include <cstring>
#include <string>
#include <algorithm>
#include <functional>
#include "size_traits.h"
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

    // Equality comparison with other FixedString
    bool operator==(const FixedString& other) const {
        return hash == other.hash && size == other.size && std::memcmp(data, other.data, size) == 0;
    }

    bool operator!=(const FixedString& other) const {
        return !(*this == other);
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

template <typename... Args>
struct SizeTraits<FixedString, Args...> {
    static size_t size(const std::string& str) {
        return sizeof(FixedString) + str.size() - 1;
    }

    /*
    static size_t size(const StringSlice& slice) {
        return sizeof(FixedString) + slice.size() - 1;
    }*/

    template <std::size_t N>
    static size_t size(const char(&)[N]) {
        return sizeof(FixedString) + N - 2; // subtract 2 because N includes the null terminator
    }
};

#endif // FIXED_STRING_H_
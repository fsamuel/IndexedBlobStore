#ifndef STRING_SLICE_H_
#define STRING_SLICE_H_

#include <string>
#include <cstring>
#include <ostream>

#ifdef _WIN32
#undef max
#undef min
#endif

// The StringSlice class represents a slice of a string, i.e., a substring 
// defined by an offset and a size within the original string.
//
// The class stores a pointer to the original string and the offset and size
// of the slice, rather than copying the slice of the string. This makes the 
// class lightweight and efficient for passing around slices of strings without 
// copying the actual string data.
//
// However, note that the StringSlice object does not own the original string 
// data and does not manage its lifetime. The user must ensure that the original 
// string data remains valid as long as the StringSlice object is in use.
//
// The class provides comparison operations that compare the slice of the string
// it represents with other StringSlice objects or with std::string objects. 
// It also provides a method to convert the slice to a std::string by copying 
// the slice of the string.

class StringSlice {
public:
    // Constructs a StringSlice representing the given slice of the string.
    StringSlice(const char* str, size_t offset, size_t size);

    // Copy constructor.
    StringSlice(const StringSlice& other);

    // Assignment operator.
    StringSlice& operator=(const StringSlice& other);

    // Returns the size of the slice.
    size_t size() const {
        return size_;
    }

    // Returns the character at the given position within the slice.
    // Throws std::out_of_range if the position is out of range.
    char at(size_t pos) const;

    // Accesses the character at the given position within the slice.
    // Throws std::out_of_range if the position is out of range.
    char operator[](size_t pos) const {
        return at(pos);
    }

    // Returns a pointer to the underlying character data.
    const char* data() const {
        return str_ + offset_;
    }

    // Returns a new StringSlice representing a slice of this StringSlice. 
    // The new slice starts at the given offset within this slice and has the given size.
    // If the given offset and size would extend beyond the end of this slice,
    // the size of the new slice is reduced to fit within this slice.
    StringSlice substring(size_t offset, size_t size) const;

    // Returns a std::string that is a copy of the slice of the string this 
    // StringSlice represents.
    std::string to_string() const;

    // Comparison operators.
    bool operator==(const StringSlice& other) const;
    bool operator!=(const StringSlice& other) const;
    bool operator<(const StringSlice& other) const;
    bool operator>(const StringSlice& other) const;
    bool operator<=(const StringSlice& other) const;
    bool operator>=(const StringSlice& other) const;

    // Equality comparison with null-terminated char*
    bool operator==(const char* str) const {
        return size_ == std::strlen(str) && std::memcmp(data(), str, size_) == 0;
    }

    // Inequality comparison with null-terminated char*
    bool operator!=(const char* str) const {
        return !(*this == str);
    }

    // Less than comparison with null-terminated char*
    bool operator<(const char* str) const {
        int cmp = std::memcmp(data(), str, std::min(size_, std::strlen(str)));
        return cmp < 0 || (cmp == 0 && size_ < std::strlen(str));
    }

    // Greater than comparison with null-terminated char*
    bool operator>(const char* str) const {
        return str < *this;
    }

    // Allow for comparison with null-terminated char* on the LHS
    friend bool operator==(const char* str, const StringSlice& slice) {
        return slice == str;
    }

    friend bool operator!=(const char* str, const StringSlice& slice) {
        return slice != str;
    }

    friend bool operator<(const char* str, const StringSlice& slice) {
        return slice > str;
    }

    friend bool operator>(const char* str, const StringSlice& slice) {
        return slice < str;
    }

private:
    const char* str_;  // pointer to the original string
    size_t offset_;    // offset of the slice within the string
    size_t size_;      // size of the slice

    // Stream insertion operator to print the StringSlice to an output stream.
    friend std::ostream& operator<<(std::ostream& os, const StringSlice& slice);
};

namespace std {
    template <>
    struct hash<StringSlice> {
        size_t operator()(const StringSlice& slice) const {
            size_t hash = 0;
            for (size_t i = 0; i < slice.size(); ++i) {
                // This is a simple hash function based on the djb2 algorithm.
                // It may not be suitable for all purposes, but it's a good starting point.
                hash = ((hash << 5) + hash) + slice[i]; // hash * 33 + c
            }
            return hash;
        }
    };
}

#endif // STRING_SLICE_H_

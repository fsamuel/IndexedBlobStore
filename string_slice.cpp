#include "string_slice.h"

StringSlice::StringSlice(const char* str, size_t offset, size_t size)
    : str_(str), offset_(offset), size_(size) {}

StringSlice::StringSlice(const StringSlice& other)
    : str_(other.str_), offset_(other.offset_), size_(other.size_) {}

StringSlice& StringSlice::operator=(const StringSlice& other) {
    if (this != &other) {
        str_ = other.str_;
        offset_ = other.offset_;
        size_ = other.size_;
    }
    return *this;
}

char StringSlice::at(size_t pos) const {
    if (pos >= size_) {
        throw std::out_of_range("Position is out of range");
    }
    return str_[offset_ + pos];
}

StringSlice StringSlice::substring(size_t offset, size_t size) const {
    if (offset > size_) {
        throw std::out_of_range("Offset is out of range");
    }
    if (offset + size > size_) {
        size = size_ - offset;  // reduce size to fit within this slice
    }
    return StringSlice(str_, offset_ + offset, size);
}

std::string StringSlice::to_string() const {
    return std::string(str_ + offset_, size_);
}

bool StringSlice::operator==(const StringSlice& other) const {
    return size_ == other.size_ && std::memcmp(str_ + offset_, other.str_ + other.offset_, size_) == 0;
}

bool StringSlice::operator!=(const StringSlice& other) const {
    return !(*this == other);
}

bool StringSlice::operator<(const StringSlice& other) const {
    int cmp = std::memcmp(str_ + offset_, other.str_ + other.offset_, std::min(size_, other.size_));
    return cmp < 0 || (cmp == 0 && size_ < other.size_);
}

bool StringSlice::operator>(const StringSlice& other) const {
    return other < *this;
}

bool StringSlice::operator<=(const StringSlice& other) const {
    return !(other < *this);
}

bool StringSlice::operator>=(const StringSlice& other) const {
    return !(*this < other);
}

std::ostream& operator<<(std::ostream& os, const StringSlice& slice) {
    for (size_t i = 0; i < slice.size_; ++i) {
        os << slice.str_[slice.offset_ + i];
    }
    return os;
}
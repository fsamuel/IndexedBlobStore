#include "gtest/gtest.h"
#include "fixed_string.h"
#include <cstring>

// Helper function to create a unique_ptr to a FixedString from a std::string
std::unique_ptr<FixedString> MakeFixedString(const std::string& str) {
    size_t size = StorageTraits<std::string>::size(str);
    void* buffer = ::operator new(size);
    return std::unique_ptr<FixedString>(new (buffer) FixedString(str));
}

// FixedString Constructor Test
TEST(FixedStringTest, Constructor) {
    std::string test_string = "hello";
    auto fs = MakeFixedString(test_string);

    EXPECT_EQ(fs->size, test_string.size()); // Check if size matches
    EXPECT_EQ(fs->hash, std::hash<std::string>{}(test_string)); // Check if hash matches
    EXPECT_EQ(std::memcmp(fs->data, test_string.data(), fs->size), 0); // Check if data matches
}

// FixedString Equality Operator Test
TEST(FixedStringTest, EqualityOperator) {
    std::string test_string = "hello";
    auto fs1 = MakeFixedString(test_string);
    auto fs2 = MakeFixedString(test_string);

    EXPECT_TRUE(*fs1 == *fs2); // Same data, should be equal

    std::string different_string = "world";
    auto fs3 = MakeFixedString(different_string);

    EXPECT_FALSE(*fs1 == *fs3); // Different data, should not be equal
}

// FixedString Inequality Operator Test
TEST(FixedStringTest, InequalityOperator) {
    std::string test_string = "hello";
    auto fs1 = MakeFixedString(test_string);
    auto fs2 = MakeFixedString(test_string);

    EXPECT_FALSE(*fs1 != *fs2); // Same data, should not be unequal

    std::string different_string = "world";
    auto fs3 = MakeFixedString(different_string);

    EXPECT_TRUE(*fs1 != *fs3); // Different data, should be unequal
}

// FixedString Less Than Operator Test
TEST(FixedStringTest, LessThanOperator) {
    std::string str1 = "abc";
    std::string str2 = "def";

    auto fs1 = MakeFixedString(str1);
    auto fs2 = MakeFixedString(str2);

    EXPECT_TRUE(*fs1 < *fs2); // "abc" is lexicographically less than "def"
    EXPECT_FALSE(*fs2 < *fs1); // "def" is not less than "abc"
}

// FixedString Conversion To std::string Test
TEST(FixedStringTest, ConversionToString) {
    std::string test_string = "hello";
    auto fs = MakeFixedString(test_string);

    std::string converted_string = static_cast<std::string>(*fs); // Convert FixedString to std::string

    EXPECT_EQ(test_string, converted_string); // The original and converted string should match
}

// FixedString Conversion To StringSlice Test
TEST(FixedStringTest, ConversionToStringSlice) {
    std::string test_string = "hello";
    auto fs = MakeFixedString(test_string);
    EXPECT_EQ((*fs)[0], 'h');
    EXPECT_EQ((*fs)[1], 'e');
    EXPECT_EQ((*fs)[2], 'l');
    EXPECT_EQ((*fs)[3], 'l');
    EXPECT_EQ((*fs)[4], 'o');

    StringSlice converted_slice = static_cast<StringSlice>(*fs); // Convert FixedString to StringSlice

    EXPECT_EQ(converted_slice.data(), fs->data); // The data pointers should match
    EXPECT_EQ(converted_slice.size(), fs->size); // The sizes should match
}

// FixedString Stream Insertion Operator Test
TEST(FixedStringTest, StreamInsertionOperator) {
    std::string test_string = "hello";
    auto fs = MakeFixedString(test_string);

    std::stringstream ss;
    ss << *fs; // Insert FixedString into stringstream

    EXPECT_EQ(ss.str(), test_string); // The string inserted into the stringstream should match the original
}
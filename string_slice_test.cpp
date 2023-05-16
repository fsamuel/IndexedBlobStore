#include "gtest/gtest.h"
#include "string_slice.h"

// This test case tests the basic functionality of the StringSlice class.
class StringSliceTest : public ::testing::Test {
protected:
    // This string will be used for all the tests.
    const char* testString = "Hello, World!";
};

// Tests the constructor and the getSize function.
TEST_F(StringSliceTest, ConstructorAndGetters) {
    // Constructs a StringSlice from the first 5 characters of the test string.
    StringSlice slice(testString, 0, 5);

    // Checks that the size of the slice is correct.
    EXPECT_EQ(slice.size(), 5);

    // Checks that the characters in the slice are correct.
    for (size_t i = 0; i < slice.size(); ++i) {
        EXPECT_EQ(slice[i], testString[i]);
    }
}

// Tests the toString function.
TEST_F(StringSliceTest, ToString) {
    // Constructs a StringSlice from the first 5 characters of the test string.
    StringSlice slice(testString, 0, 5);

    // Checks that the toString function returns the correct string.
    EXPECT_EQ(slice.to_string(), "Hello");
}

// Tests the comparison operators.
TEST_F(StringSliceTest, ComparisonOperators) {
    // Constructs two identical StringSlices.
    StringSlice slice1(testString, 0, 5);
    StringSlice slice2(testString, 0, 5);

    // Checks that the slices are equal.
    EXPECT_TRUE(slice1 == slice2);

    // Changes the second slice and checks that they are no longer equal.
    slice2 = StringSlice(testString, 0, 4);
    EXPECT_TRUE(slice1 != slice2);

    // Checks the other comparison operators.
    EXPECT_TRUE(slice1 > slice2);
    EXPECT_TRUE(slice1 >= slice2);
    EXPECT_TRUE(slice2 < slice1);
    EXPECT_TRUE(slice2 <= slice1);
}

// Tests the copy constructor and assignment operator.
TEST_F(StringSliceTest, CopyConstructorAndAssignmentOperator) {
    // Constructs a StringSlice and makes a copy with the copy constructor.
    StringSlice slice1(testString, 0, 5);
    StringSlice slice2 = slice1;

    // Checks that the copy is equal to the original.
    EXPECT_TRUE(slice1 == slice2);

    // Changes the original and checks that the copy is not affected.
    slice1 = StringSlice(testString, 0, 4);
    EXPECT_TRUE(slice1 != slice2);
}

// Tests the substring function.
TEST_F(StringSliceTest, Substring) {
    // Constructs a StringSlice and takes a substring.
    StringSlice slice(testString, 0, 5);
    StringSlice subSlice = slice.substring(1, 3);

    // Checks that the substring is correct.
    EXPECT_EQ(subSlice.to_string(), "ell");
}

// Tests the charAt function.
TEST_F(StringSliceTest, CharAt) {
    // Constructs a StringSlice.
    StringSlice slice(testString, 0, 5);

    // Checks that the charAt function returns the correct characters.
    EXPECT_EQ(slice.at(0), 'H');
    EXPECT_EQ(slice.at(4), 'o');
}

// Tests the stream insertion operator.
TEST_F(StringSliceTest, StreamInsertion) {
    // Constructs a StringSlice.
    StringSlice slice(testString, 0, 5);

    // Creates a stringstream and inserts the slice.
    std::stringstream ss;
    ss << slice;

    // Checks that the stringstream contains the correct string.
    EXPECT_EQ(ss.str(), "Hello");
}

// Tests the operator[] function.
TEST_F(StringSliceTest, OperatorSquareBrackets) {
    // Constructs a StringSlice.
    StringSlice slice(testString, 0, 5);

    // Checks that the operator[] function returns the correct characters.
    EXPECT_EQ(slice[0], 'H');
    EXPECT_EQ(slice[4], 'o');
}
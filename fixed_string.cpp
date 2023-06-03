#include "fixed_string.h"

std::ostream& operator<<(std::ostream& os, const FixedString& str) {
  for (size_t i = 0; i < str.size; ++i) {
    os << str.data[i];
  }
  return os;
}

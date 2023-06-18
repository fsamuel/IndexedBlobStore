#ifndef SIZE_TRAITS_H_
#define SIZE_TRAITS_H_

// StorageTraits is a traits class that provides information about how to store
// a given in-memory type. For example, std::string is a dynamic,
// variable-length type that relies on the heap and cannot be directly stored on
// disk or in shared memory. Instead, we map it to a fixed-length array of
// characters. This class provides the information necessary to do that mapping
// and many others.

// The default StorageTraits class is a no-op. It is used for types that can be
// stored directly on disk or in shared memory. For example, int, float, and
// double are all fixed-length types that can be stored directly.
template <typename T>
struct StorageTraits {
  using StorageType = T;
  using SearchType = T;
  using ElementType = T;

  template <typename... Args>
  static size_t size(Args&&... args) {
    return sizeof(T);
  }
};

// Specialization for const int[N] and int[N]. These are fixed-length types that
// can be stored directly as std::array<int, N>.
template <std::size_t N>
struct StorageTraits<const int[N]> {
  using StorageType = std::array<int, N>;
  using SearchType = const int*;
  using ElementType = int;

  static size_t size(const int (&)[N]) { return sizeof(int) * N; }

  static size_t size() { return sizeof(int) * N; }
};

template <std::size_t N>
struct StorageTraits<int[N]> {
  using StorageType = std::array<int, N>;
  using SearchType = int*;
  using ElementType = int;

  static size_t size(int (&)[N]) { return sizeof(int) * N; }

  static size_t size() { return sizeof(int) * N; }

  static size_t size(const StorageType& arr) { return sizeof(int) * N; }
};

#endif  // SIZE_TRAITS_H_
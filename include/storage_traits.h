#ifndef SIZE_TRAITS_H_
#define SIZE_TRAITS_H_

template <typename T>
struct is_unsized_array : std::false_type {};

template <typename T, std::size_t N>
struct is_unsized_array<std::array<T, N>> : std::false_type {};

template <typename T>
struct is_unsized_array<T[]> : std::true_type {};

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

  static ElementType& GetElement(StorageType* ptr, size_t index) {
    // This should never be called.
    assert(false);
    return ptr[index];
  }
};

template <typename T>
struct StorageTraits<T[]> {
  using StorageType = typename StorageTraits<T>::StorageType;
  using ElementType = T;
  using SearchType = T*;

  static ElementType& GetElement(StorageType* ptr, size_t index) {
    return ptr[index];
  }
};

// Specialization for const T[N] and T[N]. These are fixed-length types that
// can be stored directly as std::array<T, N>.
template <typename T, std::size_t N>
struct StorageTraits<const T[N]> {
  using StorageType = std::array<T, N>;
  using SearchType = const T*;
  using ElementType = T;

  static size_t size(const T (&)[N]) { return sizeof(T) * N; }

  static size_t size() { return sizeof(T) * N; }

  static ElementType& GetElement(StorageType* ptr, size_t index) {
    return (*ptr)[index];
  }
};

template <typename T, std::size_t N>
struct StorageTraits<T[N]> {
  using StorageType = std::array<T, N>;
  using SearchType = T*;
  using ElementType = T;

  static size_t size(T (&)[N]) { return sizeof(T) * N; }

  static size_t size() { return sizeof(T) * N; }

  static size_t size(const StorageType& arr) { return sizeof(T) * N; }

  static ElementType& GetElement(StorageType* ptr, size_t index) {
    return (*ptr)[index];
  }
};

#endif  // SIZE_TRAITS_H_
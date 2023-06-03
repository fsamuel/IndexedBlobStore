#ifndef SIZE_TRAITS_H_
#define SIZE_TRAITS_H_

// Default StorageTraits just returns sizeof(T)
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
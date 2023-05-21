#ifndef SIZE_TRAITS_H_
#define SIZE_TRAITS_H_

// Default StorageTraits just returns sizeof(T)
template <typename T>
struct StorageTraits {
    using StorageType = T;
    using SearchType = typename std::remove_extent<T>::type;

    template<typename... Args>
    static size_t size(Args&&... args) {
        return sizeof(T);
    }

    static SearchType data(const T& value) {
        return value;
    }
};

#endif // SIZE_TRAITS_H_
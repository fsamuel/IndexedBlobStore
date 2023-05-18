#ifndef SIZE_TRAITS_H_
#define SIZE_TRAITS_H_

// Default StorageTraits just returns sizeof(T)
template <typename T>
struct StorageTraits {
    using StorageType = T;

    template<typename... Args>
    static size_t size(Args&&... args) {
        return sizeof(T);
    }
};

#endif // SIZE_TRAITS_H_
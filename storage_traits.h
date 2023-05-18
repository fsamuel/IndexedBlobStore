#ifndef SIZE_TRAITS_H_
#define SIZE_TRAITS_H_

// Default StorageTraits just returns sizeof(T)
template <typename T, typename... Args>
struct StorageTraits {
    using StorageType = T;

    static size_t size(Args&&... args) {
        return sizeof(T);
    }
};

#endif // SIZE_TRAITS_H_
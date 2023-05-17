#ifndef SIZE_TRAITS_H_
#define SIZE_TRAITS_H_

// Default SizeTraits just returns sizeof(T)
template <typename T, typename... Args>
struct SizeTraits {
    static size_t size(Args&&... args) {
        return sizeof(T);
    }
};

#endif // SIZE_TRAITS_H_
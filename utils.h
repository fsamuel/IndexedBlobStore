#ifndef UTILS_H_
#define UTILS_H_

#include <array>
#include <cstddef>
#include <initializer_list>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h> // For Windows
#else
#include <unistd.h> // For Linux
#endif

namespace utils {

template<typename U, typename... Args>
void Construct(U* p, Args&&... args)
{
	new (p) U(std::forward<Args>(args)...);
}

template<typename T, std::size_t N>
void Construct(std::array<T, N>* p, std::initializer_list<T> ilist)
{
	*p = ilist;
}

template<typename U>
void Destroy(U* p)
{
	p->~U();
}

size_t GetPageSize();

size_t RoundUpToPageSize(size_t size);

}  // namespace utils

#endif  // UTILS_H_
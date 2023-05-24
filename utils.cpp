#include "utils.h"

namespace utils {

	size_t GetPageSize() {
#ifdef _WIN32
		SYSTEM_INFO system_info;
		GetSystemInfo(&system_info);
		return system_info.dwPageSize;
#else
		return sysconf(_SC_PAGE_SIZE);
#endif
	}

	size_t RoundUpToPageSize(size_t size) {
		size_t page_size = GetPageSize();
		return ((size + page_size - 1) / page_size) * page_size;
	}

}  // namespace utils
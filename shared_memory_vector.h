#ifndef SHARED_MEMORY_VECTOR_H_
#define SHARED_MEMORY_VECTOR_H_

#include <stdexcept> 
#include "shared_memory_allocator.h"

template<typename T, typename Allocator = SharedMemoryAllocator<T>>
class SharedMemoryVector {
public:
	using allocator_type = Allocator;
	using value_type = T;
	using size_type = std::size_t;
	using difference_type = typename allocator_type::offset_type;;
	using reference = T&;
	using const_reference = const T&;
	using offset_type = typename allocator_type::offset_type;
	using iterator = T*;
	using const_iterator = const T*;
	using reverse_iterator = std::reverse_iterator<iterator>;
	using const_reverse_iterator = std::reverse_iterator<const_iterator>;

private:
	// Header for the allocator state in the shared memory buffer
	struct VectorStateHeader {
		uint32_t magic_number;
		size_type size;
		offset_type data;
	};

	allocator_type& allocator_;

	void InitializeVectorStateIfNecessary() {
		auto it = allocator_.first();
		VectorStateHeader* stateHeaderPtr = reinterpret_cast<VectorStateHeader*>(&*it);
		if (it == allocator_.end()) {
			stateHeaderPtr = reinterpret_cast<VectorStateHeader*>(allocator_.Allocate(sizeof(VectorStateHeader)));
		}
		// Check if the vector state header has already been initialized
		if (stateHeaderPtr->magic_number != 0xfeedfeed) {
			// Initialize the allocator state header
			stateHeaderPtr->magic_number = 0xfeedfeed;
			stateHeaderPtr->size = 0;
			stateHeaderPtr->data = -1;
		}
	}

	VectorStateHeader* state() {
		return reinterpret_cast<VectorStateHeader*>(&*allocator_.first());
	}

	const VectorStateHeader* state() const {
		return reinterpret_cast<const VectorStateHeader*>(&*allocator_.first());
	}

public:
	explicit SharedMemoryVector(allocator_type& alloc) : allocator_(alloc) {
		InitializeVectorStateIfNecessary();
	}

	explicit SharedMemoryVector(size_type count, const T& value = T(), const allocator_type& alloc = allocator_type())
		: allocator_(alloc) {
		InitializeVectorStateIfNecessary();
		state()->size = count;
		state()->data = allocator_.ToOffset(allocator_.Allocate(count * sizeof(T)));

		for (size_type i = 0; i < state()->size; ++i) {
			allocator_.Construct(allocator_.ToPtr<T>(state()->data) + i, value);
		}
	}

	SharedMemoryVector(const SharedMemoryVector& other)
		: allocator_(other.allocator_) {
		InitializeVectorStateIfNecessary();
		state()->size = other.state()->size;
		state()->data = allocator_.ToOffset(allocator_.Allocate(state()->state * sizeof(T)));
		for (size_type i = 0; i < state()->size; ++i) {
			allocator_.Construct(allocator_.ToPtr<T>(state()->data) + i, other[i]);
		}
	}

	SharedMemoryVector(SharedMemoryVector&& other)
		: allocator_(std::move(other.allocator_))
	{
		InitializeVectorStateIfNecessary();
		state()->size = other.state()->size;
		state()->data = other.state()->data;
		other.state()->size = 0;
		other.state()->data = -1;
	}

	SharedMemoryVector& operator=(const SharedMemoryVector& other) {
		if (this != &other) {
			clear();
			allocator_.Deallocate(allocator_.ToPtr<T>(state()->data));
			state()->size = other.state()->size;
			allocator_ = other.allocator_;
			state()->data = allocator_.ToOffset(allocator_.Allocate(state()->size));
			for (size_type i = 0; i < state()->size; ++i) {
				allocator_.Construct(allocator_.ToPtr(state()->data) + i, other[i]);
			}
		}
		return *this;
	}

	SharedMemoryVector& operator=(SharedMemoryVector&& other) noexcept {
		if (this != &other) {
			clear();
			allocator_.Deallocate(allocator_.ToPtr<T>(state()->data));
			state()->size = other.state()->size;
			allocator_ = std::move(other.allocator_);
			state()->data = other.state()->data;
			other.state()->data = -1;
			other.state()->size = 0;
		}
		return *this;
	}

	~SharedMemoryVector() {
	}

	size_type size() const noexcept {
		return state()->size;
	}

	bool empty() const noexcept {
		return size() == 0;
	}

	void clear() noexcept
	{
		allocator_.Deallocate(allocator_.ToPtr<T>(state()->data));
		state()->size = 0;
		state()->data = -1;
	}

	iterator begin() noexcept {
		return allocator_.ToPtr<T>(state()->data);
	}

	const_iterator begin() const noexcept {
		return allocator_.ToPtr<T>(state()->data);
	}

	iterator end() noexcept {
		return allocator_.ToPtr<T>(state()->data + state()->size * sizeof(T));
	}

	const_iterator end() const noexcept {
		return allocator_.ToPtr<T>(state()->data + state()->size * sizeof(T));
	}

	reverse_iterator rbegin() noexcept {
		return reverse_iterator(end());
	}

	const_reverse_iterator rbegin() const noexcept {
		return const_reverse_iterator(end());
	}

	reverse_iterator rend() noexcept {
		return reverse_iterator(begin());
	}

	const_reverse_iterator rend() const noexcept {
		return const_reverse_iterator(begin());
	}

	size_type GetCapacity() const noexcept {
		return allocator_.GetCapacity(state()->data);
	}

	void reserve(size_type new_cap) {
		if (new_cap <= GetCapacity()) {
			return;
		}
		T* new_data = reinterpret_cast<T*>(allocator_.Allocate(new_cap * sizeof(T)));
		for (size_type i = 0; i < state()->size; ++i) {
			allocator_.Construct(new_data + i, std::move_if_noexcept(allocator_.ToPtr<T>(state()->data)[i]));
			allocator_.Destroy(allocator_.ToPtr<T>(state()->data) + i);
		}
		allocator_.Deallocate(state()->data);
		state()->data = allocator_.ToOffset(new_data);
	}

	const T* data() const noexcept {
		if (state()->data == -1) {
			return nullptr;
		}
		return allocator_.ToPtr<T>(state()->data);
	}

	T* data() noexcept {
		if (state()->data == -1) {
			return nullptr;
		}
		return allocator_.ToPtr<T>(state()->data);
	}

	reference operator[](size_type pos) {
		return *(begin() + pos);
	}

	const_reference operator[](size_type pos) const {
		return *(begin() + pos);
	}

	reference at(size_type pos) {
		if (pos >= size()) {
			throw std::out_of_range("SharedMemoryVector::at");
		}
		return (*this)[pos];
	}

	const_reference at(size_type pos) const {
		if (pos >= size()) {
			throw std::out_of_range("SharedMemoryVector::at");
		}
		return (*this)[pos];
	}

	reference front() {
		return allocator_.ToPtr<T>(state()->data)[0];
	}

	const_reference front() const {
		return allocator_.ToPtr<T>(state()->data)[0];
	}

	reference back() {
		return allocator_.ToPtr<T>(state()->data)[state()->size - 1];
	}

	const_reference back() const {
		return allocator_.ToPtr<T>(state()->data)[state()->size - 1];
	}

	template <typename... Args>
	void emplace_back(Args&&... args) {
		if (state()->size == GetCapacity()) {
			reserve(2 * GetCapacity() + 1);
		}
		allocator_.Construct(allocator_.ToPtr<T>(state()->data + state()->size * sizeof(T)), std::forward<Args>(args)...);
		++state()->size;
	}

	void push_back(const value_type& value) {
		emplace_back(value);
	}

	void push_back(value_type&& value) {
		emplace_back(std::move(value));
	}

	void pop_back() {
		if (empty()) {
			return;
		}
		allocator_.Destroy(allocator_.ToPtr<T>(state()->data) + state()->size - 1);
		--state()->size;
	}

	iterator erase(const_iterator pos) {
		difference_type index = pos - begin();
		for (iterator it = begin() + index; it != end() - 1; ++it) {
			std::swap(*it, *(it + 1));
		}
		pop_back();
		return begin() + index;
	}

	iterator erase(const_iterator first, const_iterator last) {
		difference_type index = first - begin();
		difference_type count = last - first;
		for (iterator it = begin() + index; it != end() - count; ++it) {
			std::swap(*it, *(it + count));
		}
		while (count--) {
			pop_back();
		}
		return begin() + index;
	}

	void Resize(size_type count, const value_type& value = value_type()) {
		if (count > state()->size) {
			reserve(count);
			for (size_type i = state()->size; i < count; ++i) {
				allocator_.Construct(allocator_.ToPtr<T>(state()->data) + i, value);
			}
		}
		else if (count < state()->size) {
			for (size_type i = count; i < state()->size; ++i) {
				allocator_.Destroy(allocator_.ToPtr<T>(state()->data) + i);
			}
		}
		state()->size = count;
	}
};

#endif  // SHARED_MEMORY_VECTOR_H_

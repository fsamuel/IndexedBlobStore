#ifndef __SHARED_MEMORY_VECTOR_H
#define __SHARED_MEMORY_VECTOR_H

#include <stdexcept> 
#include "SharedMemoryAllocator.h"

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
		uint32_t magicNumber;
		size_type size;
		offset_type data;
	};

	allocator_type& m_allocator;

	void initializeVectorStateIfNecessary() {
		auto it = m_allocator.first();
		VectorStateHeader* stateHeaderPtr = reinterpret_cast<VectorStateHeader*>(&*it);
		if (it == m_allocator.end()) {
			stateHeaderPtr = reinterpret_cast<VectorStateHeader*>(m_allocator.allocate(sizeof(VectorStateHeader)));
		}
		// Check if the vector state header has already been initialized
		if (stateHeaderPtr->magicNumber != 0xfeedfeed) {
			// Initialize the allocator state header
			stateHeaderPtr->magicNumber = 0xfeedfeed;
			stateHeaderPtr->size = 0;
			stateHeaderPtr->data = -1;
		}
	}

	VectorStateHeader* state() {
		return reinterpret_cast<VectorStateHeader*>(&*m_allocator.first());
	}

	const VectorStateHeader* state() const {
		return reinterpret_cast<const VectorStateHeader*>(&*m_allocator.first());
	}

public:
	explicit SharedMemoryVector(allocator_type& alloc) : m_allocator(alloc) {
		initializeVectorStateIfNecessary();
	}

	explicit SharedMemoryVector(size_type count, const T& value = T(), const allocator_type& alloc = allocator_type())
		: m_allocator(alloc) {
		initializeVectorStateIfNecessary();
		state()->size = count;
		state()->data = m_allocator.ToOffset(m_allocator.allocate(count * sizeof(T)));

		for (size_type i = 0; i < state()->size; ++i) {
			m_allocator.construct(m_allocator.ToPtr<T>(state()->data) + i, value);
		}
	}

	SharedMemoryVector(const SharedMemoryVector& other)
		: m_allocator(other.m_allocator) {
		initializeVectorStateIfNecessary();
		state()->size = other.state()->size;
		state()->data = m_allocator.ToOffset(m_allocator.allocate(state()->state * sizeof(T)));
		for (size_type i = 0; i < state()->size; ++i) {
			m_allocator.construct(m_allocator.ToPtr<T>(state()->data) + i, other[i]);
		}
	}

	SharedMemoryVector(SharedMemoryVector&& other)
		: m_allocator(std::move(other.m_allocator))
	{
		initializeVectorStateIfNecessary();
		state()->size = other.state()->size;
		state()->data = other.state()->data;
		other.state()->size = 0;
		other.state()->data = -1;
	}

	SharedMemoryVector& operator=(const SharedMemoryVector& other) {
		if (this != &other) {
			clear();
			m_allocator.deallocate(m_allocator.ToPtr<T>(state()->data));
			state()->size = other.state()->size;
			m_allocator = other.m_allocator;
			state()->data = m_allocator.ToOffset(m_allocator.allocate(state()->size));
			for (size_type i = 0; i < state()->size; ++i) {
				m_allocator.construct(m_allocator.ToPtr(state()->data) + i, other[i]);
			}
		}
		return *this;
	}

	SharedMemoryVector& operator=(SharedMemoryVector&& other) noexcept {
		if (this != &other) {
			clear();
			m_allocator.deallocate(m_allocator.ToPtr<T>(state()->data));
			state()->size = other.state()->size;
			m_allocator = std::move(other.m_allocator);
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
		m_allocator.deallocate(m_allocator.ToPtr<T>(state()->data));
		state()->size = 0;
		state()->data = -1;
	}

	iterator begin() noexcept {
		return m_allocator.ToPtr<T>(state()->data);
	}

	const_iterator begin() const noexcept {
		return m_allocator.ToPtr<T>(state()->data);
	}

	iterator end() noexcept {
		return m_allocator.ToPtr<T>(state()->data + state()->size * sizeof(T));
	}

	const_iterator end() const noexcept {
		return m_allocator.ToPtr<T>(state()->data + state()->size * sizeof(T));
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

	size_type capacity() const noexcept {
		return m_allocator.capacity(state()->data);
	}

	void reserve(size_type new_cap) {
		if (new_cap <= capacity()) {
			return;
		}
		T* new_data = reinterpret_cast<T*>(m_allocator.allocate(new_cap * sizeof(T)));
		for (size_type i = 0; i < state()->size; ++i) {
			m_allocator.construct(new_data + i, std::move_if_noexcept(m_allocator.ToPtr<T>(state()->data)[i]));
			m_allocator.destroy(m_allocator.ToPtr<T>(state()->data) + i);
		}
		m_allocator.deallocate(state()->data);
		state()->data = m_allocator.ToOffset(new_data);
	}

	const T* data() const noexcept {
		if (state()->data == -1) {
			return nullptr;
		}
		return m_allocator.ToPtr<T>(state()->data);
	}

	T* data() noexcept {
		if (state()->data == -1) {
			return nullptr;
		}
		return m_allocator.ToPtr<T>(state()->data);
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
		return m_allocator.ToPtr<T>(state()->data)[0];
	}

	const_reference front() const {
		return m_allocator.ToPtr<T>(state()->data)[0];
	}

	reference back() {
		return m_allocator.ToPtr<T>(state()->data)[state()->size - 1];
	}

	const_reference back() const {
		return m_allocator.ToPtr<T>(state()->data)[state()->size - 1];
	}

	template <typename... Args>
	void emplace_back(Args&&... args) {
		if (state()->size == capacity()) {
			reserve(2 * capacity() + 1);
		}
		m_allocator.construct(m_allocator.ToPtr<T>(state()->data + state()->size * sizeof(T)), std::forward<Args>(args)...);
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
		m_allocator.destroy(m_allocator.ToPtr<T>(state()->data) + state()->size - 1);
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

	void resize(size_type count, const value_type& value = value_type()) {
		if (count > state()->size) {
			reserve(count);
			for (size_type i = state()->size; i < count; ++i) {
				m_allocator.construct(m_allocator.ToPtr<T>(state()->data) + i, value);
			}
		}
		else if (count < state()->size) {
			for (size_type i = count; i < state()->size; ++i) {
				m_allocator.destroy(m_allocator.ToPtr<T>(state()->data) + i);
			}
		}
		state()->size = count;
	}
};


#endif  // __SHARED_MEMORY_VECTOR_H

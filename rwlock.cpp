#include "rwlock.h"

#include <algorithm>
#include <chrono>
#include <climits>

constexpr std::int32_t WRITE_LOCK_FLAG = 0x80000000;

bool RWLock::lockRead(std::chrono::milliseconds timeout) {
    auto start = std::chrono::high_resolution_clock::now();
    while (true) {
        if (tryLockRead()) {
            return true;
        }
        if (hasTimedOut(start, timeout)) {
            return false;
        }
        spinWait();
    }
}

bool RWLock::lockWrite(std::chrono::milliseconds timeout) {
    auto start = std::chrono::high_resolution_clock::now();
    while (true) {
        if (tryLockWrite()) {
            return true;
        }
        if (hasTimedOut(start, timeout)) {
            return false;
        }
        spinWait();
    }
}

void RWLock::unlock() {
    std::int32_t expected;

    while (true) {
        expected = state_->load();
        std::int32_t newState = std::max<int32_t>((expected & ~WRITE_LOCK_FLAG) - 1, 0);

        if (state_->compare_exchange_weak(expected, newState)) {
            break;
        }
    }
}

bool RWLock::tryLockRead() {
    int state = state_->load(std::memory_order_acquire);
  
    if (state >= 0) {
        return state_->compare_exchange_weak(state, state + 1, std::memory_order_acquire);
    }
    return false;
}

bool RWLock::tryLockWrite() {
    int state = 0;
    return state_->compare_exchange_weak(state, WRITE_LOCK_FLAG, std::memory_order_acquire);
}

void RWLock::DowngradeWriteToReadLock() {
    std::int32_t expected;

    while (true) {
        expected = WRITE_LOCK_FLAG;

        if (state_->compare_exchange_weak(expected, 1)) {
            break;
        }
    }
}

void RWLock::UpgradeReadToWriteLock() {
    std::int32_t expected;

    while (true) {
        expected = 1;

        if (state_->compare_exchange_weak(expected, WRITE_LOCK_FLAG)) {
            break;
        }
    }
}
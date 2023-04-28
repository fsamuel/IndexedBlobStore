#ifndef RWLOCK_H_
#define RWLOCK_H_

#include <atomic>
//#include <thread>
#include <chrono>
//#include <stdexcept>
//#include <limits>

#if defined(_WIN32)
#include <Windows.h>
#else
#include <unistd.h>
#endif

#undef max

// A simple spinning read-write lock implementation that allows multiple readers or a single writer.
class RWLock {
public:
    explicit RWLock(std::atomic<int>* lock_state) : state_(lock_state) {}

    // Attempt to acquire a read lock with an optional timeout in milliseconds
    bool lockRead(std::chrono::milliseconds timeout = std::chrono::milliseconds::zero());

    // Attempt to acquire a write lock with an optional timeout in milliseconds
    bool lockWrite(std::chrono::milliseconds timeout = std::chrono::milliseconds::zero());

    void unlock();

    // Try to acquire a read lock without blocking. Returns true if the lock was acquired.
    bool tryLockRead();

    // Try to acquire a write lock without blocking. Returns true if the lock was acquired.
    bool tryLockWrite();

    // Downgrades a write lock to a read lock. Blocks until the downgrade completes.
    void DowngradeWriteToReadLock();

    // Upgrades a read lock to a write lock. Blocks until the upgrade completes.
    void UpgradeReadToWriteLock();

private:
    void spinWait() {
#if defined(_WIN32)
        Sleep(0);
#else
        std::this_thread::yield();
#endif
    }

    bool hasTimedOut(std::chrono::high_resolution_clock::time_point start, std::chrono::milliseconds timeout) {
        return timeout != std::chrono::milliseconds::zero() && std::chrono::high_resolution_clock::now() - start >= timeout;
    }

    std::atomic<int>* state_;
};

#endif  // RWLOCK_H_
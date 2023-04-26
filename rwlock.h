#ifndef RWLOCK_H_
#define RWLOCK_H_

#include <string>

#if defined(_WIN32) || defined(_WIN64)
#include <Windows.h>
#else
#include <fcntl.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#endif

class RWLock {
public:
    // Constructor: Initializes a cross-process RWLock with the given name.
    explicit RWLock(const std::string& name);

    // Destructor: Cleans up resources associated with the RWLock.
    ~RWLock();

    // Acquires a read lock.
    bool AcquireReadLock();

    // Acquires a write lock.
    bool AcquireWriteLock();

    // Releases the read lock.
    bool ReleaseReadLock();

    // Releases the write lock.
    bool ReleaseWriteLock();

private:
    std::string lock_name_;
#if defined(_WIN32) || defined(_WIN64)
    SRWLOCK srw_lock_;
#else
    pthread_rwlock_t* rw_lock_;
    pthread_rwlockattr_t rw_lock_attr_;
#endif
};

#endif  // RWLOCK_H_
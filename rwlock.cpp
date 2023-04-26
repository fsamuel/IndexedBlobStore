#include "rwlock.h"

RWLock::RWLock(const std::string& name) : lock_name_(name) {
#if defined(_WIN32) || defined(_WIN64)
    InitializeSRWLock(&srw_lock_);
#else
    rw_lock_ = new pthread_rwlock_t;
    pthread_rwlockattr_init(&rw_lock_attr_);
    pthread_rwlockattr_setpshared(&rw_lock_attr_, PTHREAD_PROCESS_SHARED);
    pthread_rwlock_init(rw_lock_, &rw_lock_attr_);
#endif
}

RWLock::~RWLock() {
#if defined(_WIN32) || defined(_WIN64)
    // SRWLOCK does not need explicit destruction
#else
    pthread_rwlock_destroy(rw_lock_);
    pthread_rwlockattr_destroy(&rw_lock_attr_);
    delete rw_lock_;
#endif
}

bool RWLock::AcquireReadLock() {
#if defined(_WIN32) || defined(_WIN64)
    AcquireSRWLockShared(&srw_lock_);
    return true;
#else
    return pthread_rwlock_rdlock(rw_lock_) == 0;
#endif
}

bool RWLock::AcquireWriteLock() {
#if defined(_WIN32) || defined(_WIN64)
    AcquireSRWLockExclusive(&srw_lock_);
    return true;
#else
    return pthread_rwlock_wrlock(rw_lock_) == 0;
#endif
}

bool RWLock::ReleaseReadLock() {
#if defined(_WIN32) || defined(_WIN64)
    ReleaseSRWLockShared(&srw_lock_);
    return true;
#else
    return pthread_rwlock_unlock(rw_lock_) == 0;
#endif
}

bool RWLock::ReleaseWriteLock() {
#if defined(_WIN32) || defined(_WIN64)
    ReleaseSRWLockExclusive(&srw_lock_);
    return true;
#else
    return pthread_rwlock_unlock(rw_lock_) == 0;
#endif
}

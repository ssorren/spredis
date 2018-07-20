#ifndef SP_LOCK_H_
#define SP_LOCK_H_

#include <pthread.h>

typedef struct _SPLock {
	pthread_rwlock_t rwlock;
	pthread_rwlock_t upgrade;
} SPLock;


#define SPLockInit(a) pthread_rwlock_init(&((a).rwlock), NULL), pthread_rwlock_init(&((a).upgrade), NULL)
#define SPLockDestroy(a) pthread_rwlock_destroy(&((a).rwlock)), pthread_rwlock_destroy(&((a).upgrade))

#define SPReadLock(a) pthread_rwlock_rdlock(&((a).rwlock))
#define SPUpReadLock(a) pthread_rwlock_wrlock(&((a).upgrade)), pthread_rwlock_rdlock(&((a).rwlock))
#define SPWriteLock(a) pthread_rwlock_rdlock(&((a).upgrade)), pthread_rwlock_wrlock(&((a).rwlock))

#define SPUpgradeLock(a)  pthread_rwlock_unlock(&((a).rwlock)) , pthread_rwlock_wrlock(&((a).rwlock))
#define SPDowngradeLock(a)  pthread_rwlock_unlock(&((a).rwlock)) , pthread_rwlock_rdlock(&((a).rwlock))

#define SPReadUnlock(a) pthread_rwlock_unlock(&((a).rwlock))
#define SPWriteUnlock(a) pthread_rwlock_unlock(&((a).upgrade)), pthread_rwlock_unlock(&((a).rwlock))
#define SPWriteUnlockRP(a) pthread_rwlock_unlock(&((a).rwlock)), pthread_rwlock_unlock(&((a).upgrade))

#endif
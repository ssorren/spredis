#ifndef SP_LOCK_H_
#define SP_LOCK_H_

#include <pthread.h>

typedef struct _SPLock {
	pthread_rwlock_t rwlock;
	pthread_rwlock_t upgrade;
	// pthread_rwlockattr_t rwlattr;
	// pthread_rwlockattr_t upgradeattr;
} SPLock;


#define SPLockInit(a) \
pthread_rwlock_init(&((a).rwlock), NULL); \
pthread_rwlock_init(&((a).upgrade), NULL)

/*
do { \
	pthread_rwlockattr_init(&(a).rwlattr); \
	pthread_rwlockattr_init(&(a).upgradeattr); \
	pthread_rwlockattr_setpshared(&(a).rwlattr, PTHREAD_PROCESS_SHARED); \
	pthread_rwlockattr_setpshared(&(a).upgradeattr, PTHREAD_PROCESS_SHARED); \
	pthread_rwlock_init(&((a).rwlock), &(a).rwlattr); \
	pthread_rwlock_init(&((a).upgrade), &(a).upgradeattr); \
} while(0)
*/

#define SPLockDestroy(a)  \
pthread_rwlock_destroy(&(a).rwlock); \
pthread_rwlock_destroy(&(a).upgrade) 

/*	
	pthread_rwlockattr_destroy(&(a).rwlattr); \
	pthread_rwlockattr_destroy(&(a).upgradeattr); \
} while(0)*/

// pthread_rwlock_destroy(&((a).rwlock)), pthread_rwlock_destroy(&((a).upgrade))

#define SPReadLock(a) pthread_rwlock_rdlock(&((a).rwlock))

#define SPUpReadLock(a) pthread_rwlock_wrlock(&((a).upgrade)); pthread_rwlock_rdlock(&((a).rwlock))
#define SPWriteLock(a) pthread_rwlock_rdlock(&((a).upgrade)); pthread_rwlock_wrlock(&((a).rwlock))

#define SPUpgradeLock(a)  pthread_rwlock_unlock(&((a).rwlock)); pthread_rwlock_wrlock(&((a).rwlock))
#define SPDowngradeLock(a)  pthread_rwlock_unlock(&((a).rwlock)); pthread_rwlock_rdlock(&((a).rwlock))

#define SPReadUnlock(a) pthread_rwlock_unlock(&((a).rwlock))
#define SPWriteUnlock(a) pthread_rwlock_unlock(&((a).upgrade)); pthread_rwlock_unlock(&((a).rwlock))
#define SPWriteUnlockRP(a) pthread_rwlock_unlock(&((a).rwlock)); pthread_rwlock_unlock(&((a).upgrade))

#endif
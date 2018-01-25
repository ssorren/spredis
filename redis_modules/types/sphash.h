#ifndef __SPREDIS_HASH
#define __SPREDIS_HASH

#include "../spredis.h"
#include <pthread.h>



// typedef kbitr_t SPScoreIter;
typedef struct _SPHashCont {
	khash_t(HASH) *set;
	SPHashValueType valueType;
	pthread_rwlock_t mutex;
} SPHashCont;

// char *SpredisSMapValue(SpredisSMapCont *map, unsigned long id);
void SpredisHashRDBSave(RedisModuleIO *io, void *ptr);
void SpredisHashRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value);
void *SpredisHashRDBLoad(RedisModuleIO *io, int encver);
void SpredisHashFreeCallback(void *value);
int SpredisHashSetStr_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisHashGetStr_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisHashSetDouble_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisHashGetDouble_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisHashDel_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif
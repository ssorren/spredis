#ifndef __SPREDIS_STRING_HASH
#define __SPREDIS_STRING_HASH

#include "../spredis.h"

typedef struct _SpredisSMap_t {
	char full;
	char *value;
} SpredisSMap_t;


typedef struct _SpredisSMapCont {
	unsigned long size;
	unsigned long valueCount;
	SpredisSMap_t *map;
} SpredisSMapCont;

char *SpredisSMapValue(SpredisSMapCont *map, unsigned long id);
void SpredisSHashRDBSave(RedisModuleIO *io, void *ptr);
void SpredisSHashRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value);
void *SpredisSHashRDBLoad(RedisModuleIO *io, int encver);
void SpredisSHashFreeCallback(void *value);
int SpredisSHashSet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int base);
int SpredisSHashSet10_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisSHashSet16_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisSHashGet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisSHashDel_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif
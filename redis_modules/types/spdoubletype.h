#ifndef __SPREDIS_DOUBLE_HASH
#define __SPREDIS_DOUBLE_HASH

#include "../spredis.h"

typedef struct _SpredisDMap_t {
	char full;
	double value;
} SpredisDMap_t;

typedef struct _SpredisDMapCont {
	unsigned long size;
	unsigned long valueCount;
	SpredisDMap_t *map;
} SpredisDMapCont;

double SpredisDMapValue(SpredisDMapCont *map, unsigned long id);
void SpredisDHashRDBSave(RedisModuleIO *io, void *ptr);
void SpredisDHashRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value);
void *SpredisDHashRDBLoad(RedisModuleIO *io, int encver);
void SpredisDHashFreeCallback(void *value);
int SpredisDHashSet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int base);
int SpredisDHashSet10_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisDHashSet16_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisDHashGet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisDHashDel_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif
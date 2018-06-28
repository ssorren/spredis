#ifndef __SPREDIS_COMPOSITE
#define __SPREDIS_COMPOSITE
#include "../spredis.h"

#define SPGeoPart 1
#define SPDoublePart 2
#define SPLexPart 3

KB_TYPE(COMPIDX);


typedef int (*SPCompositeComp)(SPPtrOrD_t , SPPtrOrD_t);

typedef struct _SPCompositeCompCtx {
    uint8_t valueCount;
    uint8_t *types;
    SPCompositeComp *compare;
} SPCompositeCompCtx;

typedef struct _SPCompositeScoreSetKey
{
    SPPtrOrD_t *value;
    SPCompositeCompCtx *compCtx;
    SPScoreSetMembers *members;
} SPCompositeScoreSetKey;

typedef struct _SPCompositeScoreCont {
	SPCompositeCompCtx *compCtx;
	pthread_rwlock_t mutex;
	kbtree_t(COMPIDX) *btree;
} SPCompositeScoreCont;

int SPCompositeScoreComp(SPCompositeScoreSetKey a,SPCompositeScoreSetKey b);
int SPGetCompKeyValue(RedisModuleCtx *ctx, RedisModuleKey **key, SPCompositeScoreCont **cont, RedisModuleString *name, int mode);

void SpredisCompSetRDBSave(RedisModuleIO *io, void *ptr);
void SpredisCompSetRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value);
void *SpredisCompSetRDBLoad(RedisModuleIO *io, int encver);
void SpredisCompSetFreeCallback(void *value);


int SpredisCompSetAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisCompSetRem_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisCompSetCard_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

KBTREE_INIT(COMPIDX, SPCompositeScoreSetKey, SPCompositeScoreComp);
#endif
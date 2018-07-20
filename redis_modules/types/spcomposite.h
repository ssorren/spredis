#ifndef __SPREDIS_COMPOSITE
#define __SPREDIS_COMPOSITE
#include "./spredis.h"

#define SPGeoPart 1
#define SPDoublePart 2
#define SPLexPart 3
#define SPIDPart 4

int SPGeoPartCompare(SPPtrOrD_t a, SPPtrOrD_t b);
int SPDoublePartCompare(SPPtrOrD_t a, SPPtrOrD_t b);
int SPLexPartCompare(SPPtrOrD_t a, SPPtrOrD_t b);

// int SPGetCompKeyValue(RedisModuleCtx *ctx, RedisModuleKey **key, SPCompositeScoreCont **cont, RedisModuleString *name, int mode);

// void SpredisCompSetRDBSave(RedisModuleIO *io, void *ptr);
// void SpredisCompSetRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value);
// void *SpredisCompSetRDBLoad(RedisModuleIO *io, int encver);
// void SpredisCompSetFreeCallback(void *value);


// int SpredisCompSetAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
// int SpredisCompSetRem_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
// int SpredisCompSetCard_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

void SPAddCompScoreToSet(SPIndexCont *cont, spid_t id, SPPtrOrD_t *value);
void SPRemCompScoreFromSet(SPIndexCont *cont, spid_t id, SPPtrOrD_t *value);
#endif
#ifndef __SPREDIS_STRING_TMPRESULT
#define __SPREDIS_STRING_TMPRESULT

#include "../spredis.h"



// void __SPFreeSortData(SpredisSortData *sd);


// KMEMPOOL_INIT(SORTDATA, SpredisSortData, SPKLMEMPOOLFREE);	


// #define SPKLFREE(a) 

// KLIST_INIT(TEMPRES, SpredisSortData*, SPKLFREE);

typedef struct _SpredisTempResult {
	size_t size;
	// SpredisSortData **data;
	// klist_t(TEMPRES) * data;
	SpredisSortData *data;
} SpredisTempResult;
void SpredisTMPResDBSave(RedisModuleIO *io, void *ptr);
void SpredisTMPResRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value);
void *SpredisTMPResRDBLoad(RedisModuleIO *io, int encver);
void SpredisTMPResFreeCallback(void *value);

int SpredisTMPResGetIds_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisTMPResGetDocs_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

SpredisTempResult *SpredisTempResultCreate(RedisModuleCtx *ctx, RedisModuleString *keyName, size_t size);

void SpredisTempResultModuleInit();
#endif
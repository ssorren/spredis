
#ifndef __SPREDIS_DOCUMENT
#define __SPREDIS_DOCUMENT


#include "../spredis.h"

KHASH_DECLARE(DOC, spid_t, const char*);
KHASH_DECLARE(DOCID, const char*, spid_t);
KHASH_MAP_INIT_INT64(DOC, const char*);
KHASH_MAP_INIT_STR(DOCID, spid_t);

typedef khash_t(DOC) SPDocMap;
typedef khash_t(DOCID) SPDocIdMap;

typedef struct _SPDocContainer {
    spid_t newRecordId;
    SPDocMap *documents;
    SPDocIdMap *idMap;
} SPDocContainer;


SPDocContainer *SPDocContainerInit();
void SPDocContainerDestroy(SPDocContainer *dc);

int SpredisDocAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisDocRem_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisDocGetByDocID_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

void SpredisDocRDBSave(RedisModuleIO *io, void *ptr);
void SpredisDocRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value);
void *SpredisDocRDBLoad(RedisModuleIO *io, int encver);
void SpredisDocFreeCallback(void *value);

#endif


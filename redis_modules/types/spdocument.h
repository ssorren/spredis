
#ifndef __SPREDIS_DOCUMENT
#define __SPREDIS_DOCUMENT


#include "../spredis.h"
typedef struct _SPLZWCont {
	size_t oSize;
	char *packed;
} SPLZWCont;

KHASH_DECLARE(LZW, spid_t, SPLZWCont*);
KHASH_DECLARE(RID, spid_t, const char*);
KHASH_DECLARE(DOCID, const char*, spid_t);
KHASH_MAP_INIT_INT64(LZW, SPLZWCont*);
KHASH_MAP_INIT_INT64(RID, const char*);
KHASH_MAP_INIT_STR(DOCID, spid_t);

typedef khash_t(LZW) SPDocMap;
typedef khash_t(RID) SPRevIdMap;
typedef khash_t(DOCID) SPDocIdMap;

typedef struct _SPDocContainer {
    spid_t newRecordId;
    SPDocMap *documents;
    SPRevIdMap *revId;
    SPDocIdMap *idMap;
    pthread_rwlock_t mutex;
} SPDocContainer;


void SP_PACK(const char *doc, SPLZWCont *lzw);
char *SP_UNPACK(SPLZWCont *lzw);

SPDocContainer *SPDocContainerInit();
void SPDocContainerDestroy(SPDocContainer *dc);

int SpredisDocAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisDocRem_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisDocGetByDocID_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int SpredisInitDocumentCommands(RedisModuleCtx *ctx);


void SpredisDocRDBSave(RedisModuleIO *io, void *ptr);
void SpredisDocRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value);
void *SpredisDocRDBLoad(RedisModuleIO *io, int encver);
void SpredisDocFreeCallback(void *value);



#endif


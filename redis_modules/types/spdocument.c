#include "../spredis.h"
#include "../lib/lz4.h"

#define SP_ACCELERATION_FACTOR 8

spid_t _SPNewRecordId(SPDocContainer *dc) {
    dc->newRecordId++;
    return dc->newRecordId;
}

static inline void SP_PACK(const char *doc, SPLZWCont *lzw) {
    if (doc == NULL) return;
    lzw->oSize = strlen(doc);
    int cSize;
    int dstCapacity = LZ4_COMPRESSBOUND(lzw->oSize);
    if (lzw->packed != NULL) RedisModule_Free(lzw->packed);
    char *dst = RedisModule_Alloc(sizeof(char) * dstCapacity);
    cSize = LZ4_compress_fast(doc, dst, lzw->oSize, dstCapacity, SP_ACCELERATION_FACTOR);
    lzw->packed = RedisModule_Alloc((cSize + 1) * sizeof(char));
    lzw->packed[cSize] = 0; //null the last char
    //have to copy to new buffer and destroy the old one otherwise we won't save much memory at all
    memcpy(lzw->packed, dst, cSize);
    RedisModule_Free(dst);
}

static inline char *SP_UNPACK(SPLZWCont *lzw) {
    char *res = RedisModule_Alloc(sizeof(char) * (lzw->oSize + 1));
    res[lzw->oSize] = 0; //null the last char
    if (!LZ4_decompress_fast(lzw->packed, res, lzw->oSize)) {
        RedisModule_Free(res);
        return NULL;
    }
    return res;
}

void SpredisDocRDBSave(RedisModuleIO *io, void *ptr) {
    SPDocContainer *dc = ptr;
    RedisModule_SaveUnsigned(io, dc->newRecordId);
    RedisModule_SaveUnsigned(io, kh_size(dc->idMap));

    khint_t k, k2;
    const char *doc, *strKey;
    for (k = 0; k < kh_end(dc->idMap); ++k) {
        if (kh_exist(dc->idMap, k)) {
            strKey = kh_key(dc->idMap, k);
            spid_t rid = kh_value(dc->idMap, k);
            k2 = kh_get(DOC, dc->documents, rid);
            doc = kh_value(dc->documents, k2);

            RedisModule_SaveUnsigned(io, rid);
            // RedisModuleString *rsKey = Redi
            RedisModule_SaveStringBuffer(io, strKey, strlen(strKey));
            RedisModule_SaveStringBuffer(io, doc, strlen(doc));

            // printf("Writing %llu, %s, %zu, %zu\n", rid, strKey, strlen(strKey), strlen(doc));
        }
    }
}

void SpredisDocRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    SPDocContainer *dc = value;
    khint_t k, k2;
    for (k = 0; k < kh_end(dc->idMap); ++k) {
        if (kh_exist(dc->idMap, k)) {
            const char* strKey = kh_key(dc->idMap, k);
            spid_t rid = kh_value(dc->idMap, k);
            k2 = kh_get(DOC, dc->documents, rid);
            const char *doc = kh_value(dc->documents, k2);
            RedisModule_EmitAOF(aof,"spredis.documentadd","scc", key, strKey, doc);
        }
    }

}

// void SPPutDoc()
void *SpredisDocRDBLoad(RedisModuleIO *io, int encver) {
    SPDocContainer *dc = SPDocContainerInit();
    dc->newRecordId = RedisModule_LoadUnsigned(io);
    size_t count = (size_t)RedisModule_LoadUnsigned(io);
    // printf("Loading %llu, count:%zu\n", dc->newRecordId, count);
    khint_t k;
    int absent;
    for (size_t i = 0; i < count; ++i)
    {   
        spid_t rid = RedisModule_LoadUnsigned(io);

        // size_t slen,dlen;
        RedisModuleString *s = RedisModule_LoadString(io);
        const char* strKey = RedisModule_Strdup( RedisModule_StringPtrLen(s, NULL));
        RedisModule_FreeString(RedisModule_GetContextFromIO(io),s);
        // const char* strKey = RedisModule_LoadStringBuffer(io, &slen);

        // printf("record id: %llu, %zu\n", rid, slen);


        s = RedisModule_LoadString(io);
        const char* doc = RedisModule_Strdup( RedisModule_StringPtrLen(s, NULL));
        RedisModule_FreeString(RedisModule_GetContextFromIO(io),s);

        // const char* doc = RedisModule_LoadStringBuffer(io, &dlen);
        k = kh_put(DOCID, dc->idMap, strKey, &absent);
        kh_value(dc->idMap, k) = rid;
        k = kh_put(DOC, dc->documents, rid, &absent);
        kh_value(dc->documents, k) = doc;
        k = kh_put(DOC, dc->revId, rid, &absent);
        kh_value(dc->revId, k) = strKey;
    }

    return dc;
}

void SpredisDocFreeCallback(void *value) {
    if (value == NULL) return;
    SPDocContainerDestroy((SPDocContainer *)value);
}




SPDocContainer *SPDocContainerInit() {
    SPDocContainer *dc = RedisModule_Calloc(1, sizeof(SPDocContainer));
    dc->documents = kh_init(DOC);
    dc->idMap = kh_init(DOCID);
    dc->revId = kh_init(DOC);
    return dc;
}

void SPDocContainerDestroy(SPDocContainer *dc) {
    if (dc == NULL) return;
    khint_t k;
    for (k = 0; k < kh_end(dc->idMap); ++k) {
        if (kh_exist(dc->idMap, k)) RedisModule_Free((char*)kh_key(dc->idMap, k));
    }
    kh_destroy(DOCID, dc->idMap);

    for (k = 0; k < kh_end(dc->documents); ++k) {
        if (kh_exist(dc->documents, k)) {
            if (kh_value(dc->documents, k) != NULL) RedisModule_Free((char*)kh_value(dc->documents, k));
        }
    }
    kh_destroy(DOC, dc->documents);
    kh_destroy(DOC, dc->revId);
    RedisModule_Free(dc);
}

int SpredisInitDocumentCommands(RedisModuleCtx *ctx) {
    if (RedisModule_CreateCommand(ctx,"spredis.docadd",
        SpredisDocAdd_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.docget",
        SpredisDocGetByDocID_RedisCommand,"readonly",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.docrem",
        SpredisDocRem_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;    
}


int SpredisDocAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 4) return RedisModule_WrongArity(ctx);
    RedisModuleString *keyName = argv[1];
    const char *stringId = RedisModule_StringPtrLen(argv[2], NULL);
    const char *data = RedisModule_StringPtrLen(argv[3], NULL);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_WRITE);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPDOCTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }

    SPDocContainer *dc;
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        dc = SPDocContainerInit();
        SpredisSetRedisKeyValueType(key,SPDOCTYPE,dc);
    } else {
        dc = RedisModule_ModuleTypeGetValue(key);
    }
    khint_t k;
    spid_t rid;
    int absent;
    k = kh_get(DOCID, dc->idMap, stringId);

    if (k != kh_end(dc->idMap)) {
        rid = kh_value(dc->idMap, k);
    } else {
        rid = _SPNewRecordId(dc);
        stringId = RedisModule_Strdup(stringId);
        k = kh_put(DOCID, dc->idMap, stringId, &absent);
        kh_value(dc->idMap, k) = rid;
        k = kh_put(DOC, dc->revId, rid, &absent);
        kh_value(dc->revId, k) = stringId;
    }
    k = kh_put(DOC, dc->documents, rid, &absent);
    if (!absent) {
        char *old = (char *)kh_value(dc->documents, k);
        if (old != NULL) RedisModule_Free(old);
    }
    kh_value(dc->documents, k) = RedisModule_Strdup(data);
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithLongLong(ctx, rid);

    char ress[32];
    sprintf(ress, "%" PRIx64, (unsigned long long)rid);
    RedisModule_ReplyWithStringBuffer(ctx, ress, strlen(ress));

    return REDISMODULE_OK;
}


int SpredisDocRem_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleString *keyName = argv[1];
    const char *stringId = RedisModule_StringPtrLen(argv[2], NULL);
    // RedisModuleString *data = argv[3];

    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_WRITE);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPDOCTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    
    SPDocContainer *dc;
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        dc = RedisModule_ModuleTypeGetValue(key);
    }
    khint_t k;
    spid_t rid;
    k = kh_get(DOCID, dc->idMap, stringId);
    if (k != kh_end(dc->idMap)) {
        rid = kh_value(dc->idMap, k);

        RedisModule_Free((char*)kh_key(dc->idMap, k));
        kh_del(DOCID, dc->idMap, k);

        k = kh_get(DOC, dc->documents, rid);
        if (k != kh_end(dc->documents)) {
            char *doc = (char *)kh_value(dc->documents, k);
            if (doc != NULL) RedisModule_Free(doc);
            kh_del(DOC, dc->documents, k);
        }
        k = kh_get(DOC, dc->revId, rid);
        if (k != kh_end(dc->revId)) {
            kh_del(DOC, dc->revId, k);
        }
        RedisModule_ReplyWithLongLong(ctx, 1);
    } else {
        RedisModule_ReplyWithLongLong(ctx, 0);
    }

    return REDISMODULE_OK;
}


int SpredisDocGetByDocID_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleString *keyName = argv[1];
    const char *stringId = RedisModule_StringPtrLen(argv[2], NULL);
    // RedisModuleString *data = argv[3];

    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_WRITE);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPDOCTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    
    SPDocContainer *dc;
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        dc = RedisModule_ModuleTypeGetValue(key);
    }
    khint_t k;
    spid_t rid;
    k = kh_get(DOCID, dc->idMap, stringId);
    if (k != kh_end(dc->idMap)) {
        rid = kh_value(dc->idMap, k);
        k = kh_get(DOC, dc->documents, rid);
        
        RedisModule_ReplyWithArray(ctx, 3);
        RedisModule_ReplyWithLongLong(ctx, rid);
        char ress[32];
        sprintf(ress, "%" PRIx64, (unsigned long long)rid);
        RedisModule_ReplyWithStringBuffer(ctx, ress, strlen(ress));
        const char *doc = kh_value(dc->documents, k);
        RedisModule_ReplyWithStringBuffer(ctx, doc, strlen(doc));
    } else {
        RedisModule_ReplyWithNull(ctx);
    }

    return REDISMODULE_OK;
}
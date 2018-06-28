#include "../spredis.h"
#include "../lib/lz4.h"

#define SP_ACCELERATION_FACTOR 2

spid_t _SPNewRecordId(SPDocContainer *dc) {
    dc->newRecordId++;
    return dc->newRecordId;
}

void SP_PACK(const char *doc, SPLZWCont *lzw) {
    if (doc == NULL) return;
    lzw->oSize = strlen(doc);
    int cSize;
    int dstCapacity = LZ4_COMPRESSBOUND(lzw->oSize);
    char *dst = RedisModule_Alloc(dstCapacity);
    cSize = LZ4_compress_fast(doc, dst, lzw->oSize, dstCapacity, SP_ACCELERATION_FACTOR);
    lzw->packed = RedisModule_Alloc(cSize + 1);
    lzw->packed[cSize] = 0; //null the last char
    //have to copy to new buffer and destroy the old one otherwise we won't save much memory at all
    memcpy(lzw->packed, dst, cSize);
    // printf("%zu, %d, %f\n", lzw->oSize, cSize, (double)cSize / (double)lzw->oSize);
    RedisModule_Free(dst);
}

char *SP_UNPACK(SPLZWCont *lzw) {
    char *res = RedisModule_Alloc(lzw->oSize + 1);
    res[lzw->oSize] = 0; //null the last char
    if (!LZ4_decompress_fast(lzw->packed, res, lzw->oSize)) {
        RedisModule_Free(res);
        return NULL;
    }
    return res;
}

SPLZWCont *SP_CREATELZW(const char *s) {
    SPLZWCont *lzw = RedisModule_Alloc(sizeof(SPLZWCont));
    SP_PACK(s, lzw);
    return lzw;
}

void SP_DESTROYLZW(SPLZWCont *lzw) {
    if (lzw != NULL) {
        RedisModule_Free(lzw->packed);
        RedisModule_Free(lzw);
        // printf("Destroyed\n");
    }
}

void SpredisDocRDBSave(RedisModuleIO *io, void *ptr) {
    SPDocContainer *dc = ptr;
    // SpredisProtectReadMap(dc);//, "SpredisDocRDBSave");
    RedisModule_SaveUnsigned(io, dc->newRecordId);
    RedisModule_SaveUnsigned(io, kh_size(dc->idMap));

    khint_t k, k2;
    const char *strKey;
    char *doc;
    for (k = 0; k < kh_end(dc->idMap); ++k) {
        if (kh_exist(dc->idMap, k)) {
            strKey = kh_key(dc->idMap, k);
            spid_t rid = kh_value(dc->idMap, k);
            k2 = kh_get(LZW, dc->documents, rid);
            size_t len = kh_value(dc->documents, k2)->oSize;
            doc = SP_UNPACK(kh_value(dc->documents, k2));

            RedisModule_SaveUnsigned(io, rid);
            // RedisModuleString *rsKey = Redi
            RedisModule_SaveStringBuffer(io, strKey, strlen(strKey));
            RedisModule_SaveStringBuffer(io, doc, len);
            RedisModule_Free(doc);
            // printf("Writing %llu, %s, %zu, %zu\n", rid, strKey, strlen(strKey), strlen(doc));
        }
    }
    // SpredisUnProtectMap(dc);//, "SpredisDocRDBSave");
}

void SpredisDocRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    SPDocContainer *dc = value;
    khint_t k, k2;
    // SpredisProtectReadMap(dc);//, "SpredisDocRewriteFunc");
    for (k = 0; k < kh_end(dc->idMap); ++k) {
        if (kh_exist(dc->idMap, k)) {
            const char* strKey = kh_key(dc->idMap, k);
            spid_t rid = kh_value(dc->idMap, k);
            k2 = kh_get(LZW, dc->documents, rid);
            char *doc = SP_UNPACK(kh_value(dc->documents, k2));
            RedisModule_EmitAOF(aof,"spredis.documentadd","scc", key, strKey, doc);
            RedisModule_Free(doc);
        }
    }
    // SpredisUnProtectMap(dc);//, "SpredisDocRewriteFunc");

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


        // const char* doc = RedisModule_Strdup( RedisModule_StringPtrLen(s, NULL));
        // RedisModule_FreeString(RedisModule_GetContextFromIO(io),s);

        // const char* doc = RedisModule_LoadStringBuffer(io, &dlen);
        k = kh_put(DOCID, dc->idMap, strKey, &absent);
        kh_value(dc->idMap, k) = rid;
        k = kh_put(LZW, dc->documents, rid, &absent);
        kh_value(dc->documents, k) = SP_CREATELZW( RedisModule_StringPtrLen(s, NULL) );

        k = kh_put(RID, dc->revId, rid, &absent);
        kh_value(dc->revId, k) = strKey;
        RedisModule_FreeString(RedisModule_GetContextFromIO(io),s);
    }

    return dc;
}

void SpredisDocFreeCallback(void *value) {
    if (value == NULL) return;
    SPDocContainerDestroy((SPDocContainer *)value);
}




SPDocContainer *SPDocContainerInit() {
    SPDocContainer *dc = RedisModule_Calloc(1, sizeof(SPDocContainer));
    dc->documents = kh_init(LZW);
    dc->idMap = kh_init(DOCID);
    dc->revId = kh_init(RID);
    dc->mutex = (pthread_rwlock_t)PTHREAD_RWLOCK_INITIALIZER;
    pthread_rwlock_init ( &dc->mutex, NULL);
    return dc;
}

void SPDocContainerDestroy(SPDocContainer *dc) {
    if (dc == NULL) return;
    khint_t k;
    SpredisProtectWriteMap(dc);//, "SPDocContainerDestroy");
    for (k = 0; k < kh_end(dc->idMap); ++k) {
        if (kh_exist(dc->idMap, k)) RedisModule_Free((char*)kh_key(dc->idMap, k));
    }
    kh_destroy(DOCID, dc->idMap);

    for (k = 0; k < kh_end(dc->documents); ++k) {
        if (kh_exist(dc->documents, k)) {
            SP_DESTROYLZW(kh_value(dc->documents, k));
        }
    }
    kh_destroy(LZW, dc->documents);
    kh_destroy(RID, dc->revId);
    SpredisUnProtectMap(dc);//, "SPDocContainerDestroy");
    pthread_rwlock_destroy(&dc->mutex);
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
    SpredisProtectWriteMap(dc);//, "SpredisDocAdd_RedisCommand");
    khint_t k, k2;
    spid_t rid;
    int absent;
    k = kh_get(DOCID, dc->idMap, stringId);

    if (k != kh_end(dc->idMap)) {
        // printf("%s\n", "exists");
        rid = kh_value(dc->idMap, k);
    } else {
        // printf("%s\n", "creating new");
        rid = _SPNewRecordId(dc);
        stringId = RedisModule_Strdup(stringId);
        k = kh_put(DOCID, dc->idMap, stringId, &absent);
        kh_value(dc->idMap, k) = rid;
        k2 = kh_put(RID, dc->revId, rid, &absent);
        kh_value(dc->revId, k2) = stringId;
    }
    k = kh_put(LZW, dc->documents, rid, &absent);
    if (!absent) {
        // printf("Destroying LZW\n");
        SPLZWCont *c = kh_value(dc->documents, k); 
        SP_DESTROYLZW(c);
    }
    kh_value(dc->documents, k) = SP_CREATELZW(data);
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithLongLong(ctx, rid);

    char ress[32];
    sprintf(ress, "%" PRIx64, rid);
    RedisModule_ReplyWithStringBuffer(ctx, ress, strlen(ress));
    // printf("%s, %s %lu\n", stringId, ress, strlen(ress));
    SpredisUnProtectMap(dc);//, "SpredisDocAdd_RedisCommand");
    RedisModule_ReplicateVerbatim(ctx);
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
    SpredisProtectWriteMap(dc);//, "SpredisDocRem_RedisCommand");
    khint_t k;
    spid_t rid;
    k = kh_get(DOCID, dc->idMap, stringId);
    if (k != kh_end(dc->idMap)) {
        rid = kh_value(dc->idMap, k);

        RedisModule_Free((char*)kh_key(dc->idMap, k));
        kh_del(DOCID, dc->idMap, k);

        k = kh_get(LZW, dc->documents, rid);
        if (k != kh_end(dc->documents)) {
            SP_DESTROYLZW(kh_value(dc->documents, k));
            kh_del(LZW, dc->documents, k);
        }
        k = kh_get(RID, dc->revId, rid);
        if (k != kh_end(dc->revId)) {
            kh_del(RID, dc->revId, k);
        }
        RedisModule_ReplyWithLongLong(ctx, 1);
    } else {
        RedisModule_ReplyWithLongLong(ctx, 0);
    }
    SpredisUnProtectMap(dc);//, "SpredisDocRem_RedisCommand");
    RedisModule_ReplicateVerbatim(ctx);
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
    // SpredisProtectReadMap(dc);//, "SpredisDocGetByDocID_RedisCommand");
    khint_t k;
    spid_t rid;
    k = kh_get(DOCID, dc->idMap, stringId);
    if (k != kh_end(dc->idMap)) {
        rid = kh_value(dc->idMap, k);
        k = kh_get(LZW, dc->documents, rid);
        
        RedisModule_ReplyWithArray(ctx, 3);
        RedisModule_ReplyWithLongLong(ctx, rid);
        char ress[32];
        sprintf(ress, "%" PRIx64, rid);
        RedisModule_ReplyWithStringBuffer(ctx, ress, strlen(ress));
        SPLZWCont *lzw = kh_value(dc->documents, k);
        char *doc = SP_UNPACK(lzw);
        RedisModule_ReplyWithStringBuffer(ctx, doc, lzw->oSize);
        RedisModule_Free(doc);
    } else {
        RedisModule_ReplyWithNull(ctx);
    }
    // SpredisUnProtectMap(dc);//, "SpredisDocGetByDocID_RedisCommand");
    return REDISMODULE_OK;
}
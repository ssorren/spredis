#include "../spredis.h"


spid_t _SPNewRecordId(SPDocContainer *dc) {
    dc->newRecordId++;
    return dc->newRecordId;
}

void SpredisDocRDBSave(RedisModuleIO *io, void *ptr) {
    SPDocContainer *dc = ptr;
    RedisModule_SaveUnsigned(io, dc->newRecordId);
    RedisModule_SaveUnsigned(io, kh_size(dc->idMap));

    khint_t k, k2;
    for (k = 0; k < kh_end(dc->idMap); ++k) {
        if (kh_exist(dc->idMap, k)) {
            const char* strKey = kh_key(dc->idMap, k);
            spid_t rid = kh_value(dc->idMap, k);
            k2 = kh_get(DOC, dc->documents, rid);
            const char *doc = kh_value(dc->documents, k2);
            

            RedisModule_SaveUnsigned(io, rid);
            RedisModule_SaveStringBuffer(io, strKey, strlen(strKey));
            RedisModule_SaveStringBuffer(io, doc, strlen(doc));

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
    khint_t k;
    int absent;
    for (size_t i = 0; i < count; ++i)
    {   
        spid_t rid = RedisModule_LoadUnsigned(io);
        const char* strKey = RedisModule_LoadStringBuffer(io, NULL);
        const char* doc = RedisModule_LoadStringBuffer(io, NULL);
        k = kh_put(DOCID, dc->idMap, strKey, &absent);
        kh_value(dc->idMap, k) = rid;
        k = kh_put(DOC, dc->documents, rid, &absent);
        kh_value(dc->documents, k) = doc;        
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
    RedisModule_Free(dc);
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
        SpredisSetRedisKeyValueType(key,SPDOCTYPE,dhash);
    } else {
        dc = RedisModule_ModuleTypeGetValue(key);
    }
    khint_t k;
    spid_t rid;
    int absent;
    k = kh_get(DOCID, dc->idMap, stringId);
    if (kh_exist(dc->idMap, k)) {
        rid = kh_value(dc->idMap, k);
    } else {
        rid = _SPNewRecordId(dc);
        k = kh_put(DOCID, dc->idMap, strKey, &absent);
        kh_value(dc->idMap, k) = rid;
    }


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
    RedisModuleString *stringId = argv[2];
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


    return REDISMODULE_OK;
}
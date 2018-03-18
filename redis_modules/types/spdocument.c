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
    printf("Loading %llu, count:%zu\n", dc->newRecordId, count);
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

    // SpredisLog(ctx, "A");
    SPDocContainer *dc;
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        // SpredisLog(ctx, "B");
        dc = SPDocContainerInit();
        SpredisSetRedisKeyValueType(key,SPDOCTYPE,dc);
    } else {
        // SpredisLog(ctx, "C");
        dc = RedisModule_ModuleTypeGetValue(key);
    }
    khint_t k;
    spid_t rid;
    int absent;
    // printf("D %d, %s\n", (dc->idMap == NULL), stringId);
    k = kh_get(DOCID, dc->idMap, stringId);
    // printf("D.1 %d\n", kh_exist(dc->idMap, k));

    if (k != kh_end(dc->idMap)) {
        // SpredisLog(ctx, "E");
        rid = kh_value(dc->idMap, k);
    } else {
        // SpredisLog(ctx, "F");
        rid = _SPNewRecordId(dc);
        stringId = RedisModule_Strdup(stringId);
        // SpredisLog(ctx, "G");
        k = kh_put(DOCID, dc->idMap, stringId, &absent);
        // SpredisLog(ctx, "H");
        kh_value(dc->idMap, k) = rid;
    }
    // SpredisLog(ctx, "I");
    k = kh_put(DOC, dc->documents, rid, &absent);
    if (!absent) {
        char *old = (char *)kh_value(dc->documents, k);
        if (old != NULL) RedisModule_Free(old);
    }
    kh_value(dc->documents, k) = RedisModule_Strdup(data);
    // printf("%s\n", kh_value(dc->documents, k));
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithLongLong(ctx, rid);

    char ress[32];
    sprintf(ress, "%" PRIx64, (unsigned long long)rid);
    // SpredisLog(ctx, "K");
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
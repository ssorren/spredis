#include "../spredis.h"


typedef struct {
    RedisModuleBlockedClient *bc;
    int argc, stripEmpties, includeStore;
    RedisModuleString **argv;
    SpredisSetCont * (*command)(SpredisSetCont **, int);
} SPSetTarg;

void * _SpredisInitSet() {

    SpredisSetCont *cont = RedisModule_Alloc(sizeof(SpredisSetCont));
    pthread_rwlock_init ( &(cont->mutex),NULL );
    // pthread_rwlock_init ( &(cont->bigLock),NULL );
    cont->set = kh_init(SIDS);
    return cont;
}


void _SpredisDestroySet(void *value) {
    SpredisSetCont *dhash = value;
    SpredisProtectWriteMap(dhash);
    if (dhash->set != NULL) kh_destroy(SIDS, dhash->set);
    SpredisUnProtectMap(dhash);
    pthread_rwlock_destroy(&dhash->mutex);
    // pthread_rwlock_destroy(&dhash->bigLock);
    RedisModule_Free(dhash);
}


void SpredisSetRDBSave(RedisModuleIO *io, void *ptr) {
    SpredisSetCont *dhash = ptr;
    RedisModule_SaveUnsigned(io, kh_size(dhash->set));
    spid_t id;
    kh_foreach_key(dhash->set, id, {
        RedisModule_SaveUnsigned(io, id);
    });
}


void SpredisSetRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    SpredisSetCont *dhash = value;
    spid_t id;
    kh_foreach_key(dhash->set, id, {
        char ress[32];
        sprintf(ress, "%" PRIx64, (unsigned long long)id);
        // RedisModule_CreateStringPrintf(ctx, "%" PRIx32, id);
        RedisModule_EmitAOF(aof,"spredis.sadd","sc", key, ress);
    });
}

int SPThreadedSetReply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    // RedisModule_ReplyWithLongLong(ctx, 1);
    return REDISMODULE_OK;
    // SPStoreRadiusTarg *targ = RedisModule_GetBlockedClientPrivateData(ctx);
    // // if (targ->geoCont) SpredisUnProtectMap(targ->geoCont);
    // // if (targ->hintCont) SpredisUnProtectMap(targ->hintCont);
    // // if (targ->resCont) SpredisUnProtectMap(targ->resCont);
    // return RedisModule_ReplyWithLongLong(ctx,targ->reply);
}

void SPThreadedSetReplyFree(void *arg)
{
    // RedisModule_ReplyWithLongLong(ctx, 1);
    // return REDISMODULE_OK;
    SPSetTarg *targ = arg;//RedisModule_GetBlockedClientPrivateData(ctx);
    // if (targ->argc) {
        // for (int i = 0; i < targ->argc; ++i)
        // {
        //     RedisModule_Free(targ->argv[i]);
        // }
    // }
    RedisModule_Free(arg);
}

void *SpredisSetRDBLoad(RedisModuleIO *io, int encver) {
    if (encver != SPREDISDHASH_ENCODING_VERSION) {
        /* We should actually log an error here, or try to implement
           the ability to load older versions of our data structure. */
        return NULL;
    }
    
    SpredisSetCont *dhash = _SpredisInitSet();
    spid_t valueCount = RedisModule_LoadUnsigned(io);
    for (spid_t i = 0; i < valueCount; ++i)
    {
        spid_t id = RedisModule_LoadUnsigned(io);
        int absent;
        kh_put(SIDS, dhash->set, id, &absent);
    }
    return dhash;
}

void SpredisSetFreeCallback(void *value) {
    if (value == NULL) return;
    // SP_LAZY_DELETE_CALL(_SpredisDestroySet, value);
    // SpredisSetCont *dhash = value;
    // _SpredisDestroySet(value);
    SP_TWORK(_SpredisDestroySet, value, {
        //do nothing
    });
}

static inline int SpredisSetLenCompareLT(SpredisSetCont *a, SpredisSetCont *b, void *mcd) {
    return kh_size(a->set) < kh_size(b->set);
}

SPREDIS_SORT_INIT(SpredisSetCont, void, SpredisSetLenCompareLT)

SpredisSetCont *SpredisSIntersect(SpredisSetCont **cas, int count) {
    // printf("calling intersect %d\n", count);
    if (count == 0) return NULL;

    SpredisSetCont *res;
    size_t j;
    j = count;
    while (j) {
        SpredisProtectReadMap(cas[--j]);
    }
    SpredisSpredisSetContSort(count, cas, NULL);
    spid_t id;
    // khint_t bk;
    int absent, add;
    res = _SpredisInitSet();
    khash_t(SIDS) *product, *comp, *small;
    product = res->set;
    small = cas[0]->set;
    // printf("%s\n", small == comp[]);
    kh_foreach_key(small, id, {
        add = 1;
        for (j = 1; j < count; ++j)
        {
            comp = cas[j]->set;
            // bk = kh_get(SIDS, comp, id);
            if (!kh_contains(SIDS, comp, id)) {
                add = 0;
                break;
            }
        }
        if (add) {
            kh_put(SIDS,product,id, &absent);
        }
    });
    j = count;
    while (j) {
        SpredisUnProtectMap(cas[--j]);
    }
    // printf("Found %u\n", kh_size(product));
    // if (kh_size(product) == 0) {
    //  _SpredisDestroySet(res);
    //  return NULL;
    // }
    return res;
}


SpredisSetCont *SpredisSDifference(SpredisSetCont **cas, int count) {
    if (count == 0) return NULL;
    SpredisSetCont *ca = cas[0];
    SpredisSetCont *res;
    if (ca == NULL) return NULL;
    int j = count;
    // long long startTimer = RedisModule_Milliseconds(); 
    while (j) {
        SpredisProtectReadMap(cas[--j]);
        // printf("set size = %lu\n", kh_size(cas[j]->set));
    }
    if (count > 2) {
        SpredisSetCont **cbs = cas + 1;
        SpredisSpredisSetContSort(count - 1, cbs, NULL);
    }
    khash_t(SIDS) *a, *comp, *product;
    spid_t id;
    // khint_t bk;
    int absent, add;
    res = _SpredisInitSet();
    a = ca->set;
    product = res->set;
    // printf("COUnt ? %d\n", count);
    
    kh_foreach_key(a, id, {
        add = 1;
        for (j = 1; j < count; ++j)
        {
            comp = cas[j]->set;
            // bk = kh_get(SIDS, comp, id);
            if (kh_contains(SIDS, comp, id)) {
                add = 0;
                break;
            }
        }
        if (add) {
            kh_put(SIDS,product,id, &absent);
        }
    });
    
    
    j = count;
    while (j) {
        SpredisUnProtectMap(cas[--j]);
    }
    // if (kh_size(product) == 0) {
    //  _SpredisDestroySet(res);
    //  return NULL;
    // }
    // printf("differnce took %lldms\n", RedisModule_Milliseconds() - startTimer);
    return res;
}

SpredisSetCont *SpredisSUnion(SpredisSetCont **cas, int count) {
    SpredisSetCont *a;
    khash_t(SIDS) *set;
    SpredisSetCont *res = _SpredisInitSet();
    khash_t(SIDS) *product = res->set;
    int absent;
    spid_t id;
    while (count) {
        a = cas[--count];
        if (a != NULL) {
            SpredisProtectReadMap(a);
            set = a->set;
            kh_foreach_key(set, id, {
                kh_put(SIDS,product, id, &absent);
            });
            SpredisUnProtectMap(a);
        }
    }
    // if (kh_size(product) == 0) {
    //  _SpredisDestroySet(res);
    //  return NULL;
    // }
    return res;
}


SpredisSetCont *SpredisSAddAll(SpredisSetCont **cas, int count) {
    SpredisSetCont *a;
    khash_t(SIDS) *set;

    SpredisSetCont *res = cas[0];
    SpredisProtectWriteMap(res);
    khash_t(SIDS) *product = res->set;
    int absent;
    spid_t id;
    while (count > 0) {
        a = cas[--count];
        if (a != NULL) {
            SpredisProtectReadMap(a);
            set = a->set;
            kh_foreach_key(set, id, {
                kh_put(SIDS,product, id, &absent);
            });
            SpredisUnProtectMap(a);
        }
    }
    SpredisUnProtectMap(res);
    // if (kh_size(product) == 0) {
    //     _SpredisDestroySet(res);
    //     return NULL;
    // }
    return res;
}

int SpredisSetAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 3) return RedisModule_WrongArity(ctx);
    int argOffset = 2;
    // if ( ((argc - argOffset) % 2) != 0 ) return RedisModule_WrongArity(ctx);
    int keyCount = (argc - argOffset);
    int setCount = 0;
    int argIndex = argOffset;
    // printf("%s\n", "WTF1");
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_WRITE);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPSETTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    // printf("%s\n", "WTF2");
    SpredisSetCont *dhash;
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        dhash = _SpredisInitSet();
        SpredisSetRedisKeyValueType(key,SPSETTYPE,dhash);
    } else {
        dhash = RedisModule_ModuleTypeGetValue(key);
    }
    // RedisModule_CloseKey(key);
    // printf("%s\n", "WTF3");
    // SpredisDHash *d;
    SpredisProtectWriteMap(dhash);
    for (int i = 0; i < keyCount; ++i)
    {
        /* code */
        spid_t id = TOINTID(argv[argIndex++],16);
        int absent;
        kh_put(SIDS, dhash->set, id, &absent);
        setCount += absent;
    }
    SpredisUnProtectMap(dhash);
   // printf("%s\n", "WTF4");
    
    RedisModule_ReplyWithLongLong(ctx, setCount);
    RedisModule_ReplicateVerbatim(ctx);
    // printf("%s\n", "WTF5");
    return REDISMODULE_OK;
}

int SpredisSetCard_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // printf("Getting %d\n", TOINTKEY(argv[2]));
    RedisModule_AutoMemory(ctx);
    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1], REDISMODULE_READ);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPSETTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        // RedisModule_CloseKey(key);
        return REDISMODULE_OK;
    }

    SpredisSetCont *dhash = RedisModule_ModuleTypeGetValue(key);
    SpredisProtectReadMap(dhash);
    RedisModule_ReplyWithLongLong(ctx,kh_size(dhash->set));
    SpredisUnProtectMap(dhash);
    // RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

int SpredisSetMember_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // printf("Getting %d\n", TOINTKEY(argv[2]));
    RedisModule_AutoMemory(ctx);
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1], REDISMODULE_READ);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPSETTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithNull(ctx);
        // RedisModule_CloseKey(key);
        return REDISMODULE_OK;
    }

    SpredisSetCont *dhash = RedisModule_ModuleTypeGetValue(key);
    spid_t id = TOINTID(argv[2],16);

    SpredisProtectReadMap(dhash);
    // khint_t k = kh_get(SIDS, dhash->set , id);
    if (kh_contains(SIDS, dhash->set, id)) {
        RedisModule_ReplyWithLongLong(ctx,1);
    } else {
        RedisModule_ReplyWithLongLong(ctx,0);
    }
    SpredisUnProtectMap(dhash);

    // RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

int SpredisSetRem_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_WRITE|REDISMODULE_READ);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPSETTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx,0);
        // RedisModule_CloseKey(key);
        return REDISMODULE_OK;
    }


    SpredisSetCont *dhash = RedisModule_ModuleTypeGetValue(key);
    spid_t id = TOINTID(argv[2],16);
    SpredisProtectWriteMap(dhash);
    khint_t k = kh_get(SIDS, dhash->set , id);
    if (kh_exist(dhash->set, k)) {
        kh_del(SIDS, dhash->set, k);    
        RedisModule_ReplyWithLongLong(ctx,1);
    } else {
        RedisModule_ReplyWithLongLong(ctx,0);
    }
    SpredisUnProtectMap(dhash);

    if (kh_size(dhash->set) == 0) {
        RedisModule_DeleteKey(key);
    }

    // RedisModule_CloseKey(key);
    // RedisModule_ReplyWithLongLong(ctx, (remRes == MAP_OK) ? 1 : 0);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}



void SPSetCommandInit() {
    // SPSetCommandPool = thpool_init(SP_GEN_TPOOL_SIZE);
}


// int SpredisSTempBase_RedisCommandT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, SpredisSetCont * (*command)(SpredisSetCont **, int), int stripEmpties, int includeStore) {
int SpredisSTempBase_RedisCommandT(void *arg) {
    SPSetTarg *targ = arg;
    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(targ->bc);
    RedisModule_AutoMemory(ctx); //let redis free up our little memories
    RedisModule_ThreadSafeContextLock(ctx);

    RedisModuleString **argv = targ->argv;
    int argc = targ->argc;
    int stripEmpties = targ->stripEmpties;
    int includeStore = targ->includeStore;
    
    int keyCount = argc - 2;
    int i, keyType;


    // long long startTimer = RedisModule_Milliseconds(); 

    RedisModuleString **keyNames = argv + 2;
    // RedisModule_PoolAlloc(ctx, sizeof(RedisModuleKey*) *keyCount);
    // for (int i = 0; i < keyCount; ++i)
    // {
    //     printf("\t%s\n", RedisModule_StringPtrLen(keyNames[i], NULL));
    // }
    RedisModuleString *storeName = argv[1];
    // printf("\t%s\n", RedisModule_StringPtrLen(storeName, NULL));
    int wrongTypeCondition = 0;
    int emptyCondition = 0;
    RedisModuleKey **keys = RedisModule_Alloc(sizeof(RedisModuleKey*) * (keyCount + includeStore));
    SpredisSetCont **sets =  RedisModule_Alloc(sizeof(RedisModuleKey*) * (keyCount + includeStore));
    SpredisSetCont *set = NULL;
    SpredisSetCont *result = NULL;
    RedisModuleKey *store = NULL;
    /* open the keys */
    for (i = 0; i < keyCount; ++i)
    {
        keys[i + includeStore] = RedisModule_OpenKey(ctx,keyNames[i], REDISMODULE_READ);
    }

    /* 
        check for empties or wrong types, 
        respecting strip empties (for unions and differences, intersect should return an empty set if any key is missing)
    */
    for (i = includeStore; i < keyCount + includeStore; ++i)
    {
        if (HASH_NOT_EMPTY_AND_WRONGTYPE_CHECKONLY(keys[i], &keyType, SPSETTYPE) != 0) {
            wrongTypeCondition = 1;
            break;
        }
        if (keyType == REDISMODULE_KEYTYPE_EMPTY && !stripEmpties) {
            emptyCondition = 1;
            break;
        }
    }
    // RedisModule_ThreadSafeContextUnlock(ctx);

    if (!wrongTypeCondition && !emptyCondition) {
        int actualKeyCount = includeStore;
        for (i = includeStore; i < keyCount + includeStore; ++i)
        {
            set = RedisModule_ModuleTypeGetValue(keys[i]);
            if (set != NULL) {
                sets[actualKeyCount++] = set;
            }
        }
        
        int wasEmpty = 0;

        store = RedisModule_OpenKey(ctx, storeName, REDISMODULE_WRITE);

        if (includeStore) {
            keyType = RedisModule_KeyType(store);
            wasEmpty = (keyType == REDISMODULE_KEYTYPE_EMPTY);

            if (HASH_NOT_EMPTY_AND_WRONGTYPE_CHECKONLY(store, &keyType, SPSETTYPE) != 0) {
                wrongTypeCondition = 1;
            } else if (wasEmpty) {
                result = _SpredisInitSet();
                SpredisSetRedisKeyValueType(store, SPSETTYPE, result);
                sets[0] = result;
            } else {
                result = RedisModule_ModuleTypeGetValue(store);
                sets[0] = result;
            }
        }

        if (!wrongTypeCondition){
            // SpredisProtectWriteMap(store);
            RedisModule_ThreadSafeContextUnlock(ctx);
            result = targ->command(sets, actualKeyCount);
            RedisModule_ThreadSafeContextLock(ctx);

            keyType = RedisModule_KeyType(store);

            if (!includeStore && keyType != REDISMODULE_KEYTYPE_EMPTY) {
                // printf("deleting ... %s\n", RedisModule_StringPtrLen(argv[1], NULL));
                RedisModule_DeleteKey(store);
                store = RedisModule_OpenKey(ctx, storeName, REDISMODULE_WRITE);
            }
            if (result != NULL) {
                if (!includeStore) SpredisSetRedisKeyValueType(store, SPSETTYPE, result);
            }
        }       
    }
    
    // RedisModule_CloseKey(store);
    // for (i = includeStore; i < keyCount + includeStore; ++i)
    // {
    //     // RedisModule_CloseKey(keys[i]);
    // }
    RedisModule_ThreadSafeContextUnlock(ctx);
    if (wrongTypeCondition) {
        // printf("Wronng type %d\n", 0);
        RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    } else if (result != NULL) {
        RedisModule_ReplyWithLongLong(ctx, kh_size(result->set));
    } else {
        RedisModule_ReplyWithLongLong(ctx, 0);
    }
    
    RedisModule_Free(keys);
    RedisModule_Free(sets);
    RedisModule_UnblockClient(targ->bc, targ);

    RedisModule_FreeThreadSafeContext(ctx);
    return REDISMODULE_OK;
    
}



int SpredisSTempBase_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, SpredisSetCont * (*command)(SpredisSetCont **, int), int stripEmpties, int includeStore) {
    RedisModule_AutoMemory(ctx);
    if ( argc < 3 ) return RedisModule_WrongArity(ctx);

    SPSetTarg *targ = RedisModule_Alloc(sizeof(SPSetTarg));
    targ->bc = RedisModule_BlockClient(ctx, SPThreadedSetReply /*reply*/, SPThreadedSetReply /*timeout*/, SPThreadedSetReplyFree /*free*/ ,0);
    targ->argc = argc;
    targ->stripEmpties = stripEmpties;
    targ->includeStore = includeStore;
    targ->argv = argv;
    targ->command = command;
    // for (int i = 0; i < argc; ++i)
    // {
    //     RedisModule_RetainString(ctx, argv[i]);
    // }
    SPDoWorkInThreadPool(SpredisSTempBase_RedisCommandT, targ);
    return REDISMODULE_OK;
}

int SpredisSTempInterstore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // printf("Intersecting...\n");
 //    for (int i = 2; i < argc; ++i)
 //    {
 //        printf("\t%s\n", RedisModule_StringPtrLen(argv[i], NULL));
 //    }
 //    printf("to %s\n", RedisModule_StringPtrLen(argv[1], NULL));
    return SpredisSTempBase_RedisCommand(ctx, argv, argc, SpredisSIntersect, 0, 0);
}

int SpredisSTempDifference_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // printf("Diffing...\n");
 //    for (int i = 2; i < argc; ++i)
 //    {
 //        printf("\t%s\n", RedisModule_StringPtrLen(argv[i], NULL));
 //    }
 //    printf("to %s\n", RedisModule_StringPtrLen(argv[1], NULL));
    return SpredisSTempBase_RedisCommand(ctx, argv, argc, SpredisSDifference, 1, 0);
}

int SpredisSTempUnion_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // printf("Union...\n");
 //    for (int i = 2; i < argc; ++i)
 //    {
 //        printf("\t%s\n", RedisModule_StringPtrLen(argv[i], NULL));
 //    }
 //    printf("to %s\n", RedisModule_StringPtrLen(argv[1], NULL));
    return SpredisSTempBase_RedisCommand(ctx, argv, argc, SpredisSUnion, 1, 0);
}

int SpredisSTempAddAll_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // printf("Adding...\n");
    return SpredisSTempBase_RedisCommand(ctx, argv, argc, SpredisSAddAll, 1, 1);   
}



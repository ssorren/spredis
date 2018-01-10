#include <math.h>
#include "../spredis.h"




static inline int _SpredisResizeDMap(SpredisDMapCont * cont, unsigned long id) {

    if (id >= cont->size) {
        SpredisProtectWriteMap(cont);
        id += 1024; //create some breathing room so we're not constantly reallocing
        cont->map = RedisModule_Realloc(cont->map, sizeof(SpredisDMap_t) * id);
        if (cont->map == NULL) return REDISMODULE_ERR;
        for (int i = cont->size; i < id; ++i) cont->map[i].full = 0;
        cont->size = id;
        SpredisUnProtectMap(cont);
    }
    return REDISMODULE_OK;
};

void * _SpredisInitDMap() {
    SpredisDMapCont *dhash = RedisModule_Alloc(sizeof(SpredisDMapCont));
    dhash->size = 0;
    dhash->valueCount = 0;
    // dhash->mutex = PTHREAD_MUTEX_INITIALIZER;
    dhash->map = RedisModule_Alloc(sizeof(SpredisDMap_t));
    dhash->map[0].full = 0;
    dhash->map[0].value = 0;
    pthread_rwlock_init ( &dhash->mutex,NULL );
    // pthread_rwlock_init ( &dhash->bigLock,NULL );
    return dhash;
}

int _SpredisSetDMapValue(SpredisDMapCont *dhash, unsigned long id, double value) {
    SpredisProtectWriteMap(dhash);
    if (_SpredisResizeDMap(dhash, id) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
        // return RedisModule_ReplyWithError(ctx, "ERR could not allocate space, probably OOM");
    }
    if (dhash->map[id].full == 0) {
        ++dhash->valueCount;
    }
    dhash->map[id].full = 1;
    dhash->map[id].value = value;
    SpredisUnProtectMap(dhash);
    return REDISMODULE_OK;
}

void SpredisDHashRDBSave(RedisModuleIO *io, void *ptr) {
    SpredisDMapCont *dhash = ptr;
    RedisModule_SaveUnsigned(io, dhash->valueCount);
    for (unsigned long i = 0; i < dhash->size; ++i)
    {
        if (dhash->map[i].full == 1) {
            RedisModule_SaveUnsigned(io, i);
            RedisModule_SaveDouble(io, dhash->map[i].value);
        }
    }
}


void *SpredisDHashRDBLoad(RedisModuleIO *io, int encver) {
    if (encver != SPREDISDHASH_ENCODING_VERSION) {
        /* We should actually log an error here, or try to implement
           the ability to load older versions of our data structure. */
        return NULL;
    }
    
    SpredisDMapCont *dhash = _SpredisInitDMap();
    unsigned long valueCount = RedisModule_LoadUnsigned(io);
    for (unsigned long i = 0; i < valueCount; ++i)
    {
        unsigned long id = RedisModule_LoadUnsigned(io);
        double value = RedisModule_LoadDouble(io);
        _SpredisSetDMapValue(dhash, id, value);
        // _SpredisResizeDMap(dhash, id);
        // ++dhash->valueCount;
        // dhash->map[id].full = 1;
        // dhash->map[id].value = value;
    }
    return dhash;
}



void SpredisDHashFreeCallback(void *value) {

    // DHash_t *dhash = (DHash_t *) value;
    // kh_destroy(SPREDISD, dhash);
    SpredisDMapCont *dhash = value;
    SpredisProtectWriteMap(dhash);
    RedisModule_Free(dhash->map);
    SpredisUnProtectMap(dhash);
    pthread_rwlock_destroy(&dhash->mutex);
    // pthread_rwlock_destroy(&dhash->bigLock);
    RedisModule_Free(dhash);
}



void SpredisDHashRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    SpredisDMapCont *dhash = value;
    for (unsigned long i = 0; i < dhash->size; ++i)
    {
        if (dhash->map[i].full == 1) {
            char score[50];
            sprintf(score, "%1.17g" ,dhash->map[i].value);
            RedisModule_EmitAOF(aof,"spredis.dhashsetbase10","slc", key, i, score);
        }
    }
}


int SpredisDHashSet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int base) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) return RedisModule_WrongArity(ctx);
    int argOffset = 2;
    if ( ((argc - argOffset) % 2) != 0 ) return RedisModule_WrongArity(ctx);
    int keyCount = (argc - argOffset) / 2;
    int setCount = 0;
    int argIndex = argOffset;
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_WRITE);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPDBLTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }

    double scores[keyCount]; 
    for (int i = 0; i < keyCount; ++i)
    {
        argIndex++;
        int scoreRes = RedisModule_StringToDouble(argv[argIndex++], &(scores[i]));
        if (scoreRes != REDISMODULE_OK) {
            return RedisModule_ReplyWithError(ctx, "ERR Could not convert score to double");
        }
    }

    SpredisDMapCont *dhash;
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        dhash = _SpredisInitDMap();
        SpredisSetRedisKeyValueType(key,SPDBLTYPE,dhash);
    } else {
        dhash = RedisModule_ModuleTypeGetValue(key);
    }
    
    argIndex = argOffset;
    for (int i = 0; i < keyCount; ++i)
    {
        /* code */
        unsigned long id = strtol(RedisModule_StringPtrLen(argv[argIndex++],NULL), NULL, base);//TOINTKEY(argv[2]);
        // printf("setting %s (%d) to %lf \n", RedisModule_StringPtrLen(argv[argIndex - 1],NULL), id, scores[i]);
        if (_SpredisSetDMapValue(dhash, id, scores[i]) != REDISMODULE_OK) {
            return RedisModule_ReplyWithError(ctx, "ERR could not allocate space, probably OOM");
        }

        argIndex++;
        setCount++;
    }
   
    // int found = hashmap_get(dhash, id, (void **)&d);

    /* if we've aleady seen this id, just set the score */
    RedisModule_CloseKey(key);
    RedisModule_ReplyWithLongLong(ctx, setCount);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}





int SpredisDHashSet10_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    //this should not be used directly, only for 
    return SpredisDHashSet_RedisCommand(ctx, argv, argc, 10);
}

int SpredisDHashSet16_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return SpredisDHashSet_RedisCommand(ctx, argv, argc, 16);
}


double SpredisDMapValue(SpredisDMapCont *cont, unsigned long id) {
    return id < cont->size && cont->map[id].full ? cont->map[id].value : -HUGE_VAL; 
}

int SpredisDHashGet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_READ);
    int keyType;

    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPDBLTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithNull(ctx);
        RedisModule_CloseKey(key);
        return REDISMODULE_OK;
    }
    SpredisDMapCont *dhash = RedisModule_ModuleTypeGetValue(key);
    int id = TOINTKEY(argv[2]);
    // printf("%d, %lu, %lu\n", id,dhash->size, dhash->valueCount);
    if (id < dhash->size && dhash->map[id].full) {
        RedisModule_ReplyWithDouble(ctx, dhash->map[id].value);
    } else {
        RedisModule_ReplyWithNull(ctx);
    }
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}


int SpredisDHashDel_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_WRITE);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPDBLTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx,0);
        RedisModule_CloseKey(key);
        return REDISMODULE_OK;
    }

    // DHash_t *dhash = RedisModule_ModuleTypeGetValue(key);
    // khint_t k;
    // int id = TOINTKEY(argv[2]);
    // int res;
    SpredisDMapCont *dhash = RedisModule_ModuleTypeGetValue(key);
    int id = TOINTKEY(argv[2]);
    if (id < dhash->size && dhash->map[id].full) {
        dhash->map[id].full = 0;
        --dhash->valueCount;
        RedisModule_ReplyWithLongLong(ctx, 1);
    } else {
        RedisModule_ReplyWithLongLong(ctx,0);
    }

    RedisModule_CloseKey(key);
    if (dhash->valueCount == 0) {
        RedisModule_DeleteKey(key);
    }
    RedisModule_CloseKey(key);
    // RedisModule_ReplyWithLongLong(ctx, (remRes == MAP_OK) ? 1 : 0);
    
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}



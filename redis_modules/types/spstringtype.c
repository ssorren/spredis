#include "../spredis.h"



// char *SpredisSMapValue(SpredisSMapCont *cont, unsigned long id) {
// 	return id < cont->size && cont->map[id].full ? cont->map[id].value : NULL; 
// }

static inline int _SpredisResizeSMap(SpredisSMapCont * cont, unsigned long id) {
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

void * _SpredisInitSMap() {
    SpredisSMapCont *dhash = RedisModule_Alloc(sizeof(SpredisSMapCont));
    dhash->size = 0;
    dhash->valueCount = 0;
    dhash->map = RedisModule_Alloc(sizeof(SpredisSMap_t));
    dhash->map[0].full = 0;
    dhash->map[0].value = NULL;
    pthread_rwlock_init ( &dhash->mutex,NULL );
    // pthread_rwlock_init ( &dhash->bigLock,NULL );
    return dhash;
}

int _SpredisSetSMapValue(SpredisSMapCont *dhash, unsigned long id, const char* value) {
	SpredisProtectWriteMap(dhash);
    if (_SpredisResizeSMap(dhash, id) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
        // return RedisModule_ReplyWithError(ctx, "ERR could not allocate space, probably OOM");
    }
    if (dhash->map[id].full == 0) {
        ++dhash->valueCount;
    }
    dhash->map[id].full = 1;
    dhash->map[id].value = RedisModule_Strdup(value);
    SpredisUnProtectMap(dhash);
    return REDISMODULE_OK;
}

// int _SpredisDelSMapValue(SpredisDMapCont *dhash, unsigned long id) {
//     if (_SpredisResizeDMap(dhash, id) != REDISMODULE_OK) {
//         return REDISMODULE_ERR;
//         // return RedisModule_ReplyWithError(ctx, "ERR could not allocate space, probably OOM");
//     }
//     if (dhash->map[id].full == 0) {
//         ++dhash->valueCount;
//     }
//     dhash->map[id].full = 1;
//     dhash->map[id].value = RedisModuleStrdup(value);
//     return REDISMODULE_OK;
// }


void SpredisSHashRDBSave(RedisModuleIO *io, void *ptr) {
    SpredisSMapCont *dhash = ptr;
    RedisModule_SaveUnsigned(io, dhash->valueCount);
    for (unsigned long i = 0; i < dhash->size; ++i)
    {
        if (dhash->map[i].full == 1) {
            RedisModule_SaveUnsigned(io, i);
            RedisModule_SaveStringBuffer(io, dhash->map[i].value, strlen(dhash->map[i].value));
        }
    }
}


void SpredisSHashRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    SpredisSMapCont *dhash = value;
    for (unsigned long i = 0; i < dhash->size; ++i)
    {
        if (dhash->map[i].full == 1) {
            RedisModule_EmitAOF(aof,"spredis.dhashsetbase10","slc", key, i, dhash->map[i].value);
        }
    }
}



void *SpredisSHashRDBLoad(RedisModuleIO *io, int encver) {
    if (encver != SPREDISDHASH_ENCODING_VERSION) {
        /* We should actually log an error here, or try to implement
           the ability to load older versions of our data structure. */
        return NULL;
    }
    
    SpredisSMapCont *dhash = _SpredisInitSMap();
    unsigned long valueCount = RedisModule_LoadUnsigned(io);
    for (unsigned long i = 0; i < valueCount; ++i)
    {
        unsigned long id = RedisModule_LoadUnsigned(io);
        RedisModuleString* val = RedisModule_LoadString(io);
        _SpredisSetSMapValue(dhash, id, RedisModule_StringPtrLen(val, NULL));
        RedisModule_Free(val);
        // _SpredisResizeDMap(dhash, id);
        // ++dhash->valueCount;
        // dhash->map[id].full = 1;
        // dhash->map[id].value = value;
    }
    return dhash;
}





void SpredisSHashFreeCallback(void *value) {
    SpredisSMapCont *dhash = value;
	SpredisProtectWriteMap(dhash);
    for (unsigned long i = 0; i < dhash->size; ++i)
    {
        if (dhash->map[i].full == 1) {
        	RedisModule_Free(dhash->map[i].value);
        }
    }
    RedisModule_Free(dhash->map);
    SpredisUnProtectMap(dhash);
    pthread_rwlock_destroy(&dhash->mutex);    
    // pthread_rwlock_destroy(&dhash->bigLock);
    RedisModule_Free(dhash);
}

int SpredisSHashSet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int base) {
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
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPSTRINGTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }

    SpredisSMapCont *dhash;
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        dhash = _SpredisInitSMap();
        SpredisSetRedisKeyValueType(key,SPSTRINGTYPE,dhash);
    } else {
        dhash = RedisModule_ModuleTypeGetValue(key);
    }
    // SpredisDHash *d;
    for (int i = 0; i < keyCount; ++i)
    {
        /* code */
        unsigned long id = strtol(RedisModule_StringPtrLen(argv[argIndex++],NULL), NULL, base);
        RedisModuleString* val = argv[argIndex++];
        //TOINTKEY(argv[2]);

        // printf("setting %s (%d) to %lf \n", RedisModule_StringPtrLen(argv[argIndex - 1],NULL), id, scores[i]);
        if (_SpredisSetSMapValue(dhash, id, RedisModule_StringPtrLen(val,NULL)) != REDISMODULE_OK) {
            return RedisModule_ReplyWithError(ctx, "ERR could not allocate space, probably OOM");
        }

        setCount++;
    }
   
    /* if we've aleady seen this id, just set the score */
    RedisModule_CloseKey(key);
    RedisModule_ReplyWithLongLong(ctx, setCount);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

int SpredisSHashSet10_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    //this should not be used directly, only for 
    return SpredisSHashSet_RedisCommand(ctx, argv, argc, 10);
}

int SpredisSHashSet16_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return SpredisSHashSet_RedisCommand(ctx, argv, argc, 16);
}

int SpredisSHashGet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	// printf("Getting %d\n", TOINTKEY(argv[2]));
    RedisModule_AutoMemory(ctx);
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_READ);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPSTRINGTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithNull(ctx);
        RedisModule_CloseKey(key);
        return REDISMODULE_OK;
    }


    SpredisSMapCont *dhash = RedisModule_ModuleTypeGetValue(key);
    int id = TOINTKEY(argv[2]);
    // printf("%d, %lu, %lu\n", id,dhash->size, dhash->valueCount);
    if (id < dhash->size && dhash->map[id].full) {
        RedisModule_ReplyWithString(ctx, RedisModule_CreateString(ctx,  dhash->map[id].value, strlen(dhash->map[id].value)));
    } else {
        RedisModule_ReplyWithNull(ctx);
    }
    RedisModule_CloseKey(key);

    return REDISMODULE_OK;
}

int SpredisSHashDel_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_WRITE);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPSTRINGTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx,0);
        RedisModule_CloseKey(key);
        return REDISMODULE_OK;
    }


    SpredisSMapCont *dhash = RedisModule_ModuleTypeGetValue(key);
    int id = TOINTKEY(argv[2]);
    if (id < dhash->size && dhash->map[id].full) {
        dhash->map[id].full = 0;
        --dhash->valueCount;
        RedisModule_Free(dhash->map[id].value);
        dhash->map[id].value = NULL;
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

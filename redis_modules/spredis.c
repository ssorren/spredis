#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <float.h>
#include <math.h>
#include "spredis.h" //important that this coes before khash-- defines memory functions




static RedisModuleType* SPREDISMODULE_TYPES[32]; 



int HASH_NOT_EMPTY_AND_WRONGTYPE(RedisModuleKey *key, int *type, int targetType) {
    int keyType = RedisModule_KeyType(key);
    (*type) = keyType;
    if (keyType != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(key) != SPREDISMODULE_TYPES[targetType])
    {
        // printf("Really!!!! %d\n", targetType);
        RedisModule_CloseKey(key);
        return 1;
    }
    return 0;
}

int HASH_EMPTY_OR_WRONGTYPE(RedisModuleKey *key, int *type, int targetType) {
    int keyType = RedisModule_KeyType(key);
    (*type) = keyType;
    if (keyType == REDISMODULE_KEYTYPE_EMPTY ||
        RedisModule_ModuleTypeGetType(key) != SPREDISMODULE_TYPES[targetType])
    {
        // printf("Really!!!! %d\n", targetType);
        RedisModule_CloseKey(key);
        return 1;
    }
    return 0;
}


int SpredisSetRedisKeyValueType(RedisModuleKey *key, int type, void *value) {
    // printf("trying to set %d\n", type);
    return RedisModule_ModuleTypeSetValue(key, SPREDISMODULE_TYPES[type],value);
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx,"spredis",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    printf("SPREDIS_SORT_MEMORIES initialized (bleep bleep blorp)\n");

    for (int j = 0; j < argc; j++) {
        const char *s = RedisModule_StringPtrLen(argv[j],NULL);
        printf("Module loaded with ARGV[%d] = %s\n", j, s);
    }

    /* support commands */
    if (RedisModule_CreateCommand(ctx,"spredis.storerangebyscore",
        SpredisStoreRangeByScore_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"spredis.storerangebylex",
        SpredisStoreLexRange_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;


    if (RedisModule_CreateCommand(ctx,"spredis.sort",
        SpredisZsetMultiKeySort_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;


     if (RedisModule_CreateCommand(ctx,"spredis.facets",
        SpredisFacets_RedisCommand,"readonly",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;



    /* type commands */
    if (RedisModule_CreateCommand(ctx,"spredis.dhashset",
        SpredisDHashSet16_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.dhashsetbase10",
        SpredisDHashSet10_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.dhashget",
        SpredisDHashGet_RedisCommand,"readonly",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.dhashdel",
        SpredisDHashDel_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.hashset",
        SpredisSHashSet16_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.hashsetbase10",
        SpredisSHashSet10_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.hashget",
        SpredisSHashGet_RedisCommand,"readonly",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.hashdel",
        SpredisSHashDel_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

   

    RedisModuleTypeMethods tm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = SpredisDHashRDBLoad,
        .rdb_save = SpredisDHashRDBSave,
        .aof_rewrite = SpredisDHashRewriteFunc,
        .free = SpredisDHashFreeCallback
    };
    // SPREDISMODULE_TYPES = RedisModule_Alloc(sizeof(RedisModuleType*) * 10);
    
    SPREDISMODULE_TYPES[SPDBLTYPE] = RedisModule_CreateDataType(ctx, "Sp_DHshSS",
        SPREDISDHASH_ENCODING_VERSION, &tm);
    
    if (SPREDISMODULE_TYPES[SPDBLTYPE] == NULL) return REDISMODULE_ERR;

    RedisModuleTypeMethods sm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = SpredisSHashRDBLoad,
        .rdb_save = SpredisSHashRDBSave,
        .aof_rewrite = SpredisSHashRewriteFunc,
        .free = SpredisSHashFreeCallback
    };

    SPREDISMODULE_TYPES[SPSTRINGTYPE] = RedisModule_CreateDataType(ctx, "Sp_SHshSS",
        SPREDISDHASH_ENCODING_VERSION, &sm);

    if (SPREDISMODULE_TYPES[SPSTRINGTYPE] == NULL) return REDISMODULE_ERR;


    RedisModuleTypeMethods rm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = SpredisTMPResRDBLoad,
        .rdb_save = SpredisTMPResDBSave,
        .aof_rewrite = SpredisTMPResRewriteFunc,
        .free = SpredisTMPResFreeCallback
    };

    SPREDISMODULE_TYPES[SPTMPRESTYPE] = RedisModule_CreateDataType(ctx, "Sp_TMPRSS",
        SPREDISDHASH_ENCODING_VERSION, &rm);

    if (SPREDISMODULE_TYPES[SPTMPRESTYPE] == NULL) return REDISMODULE_ERR;
    // if (SPDBLTYPE == NULL) return REDISMODULE_ERR;
    return REDISMODULE_OK;
}




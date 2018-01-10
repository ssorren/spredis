#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <float.h>
#include <math.h>
// #include "kbtree.h"
// #include "khash.h"
#include "mc_khsort.h"

#include "spredis.h" //important that this coes before khash-- defines memory functions





static RedisModuleType **SPREDISMODULE_TYPES; 



int HASH_NOT_EMPTY_AND_WRONGTYPE(RedisModuleKey *key, int *type, int targetType) {
    int keyType = RedisModule_KeyType(key);
    (*type) = keyType;
    if (keyType != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(key) != SPREDISMODULE_TYPES[targetType])
    {
        // printf("Really 2!!!! %d\n", targetType);
        RedisModule_CloseKey(key);
        return 1;
    }
    return 0;
}

int HASH_NOT_EMPTY_AND_WRONGTYPE_CHECKONLY(RedisModuleKey *key, int *type, int targetType) {
    int keyType = RedisModule_KeyType(key);
    (*type) = keyType;
    if (keyType != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(key) != SPREDISMODULE_TYPES[targetType])
    {
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
        RedisModule_CloseKey(key);
        return 1;
    }
    return 0;
}



// #define SPZIDComp(a,b) ((((b).id) < ((a).id)) - (((a).id) < ((b).id)))
// #define SPZComp(a,b) ((((b).score) < ((a).score)) - (((a).score) < ((b).score)))

// typedef struct {
//     size_t id;
// } SPZSetIdItem;

// KBTREE_INIT(SPID, size_t, kb_generic_cmp)

// KHASH_SET_INIT_INT(IDS)

// typedef struct {
//     double score;
//     kbtree_t(SPID) *items;
// } SPZSetItem;

// KBTREE_INIT(SPSCORE, SPZSetItem, SPZComp)

// #define sortcomp(a,b) ( (a) < (b) )
// // #define sortcomp_b(a,b) ( (a) < (b) )
// KSORT_INIT(IDS, uint32_t, sortcomp)
// KSORT_INIT(SCORES, double, sortcomp)
// #define TOSIZE_TKEY(k) (size_t)strtol(RedisModule_StringPtrLen(k,NULL), NULL, 10)
// #define KB_SMALL_SIZE 64

int SpredisBTREETEST_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // printf("Getting %d\n", TOINTKEY(argv[2]));
    // RedisModule_AutoMemory(ctx);
    
    // // kbtree_t(SPID) *ids = kb_init(SPID, KB_SMALL_SIZE);
    
    // uint32_t count = TOSIZE_TKEY(argv[1]);
    // uint32_t *a = RedisModule_PoolAlloc(ctx, sizeof(size_t) * count);
    // // size_t *d = RedisModule_PoolAlloc(ctx, sizeof(double) * count);
    // uint32_t i;
    // for (i = 0; i < count; ++i) {
    //     a[i] = i * 10;
    // }
    
    // // SPZSetItem *p, *t;

    // khash_t(IDS) *scores;
    // khint_t k;
    // scores = kh_init(IDS);
    // int absent;
    // // size_t *p;
    // // kbtree_t(SPID) *scores = kb_init(SPID, 512);
    // ks_shuffle(IDS, count, a);
    // long long startTimer = RedisModule_Milliseconds();
    // for (i = 0; i < count; ++i) {
    //     k = kh_put(IDS, scores, a[i], &absent);
    //     // kb_put(SPID, scores, a[i]);
    //     // if (absent) {
    //     //     kh_value(scores, k) = RedisModule_Strdup("test");
    //     // }
    // }
    // RedisModule_ReplyWithArray(ctx, 5 * 2);

    // RedisModule_ReplyWithSimpleString(ctx, "populate");
    // RedisModule_ReplyWithLongLong(ctx, RedisModule_Milliseconds() - startTimer);
    // startTimer = RedisModule_Milliseconds();

    // uint32_t newId = (count / 2) + 1;
    // // newId[0] = ;
    // // double newScore = d[i] = (double) floor(rand() / 1000) ;;
    // // t = RedisModule_PoolAlloc(ctx, sizeof(SPZSetItem));
    // // t->score = 0;
    // // t->items = kb_init(SPID, KB_SMALL_SIZE);
    // // p = kb_get(SPID, scores, newId);

    // k = kh_put(IDS, scores, newId, &absent);
    // if (absent) {
    //     // kh_value(scores, k) = RedisModule_Strdup("test");
    //     printf("New\n");
    // } else {
    //     printf("Existed\n");
    // }
    // // if (!p) {
    // //     // t = RedisModule_PoolAlloc(ctx, sizeof(SPZSetItem));
    // //     // t->score = newScore;
    // //     // t->items = kb_init(SPID, 1024);
    // //     printf("New\n");
    // //     k = kh_put(IDS, scores, i, &absent);
    // //     // kb_put(SPID, scores, newId);
        
    // //     // kb_putp(SPSCORE, scores, t);
    // // } else {
    // //     // ++p->count;
    // //     printf("Existed\n");
    // //     // kb_put(SPID, p->items, newId);
    // // }
    // RedisModule_ReplyWithSimpleString(ctx, "add one");
    // RedisModule_ReplyWithLongLong(ctx, RedisModule_Milliseconds() - startTimer);
    // // if (count <=100) {
    // //     kbitr_t itr;
    // //     kb_itr_first(SPID, scores, &itr); // get an iterator pointing to the first
    // //     for (; kb_itr_valid(&itr); kb_itr_next(SPSCORE, scores, &itr)) { // move on
    // //         p = &kb_itr_key(SPZSetItem, &itr);
    // //         printf("%lf\t %d\n", p->score, kb_size(p->items));
    // //     }
    // // }
    // startTimer = RedisModule_Milliseconds();
    // //iterate all
    // // kbitr_t itr;
    // // kb_itr_first(SPID, scores, &itr); // get an iterator pointing to the first
    // // for (; kb_itr_valid(&itr); kb_itr_next(SPID, scores, &itr)) { // move on
    // //     p = &kb_itr_key(size_t, &itr);
    // // }
    // uint32_t id;
    // // const char *sid;
    // for (k = kh_begin(scores); k != kh_end(scores); ++k) {
    //     if (kh_exist(scores, k)) {
    //         //do nothing
    //         id = kh_key(scores,k);

    //         if (count <= 100) {
    //             printf("%d\t\n", kh_key(scores,k));
    //         }
            
    //         // REDISMODULE_NOT_USED(k);
    //     }
    // }

    // RedisModule_ReplyWithSimpleString(ctx, "iterate all");
    // RedisModule_ReplyWithLongLong(ctx, RedisModule_Milliseconds() - startTimer);


    // // uint32_t sidt;
    // // RedisModuleString *sids = RedisModule_CreateString(ctx, "12a6c9", 7);
    // startTimer = RedisModule_Milliseconds();
    // int cnt = 0;
    // for (i = 0; i < count; ++i) {
    //     k = kh_get(IDS, scores, a[i]);
    //     cnt += kh_exist(scores,k);
    //     // kh_put(IDS, scores, a[i], &absent);
    //     // kb_put(SPID, scores, a[i]);
    //     // if (absent) {
    //     //     kh_value(scores, k) = RedisModule_Strdup("test");
    //     // }
    // }   
    // // free((char*)kh_key(h, k));
    // char reply[50];
    // sprintf(reply, "get all %d", cnt);
    // RedisModule_ReplyWithSimpleString(ctx, reply);
    // RedisModule_ReplyWithLongLong(ctx, RedisModule_Milliseconds() - startTimer);


    // RedisModule_ReplyWithSimpleString(ctx, "count");
    // RedisModule_ReplyWithLongLong(ctx, kh_size(scores));
    
    // // for (k = kh_begin(scores); k != kh_end(scores); ++k) {
    // //     if (kh_exist(scores, k)) {
    // //         //do nothing
    // //         // free(kh_value(scores, k));
    // //         // REDISMODULE_NOT_USED(k);
    // //         RedisModule_Free(kh_value(scores, k));
    // //     }
    // // }

    // kh_destroy(IDS, scores);
    // // printf("BTree Create took %lldms\n", RedisModule_Milliseconds() - startTimer);
    
    // kb_destroy(SPSCORE, scores);
    return REDISMODULE_OK;
}

int SpredisSetRedisKeyValueType(RedisModuleKey *key, int type, void *value) {
    // printf("trying to set %d\n", type);
    return RedisModule_ModuleTypeSetValue(key, SPREDISMODULE_TYPES[type],value);
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx,"spredis",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;
    SPREDISMODULE_TYPES = RedisModule_Alloc(sizeof(RedisModuleType*) * 128);
    SpredisTempResultModuleInit();
    SpredisZsetMultiKeySortInit();
    SpredisFacetInit();

    printf("SPREDIS_SORT_MEMORIES initialized (bleep bleep blorp)\n");
    uint32_t x = 1222345;
    printf("A 64bit integer in hex looks like %" PRIx32 "\n", x);
    for (int j = 0; j < argc; j++) {
        const char *s = RedisModule_StringPtrLen(argv[j],NULL);
        printf("Module loaded with ARGV[%d] = %s\n", j, s);
    }


    if (RedisModule_CreateCommand(ctx,"spredis.btreetest",
        SpredisBTREETEST_RedisCommand,"readonly",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    
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

    if (RedisModule_CreateCommand(ctx,"spredis.getresids",
        SpredisTMPResGetIds_RedisCommand,"readonly",0,0,0) == REDISMODULE_ERR)
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


    if (RedisModule_CreateCommand(ctx,"spredis.sadd",
        SpredisSetAdd_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.smember",
        SpredisSetMember_RedisCommand,"readonly",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.srem",
        SpredisSetRem_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.scard",
        SpredisSetCard_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.staddall",
        SpredisSTempAddAll_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.stinterstore",
        SpredisSTempInterstore_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.stdiffstore",
        SpredisSTempDifference_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.stunionstore",
        SpredisSTempUnion_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;


    RedisModuleTypeMethods tm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = SpredisDHashRDBLoad,
        .rdb_save = SpredisDHashRDBSave,
        .aof_rewrite = SpredisDHashRewriteFunc,
        .free = SpredisDHashFreeCallback
    };
    // SPREDISMODULE_TYPES = RedisModule_Alloc(sizeof(RedisModuleType*) * 10);
    RedisModuleTypeMethods stm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = SpredisSetRDBLoad,
        .rdb_save = SpredisSetRDBSave,
        .aof_rewrite = SpredisSetRewriteFunc,
        .free = SpredisSetFreeCallback
    };

    SPREDISMODULE_TYPES[SPSETTYPE] = RedisModule_CreateDataType(ctx, "SPpSPeTSS",
        SPREDISDHASH_ENCODING_VERSION, &stm);

    if (SPREDISMODULE_TYPES[SPSETTYPE] == NULL) return REDISMODULE_ERR;



    SPREDISMODULE_TYPES[SPDBLTYPE] = RedisModule_CreateDataType(ctx, "p_DHshSSS",
        SPREDISDHASH_ENCODING_VERSION, &tm);
    
    if (SPREDISMODULE_TYPES[SPDBLTYPE] == NULL) return REDISMODULE_ERR;

    RedisModuleTypeMethods sm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = SpredisSHashRDBLoad,
        .rdb_save = SpredisSHashRDBSave,
        .aof_rewrite = SpredisSHashRewriteFunc,
        .free = SpredisSHashFreeCallback
    };

    SPREDISMODULE_TYPES[SPSTRINGTYPE] = RedisModule_CreateDataType(ctx, "_SHshSSpS",
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


    
    // if (SPDBLTYPE == NULL) return REDISMODULE_ERR;
    return REDISMODULE_OK;
}




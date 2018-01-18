// #pragma GCC diagnostic ignored "-Wunused-function"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <float.h>
#include <math.h>

#include "spredis.h" //important that this coes before khash-- defines memory functions
#include "lib/ksort.h"
#include "types/spsharedtypes.h"



static RedisModuleType **SPREDISMODULE_TYPES; 


// static inline int ScoreComp(SPScore a, SPScore b) {
//     if (a.score < b.score) return -1;
//     if (b.score < a.score) return 1;
//     if (a.id < b.id) return -1;
//     if (b.id < a.id) return 1;
//     return 0;
// }

int HASH_NOT_EMPTY_AND_WRONGTYPE(RedisModuleKey *key, int *type, int targetType) {
    int keyType = RedisModule_KeyType(key);
    (*type) = keyType;
    if (keyType != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(key) != SPREDISMODULE_TYPES[targetType])
    {
        // printf("Really 2!!!! %d\n", targetType);
        

        //RedisModule_CloseKey(key);


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
        // RedisModule_CloseKey(key);
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


int SPSortGeo(SPGeo *a, SPGeo *b) {
    return ((a->lat > b->lat) - (a->lat < b->lat)) || ((a->lon > b->lon) - (a->lon < b->lon));
}

int SPSortSore(SPScore *a, SPScore *b) {
    return (a->score < b->score);
}
#define scorelt(a,b) ( (a) < (b) )
KSORT_INIT(IDSHUFFLE, uint32_t, scorelt)
// DISTANCE_INIT(Test)
// s
KHASH_MAP_INIT_INT(GEO, SPGeo*)
#define ScorePresortLt(a,b) ((a->score) < (b->score))
#define GetScore(a) (a->score)

typedef kbtree_t(LEX) kbtree_t(TESTLEX);
typedef kbtree_t(SCORE) kbtree_t(TESTSCORE);
typedef khash_t(SCORE) khash_t(TESTSCORE);


SPLEX_BTREE_INIT(TESTLEX); 
SPSCORE_BTREE_INIT(TESTSCORE); 

KHASH_MAP_INIT_INT(TESTSCORE, SPScore*);
static inline double SPDist(double th1, double ph1, double th2, double ph2)
{
    double dx, dy, dz;
    ph1 -= ph2;
    ph1 *= 0.017453292519943295, th1 *= 0.017453292519943295, th2 *= 0.017453292519943295;
 
    dz = sin(th1) - sin(th2);
    dx = cos(ph1) * cos(th1) - cos(th2);
    dy = sin(ph1) * cos(th1);
    return asin(sqrt(dx * dx + dy * dy + dz * dz) / 2) * 2 * 6372797.560856;
}


#include "lib/sp_kbtree.h"
// #define ScoreComp(a,b) (((((b).score) < ((a).score)) - (((a).score) < ((b).score))) || ((((b).id) < ((a).id)) - (((a).id) < ((b).id))))

// KBTREE_INIT(SCORES, SPScore, SPScoreComp)
// SPSCORE_BTREE_INIT(TEST_SCORES)
// SPREDIS_SORT_INIT(SCORESORT, SPScore*, SPSortSore)


int SpredisTEST_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // printf("Getting %d\n", TOINTKEY(argv[2]));
    RedisModule_AutoMemory(ctx);
    

    double slat = 41.48518796577633;
    double slon = -87.5162054198453;
    double radius = 500 * 1000;

    
    long long startTimer = RedisModule_Milliseconds();
    RedisModuleCallReply *reply;
        reply = RedisModule_Call(ctx,"georadius","sccscc", argv[1], "-87.5162054198453", "41.48518796577633", argv[2], "km", "WITHCOORD");
     
    if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }

    RedisModule_ReplyWithArray(ctx, 14);
    RedisModule_ReplyWithSimpleString(ctx, "ZSearch took:");
    RedisModule_ReplyWithLongLong(ctx, RedisModule_Milliseconds() - startTimer);

    size_t reply_len = RedisModule_CallReplyLength(reply);
    SPGeo **allCoords = RedisModule_PoolAlloc(ctx, sizeof(SPGeo *) * reply_len);
    SPGeo *coords;
    for (size_t i = 0; i < reply_len; ++i)
    {   
        /* code */
        RedisModuleCallReply *subreply;
        subreply = RedisModule_CallReplyArrayElement(reply,i);
        uint32_t tid = TOINTKEY(RedisModule_CreateStringFromCallReply(RedisModule_CallReplyArrayElement(subreply,0)));

        RedisModuleCallReply *coordreply;
        coordreply = RedisModule_CallReplyArrayElement(subreply,1);
        double lon, lat;
        RedisModule_StringToDouble(RedisModule_CreateStringFromCallReply(RedisModule_CallReplyArrayElement(coordreply,0)), &lon);
        RedisModule_StringToDouble(RedisModule_CreateStringFromCallReply(RedisModule_CallReplyArrayElement(coordreply,1)), &lat);

        // printf("%u = [%f, %f]\n", id, lat, lon);
        coords = RedisModule_PoolAlloc(ctx, sizeof(SPGeo));
        coords->id  = tid;
        coords->lat  = lat;
        coords->lon  = lon;
        // coords->hh  = NULL;
        allCoords[i] = coords;
    }
    // SPGeo *theCoords = NULL;
    khash_t(GEO) *theCoords = kh_init(GEO);
    // khash_t(SIDS) *set;
    int absent;
    khint_t k;
    startTimer = RedisModule_Milliseconds();
    for (uint32_t i = 0; i < reply_len; ++i)
    {
        // coords = allCoords[i];
        coords = allCoords[i];
        k = kh_put(GEO, theCoords, coords->id, &absent);
        if (absent) {
            kh_put_value(theCoords, k, coords);
        }
        // HASH_ADD_INT(theCoords, id, allCoords[i]);    
    }
    RedisModule_ReplyWithSimpleString(ctx, "UT add all took:");
    RedisModule_ReplyWithLongLong(ctx, RedisModule_Milliseconds() - startTimer);
    RedisModule_ReplyWithSimpleString(ctx, "items:");
    RedisModule_ReplyWithLongLong(ctx, kh_size(theCoords));

    startTimer = RedisModule_Milliseconds();
    double distance;
    size_t found = 0;
    // uint32_t id;
    // coords = theCoords->head;
    kh_foreach_value(theCoords, coords, {
        distance = SPDist(slat, slon, coords->lat, coords->lon);
        found += (distance <= radius);
    })

    
    // while(coords) {
    //     distance = SPDist(slat, slon, coords->lat, coords->lon);
    //     found += (distance <= radius);
    //     coords = coords->next;
    // }
    // for (k = 0; k < kh_end(theCoords); ++k) {
    //     if (kh_exist(theCoords, k)) {
    //         coords = kh_value(theCoords, k);
    //         distance = SPDist(slat, slon, coords->lat, coords->lon);
    //         found += (distance <= radius);
    //     }
    // }
            
    // kh_foreach(theCoords, id, coords, {
    //     distance = SPDist(slat, slon, coords->lat, coords->lon);
    //     found += (distance <= radius);
    // });
    // HASH_ITER(hh, theCoords, coords, tmp) {
    //     distance = SPDist(slat, slon, coords->lat, coords->lon);
        
    // }
    RedisModule_ReplyWithSimpleString(ctx, "haversine took:");
    RedisModule_ReplyWithLongLong(ctx, RedisModule_Milliseconds() - startTimer);
    RedisModule_ReplyWithLongLong(ctx, found);
    RedisModule_ReplyWithDouble(ctx, distance);



    const char * _keyName = "V:A:I:N:startPrice:::V:M";//"V:A:I:H:startPrice:::SALLVALS:M";
    RedisModuleString * keyName = RedisModule_CreateString(ctx, _keyName, strlen(_keyName));
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ);

    SPScoreCont *testLexCont = RedisModule_ModuleTypeGetValue(key);

    khash_t(TESTSCORE) *map =  testLexCont->set;
    startTimer = RedisModule_Milliseconds();
    kbtree_t(TESTSCORE) *testTree = kb_init(TESTSCORE, 1024);
    SPScore *n, *x;
    // khint_t k;
    
    size_t cnt = 0;
    uint32_t xid;
    
    kh_foreach_key(map, xid, {
        k = kh_get(TESTSCORE, map, xid);
        x = kh_value(map, k);
        n = RedisModule_PoolAlloc(ctx, sizeof(SPScore));
        n->id = xid;
        n->score = x->score;
        kb_putp(TESTSCORE, testTree, n);
        cnt++;
    });
    RedisModule_ReplyWithSimpleString(ctx, "adding sorted took:");
    RedisModule_ReplyWithLongLong(ctx, RedisModule_Milliseconds() - startTimer);
    RedisModule_ReplyWithSimpleString(ctx, "add:");
    RedisModule_ReplyWithLongLong(ctx, cnt);
    // kbitr_t itr;
    // kb_itr_first(TESTSCORE, testTree, &itr);
    // for (; kb_itr_valid(&itr); kb_itr_next(TESTSCORE, testTree, &itr)) { // move on
    //     n = &kb_itr_key(SPScore, &itr);
    //     if (n) RedisModule_Free(n);
    // }
    kb_destroy(TESTSCORE, testTree);
    
    return REDISMODULE_OK;
}

int SpredisSetRedisKeyValueType(RedisModuleKey *key, int type, void *value) {
    // printf("trying to set %d\n", type);
    return RedisModule_ModuleTypeSetValue(key, SPREDISMODULE_TYPES[type],value);
}



int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // SPLazyPool = thpool_init(1);

    if (RedisModule_Init(ctx,"spredis",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;
    SPREDISMODULE_TYPES = RedisModule_Alloc(sizeof(RedisModuleType*) * 128);
    SpredisTempResultModuleInit();
    SpredisZsetMultiKeySortInit();
    SpredisFacetInit();

    printf("SPREDIS_SORT_MEMORIES initialized (bleep bleep blorp)\n");
    // uint32_t x = 1222345;
    // printf("A 64bit integer in hex looks like %" PRIx32 "\n", x);
    for (int j = 0; j < argc; j++) {
        const char *s = RedisModule_StringPtrLen(argv[j],NULL);
        printf("Module loaded with ARGV[%d] = %s\n", j, s);
    }


    if (RedisModule_CreateCommand(ctx,"spredis.haversinetest",
        SpredisTEST_RedisCommand,"readonly",0,0,0) == REDISMODULE_ERR)
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


    
    if (RedisModule_CreateCommand(ctx,"spredis.zadd",
        SpredisZSetAdd_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.zscore",
        SpredisZSetScore_RedisCommand,"readonly",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.zrem",
        SpredisZSetRem_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.zcard",
        SpredisZSetCard_RedisCommand,"readonly",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;


    if (RedisModule_CreateCommand(ctx,"spredis.zladd",
        SpredisZLexSetAdd_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.zlscore",
        SpredisZLexSetScore_RedisCommand,"readonly",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.zlrem",
        SpredisZLexSetRem_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.zlcard",
        SpredisZLexSetCard_RedisCommand,"readonly",0,0,0) == REDISMODULE_ERR)
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
        SpredisSetCard_RedisCommand,"readonly",0,0,0) == REDISMODULE_ERR)
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


    RedisModuleTypeMethods ztm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = SpredisZSetRDBLoad,
        .rdb_save = SpredisZSetRDBSave,
        .aof_rewrite = SpredisZSetRewriteFunc,
        .free = SpredisZSetFreeCallback
    };

    SPREDISMODULE_TYPES[SPZSETTYPE] = RedisModule_CreateDataType(ctx, "SPpSZeTSS",
        SPREDISDHASH_ENCODING_VERSION, &ztm);


    if (SPREDISMODULE_TYPES[SPZSETTYPE] == NULL) return REDISMODULE_ERR;

    RedisModuleTypeMethods zltm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = SpredisZLexSetRDBLoad,
        .rdb_save = SpredisZLexSetRDBSave,
        .aof_rewrite = SpredisZLexSetRewriteFunc,
        .free = SpredisZLexSetFreeCallback
    };

    SPREDISMODULE_TYPES[SPZLSETTYPE] = RedisModule_CreateDataType(ctx, "LsPpSZTSS",
        SPREDISDHASH_ENCODING_VERSION, &zltm);


    if (SPREDISMODULE_TYPES[SPZLSETTYPE] == NULL) return REDISMODULE_ERR;

    
    // if (SPDBLTYPE == NULL) return REDISMODULE_ERR;
    return REDISMODULE_OK;
}




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


#define scorelt(a,b) ( (a) < (b) )
KSORT_INIT(IDSHUFFLE, uint64_t, scorelt);

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

// int INBOUNDS(double lat, double lon, )
int SpredisTEST_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // printf("Getting %d\n", TOINTKEY(argv[2]));
    RedisModule_AutoMemory(ctx);
    

    double slat = 41.48518796577633;
    double slon = -87.5162054198453;
    double km;
    RedisModule_StringToDouble(argv[1], &km);
    double radius = km * 1000;

    RedisModule_DeleteKey( RedisModule_OpenKey(ctx, RedisModule_CreateString(ctx, "TEMPDIST", strlen("TEMPDIST")), REDISMODULE_WRITE));
    long long startTimer = RedisModule_Milliseconds();

    RedisModuleCallReply *reply;
        reply = RedisModule_Call(ctx,"georadius","cccsccc", "V:A:I:G:coords:::V:G_r", "-87.5162054198453", "41.48518796577633", argv[1], "km", "STORE", "TEMPDIST");
     
    if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }

    RedisModule_ReplyWithArray(ctx, 6);
    RedisModule_ReplyWithSimpleString(ctx, "ZSearch took:");
    RedisModule_ReplyWithLongLong(ctx, RedisModule_CallReplyLength(reply));
    RedisModule_ReplyWithLongLong(ctx, RedisModule_Milliseconds() - startTimer);
    


    RedisModuleString * keyName = RedisModule_CreateString(ctx, "V:A:I:G:coords:::V:G", strlen("V:A:I:G:coords:::V:G"));
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ);

    SPGeoScoreCont *cont = RedisModule_ModuleTypeGetValue(key);

    kbtree_t(GEO) *btree = cont->btree;
    kbitr_t itr;
    // size_t found = 0;
    // size_t found = 0;
    // size_t examined = 0;
    // kb_itr_getp(GEO, testScore, l, &itr);
    SPScoreKey *cand;
    double lat, lon, distance = 0.0;

    startTimer = RedisModule_Milliseconds();
    // SPGeoHashBits ghash;
    SPGeoHashArea bounds;
    // SPGeoHashNeighbors neighbors;
    SPGeoSearchAreas areas;
    uint64_t start, stop;// = SPGeoHashEncodeForRadius(slat, slon, radius, &ghash);
    // double min_lon, max_lon, min_lat, max_lat;
    // double bounds[4];

    
    SPGetSearchAreas(slat, slon, radius, &areas, &bounds);
    int absent;
    khash_t(SCORE) *res = kh_init(SCORE);
    khint_t k;
    SPScoreKey *l, *u, *use;
    SPScore *score;
    for (int i = 0; i < 9; ++i)
    {
        /* code */
        SPGeoHashArea area = areas.area[i];
        if (!area.hash.bits) continue;
        start = area.hash.bits << (62 - (area.hash.step * 2));
        stop = ++area.hash.bits << (62 - (area.hash.step * 2));
        SPScoreKey t = {
            .id = 0,
            .score = start
        };
        // afound = 0;
        kb_intervalp(SCORE, btree, &t, &l, &u);
        use = u == NULL ? l : u;
        kb_itr_getp(GEO, btree, use, &itr);
        for (; kb_itr_valid(&itr); kb_itr_next(SCORE, btree, &itr)) { // move on
            cand = (&kb_itr_key(SPScoreKey, &itr));
            if (cand) {
                if (cand->score >= start) {
                    if (cand->score >= stop) break;
                    SPGeoHashDecode(cand->score, &lat, &lon);
                    if (lon >= bounds.longitude.min && lon <= bounds.longitude.max 
                            && lat >= bounds.latitude.min && lat <= bounds.latitude.max) {
                        SPGeoDist(slat, slon, lat, lon, &distance);
                        if (distance <= radius) {
                            k = kh_put(SCORE, res, cand->id, &absent);
                            if (absent) {
                                score = RedisModule_Calloc(1, sizeof(SPScore));
                                score->id = cand->id;
                                score->score = distance;
                                kh_put_value(res, k, score);
                            } else {
                                kh_value(res, k)->score = distance;
                            }
                        }
                        // found += (distance <= radius);
                        // examined++;
                    }
                    //  else if ((lon < bounds.longitude.min && lon > bounds.longitude.max) 
                    //         || (lat < bounds.latitude.min && lat > bounds.latitude.max)) {
                    //     break;
                    // }
                }
            }
        }
    }

    RedisModule_ReplyWithSimpleString(ctx, "haversine took:");
    RedisModule_ReplyWithLongLong(ctx, RedisModule_Milliseconds() - startTimer);
    RedisModule_ReplyWithLongLong(ctx, kh_size(res));
    // RedisModule_ReplyWithSimpleString(ctx, "examined took:");
    // RedisModule_ReplyWithLongLong(ctx, examined);

    kh_foreach_value(res, score, {
        // if (score->lex) RedisModule_Free(score->lex);
        RedisModule_Free(score);
    });

    kh_destroy(SCORE, res);
    // RedisModule_ReplyWithDouble(ctx, distance);


    return REDISMODULE_OK;
    // size_t reply_len = RedisModule_CallReplyLength(reply);
    // SPScore **allCoords = RedisModule_PoolAlloc(ctx, sizeof(SPScore *) * reply_len);
    // SPScore *coords;
    // for (size_t i = 0; i < reply_len; ++i)
    // {   
    //     /* code */
    //     RedisModuleCallReply *subreply;
    //     subreply = RedisModule_CallReplyArrayElement(reply,i);
    //     uint64_t tid = TOINTKEY(RedisModule_CreateStringFromCallReply(RedisModule_CallReplyArrayElement(subreply,0)));

    //     RedisModuleCallReply *coordreply;
    //     coordreply = RedisModule_CallReplyArrayElement(subreply,1);
    //     double lon, lat;
    //     RedisModule_StringToDouble(RedisModule_CreateStringFromCallReply(RedisModule_CallReplyArrayElement(coordreply,0)), &lon);
    //     RedisModule_StringToDouble(RedisModule_CreateStringFromCallReply(RedisModule_CallReplyArrayElement(coordreply,1)), &lat);

    //     // printf("%u = [%f, %f]\n", id, lat, lon);
    //     coords = RedisModule_PoolAlloc(ctx, sizeof(SPScore));
    //     coords->id  = tid;
    //     // coords->lat  = lat;
    //     // coords->lon  = lon;
    //     // coords->hh  = NULL;
    //     allCoords[i] = coords;
    // }
    // // SPGeo *theCoords = NULL;
    // khash_t(SCORE) *theCoords = kh_init(SCORE);
    // // khash_t(SIDS) *set;
    // int absent;
    // khint_t k;
    // startTimer = RedisModule_Milliseconds();
    // for (uint64_t i = 0; i < reply_len; ++i)
    // {
    //     // coords = allCoords[i];
    //     coords = allCoords[i];
    //     k = kh_put(SCORE, theCoords, coords->id, &absent);
    //     if (absent) {
    //         kh_put_value(theCoords, k, coords);
    //     }
    //     // HASH_ADD_INT(theCoords, id, allCoords[i]);    
    // }
    // RedisModule_ReplyWithSimpleString(ctx, "UT add all took:");
    // RedisModule_ReplyWithLongLong(ctx, RedisModule_Milliseconds() - startTimer);
    // RedisModule_ReplyWithSimpleString(ctx, "items:");
    // RedisModule_ReplyWithLongLong(ctx, kh_size(theCoords));

    // startTimer = RedisModule_Milliseconds();
    // double distance;
    // size_t found = 0;
    // // uint64_t id;
    // // coords = theCoords->head;
    // kh_foreach_value(theCoords, coords, {
    //     // distance = SPDist(slat, slon, coords->lat, coords->lon);
    //     // found += (distance <= radius);
    // })

    // V:A:I:G:coords:::V:G



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
    



    // const char * _keyName = "V:A:I:N:startPrice:::V:M";//"V:A:I:H:startPrice:::SALLVALS:M";
    // RedisModuleString * keyName = RedisModule_CreateString(ctx, _keyName, strlen(_keyName));
    // RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ);

    // SPScoreCont *testLexCont = RedisModule_ModuleTypeGetValue(key);

    // khash_t(TESTSCORE) *map =  testLexCont->set;
    // startTimer = RedisModule_Milliseconds();
    // kbtree_t(TESTSCORE) *testTree = kb_init(TESTSCORE, 1024);
    // SPScore *n, *x;
    // // khint_t k;
    
    // size_t cnt = 0;
    // uint64_t xid;
    
    // kh_foreach_key(map, xid, {
    //     k = kh_get(TESTSCORE, map, xid);
    //     x = kh_value(map, k);
    //     n = RedisModule_PoolAlloc(ctx, sizeof(SPScore));
    //     n->id = xid;
    //     n->score = x->score;
    //     kb_putp(TESTSCORE, testTree, n);
    //     cnt++;
    // });
    // RedisModule_ReplyWithSimpleString(ctx, "adding sorted took:");
    // RedisModule_ReplyWithLongLong(ctx, RedisModule_Milliseconds() - startTimer);
    // RedisModule_ReplyWithSimpleString(ctx, "add:");
    // RedisModule_ReplyWithLongLong(ctx, cnt);
    // kbitr_t itr;
    // kb_itr_first(TESTSCORE, testTree, &itr);
    // for (; kb_itr_valid(&itr); kb_itr_next(TESTSCORE, testTree, &itr)) { // move on
    //     n = &kb_itr_key(SPScore, &itr);
    //     if (n) RedisModule_Free(n);
    // }
    // kb_destroy(TESTSCORE, testTree);
    
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
    // uint64_t x = 1222345;
    // printf("A 64bit integer in hex looks like %" PRIx32 "\n", x);
    // printf("khint32_t=%zu, khint64_t=%zu, ULONG_MAX=%lu, ULLONG_MAX=%llu, %d\n", sizeof(khint32_t), sizeof(khint64_t), ULONG_MAX, ULLONG_MAX, ULONG_MAX == ULLONG_MAX);
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

    if (RedisModule_CreateCommand(ctx,"spredis.storerangebyradius",
        SpredisStoreRangeByRadius_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
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
    
    if (RedisModule_CreateCommand(ctx,"spredis.hsetstr",
        SpredisHashSetStr_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.hsetdbl",
        SpredisHashSetDouble_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.hgetstr",
        SpredisHashGetStr_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.hgetdbl",
        SpredisHashGetDouble_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.hdel",
        SpredisHashDel_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
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

    if (RedisModule_CreateCommand(ctx,"spredis.zlapplyscores",
        SpredisZLexSetApplySortScores_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;




    if (RedisModule_CreateCommand(ctx,"spredis.geoadd",
        SpredisZGeoSetAdd_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.geoscore",
        SpredisZGeoSetScore_RedisCommand,"readonly",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.georem",
        SpredisZGeoSetRem_RedisCommand,"write",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"spredis.geocard",
        SpredisZGeoSetCard_RedisCommand,"readonly",0,0,0) == REDISMODULE_ERR)
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



    RedisModuleTypeMethods htm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = SpredisHashRDBLoad,
        .rdb_save = SpredisHashRDBSave,
        .aof_rewrite = SpredisHashRewriteFunc,
        .free = SpredisHashFreeCallback
    };

    SPREDISMODULE_TYPES[SPHASHTYPE] = RedisModule_CreateDataType(ctx, "HsPpShTSS",
        SPREDISDHASH_ENCODING_VERSION, &htm);

    if (SPREDISMODULE_TYPES[SPHASHTYPE] == NULL) return REDISMODULE_ERR;

    


    RedisModuleTypeMethods gtm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = SpredisZGeoSetRDBLoad,
        .rdb_save = SpredisZGeoSetRDBSave,
        .aof_rewrite = SpredisZGeoSetRewriteFunc,
        .free = SpredisZGeoSetFreeCallback
    };

    SPREDISMODULE_TYPES[SPGEOTYPE] = RedisModule_CreateDataType(ctx, "GsPpSZTSS",
        SPREDISDHASH_ENCODING_VERSION, &gtm);


    if (SPREDISMODULE_TYPES[SPGEOTYPE] == NULL) return REDISMODULE_ERR;

    // if (SPDBLTYPE == NULL) return REDISMODULE_ERR;
    return REDISMODULE_OK;
}




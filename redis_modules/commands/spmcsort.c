#include <math.h>
#include "../spredis.h"


// typedef struct SpredisSortData {
//   int id;
//   RedisModuleString* key;
//   double *scores;
// } SpredisSortData;

// typedef struct SpredisColumnData {
//     DHash_t **cols;
//     int *orders;
//     int colCount;
// } SpredisColumnData;

// static inline void SpredisGetScore(SpredisDMapCont *dhash, int id, double *score) {

// 	(*score) =  SpredisDMapValue(dhash, id);//dhash->map[id].full ? dhash->map[id].value : -HUGE_VAL;
//     // khint_t k = DHashGet( dhash, id);
//     // (*score) =  (k != kh_end(dhash)) ? kh_value(dhash, k) : -HUGE_VAL;
// }

// const char * SPREDIS_ASC = "asc";

static inline int SpredisSortDataCompareLT(SpredisSortData a, SpredisSortData b, SpredisColumnData *mcd) {
    //the column were inserted in reverse order so...
    // for (int i = 0; i < mcd->colCount; ++i) {
	// printf("%s %d %d %d %d\n", "ABOUT TO COMPARE", a.id, b.id, a.scores == NULL, b.scores == NULL);
    int i = mcd->colCount;
    while (i) {
        i--;
        // printf("%s %d\n", "ABOUT TO COMPARE2", mcd->orders[i]);
        if (a.scores[i] < b.scores[i]) return (1 == mcd->orders[i]);
        // printf("%s %d\n", "ABOUT TO COMPARE3", mcd->orders[i]);
        if (a.scores[i] > b.scores[i]) return (0 == mcd->orders[i]);
    }
    // while (i);
    return 0;
}

SPREDIS_SORT_INIT(SpredisSortData, SpredisColumnData, SpredisSortDataCompareLT)



// typedef struct {
// int a, b, c;
// } MyStruct;
// #define MyStructFreer(x) // Nothing to free because MyStruct doesn't contain any pointers
// KMEMPOOL_INIT (msp, MyStruct, MyStructFreer); // Sets up the macros
// kmempool_t(msp) *myMemPool; // Declares a pointer to a mempool
// myMemPool = kmp_init(msp); // Allocs the mempool
// MyStruct *a, *b, *c; // Lets alloc some mystruct's
// a = kmp_alloc(msp, myMemPool);
// b = kmp_alloc(msp, myMemPool);
// kmp_free(msp, myMemPool, a);
// c = kmp_alloc(msp, myMemPool);
// kmp_destroy(msp, myMemPool);


static inline int SpredisMergeSortZset(RedisModuleCtx *ctx, RedisModuleKey *zkey, unsigned int count, SpredisColumnData *mcd, long long replyStart, long long replyCount, RedisModuleString *dest) {

    if (RedisModule_ZsetFirstInScoreRange(zkey, REDISMODULE_NEGATIVE_INFINITE, REDISMODULE_POSITIVE_INFINITE, 0, 0) != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid range");
    }

    RedisModuleKey *list = RedisModule_OpenKey(ctx, dest, REDISMODULE_WRITE);
    if (RedisModule_KeyType(list) != REDISMODULE_KEYTYPE_EMPTY) {
        /* if we can't write to the list, no point in going the sort */
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    

    SpredisSortData *datas = RedisModule_PoolAlloc(ctx, sizeof(SpredisSortData) * count);
    
    

    size_t i = 0;

    RedisModuleString * ele;
    SpredisSortData *d;
    // long long startTimer = RedisModule_Milliseconds();
    // printf("%s\n", "WTF1");
    while(!RedisModule_ZsetRangeEndReached(zkey)) {
        d = &datas[i];//RedisModule_PoolAlloc(ctx, sizeof(SpredisSortData));

        ele = RedisModule_ZsetRangeCurrentElement(zkey,NULL);
        // printf("%s\n", "WTF2");
        d->id = TOINTKEY(ele);
        // printf("%s\n", "WTF3");
        d->key = ele;//RedisModule_Strdup( RedisModule_StringPtrLen(ele,NULL));
        // printf("%s\n", "WTF4");
        d->scores = RedisModule_PoolAlloc(ctx, sizeof(double) * mcd->colCount);
        // printf("%s\n", "WTF5");
        /* always fill in the first value, we will need this for sure */
        
        /* fill in the rest with +inf, later if we need it, we willl fetch */
        int k = mcd->colCount;
        while (k) {
            --k;
            // printf("%s\n", "WTF6");
            d->scores[k] = SpredisDMapValue(mcd->cols[k], d->id);
            // SpredisGetScore(mcd->cols[k], datas[i].id, &(d.scores[k]));
        }
        i++;
        RedisModule_ZsetRangeNext(zkey);
    }
    RedisModule_ZsetRangeStop(zkey);
    // printf("Hydrating array took %lldms\n", RedisModule_Milliseconds() - startTimer);
    // startTimer = RedisModule_Milliseconds();
    // ks_heapmake_Spredis(count, datas, mcd);
    // ks_heapsort_Spredis(count, datas, mcd);
    // printf("about to sort %d items %d cols\n",count, mcd->colCount);
    if (mcd->colCount > 0) SpredisSpredisSortDataSort(count, datas, mcd); //exteremly important to not call the sort with an empty column count
    // printf("sorted\n");
    // printf("Sorting array took %lldms\n", RedisModule_Milliseconds() - startTimer);
    int finalCount = replyCount;
    /* make sure we don't overrun the array length */
    if (replyStart + replyCount > count) {
        finalCount = count - (replyStart + replyCount);
        if (finalCount < 0) finalCount = 0;
    }
    
    RedisModule_ReplyWithLongLong(ctx,finalCount);
    while(finalCount > 0) {
        // RedisModule_CreateStringFromString(ctx, datas[replyStart]->key)
        // RedisMosuleString *s = TOCHARKEY(datas[replyStart]->key);
        // RedisModule_ListPush(list, REDISMODULE_LIST_TAIL, RedisModule_CreateString(ctx, datas[replyStart]->key, strlen(datas[replyStart]->key)));
        RedisModule_ListPush(list, REDISMODULE_LIST_TAIL,  datas[replyStart].key);
        finalCount--;
        replyStart++;
    }
    RedisModule_CloseKey(list);
    // RedisModule_Free(datas);
    return REDISMODULE_OK;   
}

static inline int SpredisFetchColumnsAndOrders(RedisModuleCtx *ctx, RedisModuleString **argv, SpredisColumnData *mcd, int argOffset) {
    int argIndex = argOffset;

    //lets put these in reverse order so we can spped up out sort loop later;
    for (int i = mcd->colCount - 1; i >= 0; --i)
    {
        RedisModuleString *zkey = argv[argIndex++];
        RedisModuleString *order = argv[argIndex++];;

        RedisModuleKey *key = RedisModule_OpenKey(ctx,zkey,
            REDISMODULE_READ);
        
        int keyType;
        if (HASH_EMPTY_OR_WRONGTYPE(key, &keyType, SPDBLTYPE)){
            RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
            return REDISMODULE_ERR;
        }
        mcd->cols[i] = RedisModule_ModuleTypeGetValue(key);
        RedisModule_CloseKey(key);
        const char * theOrder = RedisModule_StringPtrLen(order, NULL);
        // int asc = SPREDIS_ORDER(theOrder)strcasecmp(SPREDIS_ASC, theOrder);
        mcd->orders[i] = SPREDIS_ORDER(theOrder);//(asc == 0) ? 1 : 0;
    }
    return REDISMODULE_OK;
}

/* going to use a tree sort here, favoring performance over memory */
int SpredisZsetMultiKeySort_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); //let redis free up our little memories

    if ( ((argc - 5) % 2) != 0 ) return RedisModule_WrongArity(ctx);
    int columnCount = (argc - 5) / 2;
    // printf("columnCount %i\n", columnCount);
    RedisModuleString *zkey = argv[1];
    RedisModuleString *dest = argv[2];
    long long start;
    long long count;
    RedisModule_StringToLongLong(argv[3],&start);
    RedisModule_StringToLongLong(argv[4],&count);
    // RedisModuleString *count = argv[4];


    RedisModuleCallReply *reply = RedisModule_Call(ctx,"zcard","s",zkey);
    if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_INTEGER) {
        RedisModuleKey *key = RedisModule_OpenKey(ctx,zkey,
            REDISMODULE_READ);
        unsigned int n = (unsigned int)RedisModule_CallReplyInteger(reply);
        if (n == 0) return REDISMODULE_OK;
        
        // RedisModuleString **arr = RedisModule_PoolAlloc(ctx, sizeof(RedisModuleString *) * n);
        SpredisColumnData *mcd = RedisModule_PoolAlloc(ctx, sizeof(SpredisColumnData));
        mcd->cols = RedisModule_PoolAlloc(ctx, sizeof(SpredisDMapCont *) * columnCount);
        mcd->orders = RedisModule_PoolAlloc(ctx, sizeof(int) * columnCount);
        mcd->colCount = columnCount;
        // SPREDIS_SORT_MEMORIES = RedisModule_Realloc(SPREDIS_SORT_MEMORIES, sizeof(RedisModuleKey *) * columnCount);
        for (int i = 0; i < columnCount; ++i) mcd->cols[i] = NULL;
        int cRes = SpredisFetchColumnsAndOrders(ctx, argv, mcd, 5);
        if (cRes == REDISMODULE_OK) {
            int res = SpredisMergeSortZset(ctx, key, n, mcd, start, count, dest);
            // int res = SpredisTreeSortZset(ctx, key, arr, columns, orders, columnCount, start, count, dest);
            RedisModule_CloseKey(key);
            return res;    
        } else {
            RedisModule_CloseKey(key);
            return cRes;
        }
    }
    
    return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    
}

int SpredisStoreLexRange_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */
    const char DELIM = ';';
    long long arraylen = 0;
    double newScore = 0;

    if (argc != 5) return RedisModule_WrongArity(ctx);
    RedisModuleKey *store = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_WRITE);

    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[2],
        REDISMODULE_READ|REDISMODULE_WRITE);

    int keyType = RedisModule_KeyType(key);
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx,arraylen);
        RedisModule_CloseKey(store);
        return REDISMODULE_OK;
    }
    if (keyType != REDISMODULE_KEYTYPE_ZSET) {
        RedisModule_CloseKey(key);
        RedisModule_CloseKey(store);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    int storeType = RedisModule_KeyType(store);
    if (storeType != REDISMODULE_KEYTYPE_EMPTY && storeType != REDISMODULE_KEYTYPE_ZSET) {
        RedisModule_CloseKey(key);
        RedisModule_CloseKey(store);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    if (RedisModule_ZsetFirstInLexRange(key,argv[3],argv[4]) != REDISMODULE_OK) {
        RedisModule_CloseKey(key);
        RedisModule_CloseKey(store);
        return RedisModule_ReplyWithError(ctx,"ERR invalid range");
    }
    
    while(!RedisModule_ZsetRangeEndReached(key)) {
        double score;
        RedisModuleString *ele = RedisModule_ZsetRangeCurrentElement(key,&score);
        size_t l;
        const char *theStr = RedisModule_StringPtrLen(ele, &l);
        const char *newEle = strrchr(theStr, DELIM);
        if (newEle) {
            size_t nl = strlen(newEle);
            if (nl > 1) {
                RedisModuleString *id = RedisModule_CreateString(ctx, newEle + 1, nl - 1);
                RedisModule_ZsetAdd(store, ++newScore, id, NULL);
                // RedisModule_FreeString(ctx,id); 
            }
            
        }
        
        // RedisModule_FreeString(ctx,ele);
        RedisModule_ZsetRangeNext(key);
        arraylen++;
    }
    RedisModule_ZsetRangeStop(key);
    RedisModule_ReplyWithLongLong(ctx,arraylen);
    RedisModule_CloseKey(key);
    RedisModule_CloseKey(store);
    return REDISMODULE_OK;
}

int SpredisStoreRangeByScore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    long long arraylen = 0;
    double newScore = 0;

    if (argc != 5) return RedisModule_WrongArity(ctx);
    RedisModuleKey *store = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_WRITE);

    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[2],
        REDISMODULE_READ|REDISMODULE_WRITE);
    int keyType = RedisModule_KeyType(key);
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx,arraylen);
        RedisModule_CloseKey(store);
        return REDISMODULE_OK;
    }
    if (keyType !=  REDISMODULE_KEYTYPE_ZSET) {
        RedisModule_CloseKey(key);
        RedisModule_CloseKey(store);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    
    int storeType = RedisModule_KeyType(store);
    if (storeType != REDISMODULE_KEYTYPE_EMPTY && storeType != REDISMODULE_KEYTYPE_ZSET) {
        RedisModule_CloseKey(key);
        RedisModule_CloseKey(store);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    double min;
    double max;
    RedisModule_StringToDouble(argv[3], &min);
    RedisModule_StringToDouble(argv[4], &max);
    if (RedisModule_ZsetFirstInScoreRange(key, min, max, 0, 0) != REDISMODULE_OK) {
        RedisModule_CloseKey(key);
        RedisModule_CloseKey(store);
        return RedisModule_ReplyWithError(ctx,"ERR invalid range");
    }

   
    while(!RedisModule_ZsetRangeEndReached(key)) {
        double score;
        RedisModuleString *ele = RedisModule_ZsetRangeCurrentElement(key,&score);
        RedisModule_ZsetAdd(store, ++newScore, ele, NULL);
        // RedisModule_FreeString(ctx,ele);
        RedisModule_ZsetRangeNext(key);
        arraylen++;
    }
    RedisModule_ZsetRangeStop(key);
    RedisModule_ReplyWithLongLong(ctx,arraylen);
    RedisModule_CloseKey(key);
    RedisModule_CloseKey(store);
    return REDISMODULE_OK;
}


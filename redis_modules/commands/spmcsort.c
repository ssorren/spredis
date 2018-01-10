#include <math.h>
#include "../spredis.h"
#include "../lib/thpool.h"
// #include "../types/spdoubletype.h"
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>

// #define QUEUE_COUNT  1024

static inline int SpredisSortDataCompareLT(SpredisSortData *a, SpredisSortData *b, SpredisColumnData *mcd) {

    int i = mcd->colCount;
    while (i) {
        i--;
        if (a->scores[i] < b->scores[i]) return (1 == mcd->orders[i]);
        if (a->scores[i] > b->scores[i]) return (0 == mcd->orders[i]);
    }
    return 0;
}

SPREDIS_SORT_INIT(SpredisSortData, SpredisColumnData, SpredisSortDataCompareLT)

// pthread_mutex_t lock;

static threadpool SPSortPoolFast;
static threadpool SPSortPoolSlow;
static threadpool SPSortPoolReallySlow;

void SpredisZsetMultiKeySortInit() {
	SPSortPoolFast = thpool_init(SP_FAST_TPOOL_SIZE);
	SPSortPoolSlow = thpool_init(SP_SLOW_TPOOL_SIZE);
	SPSortPoolReallySlow = thpool_init(SP_RLYSLOW_TPOOL_SIZE);
}

typedef struct _SPThreadedSortArg {
	RedisModuleBlockedClient *bc;
	unsigned int count;
	SpredisSortData **datas;
	SpredisColumnData *mcd;
    SpredisSetCont *set;
} SPThreadedSortArg;

static inline double _SpredisSortDMapValue(SpredisDMapCont *cont, unsigned long id) {
    return id < cont->size && cont->map[id].full ? cont->map[id].value : -HUGE_VAL; 
}

int SPThreadedSort_reply_func(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    return RedisModule_ReplyWithSimpleString(ctx,"OK");
}

int SPThreadedSort_timeout_func(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    return RedisModule_ReplyWithNull(ctx);
}

void SPThreadedSort_FreePriv(void *prv)
{	
    // return RedisModule_ReplyWithNull(ctx);

	SPThreadedSortArg *targ = prv;
    if (targ->mcd->colCount) {
    	RedisModule_Free(targ->mcd->cols);
    	RedisModule_Free(targ->mcd->orders);
    }
	RedisModule_Free(targ->mcd);
    RedisModule_Free(targ);
}

void SPThreadedSort(void *arg) {
	SPThreadedSortArg *targ = arg;
    size_t i = 0;
    khash_t(SIDS) *set = targ->set->set;
    SpredisColumnData *mcd = targ->mcd;
    SpredisSortData **datas = targ->datas;
    SpredisSortData *d;
    uint32_t id;
    int j = mcd->colCount;
    SpredisProtectReadMap(targ->set);
    while (j) {
        SpredisProtectReadMap(mcd->cols[--j]);
    }
    kh_foreach_key(set, id, {
        d = RedisModule_Alloc(sizeof(SpredisSortData));
        datas[i] = d;
        d->id = id;
        d->scores = RedisModule_Alloc(sizeof(double) * mcd->colCount);
        
        int k = mcd->colCount;
        while (k) {
            --k;
            d->scores[k] = _SpredisSortDMapValue(mcd->cols[k], d->id);
        }
        i++;
    });
    // printf("%s\n", "WTF3");
    if (mcd->colCount > 0) {
        SpredisSpredisSortDataSort(targ->count, datas, mcd);    
    }
	
    j = mcd->colCount;
    while (j) {
        SpredisUnProtectMap(mcd->cols[--j]);
    }
    SpredisUnProtectMap(targ->set);
	RedisModule_UnblockClient(targ->bc,targ);
}


int SpredisIntroSortSset(RedisModuleCtx *ctx, RedisModuleKey *zkey, SpredisSetCont *cont, SpredisColumnData *mcd, RedisModuleString *dest) {

	// printf("%s %d\n", "Calling sort...", cont == NULL);

    khash_t(SIDS) *set = cont->set;
    // printf("%s %d\n", "Calling sort2...", set == NULL);
    size_t count = kh_size(set);
    
    // if (RedisModule_ZsetFirstInScoreRange(zkey, REDISMODULE_NEGATIVE_INFINITE, REDISMODULE_POSITIVE_INFINITE, 0, 0) != REDISMODULE_OK) {
    //     return RedisModule_ReplyWithError(ctx,"ERR invalid range");
    // }
	
    SpredisTempResult *tempRes = SpredisTempResultCreate(ctx, dest, kh_size(set));//RedisModule_ModuleTypeGetValue(trKey);//SpredisTempResultCreate(ctx, dest, count);
    if (tempRes == NULL) {
    	return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    // SpredisSortData **datas = tempRes->data;
	SPThreadedSortArg *targ = RedisModule_Alloc(sizeof(SPThreadedSortArg));
	RedisModuleBlockedClient *bc =
        RedisModule_BlockClient(ctx, SPThreadedSort_reply_func, SPThreadedSort_timeout_func, SPThreadedSort_FreePriv ,0);

    targ->set = cont;
    targ->bc = bc;
	targ->count = count;
	targ->datas = tempRes->data;
	targ->mcd = mcd;

	threadpool pool = SPSortPoolFast;
	if (count > SP_SLOW_THRESHOLD) pool = (count > SP_REALLY_SLOW_THRESHOLD) ? SPSortPoolReallySlow : SPSortPoolSlow;

    SpredisProtectReadMap(cont);
    int j = mcd->colCount;
    while (j) {
        SpredisProtectReadMap(mcd->cols[--j]);
    }
    if (thpool_add_work(pool, (void*)SPThreadedSort, targ) != 0) {
    	printf("could not create thread!!, trying to run on main thread\n");
    	RedisModule_AbortBlock(bc);
    	SPThreadedSort((void *)targ);
    	RedisModule_ReplyWithLongLong(ctx,count);
        SPThreadedSort_FreePriv((void *)targ);
    }
    j = mcd->colCount;
    while (j) {
        SpredisUnProtectMap(mcd->cols[--j]);
    }
    SpredisUnProtectMap(cont);
    return REDISMODULE_OK;   
}



int SpredisFetchColumnsAndOrders(RedisModuleCtx *ctx, RedisModuleString **argv, SpredisColumnData *mcd, int argOffset) {
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

    if ( ((argc - 3) % 2) != 0 ) return RedisModule_WrongArity(ctx);
    int columnCount = (argc - 3) / 2;
    RedisModuleString *zkey = argv[1];
    RedisModuleString *dest = argv[2];


    int keyType;
    RedisModuleKey *key = RedisModule_OpenKey(ctx, zkey, REDISMODULE_READ);

    SpredisSetCont *set = RedisModule_ModuleTypeGetValue(key);
    
	if (HASH_NOT_EMPTY_AND_WRONGTYPE_CHECKONLY(key, &keyType, SPSETTYPE) == 0) {
		
		if (set == NULL || kh_size(set->set) == 0) {
			RedisModule_CloseKey(key);
            RedisModule_ReplyWithLongLong(ctx, 0);
			return REDISMODULE_OK;	
		}
        SpredisColumnData *mcd = RedisModule_Alloc(sizeof(SpredisColumnData));
        mcd->colCount = columnCount;
        if (columnCount > 0) {
	        mcd->cols = RedisModule_Alloc(sizeof(SpredisDMapCont *) * columnCount);
	        mcd->orders = RedisModule_Alloc(sizeof(int) * columnCount);
        }
        
        for (int i = 0; i < columnCount; ++i) mcd->cols[i] = NULL;
        int cRes = SpredisFetchColumnsAndOrders(ctx, argv, mcd, 3);
        if (cRes == REDISMODULE_OK) {
            int res = SpredisIntroSortSset(ctx, key, set, mcd, dest);
            RedisModule_CloseKey(key);
            return res;    
        } else {
            RedisModule_CloseKey(key);
            return cRes;
        }

    } else if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        // printf("%s\n", "Empty Key!");
        return RedisModule_ReplyWithLongLong(ctx, 0);
    }
    
    return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    
}

int SpredisStoreLexRange_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */
    const char DELIM = ';';
    // long long arraylen = 0;

    if (argc != 6) return RedisModule_WrongArity(ctx);
    

    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[2], REDISMODULE_READ|REDISMODULE_WRITE);

    int keyType = RedisModule_KeyType(key);
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx,0);
        return REDISMODULE_OK;
    }
    if (keyType != REDISMODULE_KEYTYPE_ZSET) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    RedisModuleKey *store = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_WRITE);

    int storeType = RedisModule_KeyType(store);
    if (HASH_NOT_EMPTY_AND_WRONGTYPE_CHECKONLY(store, &storeType ,SPSETTYPE) == 1) {
        RedisModule_CloseKey(key);
        RedisModule_CloseKey(store);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    if (RedisModule_ZsetFirstInLexRange(key,argv[4],argv[5]) != REDISMODULE_OK) {
        RedisModule_CloseKey(key);
        RedisModule_CloseKey(store);
        return RedisModule_ReplyWithError(ctx,"ERR invalid range");
    }
    
    RedisModuleString *hintName = argv[3];
    size_t len;
    RedisModule_StringPtrLen(hintName, &len);
    RedisModuleKey *hintKey = NULL;
    SpredisSetCont *hintCont = NULL;
    khash_t(SIDS) *hint = NULL;
    if (len) {
        hintKey = RedisModule_OpenKey(ctx,argv[3], REDISMODULE_READ);
        int hintType;
        if (HASH_NOT_EMPTY_AND_WRONGTYPE_CHECKONLY(hintKey, &hintType ,SPSETTYPE) == 1) {
            RedisModule_CloseKey(key);
            RedisModule_CloseKey(store);
            RedisModule_CloseKey(hintKey);
            return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        hintCont = RedisModule_ModuleTypeGetValue(hintKey);
        if (hintCont != NULL) hint = hintCont->set;
    }

    SpredisSetCont *resCont = _SpredisInitSet();
    khash_t(SIDS) *res = resCont->set;
    SpredisSetRedisKeyValueType(store, SPSETTYPE, resCont);
    double score;
    
    uint32_t id;
    int absent;
    if (hint == NULL) {
        while(!RedisModule_ZsetRangeEndReached(key)) {
            RedisModuleString *ele;
            const char *theStr, *newEle;
            ele = RedisModule_ZsetRangeCurrentElement(key,&score);
            theStr = RedisModule_StringPtrLen(ele, NULL);
            newEle = strrchr(theStr, DELIM);
            if (newEle) {
                size_t nl = strlen(newEle);
                if (nl > 1) {
                    
                    id = TOINTFROMCHAR(newEle + 1);//RedisModule_CreateString(ctx, newEle + 1, nl - 1);
                    kh_put(SIDS, res, id, &absent);
                    // RedisModule_ZsetAdd(store, ++newScore, id, NULL);
                }
                
            }
            RedisModule_ZsetRangeNext(key);
            // arraylen++;
        }
    } else {
        while(!RedisModule_ZsetRangeEndReached(key)) {
            RedisModuleString *ele;
            const char *theStr, *newEle;
            ele = RedisModule_ZsetRangeCurrentElement(key,&score);
            theStr = RedisModule_StringPtrLen(ele, NULL);
            newEle = strrchr(theStr, DELIM);
            if (newEle) {
                size_t nl = strlen(newEle);
                if (nl > 1) {
                    
                    id = TOINTFROMCHAR(newEle + 1);//RedisModule_CreateString(ctx, newEle + 1, nl - 1);
                    // printf("soring %u\n", id);
                    if ( kh_contains(SIDS, hint, id) ) {
                        kh_put(SIDS, res, id, &absent);
                    }
                    // RedisModule_ZsetAdd(store, ++newScore, id, NULL);
                }
                
            }
            RedisModule_ZsetRangeNext(key);
            // arraylen++;
        }
    }
    RedisModule_ZsetRangeStop(key);
    RedisModule_ReplyWithLongLong(ctx,kh_size(res));
    RedisModule_CloseKey(key);
    RedisModule_CloseKey(store);
    RedisModule_CloseKey(hintKey);
    return REDISMODULE_OK;
}

long long _SpredisZCard(RedisModuleCtx *ctx, RedisModuleString *zset) {
	RedisModuleCallReply *reply;
		reply = RedisModule_Call(ctx,"ZCARD","s",zset);
	if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_INTEGER) {
		long long res = RedisModule_CallReplyInteger(reply);
        RedisModule_FreeCallReply(reply);
        return res;
	}
	return 0;
}

int SpredisStoreRangeByScore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    // long long arraylen = 0;
    // double newScore = 0;

    if (argc != 6) return RedisModule_WrongArity(ctx);

    long long zcard = _SpredisZCard(ctx, argv[2]);
    if (zcard == 0) return RedisModule_ReplyWithLongLong(ctx, 0);

    RedisModuleKey *store = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_WRITE);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[2],
        REDISMODULE_READ|REDISMODULE_WRITE);
    int keyType = RedisModule_KeyType(key);
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx,0);
        RedisModule_CloseKey(store);
        return REDISMODULE_OK;
    }

    if (keyType !=  REDISMODULE_KEYTYPE_ZSET) {
        RedisModule_CloseKey(key);
        RedisModule_CloseKey(store);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    
    int storeType = RedisModule_KeyType(store);
    if (HASH_NOT_EMPTY_AND_WRONGTYPE_CHECKONLY(store, &storeType ,SPSETTYPE) == 1) {
        RedisModule_CloseKey(key);
        RedisModule_CloseKey(store);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    RedisModuleString *hintName = argv[3];
    size_t len;
    RedisModule_StringPtrLen(hintName, &len);
    RedisModuleKey *hintKey = NULL;
    SpredisSetCont *hintCont = NULL;
    khash_t(SIDS) *hint = NULL;


    if (len) {
    	hintKey = RedisModule_OpenKey(ctx,argv[3], REDISMODULE_READ);
    	int hintType;
    	if (HASH_NOT_EMPTY_AND_WRONGTYPE_CHECKONLY(hintKey, &hintType ,SPSETTYPE) == 1) {
    		RedisModule_CloseKey(key);
		    RedisModule_CloseKey(store);
		    RedisModule_CloseKey(hintKey);
		    return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    	}
    	hintCont = RedisModule_ModuleTypeGetValue(hintKey);
    	if (hintCont != NULL) hint = hintCont->set;
    }
    
    double min;
    double max;
    RedisModule_StringToDouble(argv[4], &min);
    RedisModule_StringToDouble(argv[5], &max);
    int shouldIntersore = 0;
    if (hint != NULL && (min == -HUGE_VAL || max == HUGE_VAL || kh_size(hint) < zcard / 2)) { //at some point it is not faster to intersore first
    	shouldIntersore = 1;
    } else {

	    if (RedisModule_ZsetFirstInScoreRange(key, min, max, 0, 0) != REDISMODULE_OK) {
	        RedisModule_CloseKey(key);
	        RedisModule_CloseKey(store);
	        RedisModule_CloseKey(hintKey);
	        return RedisModule_ReplyWithError(ctx,"ERR invalid range");
	    }
    }

    SpredisSetCont *resCont = _SpredisInitSet();
	SpredisSetRedisKeyValueType(store, SPSETTYPE, resCont);    
    khash_t(SIDS) *res = resCont->set;
    int absent;
    uint32_t id;
    if (shouldIntersore) {
    	double score;
    	int zres;
    	uint32_t id;
    	RedisModuleString *ele;
    	kh_foreach_key(hint, id, {
    		ele = RedisModule_CreateStringPrintf(ctx, "%"PRIx32, id);
    		zres = RedisModule_ZsetScore(key, ele, &score);
    		if (zres == REDISMODULE_OK && score >= min && score <= max) {
    			kh_put(SIDS, res, id, &absent);
    		}
    	});
    	// int RedisModule_ZsetScore(RedisModuleKey *key, RedisModuleString *ele, double *score)


    } else if (hint == NULL) { //more wordy, but also more efficient to do one if statement rather tht 1 per iteration
   		 while(!RedisModule_ZsetRangeEndReached(key)) {
	        double score;
	        RedisModuleString *ele = RedisModule_ZsetRangeCurrentElement(key,&score);
	        id = TOINTKEY(ele);
	        kh_put(SIDS, res, id, &absent);
	        RedisModule_ZsetRangeNext(key);
	        // arraylen++;
	    }		
   	} else {
   		// khint_t k;
   		while(!RedisModule_ZsetRangeEndReached(key)) {
	        double score;
	        RedisModuleString *ele = RedisModule_ZsetRangeCurrentElement(key,&score);
	        id = TOINTKEY(ele);
	        // k = kh_get(SIDS, hint, id);
	        if ( kh_contains(SIDS, hint, id) ) {
	        	kh_put(SIDS, res, id, &absent);	
	        }
	        RedisModule_ZsetRangeNext(key);
	        // arraylen++;
	    }
   	}
    RedisModule_ZsetRangeStop(key);
    RedisModule_ReplyWithLongLong(ctx,kh_size(res));
    RedisModule_CloseKey(key);
    RedisModule_CloseKey(store);
    RedisModule_CloseKey(hintKey);
    return REDISMODULE_OK;
}


#include <math.h>
#include "../spredis.h"
#include "../lib/thpool.h"
// #include "../types/spdoubletype.h"
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>


static inline int SpredisSortDataCompareLT(SpredisSortData *a, SpredisSortData *b, SpredisColumnData *mcd) {
    int i = mcd->colCount;
    while (i--) { //the columns were created in reverse order!
        if (a->scores[i] < b->scores[i]) return 1;
        if (a->scores[i] > b->scores[i]) return 0;
    }
    return a->id < b->id;
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

// static inline double _SpredisSortDMapValue(SpredisDMapCont *cont, size_t id) {
//     return id < cont->size && cont->map[id].full ? cont->map[id].value : -HUGE_VAL; 
// }

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
        // mcd->colFns[i]->protect(mcd->colPtrs[--j]);
        SpredisProtectReadMap(mcd->cols[--j]);
    }
    khint_t kk;
    khash_t(SCORE) *scoreMap;
    kh_foreach_key(set, id, {
        d = RedisModule_Calloc(1, sizeof(SpredisSortData));
        datas[i] = d;
        d->id = id;
        d->scores =  mcd->colCount ? RedisModule_Alloc(sizeof(double) * mcd->colCount) : NULL;
        
        int k = mcd->colCount;
        while (k) {
            --k;
            scoreMap = mcd->cols[k]->set;
            // d->scores[k] = mcd->colFns[k]->getScore(mcd->colPtrs[k], d->id);
            kk = kh_get(SCORE, scoreMap, d->id);
            d->scores[k] = ((kk == kh_end(scoreMap) ? -HUGE_VAL : kh_val(scoreMap, kk)->score)) * mcd->orders[k];
            // _SpredisSortDMapValue(mcd->cols[k], d->id);
        }
        i++;
    });
    // printf("%s\n", "WTF3");
    if (mcd->colCount > 0) {
        SpredisSpredisSortDataSort(targ->count, datas, mcd);    
    }
	
    j = mcd->colCount;
    while (j) {
        // mcd->colFns[i]->unprotect(mcd->colPtrs[--j]);
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
        
        int keyType, keyType2;
        if (!(HASH_EMPTY_OR_WRONGTYPE(key, &keyType, SPZSETTYPE) || HASH_EMPTY_OR_WRONGTYPE(key, &keyType2, SPZLSETTYPE))){
            RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
            return REDISMODULE_ERR;
        }
        mcd->cols[i] = RedisModule_ModuleTypeGetValue(key);
        RedisModule_CloseKey(key);
        const char * theOrder = RedisModule_StringPtrLen(order, NULL);
        mcd->orders[i] = SPREDIS_ORDER_INT(theOrder);
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
        SpredisColumnData *mcd = RedisModule_Calloc(1, sizeof(SpredisColumnData));
        mcd->colCount = columnCount;
        if (columnCount > 0) {
	        mcd->cols = RedisModule_Calloc(columnCount, sizeof(SPScoreCont *));
	        mcd->orders = RedisModule_Alloc(sizeof(int) * columnCount);
            // mcd->colPtrs = RedisModule_Calloc(columnCount, sizeof(void *));
            // mcd->colFns = RedisModule_Calloc(columnCount, sizeof(SPColFns *));
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







#include <math.h>
#include "../spredis.h"

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

// static threadpool SPSortPoolFast;
// static threadpool SPSortPoolSlow;
// static threadpool SPSortPoolReallySlow;

void SpredisZsetMultiKeySortInit() {
    // SPSortPoolFast = thpool_init(SP_FAST_TPOOL_SIZE);
    // SPSortPoolSlow = thpool_init(SP_SLOW_TPOOL_SIZE);
    // SPSortPoolReallySlow = thpool_init(SP_RLYSLOW_TPOOL_SIZE);
}

typedef struct _SPThreadedSortArg {
    // RedisModuleBlockedClient *bc;
    unsigned int count;
    SpredisSortData **datas;
    SpredisColumnData *mcd;
    SpredisSetCont *set;
} SPThreadedSortArg;

typedef struct {
    SpredisColumnData *mcd;
    SpredisSortData **datas;
    size_t start, end;
} SPPopScoreArg;
// static inline double _SpredisSortDMapValue(SpredisDMapCont *cont, size_t id) {
//     return id < cont->size && cont->map[id].full ? cont->map[id].value : -HUGE_VAL; 
// }

// int SPThreadedSort_reply_func(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
// {
//     return RedisModule_ReplyWithSimpleString(ctx,"OK");
// }

// int SPThreadedSort_timeout_func(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
// {
//     return RedisModule_ReplyWithNull(ctx);
// }

void SPThreadedSort_FreeArg(SPThreadedSortArg *targ)
{   
    // return RedisModule_ReplyWithNull(ctx);

    // SPThreadedSortArg *targ = prv;
    if (targ->mcd->colCount) {
        RedisModule_Free(targ->mcd->cols);
        RedisModule_Free(targ->mcd->sets);
        RedisModule_Free(targ->mcd->orders);
        RedisModule_Free(targ->mcd->inputs);
        RedisModule_Free(targ->mcd->resolve);
    }
    RedisModule_Free(targ->mcd);
    RedisModule_Free(targ);
}

void _SPResolveScore(SPPtrOrD_t value, void *input, double *score) {
    (*score) = value.asDouble; //((k[0] == kh_end(set) ? -HUGE_VAL : kh_val(set, k[0])->score)) * order[0];
}

void SPPopulateScores(void *arg) {
    SPPopScoreArg *targ = arg;
    SpredisColumnData *mcd = targ->mcd;
    SpredisSortData **datas = targ->datas;
    SpredisSortData *d;
    
    khint_t kk;
    int k;
    khash_t(SORTTRACK) *scoreMap;
    size_t end = targ->end;
    uint64_t id;

    // printf("Doing work for %zu - %zu\n", targ->start, targ->end);
    for (size_t i = targ->start; i < end; ++i)
    {
        d = datas[i];
        id = d->id;
        d->scores =  mcd->colCount ? RedisModule_Alloc(sizeof(double) * mcd->colCount) : NULL;
        k = mcd->colCount;
        while (k) {
            --k;
            scoreMap = mcd->sets[k];
            kk = kh_get(SORTTRACK, scoreMap, id);
            (kk == kh_end(scoreMap)) ? (d->scores[k] = -HUGE_VAL) : (mcd->resolve[k](kh_val(scoreMap, kk)->score, mcd->inputs[k], &d->scores[k]));
            d->scores[k] *= mcd->orders[k];
        }
    }    
}

void SPThreadedSort(SPThreadedSortArg *targ) {
    // SPThreadedSortArg *targ = arg;
    size_t i = 0;
    khash_t(SIDS) *set = targ->set->set;
    SpredisColumnData *mcd = targ->mcd;
    SpredisSortData **datas = targ->datas;
    SpredisSortData *d;
    uint64_t id;

    
    kh_foreach_key(set, id, {
        d = RedisModule_Calloc(1, sizeof(SpredisSortData));
        datas[i++] = d;
        d->id = id;
    });
    if (mcd->colCount > 0) {
        // printf("Locking B\n");
        SpredisSortReadLock();
        // printf("Locked B\n");
        if (i < SP_PTHRESHOLD) {
            SPPopScoreArg parg = {.mcd=mcd, .datas=datas, .start=0, .end=i};
            SPPopulateScores(&parg);
        } else {
            void (*func[SP_PQ_TCOUNT_SIZE])(void*);
            void *pargs[SP_PQ_TCOUNT_SIZE];
            size_t start = 0;
            size_t incr = i / SP_PQ_TCOUNT_SIZE;
            SPPopScoreArg *arg;
            for (int j = 0; j < SP_PQ_TCOUNT_SIZE; ++j)
            {
                func[j] = SPPopulateScores;
                arg = RedisModule_Alloc(sizeof(SPPopScoreArg));
                pargs[j] = arg;
                arg->mcd = mcd;
                arg->datas = datas;
                arg->start = start;
                if (j == (SP_PQ_TCOUNT_SIZE - 1)) {
                    arg->end = i;
                } else {
                    start += incr;
                    arg->end = start;
                }

            }
            SPDoWorkInParallel(func,pargs,SP_PQ_TCOUNT_SIZE);
            for (int j = 0; j < SP_PQ_TCOUNT_SIZE; ++j) {
                RedisModule_Free(pargs[j]);
            }
        }
        // printf("UnLocking B\n");
        SpredisSortUnLock();
        // printf("UnLocked B\n");

    // printf("%s\n", "WTF3");
    
        SpredisSpredisSortDataSort(targ->count, datas, mcd);    
    }
}


int SpredisIntroSortSset(RedisModuleCtx *ctx, RedisModuleKey *zkey, SpredisSetCont *cont, SpredisColumnData *mcd, RedisModuleString *dest) {


    khash_t(SIDS) *set = cont->set;
    size_t count = kh_size(set);
    
    SPLockContext(ctx);
    SpredisTempResult *tempRes = SpredisTempResultCreate(ctx, dest, kh_size(set));
    SPUnlockContext(ctx);

    if (tempRes == NULL) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    SPThreadedSortArg *targ = RedisModule_Alloc(sizeof(SPThreadedSortArg));
    targ->set = cont;
    targ->count = count;
    targ->datas = tempRes->data;
    targ->mcd = mcd;

    SpredisProtectReadMap(cont, "SpredisIntroSortSset");
    int j = mcd->colCount;
    while (j) {
        SpredisProtectReadMap(mcd->cols[--j], "SpredisIntroSortSset");
    }
    SPThreadedSort(targ);
    j = mcd->colCount;
    while (j) {
        SpredisUnProtectMap(mcd->cols[--j]);//, "SpredisIntroSortSset");
    }
    SpredisUnProtectMap(cont);//, "SpredisIntroSortSset");
    
    SPThreadedSort_FreeArg(targ);
    RedisModule_ReplyWithSimpleString(ctx,"OK");
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
        

        int keyType, spType;
        SP_GET_KEYTYPES(key, &keyType, &spType);
        if (keyType == REDISMODULE_KEYTYPE_EMPTY || (spType != SPZSETTYPE && spType != SPZLSETTYPE && spType != SPEXPRTYPE)) {
            RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
            return REDISMODULE_ERR;
        }
        const char * theOrder = RedisModule_StringPtrLen(order, NULL);
        mcd->orders[i] = SPREDIS_ORDER_INT(theOrder);

        if (spType == SPZSETTYPE || spType == SPZLSETTYPE) {
            mcd->cols[i] = RedisModule_ModuleTypeGetValue(key);
            mcd->sets[i] = mcd->cols[i]->st;
            
            mcd->inputs[i] = NULL;
            mcd->resolve[i] = _SPResolveScore;
        } else if (spType == SPEXPRTYPE) {
            mcd->cols[i] = RedisModule_ModuleTypeGetValue(key);
            mcd->sets[i] = ((SpExpResolverCont*)mcd->cols[i])->set;
            mcd->inputs[i] = ((SpExpResolverCont*)mcd->cols[i])->input;
            mcd->resolve[i] = ((SpExpResolverCont*)mcd->cols[i])->resolve;
        }
    }
    return REDISMODULE_OK;
}

/* going to use a tree sort here, favoring performance over memory */
int SpredisZsetMultiKeySort_RedisCommandT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if ( ((argc - 3) % 2) != 0 ) return RedisModule_WrongArity(ctx);
    SPLockContext(ctx);
    int columnCount = (argc - 3) / 2;
    RedisModuleString *zkey = argv[1];
    RedisModuleString *dest = argv[2];


    int keyType;
    RedisModuleKey *key = RedisModule_OpenKey(ctx, zkey, REDISMODULE_READ);

    SpredisSetCont *set = RedisModule_ModuleTypeGetValue(key);
    
    if (HASH_NOT_EMPTY_AND_WRONGTYPE_CHECKONLY(key, &keyType, SPSETTYPE) == 0) {
        
        if (set == NULL || kh_size(set->set) == 0) {
            // RedisModule_CloseKey(key);
            RedisModule_ReplyWithLongLong(ctx, 0);
            SPUnlockContext(ctx);
            return REDISMODULE_OK;  
        }
        SpredisColumnData *mcd = RedisModule_Calloc(1, sizeof(SpredisColumnData));

        mcd->colCount = columnCount;
        if (columnCount > 0) {
            mcd->cols = RedisModule_Calloc(columnCount, sizeof(SPScoreCont *));
            mcd->sets = RedisModule_Calloc(columnCount, sizeof(khash_t(SCORE) *));
            mcd->orders = RedisModule_Alloc(sizeof(int) * columnCount);
            mcd->inputs = RedisModule_Calloc(columnCount, sizeof(void *));
            mcd->resolve = RedisModule_Calloc(columnCount, sizeof(void *));
        }
        
        int cRes = SpredisFetchColumnsAndOrders(ctx, argv, mcd, 3);
        SPUnlockContext(ctx);
        if (cRes == REDISMODULE_OK) {
            int res = SpredisIntroSortSset(ctx, key, set, mcd, dest);
            return res;    
        } else {
            return cRes;
        }

    } else if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        SPUnlockContext(ctx);
        return RedisModule_ReplyWithLongLong(ctx, 0);
    }
    SPUnlockContext(ctx);
    return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    
}

int SpredisZsetMultiKeySort_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return SPThreadedWork(ctx, argv, argc, SpredisZsetMultiKeySort_RedisCommandT);
}

int SpredisExprResolve_RedisCommandT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // RedisModule_AutoMemory(ctx);
    // 'spredis.resolveexpr', resultSet, exprIndex, start, count
    if (argc != 5) return RedisModule_WrongArity(ctx);

    long long start;
    long long count;
    int startOk = RedisModule_StringToLongLong(argv[3],&start);
    int countOk = RedisModule_StringToLongLong(argv[4],&count);

    if (startOk == REDISMODULE_ERR || countOk == REDISMODULE_ERR) {    
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    SPLockContext(ctx);
    RedisModuleKey *resultKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    RedisModuleKey *exprKey = RedisModule_OpenKey(ctx, argv[2], REDISMODULE_READ);
    int keyType;
    if (HASH_EMPTY_OR_WRONGTYPE(resultKey, &keyType, SPTMPRESTYPE) != 0) {
        SPUnlockContext(ctx);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    if (HASH_EMPTY_OR_WRONGTYPE(exprKey, &keyType, SPEXPRTYPE) != 0) {
        SPUnlockContext(ctx);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    SpredisTempResult *res = RedisModule_ModuleTypeGetValue(resultKey);
    SpExpResolverCont *exp = RedisModule_ModuleTypeGetValue(exprKey);
    SpredisProtectReadMap(exp, "SpredisExprResolve_RedisCommandT");
    SPUnlockContext(ctx);

    khash_t(SORTTRACK) *set = exp->set;
    long long finalCount = 0;    
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    uint64_t id;
    double score;
    khint_t k;
    while(start < res->size && finalCount < count) {
        id = res->data[start]->id;
        k = kh_get(SORTTRACK, set, id);
        // RedisModule_ReplyWithArray(ctx, 2);
        // RedisModule_ReplyWithLongLong(ctx, 2);
        if (k == kh_end(set)) {
            // score = -HUGE_VAL;
            RedisModule_ReplyWithNull(ctx);
        } else {
            exp->resolve(kh_val(set, k)->score, exp->input, &score);
            RedisModule_ReplyWithDouble(ctx, score);
        }    
        finalCount++;
        start++;
    }
    SpredisUnProtectMap(exp);//, "SpredisExprResolve_RedisCommandT");
    RedisModule_ReplySetArrayLength(ctx, finalCount);
    return REDISMODULE_OK;
}

int SpredisExprResolve_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return SPThreadedWork(ctx, argv, argc, SpredisExprResolve_RedisCommandT);
}




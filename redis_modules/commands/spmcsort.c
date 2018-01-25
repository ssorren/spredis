#include <math.h>
#include "../spredis.h"
#include "../lib/thpool.h"
// #include "../types/spdoubletype.h"
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>

// #define QUEUE_COUNT  1024

#define IS_EXCLUSIVE(arg) ( memcmp(arg, "(", 1) ? 0 : 1 )
#define ARG_MINUS_INC_EXC(arg) ( !memcmp(arg, "(", 1) || !memcmp(arg, "[", 1) ? arg + 1 : arg )

static inline int SpredisLexGT(int a) {
    return a > 0;
}

static inline int SpredisLexGTE(int a) {
    return a == 0;
}

static inline int SpredisLexLT(int a) {
    return a < 0;
}

static inline int SpredisLexLTE(int a) {
    return a <= 0;
}



#define SP_LEXLTCMP(arg) (IS_EXCLUSIVE(arg) ? SpredisLexLT : SpredisLexLTE)
#define SP_LEXGTCMP(arg) (IS_EXCLUSIVE(arg) ? SpredisLexGT : SpredisLexGTE)


static inline int SpredisScoreGT(double a, double b) {
    return (a > b);
}

static inline int SpredisScoreGTE(double a, double b) {
    return (a >= b);
}

static inline int SpredisScoreLT(double a, double b) {
    // printf("scoreing lt...\n");
    return (a < b);
}

static inline int SpredisScoreLTE(double a, double b) {
    // printf("scoreing lte...\n");
    return (a <= b);
}


#define SP_SCRLTCMP(arg) (IS_EXCLUSIVE(arg) ? SpredisScoreLT : SpredisScoreLTE)
#define SP_SCRGTCMP(arg) (IS_EXCLUSIVE(arg) ? SpredisScoreGT : SpredisScoreGTE)

typedef kbtree_t(SCORE) kbtree_t(SPSCORECOM);
SPSCORE_BTREE_INIT(SPSCORECOM);

typedef kbtree_t(LEX) kbtree_t(SPLEXCOM);
SPLEX_BTREE_INIT(SPLEXCOM);

typedef khash_t(LEX) khash_t(SPLEXCOM);
KHASH_MAP_INIT_INT64(SPLEXCOM, SPScore*);


typedef khash_t(SCORE) khash_t(SPSCORECOM);

KHASH_MAP_INIT_INT64(SPSCORECOM, SPScore*);


typedef khash_t(SIDS) khash_t(SIDS_SORT);
KHASH_SET_INIT_INT64(SIDS_SORT);

static inline int SpredisSortDataCompareLT(SpredisSortData *a, SpredisSortData *b, SpredisColumnData *mcd) {
    int i = mcd->colCount;
    while (i--) {
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
    khash_t(SIDS_SORT) *set = targ->set->set;
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
    khash_t(SPSCORECOM) *scoreMap;
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
            kk = kh_get(SPSCORECOM, scoreMap, d->id);
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

    khash_t(SIDS_SORT) *set = cont->set;
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

int SpredisStoreLexRange_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */
    // const char DELIM = ';';
    // long long arraylen = 0;

    if (argc != 6) return RedisModule_WrongArity(ctx);
    

    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[2], REDISMODULE_READ);

    int keyType;
    
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType ,SPZLSETTYPE) == 1) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx,0);
        return REDISMODULE_OK;
    }

    RedisModuleKey *store = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_WRITE|REDISMODULE_READ);

    int storeType = RedisModule_KeyType(store);
    if (HASH_NOT_EMPTY_AND_WRONGTYPE_CHECKONLY(store, &storeType ,SPSETTYPE) == 1) {;
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    const unsigned char * gtCmp = (const unsigned char *)RedisModule_StringPtrLen(argv[4], NULL);
    const unsigned char * ltCmp = (const unsigned char *)RedisModule_StringPtrLen(argv[5], NULL);

    int (*GT)(int) = SP_LEXGTCMP(gtCmp);
    int (*LT)(int) = SP_LEXLTCMP(ltCmp);

    gtCmp = ARG_MINUS_INC_EXC(gtCmp);
    ltCmp = ARG_MINUS_INC_EXC(ltCmp);

    size_t gtLen = strlen((const char *)gtCmp);
    size_t ltLen = strlen((const char *)ltCmp);

    if ( ltLen > 0 && ( (unsigned char)(ltCmp[ ltLen - 1 ]) - 0xff) < 0) {
        ltLen += 1;
        gtLen += 1;
    }

    RedisModuleString *hintName = argv[3];
    size_t len;
    RedisModule_StringPtrLen(hintName, &len);
    RedisModuleKey *hintKey = NULL;
    SpredisSetCont *hintCont = NULL;
    khash_t(SIDS_SORT) *hint = NULL;
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
        if (hintCont != NULL) {
            hint = hintCont->set;
        }
    }

    SPScoreCont *testLexCont = RedisModule_ModuleTypeGetValue(key);
    kbtree_t(SPLEXCOM) *testLex = testLexCont->btree;

    SPScoreKey *l, *u;
    SPScore *cand;
    SpredisSetCont *resCont = _SpredisInitSet();
    khash_t(SIDS_SORT) *res = resCont->set;
    SpredisSetRedisKeyValueType(store, SPSETTYPE, resCont);
    int absent;
    kbitr_t itr;

    SPScoreKey t = {
        .id = 0,
        .score = (uint64_t)gtCmp
    };
    
    kb_intervalp(SPLEXCOM, testLex, &t, &l, &u);

    if (l == NULL) {
        RedisModule_ReplyWithLongLong(ctx,0);
        RedisModule_CloseKey(key);
        RedisModule_CloseKey(store);
        RedisModule_CloseKey(hintKey);
        return REDISMODULE_OK;
    }

    int shouldIntersore = 0;
    if (hint != NULL && ltLen < 3 && (kh_size(hint) < (kb_size(testLex) / 2))) { //at some point it is not faster to intersore first
        shouldIntersore = 1;
    }

    if (shouldIntersore) {
        uint32_t id;
        khint_t k;
        kh_foreach_key(hint, id, {
            k = kh_get(SPLEXCOM, testLexCont->set, id);
            if (k != kh_end(testLexCont->set)) {
                cand = kh_value(testLexCont->set, k);
                if (cand && cand->lex && GT(memcmp(gtCmp, cand->lex, gtLen )) && LT(memcmp(cand->lex,ltCmp , ltLen ))) {
                    kh_put(SIDS_SORT, res, id, &absent);
                }
            }
        });
    } else {
        int reached = 0;
        kb_itr_getp(SPLEXCOM, testLex, l, &itr); // get an iterator pointing to the first
        if (hint == NULL) { //wordy to do it this way bu way more efficient
            for (; kb_itr_valid(&itr); kb_itr_next(SPLEXCOM, testLex, &itr)) { // move on
                cand = (&kb_itr_key(SPScoreKey, &itr))->value;
                if (cand && cand->lex ) {
                    // printf("%s,%u,, %d, %d\n", cand->lex, cand->id, memcmp(gtCmp, cand->lex, gtLen ),  memcmp(cand->lex, ltCmp, ltLen ));
                    if (reached || GT(memcmp(gtCmp, cand->lex, gtLen ))) {
                        if (LT(memcmp(cand->lex,ltCmp , ltLen ))) {
                            reached = 1;
                            kh_put(SIDS_SORT, res, cand->id, &absent);
                        } else {
                            break;
                        }
                    }
                }
            }
        } else {
            for (; kb_itr_valid(&itr); kb_itr_next(SPLEXCOM, testLex, &itr)) { // move on
                cand = (&kb_itr_key(SPScoreKey, &itr))->value;
                if (cand && cand->lex ) {
                    if (reached || GT(memcmp(gtCmp, cand->lex, gtLen ))) {
                        if (LT(memcmp(cand->lex,ltCmp , ltLen ))) {
                            reached = 1;
                            if ( kh_contains(SIDS_SORT, hint, cand->id)) kh_put(SIDS_SORT, res, cand->id, &absent);
                        } else {
                            break;
                        }
                    }     
                }
            }
        }
    }
    // RedisModule_ZsetRangeStop(key);
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

    RedisModuleKey *store = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_WRITE);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[2],
        REDISMODULE_READ|REDISMODULE_WRITE);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType ,SPZSETTYPE) == 1) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx,0);
        return REDISMODULE_OK;
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
    khash_t(SIDS_SORT) *hint = NULL;

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

    SPScoreCont *testScoreCont = RedisModule_ModuleTypeGetValue(key);
    kbtree_t(SPSCORECOM) *testScore = testScoreCont->btree;

    const char * gtCmp = RedisModule_StringPtrLen(argv[4], NULL);
    const char * ltCmp = RedisModule_StringPtrLen(argv[5], NULL);

    int (*GT)(double,double) = SP_SCRGTCMP(gtCmp);
    int (*LT)(double,double) = SP_SCRLTCMP(ltCmp);

    gtCmp = ARG_MINUS_INC_EXC(gtCmp);
    ltCmp = ARG_MINUS_INC_EXC(ltCmp);

    double min;
    double max;
    RedisModule_StringToDouble(RedisModule_CreateString(ctx, gtCmp, strlen(gtCmp)), &min);
    RedisModule_StringToDouble(RedisModule_CreateString(ctx, ltCmp, strlen(ltCmp)), &max);
    SPScoreKey *l, *u;
    SPScore *cand;
    SPScoreKey t = {
        .id = INT32_MAX,
        .score = min
    };
    kb_intervalp(SPSCORECOM, testScore, &t, &l, &u);
    if (l == NULL) {
        RedisModule_ReplyWithLongLong(ctx,0);
        RedisModule_CloseKey(key);
        RedisModule_CloseKey(store);
        RedisModule_CloseKey(hintKey);
        return REDISMODULE_OK;
    }
    int shouldIntersore = 0;
    if (hint != NULL && (min == -HUGE_VAL || max == HUGE_VAL || kh_size(hint) < kb_size(testScore) / 5)) { //at some point it is not faster to intersore first
    	shouldIntersore = 1;
    }

    SpredisSetCont *resCont = _SpredisInitSet();
	SpredisSetRedisKeyValueType(store, SPSETTYPE, resCont);    
    khash_t(SIDS_SORT) *res = resCont->set;
    int absent;
    int reached = 0;
    uint32_t id;
    
    kbitr_t itr;

    if (shouldIntersore) {
        khint_t k;
        kh_foreach_key(hint, id, {
            k = kh_get(SPSCORECOM, testScoreCont->set, id);
            if (k != kh_end(testScoreCont->set)) {
                cand = kh_value(testScoreCont->set, k);
                if (cand && GT( cand->score, min ) && LT(cand->score, max)) {
                    kh_put(SIDS_SORT, res, id, &absent);
                }
            }
        });
    	// int RedisModule_ZsetScore(RedisModuleKey *key, RedisModuleString *ele, double *score)


    } else if (hint == NULL) { //more wordy, but also more efficient to do one if statement rather tht 1 per iteration
        kb_itr_getp(SPSCORECOM, testScore, l, &itr);
        for (; kb_itr_valid(&itr); kb_itr_next(SPSCORECOM, testScore, &itr)) { // move on
            cand = (&kb_itr_key(SPScoreKey, &itr))->value;
            if (cand) {
                if (reached || GT(cand->score, min)) {
                    if (LT(cand->score, max)) {
                        reached = 1;
                        kh_put(SIDS_SORT, res, cand->id, &absent);
                    } else {
                        break;
                    }
                }
            }
        }		
   	} else {
   		// khint_t k;
        kb_itr_getp(SPSCORECOM, testScore, l, &itr);
        for (; kb_itr_valid(&itr); kb_itr_next(SPSCORECOM, testScore, &itr)) { // move on
            cand = (&kb_itr_key(SPScoreKey, &itr))->value;
            if (cand) {
                if (reached || GT(cand->score, min)) {
                    if (LT(cand->score, max)) {
                        reached = 1;
                        if (kh_contains(SIDS_SORT, hint, cand->id)) kh_put(SIDS_SORT, res, cand->id, &absent);
                    } else {
                        break;
                    }
                }
            }
        }
   	}
    
    RedisModule_ReplyWithLongLong(ctx,kh_size(res));
    RedisModule_CloseKey(key);
    RedisModule_CloseKey(store);
    RedisModule_CloseKey(hintKey);
    return REDISMODULE_OK;
}


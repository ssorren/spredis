#include "../spredis.h"
#include "../lib/thpool.h"
// #include "../types/spdoubletype.h"
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>

// struct SpredisFacetMap {
// 	const char *key;
// 	size_t count;
// 	UT_hash_handle hh;
// };

// KHASH_MAP_INIT_STR(SPREDISFACET, long long) ;
// typedef khash_t(HASH) khash_t(FHASH);
// KHASH_MAP_INIT_INT64(FHASH, SPHashValue*);

typedef struct _SPFacetResult {
	const char *val;
	long long count;
	UT_hash_handle hh;
} SPFacetResult;


typedef struct _SPFacetData {
	// RedisModuleString *keyName;
	
	// SpredisSMapCont *col;
	SPHashCont *col;
	int type;
	uint64_t count;
	int order;
	// khash_t(SPREDISFACET) *valMap;
	SPFacetResult *valMap;
	SPFacetResult** results;
	long long replyCount;
} SPFacetData;



// static threadpool SPFacetPoolFast;
// static threadpool SPFacetPoolSlow;
// static threadpool SPFacetPoolReallySlow;

void SpredisFacetInit() {
	// SPFacetPoolFast = thpool_init(SP_FAST_TPOOL_SIZE);
	// SPFacetPoolSlow = thpool_init(SP_SLOW_TPOOL_SIZE);
	// SPFacetPoolReallySlow = thpool_init(SP_RLYSLOW_TPOOL_SIZE);
}

typedef struct _SPThreadedFacetArg {
	RedisModuleBlockedClient *bc;
	SPFacetData **facets;
	unsigned int facetCount;
	SpredisSortData **datas;
	size_t dataCount;
} SPThreadedFacetArg;

int SpedisPrepareFacetResult(SPFacetData** facets, int facetCount);
int SpedisBuildFacetResult(RedisModuleCtx *ctx, SPFacetData** facets, int facetCount);

void __SPCloseAllFacets(SPFacetData** facets, int keyCount) {
	for (int i = 0; i < keyCount; ++i)
    {
	    SPFacetData *facet = facets[i];
	    if (facet->results != NULL) {
	    	RedisModule_Free(facet->results);
	    }
	    if (facet->valMap != NULL) {
	    	SPFacetResult *current, *tmp;

	    	HASH_ITER(hh, facet->valMap, current, tmp) {
				HASH_DEL(facet->valMap,current);
				RedisModule_Free(current);
			};
	    }
	    RedisModule_Free(facet);
    }
    RedisModule_Free(facets);
}

int SPThreadedFacet_reply_func(RedisModuleCtx *ctx, RedisModuleString **argv,
               int argc)
{
	SPThreadedFacetArg *targ = RedisModule_GetBlockedClientPrivateData(ctx);
	SpedisBuildFacetResult(ctx, targ->facets, targ->facetCount);
    return REDISMODULE_OK;
}

int SPThreadedFacet_timeout_func(RedisModuleCtx *ctx, RedisModuleString **argv,
               int argc)
{
    return RedisModule_ReplyWithNull(ctx);
}

void SPThreadedFacet_FreePriv(void *prv)
{	
	SPThreadedFacetArg *targ = prv;
	__SPCloseAllFacets(targ->facets, targ->facetCount);
	RedisModule_Free(targ);
}

// typedef struct {
// 	SPFacetData **facets;
// 	SpredisSortData *d;
// 	SPFacetResult **valMaps;
// 	uint8_t facetCount;
// 	size_t start, end;
// } SPPopFacetArg;

void SPThreadedFacet(void *arg) {
	SPThreadedFacetArg *targ = arg;

	SpredisSortData **datas = targ->datas;
	size_t dSize = targ->dataCount;
	SPFacetData **facets = targ->facets;
	SpredisSortData *d;
	SPFacetData *facet;
	// char *av;
	SPHashValue *av;
	// khint_t x;
	
	for (int i = 0; i < targ->facetCount; ++i)
	{
		SpredisProtectReadMap(facets[i]->col);
	}
	int keyI;
	int facetCount = targ->facetCount;
	SPFacetResult *fm;
	khint_t k;
	char *sav;
	uint16_t pos = 0;
	SPPtrOrD_t value;
	// printf("here?\n");
	while(dSize)
	{
		keyI = facetCount;
		d = datas[--dSize];
		while(keyI) {
			facet = facets[--keyI];
			if (facet->type == SPHASHTYPE) {
				k = kh_get(HASH, facet->col->set, d->id);

				if (k == kh_end(facet->col->set) || !kh_exist(facet->col->set, k)) continue;
				av = kh_value(facet->col->set, k);//SpredisSMapValue(facet->col, d->id);

			    if (av != NULL && av->type == SPHashStringType) {
			    	// printf("found av\n");
			    	kv_foreach_hv_value(av, &value, &pos, {
			    		sav = (char*)value;
			    		HASH_FIND_STR(facet->valMap, sav, fm);
				    	if (fm) {
				    		fm->count++;
				    	} else {
				    		fm = (SPFacetResult*)RedisModule_Calloc(1, sizeof(SPFacetResult));
				    		fm->val = sav;
				    		fm->count = 1;
				    		HASH_ADD_KEYPTR( hh, facet->valMap, fm->val, strlen(fm->val), fm );
				    	}
			    	});
			    } else if (av != NULL && av->type == SPHashDoubleType) {

			    }
			}
		}
	}
	for (int i = 0; i < targ->facetCount; ++i)
	{
		SpredisUnProtectMap(facets[i]->col);
	}
	SpedisPrepareFacetResult(facets, targ->facetCount);
	RedisModule_UnblockClient(targ->bc, targ);
}




int SPFacetResultCompareLT(SPFacetResult *a, SPFacetResult *b, SPFacetData *facet) {
	// (((b) < (a)) - ((a) < (b)))
	if (a->count < b->count) return (facet->order == 1);
	if (a->count > b->count) return (facet->order == 0);
	int res = strcasecmp(a->val, b->val);
	if (res < 0) return (facet->order == 1);
	if (res > 0) return (facet->order == 0);
	return 0;
}

int SPFacetResultCompareNameLT(SPFacetResult *a, SPFacetResult *b, SPFacetData *facet) {
	// (((b) < (a)) - ((a) < (b)))
	int res = strcasecmp(a->val, b->val);
	if (res < 0) return (facet->order == 1);
	if (res > 0) return (facet->order == 0);
	if (a->count < b->count) return (facet->order == 1);
	if (a->count > b->count) return (facet->order == 0);
	return 0;
}

SPREDIS_SORT_INIT(SPFacetResult, SPFacetData , SPFacetResultCompareLT);

typedef SPFacetResult SPFacetResultByName;

SPREDIS_SORT_INIT(SPFacetResultByName, SPFacetData , SPFacetResultCompareNameLT);

int SpedisPrepareFacetResult(SPFacetData** facets, int facetCount) {
	int counted, max;
	SPFacetData *facet;
	// SPFacetResult *fr;
	// const char *k;
	// long long v;
	SPFacetResult *current, *tmp;
	for (int i = 0; i < facetCount; ++i)
	{
		facet = facets[i];
		facet->count = HASH_CNT(hh, facet->valMap); //(int)kh_size(facet->valMap);
		max = facet->replyCount ? facet->replyCount : facet->count;
		facet->results = RedisModule_Alloc(sizeof(SPFacetResult*) * facet->count);
		counted = 0;
		HASH_ITER(hh, facet->valMap, current, tmp) {
	        facet->results[counted] = current;
	        ++counted;
		}
	    max = (max > facet->count) ? facet->count : max;
	    facet->replyCount = max;
	    SpredisSPFacetResultSort(facet->count, facet->results, facet);
	    // SpredisSPFacetResultByNameSort(facet->count, facet->results, facet);
	}
	return REDISMODULE_OK;
}

int SpedisBuildFacetResult(RedisModuleCtx *ctx, SPFacetData** facets, int facetCount) {
	RedisModule_ReplyWithArray(ctx, facetCount);
	SPFacetData *facet;
	SPFacetResult *fr;

	for (int i = 0; i < facetCount; ++i)
	{
		facet = facets[i];
	    RedisModule_ReplyWithArray(ctx, facet->replyCount * 2);
	    for (int k = 0; k < facet->replyCount; ++k)
	    {
	    	fr = facet->results[k];
	    	RedisModule_ReplyWithStringBuffer(ctx, fr->val, strlen(fr->val));
	    	RedisModule_ReplyWithLongLong(ctx, fr->count);
	    }
	}
	return REDISMODULE_OK;
}

int SpredisFacets_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);

	if (argc < 5) return RedisModule_WrongArity(ctx);
    int argOffset = 2;
    if ( ((argc - argOffset) % 3) != 0 ) return RedisModule_WrongArity(ctx);
    int keyCount = (argc - argOffset) / 3;
	RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ);
    int keyType;// = RedisModule_KeyType(key);

    if (HASH_EMPTY_OR_WRONGTYPE(key, &keyType, SPTMPRESTYPE)) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    SpredisTempResult *res = RedisModule_ModuleTypeGetValue(key);
    // RedisModule_CloseKey(key);
    int argI = argOffset;
    /** populate **/
    int ok = REDISMODULE_OK;
    SPFacetData** facets = RedisModule_Alloc(sizeof(SPFacetData) * keyCount);
    /* we are doing this in stages so we can safely free memory if conditions are not satisfied*/
    int i;
    //1: get all the keys
    RedisModuleKey **keys = RedisModule_PoolAlloc(ctx, sizeof(RedisModuleKey*) *keyCount);
    for (i = 0; i < keyCount; ++i) {
    	keys[i] = RedisModule_OpenKey(ctx,argv[argI++], REDISMODULE_READ);
    	argI+=2;
    }
	//2: allocate all the facets
	SPFacetData *facet;
	for (i = 0; i < keyCount; ++i) {
	    facets[i] = RedisModule_Calloc(1, sizeof(SPFacetData));//facet;
	}
	//3: get the base information for each facet
	argI = argOffset;
    for (i = 0; i < keyCount; ++i)
    {
	    facet = facets[i];
	    argI++;
	    ok = RedisModule_StringToLongLong(argv[argI++], &(facet->replyCount));
	    if (ok == REDISMODULE_ERR) {
	    	RedisModule_ReplyWithError(ctx,"ERR Could not parse count");
	    	break;	
	    }
	    facet->order = SPREDIS_ORDER(RedisModule_StringPtrLen(argv[argI++], NULL));
	    int keyType;
	    if (HASH_EMPTY_OR_WRONGTYPE(keys[i], &keyType, SPHASHTYPE) != 0) {
	    	ok = REDISMODULE_ERR;
	        RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
	        break;
	    }
	    facet->col = RedisModule_ModuleTypeGetValue(keys[i]);
	    facet->type = SPHASHTYPE;
    }
    //   we won't need to keep them open on the facet thread as they are thread read safe
    /** end populalate **/
    if (ok == REDISMODULE_OK) {
    	/** let's build these on another thread and free up redis to serve more requests **/
    	SPThreadedFacetArg *targ = RedisModule_Alloc(sizeof(SPThreadedFacetArg));
    	RedisModuleBlockedClient *bc =
	        RedisModule_BlockClient(ctx, SPThreadedFacet_reply_func, SPThreadedFacet_timeout_func, SPThreadedFacet_FreePriv ,0);

	    
	    targ->bc = bc;
		targ->facets = facets;
		targ->facetCount = keyCount;
		targ->datas = res->data;
		targ->dataCount = res->size;

	    SP_TWORK(SPThreadedFacet, targ, {
	        printf("could not create thread..No soup for you!\n");
	    	RedisModule_AbortBlock(bc);
	    	__SPCloseAllFacets(facets, keyCount);
	    	RedisModule_ReplyWithNull(ctx);
	    });
  
    } else {
    	__SPCloseAllFacets(facets, keyCount);
    }
    
    
    
    // printf("Hydrating facets took %lldms\n", RedisModule_Milliseconds() - startTimer);
	return REDISMODULE_OK;
}


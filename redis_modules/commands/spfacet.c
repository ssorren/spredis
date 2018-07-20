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
	SPPtrOrD_t val;
	// double dkey;
	SPHashValueType valType;
	long long count;
	UT_hash_handle hh;
} SPFacetResult;


typedef struct _SPFacetData {
	// RedisModuleString *keyName;
	
	// SpredisSMapCont *col;
	// SPHashCont *col;
	int fieldIndex;
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

// typedef struct _SPThreadedFacetArg {
// 	RedisModuleBlockedClient *bc;
// 	SPFacetData **facets;
// 	unsigned int facetCount;
// 	SpredisSortData *datas;
// 	size_t dataCount;
// } SPThreadedFacetArg;

int SpedisPrepareFacetResult(SPFacetData* facets, int facetCount);
int SpedisBuildFacetResult(RedisModuleCtx *ctx, SPFacetData*facets, int facetCount);

void __SPCloseAllFacets(SPFacetData* facets, int keyCount) {
	for (int i = 0; i < keyCount; ++i)
    {
	    SPFacetData *facet = &facets[i];
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
	    // RedisModule_Free(facet);
    }
    RedisModule_Free(facets);
}

// int SPThreadedFacet_reply_func(RedisModuleCtx *ctx, RedisModuleString **argv,
//                int argc)
// {
// 	SPThreadedFacetArg *targ = RedisModule_GetBlockedClientPrivateData(ctx);
// 	SpedisBuildFacetResult(ctx, targ->facets, targ->facetCount);
//     return REDISMODULE_OK;
// }

// int SPThreadedFacet_timeout_func(RedisModuleCtx *ctx, RedisModuleString **argv,
//                int argc)
// {
//     return RedisModule_ReplyWithNull(ctx);
// }

// void SPThreadedFacet_FreePriv(void *prv)
// {	
// 	SPThreadedFacetArg *targ = prv;
// 	__SPCloseAllFacets(targ->facets, targ->facetCount);
// 	RedisModule_Free(targ);
// }

// typedef struct {
// 	SPFacetData **facets;
// 	SpredisSortData *d;
// 	SPFacetResult **valMaps;
// 	uint8_t facetCount;
// 	size_t start, end;
// } SPPopFacetArg;






int SPFacetResultCompareLT(SPFacetResult *a, SPFacetResult *b, SPFacetData *facet) {
	// (((b) < (a)) - ((a) < (b)))
	if (a->count < b->count) return (facet->order == 1);
	if (a->count > b->count) return (facet->order == 0);
	if (a->valType == SPHashStringType) {
		int res = strcasecmp(a->val.asChar, b->val.asChar);
		if (res < 0) return (facet->order == 1);
		if (res > 0) return (facet->order == 0);
	} else {
		if (a->val.asDouble < b->val.asDouble) return (facet->order == 1);
		if (a->val.asDouble > b->val.asDouble) return (facet->order == 0);
	}
	return 0;
}

int SPFacetResultCompareNameLT(SPFacetResult *a, SPFacetResult *b, SPFacetData *facet) {
	// (((b) < (a)) - ((a) < (b)))
	int res = strcasecmp(a->val.asChar, b->val.asChar);
	if (res < 0) return (facet->order == 1);
	if (res > 0) return (facet->order == 0);
	if (a->count < b->count) return (facet->order == 1);
	if (a->count > b->count) return (facet->order == 0);
	return 0;
}

SPREDIS_SORT_INIT(SPFacetResult, SPFacetData , SPFacetResultCompareLT);

typedef SPFacetResult SPFacetResultByName;

SPREDIS_SORT_INIT(SPFacetResultByName, SPFacetData , SPFacetResultCompareNameLT);

int SpedisPrepareFacetResult(SPFacetData *facets, int facetCount) {
	int counted, max;
	SPFacetData *facet;
	// SPFacetResult *fr;
	// const char *k;
	// long long v;
	SPFacetResult *current, *tmp;
	for (int i = 0; i < facetCount; ++i)
	{
		facet = &facets[i];
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

int SpedisBuildFacetResult(RedisModuleCtx *ctx, SPFacetData *facets, int facetCount) {
	RedisModule_ReplyWithArray(ctx, facetCount);
	SPFacetData *facet;
	SPFacetResult *fr;

	for (int i = 0; i < facetCount; ++i)
	{
		facet = &facets[i];
	    RedisModule_ReplyWithArray(ctx, facet->replyCount * 2);
	    for (int k = 0; k < facet->replyCount; ++k)
	    {
	    	fr = facet->results[k];
	    	if (fr->valType == SPHashStringType) {
	    		RedisModule_ReplyWithStringBuffer(ctx, fr->val.asChar, strlen(fr->val.asChar));
		    	RedisModule_ReplyWithLongLong(ctx, fr->count);	
	    	} else {
	    		RedisModule_ReplyWithDouble(ctx, fr->val.asDouble);
		    	RedisModule_ReplyWithLongLong(ctx, fr->count);	
	    	}
	    	
	    	
	    }
	}
	return REDISMODULE_OK;
}


void SPThreadedFacet(SPFacetData *facets, int facetCount, SPCursor *cursor) {
	SPItem *items = cursor->items, *item;
	size_t dSize = cursor->count;
	SPFacetData *facet;
	int keyI;
	SPFacetResult *fm = NULL;
	SPPtrOrD_t sav;
	// char *sav;
	SPFieldData *data;

	while(dSize)
	{
		keyI = facetCount;
		item = &items[--dSize];
		if (!item->record->exists || item->record->fields == NULL) continue;
		while(keyI) {
			facet = &facets[--keyI];
			if (facet->fieldIndex >= 0) {
				data = &item->record->fields[facet->fieldIndex];
				if (facet->type == SPLexPart) {
					if (data->av == NULL || data->alen == 0) continue;
					for (int i = 0; i < data->alen; ++i)
					{
						sav = data->av[i];
						fm = NULL;
						HASH_FIND_STR(facet->valMap, sav.asChar, fm);
				    	if (fm) {
				    		fm->count++;
				    	} else {
				    		fm = (SPFacetResult*)RedisModule_Calloc(1, sizeof(SPFacetResult));
				    		fm->valType = SPHashStringType;
				    		fm->val = sav;
				    		fm->count = 1;
				    		HASH_ADD_KEYPTR( hh, facet->valMap, fm->val.asChar, strlen(fm->val.asChar), fm );
				    	}
					}

				} else if (facet->type == SPDoublePart) {
					if (data->iv == NULL || data->ilen == 0) continue;
					for (int i = 0; i < data->ilen; ++i)
					{
						sav = data->iv[i];
						fm = NULL;
						HASH_FIND(hh, facet->valMap, &sav, sizeof(SPPtrOrD_t), fm);
				    	if (fm) {
				    		fm->count++;
				    	} else {
				    		fm = (SPFacetResult*)RedisModule_Calloc(1, sizeof(SPFacetResult));
				    		fm->valType = SPHashDoubleType;
				    		fm->val = sav;
				    		fm->count = 1;
				    		HASH_ADD(hh, facet->valMap, val, sizeof(SPPtrOrD_t), fm);
				    	}
					}

				} else {
					continue;
				}
			}
		}
	}
	
	
}

int SpredisFacets_RedisCommandT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

	RedisModuleKey *key;
    SPNamespace *ns = NULL;
    SPLockContext(ctx);
    int keyOk = SPGetNamespaceKeyValue(ctx, &key, &ns, argv[1], REDISMODULE_READ);
    SPUnlockContext(ctx);
    if (keyOk != REDISMODULE_OK) {
        return keyOk;
    }
    if (ns == NULL) {
        // return RedisModule_ReplyWithLongLong(ctx, 0);
        return RedisModule_ReplyWithNull(ctx);
    }

	SPCursor *cursor = SPGetCursor(argv[2]);
	if (cursor == NULL) {
		return RedisModule_ReplyWithNull(ctx);
	}

	SPReadLock(ns->lock);
	SPReadLock(ns->rs->lock);
	// SPReadLock(ns->rs->deleteLock);



    int argOffset = 3;
    if ( ((argc - argOffset) % 3) != 0 ) return RedisModule_WrongArity(ctx);
    int facetCount = (argc - argOffset) / 3;
    
    
    // int ok = REDISMODULE_OK;

    SPFacetData *facets = RedisModule_Calloc(facetCount, sizeof(SPFacetData));
    SPFacetData *facet;
	int argI = argOffset;
	SPFieldDef *fd;

	/** populate **/
	// printf("0\n");
    for (int i = 0; i < facetCount; ++i)
    {
	    facet = &facets[i];
	    fd = SPFieldDefForName(ns, argv[argI]);
	    facet->fieldIndex = SPFieldIndex(ns, RedisModule_StringPtrLen(argv[argI++], NULL)) ;
	    RedisModule_StringToLongLong(argv[argI++], &(facet->replyCount));
	    facet->order = SPREDIS_ORDER(RedisModule_StringPtrLen(argv[argI++], NULL));
	    facet->type = fd->fieldType;
    }
    // printf("A\n");
    //get the facet counts
    SPThreadedFacet(facets, facetCount, cursor);
    // printf("B\n");
    // SPReadUnlock(ns->rs->deleteLock);
    SPReadUnlock(ns->rs->lock);
    SPReadUnlock(ns->lock);
    // printf("C\n");
	SpedisPrepareFacetResult(facets, facetCount);
	// printf("D\n");
	SpedisBuildFacetResult(ctx, facets, facetCount);
	// printf("E\n");
	__SPCloseAllFacets(facets, facetCount);
	// printf("F\n");
    // printf("Hydrating facets took %lldms\n", RedisModule_Milliseconds() - startTimer);
	return REDISMODULE_OK;
}

int SpredisFacets_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	if (argc < 6) return RedisModule_WrongArity(ctx);
	if ( ((argc - 3) % 3) != 0 ) return RedisModule_WrongArity(ctx);
	return SPThreadedWork(ctx, argv, argc, SpredisFacets_RedisCommandT);
	// SpredisFacets_RedisCommand(ctx, argv, argc);
}


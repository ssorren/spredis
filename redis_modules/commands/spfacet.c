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

KHASH_DECLARE(FACET, const char *, uint32_t)
KHASH_MAP_INIT_STR(FACET, uint32_t) ;
// typedef khash_t(HASH) khash_t(FHASH);
// KHASH_MAP_INIT_INT64(FHASH, SPHashValue*);

typedef struct _SPFacetResult {
	SPPtrOrD_t val;
	// double dkey;
	SPHashValueType valType;
	long long count;
	UT_hash_handle hh;
	// khash_t(FACET) *hash;
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
	    	// kh_destroy(facet->valMap->hash);
	    	HASH_ITER(hh, facet->valMap, current, tmp) {
				HASH_DEL(facet->valMap,current);
				RedisModule_Free(current);
			};
	    }
	    // RedisModule_Free(facet);
    }
    RedisModule_Free(facets);
}


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
	    SpredisSPFacetResultSort(counted, facet->results, facet);
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

// opportunity for parallel processing here, but need to figure out how to do a merge of the results first
typedef struct _SPThreadedFacetArg {
	SPFacetData *facets;
	int facetCount;
	SPCursor *cursor;
	size_t start;
	size_t end;
} SPThreadedFacetArg;

void SPThreadedFacet(void *varg) {

	SPThreadedFacetArg *arg = varg;
	SPFacetData *facets = arg->facets;
	int facetCount = arg->facetCount;
	SPCursor *cursor = arg->cursor;
	SPItem *items = cursor->items, *item;
	size_t dSize = arg->end;
	size_t start = arg->start;

	// printf("Start: %zu, End:%zu\n", start, dSize);
	SPFacetData *facet;
	int keyI;
	SPFacetResult *fm = NULL;
	SPPtrOrD_t sav;
	// char *sav;
	SPFieldData *data;

	while(dSize > start)
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

void SPMergeFacets(SPFacetData *dst, SPFacetData *src, int facetCount) {
	SPFacetResult *targ, *current, *tmp, *fm = NULL;

	for (int i = 0; i < facetCount; ++i)
	{
		targ = dst[i].valMap;

		HASH_ITER(hh, src[i].valMap, current, tmp) {
			fm = NULL;
			if (dst[i].type == SPLexPart) {
				HASH_FIND_STR(targ, current->val.asChar, fm);
		    	if (fm) {
		    		fm->count += current->count;
		    	} else {
		    		fm = (SPFacetResult*)RedisModule_Calloc(1, sizeof(SPFacetResult));
		    		fm->valType = SPHashStringType;
		    		fm->val = current->val;
		    		fm->count = current->count;
		    		HASH_ADD_KEYPTR( hh, targ, fm->val.asChar, strlen(fm->val.asChar), fm );
		    	}
			} else if (dst[i].type == SPDoublePart) {

				// HASH_FIND_STR(targ, current->val.asChar, fm);
				HASH_FIND(hh, targ, &current->val, sizeof(SPPtrOrD_t), fm);
		    	if (fm) {
		    		fm->count += current->count;
		    	} else {
		    		fm = (SPFacetResult*)RedisModule_Calloc(1, sizeof(SPFacetResult));
		    		fm->valType = SPHashDoubleType;
		    		fm->val = current->val;
		    		fm->count = current->count;
		    		HASH_ADD(hh, targ, val, sizeof(SPPtrOrD_t), fm);
		    		// HASH_ADD_KEYPTR( hh, targ, fm->val.asChar, strlen(fm->val.asChar), fm );
		    	}
			} 
			// HASH_DEL(facet->valMap,current);
			// RedisModule_Free(current);
		};
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

    int argOffset = 3;
    if ( ((argc - argOffset) % 3) != 0 ) return RedisModule_WrongArity(ctx);
    int facetCount = (argc - argOffset) / 3;
    
    

    SPFacetData *facets = RedisModule_Calloc(facetCount, sizeof(SPFacetData));
    SPFacetData *facet;
	int argI = argOffset;
	SPFieldDef *fd;

	/** populate **/
    for (int i = 0; i < facetCount; ++i)
    {
	    facet = &facets[i];
	    fd = SPFieldDefForName(ns, argv[argI]);
	    facet->fieldIndex = SPFieldIndex(ns, RedisModule_StringPtrLen(argv[argI++], NULL)) ;
	    RedisModule_StringToLongLong(argv[argI++], &(facet->replyCount));
	    facet->order = SPREDIS_ORDER(RedisModule_StringPtrLen(argv[argI++], NULL));
	    facet->type = fd->fieldType;
    }

    SPReadLock(ns->rs->deleteLock);

    if (cursor->count < 1024) {
	    SPThreadedFacetArg arg = {	.facets =facets,
									.facetCount = facetCount,
									.cursor = cursor,
									.start = 0,
									.end = cursor->count};

	    SPThreadedFacet(&arg);

	    SpedisPrepareFacetResult(facets, facetCount);
		SpedisBuildFacetResult(ctx, facets, facetCount);
		__SPCloseAllFacets(facets, facetCount);

    } else { //parallel mode
    	size_t start = 0;
        size_t incr = cursor->count / SP_PQ_TCOUNT_SIZE;
    	SPThreadedFacetArg *pargs[SP_PQ_TCOUNT_SIZE];
    	void (*func[SP_PQ_TCOUNT_SIZE])(void*);
    	for (int i = 0; i < SP_PQ_TCOUNT_SIZE; ++i)
    	{
    		func[i] = SPThreadedFacet;
    		SPThreadedFacetArg *arg = RedisModule_Calloc(1, sizeof(SPThreadedFacetArg));
    		pargs[i] = arg;

    		arg->start = start;
            arg->cursor = cursor;
            arg->facetCount = facetCount;
            if (i == 0) {
            	//this is our base, all other results will be merged to these facets
            	arg->facets = facets;
            } else {
            	//need to duplicate facets
            	SPFacetData *tfacets = RedisModule_Calloc(facetCount, sizeof(SPFacetData));
            	arg->facets = tfacets;
            	for (int k = 0; k < facetCount; ++k)
            	{
            		facet = &tfacets[k];
				    facet->fieldIndex = facets[k].fieldIndex;
				    facet->replyCount = facets[k].replyCount;
				    facet->order = facets[k].order;
				    facet->type =  facets[k].type;
				    facet->valMap = NULL;
            	}
            }

    		if (i == (SP_PQ_TCOUNT_SIZE - 1)) {
                arg->end = cursor->count;
            } else {
                start += incr;
                arg->end = start;
            }
    	}

    	SPDoWorkInParallel(func,(void **)pargs,SP_PQ_TCOUNT_SIZE);

    	//merge the facets, pargs[0] contains the destination, so start from 1
    	for (int i = 1; i < SP_PQ_TCOUNT_SIZE; ++i) {
    		SPMergeFacets(facets, pargs[i]->facets, facetCount);
    	}

    	SpedisPrepareFacetResult(facets, facetCount);
		SpedisBuildFacetResult(ctx, facets, facetCount);
	
    	for (int i = 0; i < SP_PQ_TCOUNT_SIZE; ++i) {
    		__SPCloseAllFacets(pargs[i]->facets, facetCount);
    	}

    	for (int i = 0; i < SP_PQ_TCOUNT_SIZE; ++i) {
    		RedisModule_Free(pargs[i]);
    	}
    }
    SPReadUnlock(ns->rs->deleteLock);
    SPReadUnlock(ns->lock);
    
	
	
	return REDISMODULE_OK;
}

int SpredisFacets_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	if (argc < 6) return RedisModule_WrongArity(ctx);
	if ( ((argc - 3) % 3) != 0 ) return RedisModule_WrongArity(ctx);
	return SPThreadedWork(ctx, argv, argc, SpredisFacets_RedisCommandT);
	// SpredisFacets_RedisCommand(ctx, argv, argc);
}


#include "../spredis.h"

typedef struct {
	SPHashCont *field;
	const char *label;
	double min;
	double max;
	long long minExcl;
	long long maxExcl;
	size_t count;

} SPFacetRange;
#define SP_ISBETWEEN(val, min, max, minExcl, maxExcl)  ( (minExcl ? (val > min) : (val >= min)) && (maxExcl ? (val < max) : (val <= max))  )

int SpredisFacetRange_RedisCommandT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    SPLockContext(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ);
    int keyType;
    if (HASH_EMPTY_OR_WRONGTYPE(key, &keyType, SPTMPRESTYPE)) {
    	SPUnlockContext(ctx);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    SpredisTempResult *res = RedisModule_ModuleTypeGetValue(key);
    SPUnlockContext(ctx);
    int facetCount = (argc - 2) / 6;
    int argIndex = 2;
    
    SPFacetRange *facets = RedisModule_Calloc(facetCount, sizeof(SPFacetRange));
    SPFacetRange *facet;
    
    RedisModuleKey *fkey;
    int rangeRes1, rangeRes2;
    for (int i = 0; i < facetCount; ++i)
    {
    	facet = &facets[i];
    	SPLockContext(ctx);

    	fkey = RedisModule_OpenKey(ctx, argv[argIndex++], REDISMODULE_READ);
    	rangeRes1 = SpredisStringToDouble(argv[argIndex++], &(facet->min));
    	rangeRes2 = SpredisStringToDouble(argv[argIndex++], &(facet->max));
		RedisModule_StringToLongLong(argv[argIndex++], &(facet->minExcl));
		RedisModule_StringToLongLong(argv[argIndex++], &(facet->maxExcl));
    	facet->label = RedisModule_StringPtrLen(argv[argIndex++], NULL);
    	if (rangeRes1 == REDISMODULE_OK && rangeRes2 == REDISMODULE_OK ) {
    		if (HASH_EMPTY_OR_WRONGTYPE(fkey, &keyType, SPHASHTYPE)) {
    			facet->field = NULL;
		    } else {
		    	facet->field = RedisModule_ModuleTypeGetValue(fkey);
		    	SPUnlockContext(ctx);
		    	SpredisProtectReadMap(facet->field, "SpredisFacetRange_RedisCommandT");
		    }
    	} else {
    		SPUnlockContext(ctx);	
    	}
    	
    }
	SpredisSortData **datas = res->data;
	SpredisSortData *d;
	size_t dSize = res->size;
	khint_t k;
	SPHashValue *av;
	// double itemVal;
	SPPtrOrD_t itemVal;
	uint16_t pos = 0;
	while(dSize) {
		d = datas[--dSize];
		for (int i = 0; i < facetCount; ++i)
		{
			facet = &facets[i];
			if (facet->field != NULL) {
				k = kh_get(HASH, facet->field->set, d->id);
				if (k == kh_end(facet->field->set) || !kh_exist(facet->field->set, k)) continue;
				av = kh_value(facet->field->set, k);
				if (av != NULL && av->type == SPHashDoubleType) {
			    	kv_foreach_hv_value(av, &itemVal, &pos, {
				    	if (SP_ISBETWEEN(itemVal.asDouble, facet->min, facet->max, facet->minExcl, facet->maxExcl)) {
				    		facet->count += 1;
				    		break;
				    	}
			    	});
			    }
			}
		}
	}

	for (int i = 0; i < facetCount; ++i) {
		facet = &facets[i];
		if (facet->field != NULL) SpredisUnProtectMap(facet->field);//, "SpredisFacetRange_RedisCommandT");
	}
	RedisModule_ReplyWithArray(ctx, facetCount);
	for (int i = 0; i < facetCount; ++i) {
		facet = &facets[i];
		RedisModule_ReplyWithArray(ctx, 2);
		RedisModule_ReplyWithStringBuffer(ctx, facet->label, strlen(facet->label));
		RedisModule_ReplyWithLongLong(ctx, facet->count);
	}
	RedisModule_Free(facets);
	return REDISMODULE_OK;
}

int SpredisFacetRange_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	if (argc < 8 || ( ((argc - 2) % 6) != 0 )) {
		 return RedisModule_WrongArity(ctx);
	}
	return SPThreadedWork(ctx, argv, argc, SpredisFacetRange_RedisCommandT);
}
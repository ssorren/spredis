#include "../spredis.h"

typedef struct {
	int fieldIndex;
	const char *label;
	double min;
	double max;
	long long minExcl;
	long long maxExcl;
	size_t count;

} SPFacetRange;

#define SP_ISBETWEEN(val, min, max, minExcl, maxExcl)  ( (minExcl ? (val > min) : (val >= min)) && (maxExcl ? (val < max) : (val <= max))  )

int SpredisFacetRange_RedisCommandT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
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
	
	// SPReadLock(ns->rs->deleteLock);

    int facetCount = (argc - 3) / 6;
    int argIndex = 3;
    
    SPFacetRange *facets = RedisModule_Calloc(facetCount, sizeof(SPFacetRange));
    SPFacetRange *facet;
    
    RedisModuleString *fkey;
    int rangeRes1, rangeRes2;
    for (int i = 0; i < facetCount; ++i)
    {
    	facet = &facets[i];
    	fkey = argv[argIndex++];
    	facet->fieldIndex = SPFieldIndex(ns, RedisModule_StringPtrLen(fkey, NULL));
    	rangeRes1 = SpredisStringToDouble(argv[argIndex++], &(facet->min));
    	rangeRes2 = SpredisStringToDouble(argv[argIndex++], &(facet->max));
		RedisModule_StringToLongLong(argv[argIndex++], &(facet->minExcl));
		RedisModule_StringToLongLong(argv[argIndex++], &(facet->maxExcl));
    	facet->label = RedisModule_StringPtrLen(argv[argIndex++], NULL);
    }
    SPItem *items = cursor->items;
	
	SPItem *d;
	size_t dSize = cursor->count;
	
	SPFieldData *data;
	SPReadLock(ns->rs->deleteLock);
	while(dSize) {
		d = &items[--dSize];
		if (!d->record->exists) continue;
		for (int i = 0; i < facetCount; ++i)
		{
			facet = &facets[i];
			if (facet->fieldIndex >= 0) {
				data = &d->record->fields[facet->fieldIndex];
				if (data->iv ==  NULL || data->ilen == 0) continue;
				for (int k = 0; k < data->ilen; ++k)
				{
					if (SP_ISBETWEEN(data->iv[k].asDouble, facet->min, facet->max, facet->minExcl, facet->maxExcl)) {
			    		facet->count += 1;
			    		break;
			    	}
				}
			}
		}
	}

	// SPReadUnlock(ns->rs->deleteLock);
	SPReadUnlock(ns->rs->deleteLock);
	SPReadUnlock(ns->lock);

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
	if (argc < 9 || ( ((argc - 3) % 6) != 0 )) {
		 return RedisModule_WrongArity(ctx);
	}
	return SPThreadedWork(ctx, argv, argc, SpredisFacetRange_RedisCommandT);
}
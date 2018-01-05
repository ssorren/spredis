#include "../spredis.h"

// #include "../kbtree.h"

KHASH_MAP_INIT_STR(SPREDISFACET, long long) ;

typedef struct _SPFacetResult {
	const char *val;
	long long count;
} SPFacetResult;


typedef struct _SPFacetData {
	// RedisModuleString *keyName;
	RedisModuleKey *key;
	SpredisSMapCont *col;
	long long count;
	int order;
	khash_t(SPREDISFACET) *valMap;
	SPFacetResult* results;
} SPFacetData;

void __SPCloseAllFacets(SPFacetData** facets, int keyCount) {
	for (int i = 0; i < keyCount; ++i)
    {
	    SPFacetData *facet = facets[i];
	    if (facet->key != NULL) RedisModule_CloseKey(facet->key);
	    if (facet->valMap != NULL) kh_destroy(SPREDISFACET, facet->valMap);
    }
}
int SPFacetResultCompareLT(SPFacetResult a, SPFacetResult b, int *order) {
	if (a.count < b.count) return (1 == order[0]);
	if (a.count > b.count) return (0 == order[0]);
	return 0;
}

SPREDIS_SORT_INIT(SPFacetResult, int , SPFacetResultCompareLT)

int SpedisBuildFacetResult(RedisModuleCtx *ctx, SPFacetData** facets, int facetCount) {
	RedisModule_ReplyWithArray(ctx, facetCount);
	int counted, count, max;
	SPFacetData *facet;
	SPFacetResult *fr;
	const char *k;
	long long v;
	for (int i = 0; i < facetCount; ++i)
	{
		facet = facets[i];
		count = (int)kh_size(facet->valMap);
		max = facet->count ? facet->count : 10;
		facet->results = RedisModule_PoolAlloc(ctx, sizeof(SPFacetResult) * count);
		counted = 0;

		kh_foreach(facet->valMap, k, v, {
			fr = &facet->results[counted];
	        // facet->results[counted] = RedisModule_PoolAlloc(ctx, sizeof(SPFacetResult));
	        fr->val = k;
	        fr->count = v;
	        ++counted;
	    });
	    max = (max > counted) ? counted : max;

	    SpredisSPFacetResultSort(counted, facet->results, &(facet->order));

	    RedisModule_ReplyWithArray(ctx, max * 2);
	    for (int i = 0; i < max; ++i)
	    {
	    	fr = &facet->results[i];
	    	RedisModule_ReplyWithStringBuffer(ctx, fr->val, strlen(fr->val));
	    	RedisModule_ReplyWithLongLong(ctx, fr->count);
	    }

	}
	return REDISMODULE_OK;
}

typedef struct {
    const char *key;
    int count;
} elem_t;

// #define elem_cmp(a, b) (strcmp((a).key, (b).key))
// KBTREE_INIT(str, elem_t, elem_cmp)

int SpredisFacets_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);
	// long long startTimer = RedisModule_Milliseconds();

	if (argc < 5) return RedisModule_WrongArity(ctx);
    int argOffset = 2;
    if ( ((argc - argOffset) % 3) != 0 ) return RedisModule_WrongArity(ctx);
    int keyCount = (argc - argOffset) / 3;
	RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ);
    int keyType = RedisModule_KeyType(key);
    if (keyType == REDISMODULE_KEYTYPE_EMPTY || keyType !=  REDISMODULE_KEYTYPE_ZSET) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    int argI = argOffset;

    /* populate */
    int ok = REDISMODULE_OK;
    SPFacetData** facets = RedisModule_PoolAlloc(ctx, sizeof(SPFacetData) * keyCount);
    // printf("%s\n", "WTF3");
    // for (int i = 0; i < keyCount; ++i) {
    // 	 make sure ll keys are iniyialized to NULL so we can close the safely on err
    // 	facets[i].key = NULL;
    // 	facets[i].valMap = NULL;
    // }
    for (int i = 0; i < keyCount; ++i)
    {
	    SPFacetData *facet = RedisModule_PoolAlloc(ctx, sizeof(SPFacetData));
	    facets[i] = facet;
	    // facet->keyName = argv[argI];
	    facet->key = RedisModule_OpenKey(ctx,argv[argI++], REDISMODULE_READ);
	    ok = RedisModule_StringToLongLong(argv[argI++], &(facet->count));
	    if (ok == REDISMODULE_ERR) {
	    	RedisModule_ReplyWithError(ctx,"ERR Could not parse count");
	    	break;	
	    }
	    facet->order = SPREDIS_ORDER(RedisModule_StringPtrLen(argv[argI++], NULL));
	    int keyType;
	    if (HASH_EMPTY_OR_WRONGTYPE(facet->key, &keyType, SPSTRINGTYPE) != 0) {
	    	ok = REDISMODULE_ERR;
	    	// printf("BAD KEY: %s\n", RedisModule_StringPtrLen(facet.key,NULL));
	        RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
	        break;
	    }
	    facet->col = RedisModule_ModuleTypeGetValue(facet->key);
	    facet->valMap = kh_init(SPREDISFACET);

    }
    int ele;
    if (ok == REDISMODULE_OK) {
    	if (RedisModule_ZsetFirstInScoreRange(key, REDISMODULE_NEGATIVE_INFINITE, REDISMODULE_POSITIVE_INFINITE, 0, 0) == REDISMODULE_OK) {
    		while(!RedisModule_ZsetRangeEndReached(key)) {
    			int keyI = keyCount;
    			ele =  TOINTKEY( RedisModule_ZsetRangeCurrentElement(key,NULL) );
    			// for (int keyI = 0; keyI < keyCount; ++keyI) {
    			// {
    			// 	/* code */
    			// }
    			while(keyI) {
    				--keyI;
    				SPFacetData *facet = facets[keyI];

    				// printf("%s %d\n", RedisModule_StringPtrLen(facet->keyName, NULL), HASH_EMPTY_OR_WRONGTYPE(facet->key, &keyType, SPSTRINGTYPE));
    				// printf("%s %d\n", "WTF8.3", HASH_EMPTY_OR_WRONGTYPE(facet.key, &keyType, SPSTRINGTYPE));
    				// RedisModuleString *actualVal = SHashGet(facet.col, ele);



    				// char *val = facet->col->map->values[id];

    				// khint_t k = SHashGet(facet->col, ele);
    				char *av = SpredisSMapValue(facet->col, ele);
				    if (av != NULL) {
				    	// RedisModuleString *actualVal = SHashGet(facet.col, ele);
				    	// char * id = facet->col->map[ele].value;//TOCHARKEY( kh_value(facet->col, k) );
				    	int res;
				        khint_t x = kh_put(SPREDISFACET, facet->valMap, av , &res);
				        if (res) {
				        	kh_value(facet->valMap, x) = 1;
				        } else {
				        	kh_value(facet->valMap, x) += 1;
				        }
				    }
    			}
		        RedisModule_ZsetRangeNext(key);
		    }
		    RedisModule_ZsetRangeStop(key);
		    SpedisBuildFacetResult(ctx, facets, keyCount);
	    } else {
	    	RedisModule_ReplyWithError(ctx,"ERR invalid range");
	    }
    }
	

	// const char * testA[10] = {"a","a","a","z","z","c","c", "c", "j", "s"};
	// kbtree_t(str) *b;
 //    elem_t *p, t;
 //    kbitr_t itr;
 //    int i;
 //    b = kb_init(str, KB_DEFAULT_SIZE);
 //    for (i = 0; i < 10; ++i) {
 //        // no need to allocate; just use pointer
 //        t.key = testA[i], t.count = 1;
 //        p = kb_getp(str, b, &t); // kb_get() also works
 //        // IMPORTANT: put() only works if key is absent
 //        if (!p) kb_putp(str, b, &t);
 //        else ++p->count;
 //    }
 //    // ordered tree traversal
 //    kb_itr_first(str, b, &itr); // get an iterator pointing to the first
 //    for (; kb_itr_valid(&itr); kb_itr_next(str, b, &itr)) { // move on
 //        p = &kb_itr_key(elem_t, &itr);
 //        printf("%d\t%s\n", p->count, p->key);
 //    }
 //    kb_destroy(str, b);


    RedisModule_CloseKey(key);
    __SPCloseAllFacets(facets, keyCount);
    
    // printf("Hydrating facets took %lldms\n", RedisModule_Milliseconds() - startTimer);
	return REDISMODULE_OK;
}
// local source,ensuredZSet,facets,flen,facetCount = KEYS[1],KEYS[2],{},#ARGV,#ARGV/5
// local t = redis.call('TYPE', source)
// t = t.ok or t
// -- print(t)
// local set = nil;

// if t == 'set' then
// 	source = ensuredZSet
// end

// -- if t == 'zset' then 
// set = redis.call('ZRANGE', source, 0, -1)
// -- else
// -- 	set = redis.call('SMEMBERS', source)
// -- end

// -- local facetKeys = {}
// for i=1,flen,5 do
// 	local facet = {}
// 	facet.field = ARGV[i]
// 	facet.count = ARGV[i + 1]
// 	facet.store = ARGV[i + 2]
// 	facet.order = ARGV[i + 3]
// 	facet.key = ARGV[i + 4]
// 	facet.results = {}
// 	table.insert(facets, facet)
// end
// -- print(cjson.encode(facets))

// local len = #set

// local el = nil
// local facet = {}
// for i=1,len do

// 	for k=1,facetCount do
// 		facet = facets[k]
// 		el = redis.call('spredis.hashget', facet.key, set[i])
// 		if el then
// 			facet.results[el] = (facet.results[el] or 0) + 1
// 		end
// 	end

// end

// for k=1,facetCount do
// 	facet = facets[k]
// 	-- el = redis.call('HGET', facet.key, set[i])
// 	for name,v in pairs(facet.results) do
// 		redis.call('ZADD', facet.store, v, name)
// 	end
// end
// -- print(cjson.encode(facets))
// -- if true then return {} end

// -- for name,v in pairs(hash) do
// -- 	-- print(name)
// -- 	-- print(v)
// -- 	redis.call('ZADD', key, v, name)
// -- end
// -- if true then return facets end
// local results = {}
// local zrange = 'ZRANGE'
// for k=1,facetCount do
// 	facet = facets[k];
// 	zrange = 'ZRANGE'
// 	if facet.order == 'DESC' then
// 		zrange = 'ZREVRANGE'
// 	end
// 	table.insert(results, redis.call(zrange, facet.store, 0, facet.count, 'WITHSCORES'))
// end


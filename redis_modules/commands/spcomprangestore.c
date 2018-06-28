#include "./spcomprangestore.h"

typedef struct _SPCompRange {
	SPPtrOrD_t min, max;
	double lat, lon, radius;
	uint8_t wildcard;
	RedisModuleString *unit, *radiusField;
} SPCompRange;

typedef struct _SPCompQueryComparator {
	int (*gt)(SPPtrOrD_t, SPPtrOrD_t);
	int (*lt)(SPPtrOrD_t, SPPtrOrD_t);
} SPCompQueryComparator;


typedef struct _SPCompQueryPart {
	SPCompRange range;
	SPCompRange *allRanges;
	uint32_t rangeCount;
	uint8_t valueIndex, needsSatifying;
	SPCompQueryComparator comp;
	SPCompQueryComparator *comps;
} SPCompQueryPart;


typedef int (*SPCompSatisfier)(SPCompQueryPart*,SPCompositeScoreSetKey*);
typedef kvec_t(SPCompositeScoreSetKey*) SPKeyVec;

static int SpCompDoubleGT(SPPtrOrD_t a, SPPtrOrD_t b) {
	return a.asDouble > b.asDouble; 
}
static int SpCompDoubleLT(SPPtrOrD_t a, SPPtrOrD_t b) {
	return a.asDouble < b.asDouble;	
}
static int SpCompDoubleGTE(SPPtrOrD_t a, SPPtrOrD_t b) {
	return a.asDouble >= b.asDouble;
}
static int SpCompDoubleLTE(SPPtrOrD_t a, SPPtrOrD_t b) {
	return a.asDouble <= b.asDouble;
}


// static int SpCompLexGT(SPPtrOrD_t a, SPPtrOrD_t b) {
// 	return a.asDouble > b.asDouble; 
// }
// static int SpCompLexLT(SPPtrOrD_t a, SPPtrOrD_t b) {
// 	return a.asDouble < b.asDouble;	
// }
static int SpCompLexGTE(SPPtrOrD_t a, SPPtrOrD_t b) {
	return a.asDouble >= b.asDouble;
}
static int SpCompLexLTE(SPPtrOrD_t a, SPPtrOrD_t b) {
	return a.asDouble <= b.asDouble;
}

static int SpCompLexEQ(SPPtrOrD_t a, SPPtrOrD_t b) {
	return strcmp(a.asChar, b.asChar) ? 0 : 1;
}

static int SpredisBuildDoubleQueryPart(RedisModuleCtx *ctx, SPCompQueryPart *qp, RedisModuleString **argv, int count, int idx, int *argIdx) {
	double min,max;
	long long minExcl, maxExcl;
	qp->rangeCount = count;
	qp->needsSatifying = 0;
	if (count > 1) {
		qp->allRanges = RedisModule_Calloc(count, sizeof(SPCompRange));
		qp->comps = RedisModule_Calloc(count, sizeof(SPCompQueryComparator));
		for (int i = 0; i < count; ++i)
		{
			RedisModule_StringToDouble(argv[++idx], &min);
			RedisModule_StringToDouble(argv[++idx], &max);
			RedisModule_StringToLongLong(argv[++idx], &minExcl);
			RedisModule_StringToLongLong(argv[++idx], &maxExcl);
			// printf("min: %f, max: %f, minExcl %d, maxExcl %d\n", min, max, minExcl, maxExcl);
			if (i == 0) {
				qp->range.min.asDouble = min;
				qp->range.max.asDouble = max;
				qp->comp.gt = SpCompDoubleGTE;
				qp->comp.lt = SpCompDoubleLTE;
			} else {
				if (min < qp->range.min.asDouble) qp->range.min.asDouble = min;
				if (max > qp->range.max.asDouble) qp->range.max.asDouble = max;
			}
			qp->allRanges[i].min.asDouble = min;
			qp->allRanges[i].max.asDouble = max;
			qp->comps[i].gt = minExcl ? SpCompDoubleGT : SpCompDoubleGTE;
			qp->comps[i].lt = maxExcl ? SpCompDoubleLT : SpCompDoubleLTE;

		}
	} else {
		RedisModule_StringToDouble(argv[++idx], &min);
		RedisModule_StringToDouble(argv[++idx], &max);
		RedisModule_StringToLongLong(argv[++idx], &minExcl);
		RedisModule_StringToLongLong(argv[++idx], &maxExcl);
		qp->range.min.asDouble = min;
		qp->range.max.asDouble = max;
		// printf("min: %f, max: %f, minExcl %d, maxExcl %d\n", min, max, minExcl, maxExcl);
		qp->comp.gt = minExcl ? SpCompDoubleGT : SpCompDoubleGTE;
		qp->comp.lt = maxExcl ? SpCompDoubleLT : SpCompDoubleLTE;

	}
	(*argIdx) = idx;
	return REDISMODULE_OK;
}

static int SpredisBuildGeoQueryPart(RedisModuleCtx *ctx, SPCompQueryPart *qp, RedisModuleString **argv, int count, int idx, int *argIdx) {
	double lat, lon, radius;
	RedisModuleString *unit, *field;
	qp->rangeCount = count;
	qp->needsSatifying = 1;
	for (int i = 0; i < count; ++i)
	{
		RedisModule_StringToDouble(argv[++idx], &lat);
		RedisModule_StringToDouble(argv[++idx], &lon);
		RedisModule_StringToDouble(argv[++idx], &radius);
		unit = argv[++idx];
		field = argv[++idx];
		if (i == 0) {
			// qp->range.lat = lat;
			// qp->range.lon = lon;
			// qp->range.radius = radius;
		}
	}
	(*argIdx) = idx;
	return REDISMODULE_OK;
}

static int SpredisBuildLexQueryPart(RedisModuleCtx *ctx, SPCompQueryPart *qp, RedisModuleString **argv, int count, int idx, int *argIdx) {
	const char *min;
	// int minExcl, maxExcl;
	qp->rangeCount = count;
	qp->needsSatifying = 0;
	long long wc;
	if (count > 1) {
		qp->allRanges = RedisModule_Calloc(count, sizeof(SPCompRange));
		qp->comps = RedisModule_Calloc(count, sizeof(SPCompQueryComparator));
		for (int i = 0; i < count; ++i)
		{
			min = RedisModule_StringPtrLen(argv[++idx], NULL);
			RedisModule_StringToLongLong(argv[++idx], &wc);
			if (i == 0) {
				qp->range.min.asChar = min;
				qp->range.max.asChar = min;
				qp->range.wildcard = (wc) ? 1 : 0;
				qp->allRanges[i].min.asChar = min;
				qp->allRanges[i].max.asChar = min;
				qp->allRanges[i].wildcard = (wc) ? 1 : 0;
			}
			//TODO: compare mins and maxes
			qp->allRanges[i].min.asChar = min;
			qp->allRanges[i].max.asChar = min;
			qp->allRanges[i].wildcard = (wc) ? 1 : 0;
		}
	} else {
		min = RedisModule_StringPtrLen(argv[++idx], NULL);
		RedisModule_StringToLongLong(argv[++idx], &wc);
		qp->range.min.asChar = min;
		qp->range.max.asChar = min;
		qp->range.wildcard = (wc) ? 1 : 0;
		if (wc) {
			qp->comp.gt = SpCompLexGTE;
			qp->comp.lt = SpCompLexLTE;
		} else {
			qp->comp.gt = SpCompLexEQ;
			qp->comp.lt = SpCompLexEQ;
		}
	}

	(*argIdx) = idx;
	return REDISMODULE_OK;
}
static int SpredisPopulateQueryParts(RedisModuleCtx *ctx, SPCompositeScoreCont *cont, SPCompQueryPart *qp, RedisModuleString **argv, int argc) {
	uint8_t count = cont->compCtx->valueCount;
	int parseRes;
	uint8_t type, i;
	long long ltype, qcount;
	int argIdx = 4;

	for (i = 0; i < count; ++i)
	{
		
		if (argIdx >= argc) {
			printf("err %d\n", 1);
			return REDISMODULE_ERR;
		}
		parseRes = RedisModule_StringToLongLong(argv[argIdx], &ltype);
		if (parseRes != REDISMODULE_OK) return parseRes;
		type = (uint8_t)ltype;

		//sanity check: make sure the client is aligned with server on types 
		if (type != cont->compCtx->types[i]) {
			printf("err %d, %d, %d, %lld\n", 2, type, cont->compCtx->types[i], ltype);
			return REDISMODULE_ERR;
		}
		if (++argIdx >= argc) {
			printf("err %d\n", 3);
			return REDISMODULE_ERR;
		}
		parseRes = RedisModule_StringToLongLong(argv[argIdx], &qcount);
		if (parseRes != REDISMODULE_OK) return parseRes;

		//each individual clause contains 4 or 5 parts: min,max,min exclude, max exclude
		//geo = lat,lon, radius, unit, field
		
		if (type == SPGeoPart) {
			if ((argIdx + (((int)qcount) * 5)) > argc) {
				printf("err %d\n", 4); return REDISMODULE_ERR;
			}
			parseRes = SpredisBuildGeoQueryPart(ctx, &qp[i], argv, (int)qcount, argIdx, &argIdx);
			if (parseRes != REDISMODULE_OK) return parseRes;
		} else if (type == SPDoublePart) {
			if ((argIdx + (((int)qcount) * 4)) > argc) {
				printf("err %d\n", 5);
				return REDISMODULE_ERR;
			}
			parseRes = SpredisBuildDoubleQueryPart(ctx, &qp[i], argv, (int)qcount, argIdx, &argIdx);
			if (parseRes != REDISMODULE_OK) {
				printf("err %d\n", 6);
				return parseRes;
			}
		} else if (type == SPLexPart) {
			if ((argIdx + (((int)qcount) * 2)) > argc) {
				printf("err %d\n", 7);
				return REDISMODULE_ERR;
			}
			parseRes = SpredisBuildLexQueryPart(ctx, &qp[i], argv, (int)qcount, argIdx, &argIdx);
			if (parseRes != REDISMODULE_OK) {
				printf("err %d\n", 8);
				return parseRes;
			}
		}
		argIdx++;
	}
	return REDISMODULE_OK;
}

static int SPCompGT(SPPtrOrD_t *val, SPCompQueryPart *qp, uint8_t valueCount) {
	for (uint8_t i = 0; i < valueCount; ++i)
	{
		if (!qp[i].comp.gt(val[i], qp[i].range.min)) return 0;
	}
	return 1;
}

static int SPCompLT(SPPtrOrD_t *val, SPCompQueryPart *qp, uint8_t valueCount) {
	for (uint8_t i = 0; i < valueCount; ++i)
	{
		if (!qp[i].comp.lt(val[i], qp[i].range.max)) return 0;
	}
	return 1;
}

static void SPCompProduceRes(SPKeyVec *vecp, khash_t(SIDS) *res, khash_t(SIDS) *hint) {
	SPKeyVec vec = vecp[0];
	if (kv_size(vec) == 0) return;
	SPCompositeScoreSetKey *largest = NULL, *current;

	//TODO: opportunity for parallel processing here?
	if (hint == NULL) {
		for (int i = 0; i < kv_size(vec); ++i)
		{
			if (kv_A(vec,i) == NULL) continue;
			if (largest) {
				if (kh_size(kv_A(vec,i)->members->set) > kh_size(largest->members->set)) largest = kv_A(vec,i);
			} else {
				largest = kv_A(vec,i);
			}
		}
		if (largest == NULL) return;
		kh_dup_set(spid_t, res, largest->members->set);
		for (int i = 0; i < kv_size(vec); ++i)
		{
			if (current == NULL || current == largest) continue;
			current = kv_A(vec,i);
			SPAddAllToSet(res, current, hint);
		}	
	} else {
		for (int i = 0; i < kv_size(vec); ++i)
		{
			current = kv_A(vec,i);
			if (current == NULL) continue;
			SPAddAllToSet(res, current, hint);
		}
	}
}
SPKeyVec SPCompGetSatisfaction(
	SPKeyVec *candidates, 
	SPCompQueryPart *qp, 
	uint8_t valueCount)
{

	//TODO: GEO filtering
	return *candidates;
}

static void SpredisDoCompositeSearch(SPCompositeScoreCont *cont, 
	khash_t(SIDS) *res, 
	khash_t(SIDS) *hint, 
	SPCompQueryPart *qp)
{
	uint8_t valueCount, cv, doAdd; 
	valueCount = cont->compCtx->valueCount;
	SPPtrOrD_t *value = RedisModule_Calloc(valueCount, sizeof(SPPtrOrD_t));
	SPPtrOrD_t *uvalue = RedisModule_Calloc(valueCount, sizeof(SPPtrOrD_t));
	uint8_t satisfyCount = 0;

	for (uint8_t i = 0; i < valueCount; ++i)
	{
		value[i] = qp[i].range.min;
		uvalue[i] = qp[i].range.max;
		if (cont->compCtx->types[i] == SPGeoPart) {
			satisfyCount += 1;
		}
	}
	kbtree_t(COMPIDX) *btree = cont->btree;
	SPCompositeScoreSetKey *l, *u, *use, *candKey, *last;
	SPCompositeScoreSetKey search = {.compCtx = cont->compCtx, .value = value};
	SPCompositeScoreSetKey usearch = {.compCtx = cont->compCtx, .value = uvalue};

	kb_intervalp(COMPIDX, btree, &search, &l, &u);

	use = u == NULL ? l : u;
	u = NULL;
	kb_intervalp(COMPIDX, btree, &usearch, &l, &u);
	last = u;

	kbitr_t itr;
	int reached = 0;
	kb_itr_getp(COMPIDX, btree, use, &itr);
	// printf("use NULL %d\n", use == NULL);
	SPKeyVec candidates;
	kv_init(candidates);
	SPCompQueryComparator *qcs;
	SPCompQueryComparator cqc;
	SPCompQueryPart cqp;
	uint32_t cri;
	SPCompRange cr;
    for (; kb_itr_valid(&itr); kb_itr_next(COMPIDX, btree, &itr)) { 
    	candKey = (&kb_itr_key(SPCompositeScoreSetKey, &itr));
		if (candKey == NULL) continue;
		if (reached || SPCompGT(candKey->value, qp, valueCount)) {
			// printf("A\n");
            if (SPCompLT(candKey->value, qp, valueCount)) {
            	// printf("B\n");
                reached = 1;
                cv = 0;
                doAdd = 0;
                while (cv < valueCount) {
                	// printf("C\n");
                	cqp = qp[cv];
                	qcs = cqp.comps;
                	if (qcs) {
                		cri = 0;
                		//this is essentially a  list of or's
                		while (cri < cqp.rangeCount) {
                			cr = cqp.allRanges[cri];
                			cqc = qcs[cri++];
                			if (cqc.gt(candKey->value[cv], cr.min) && cqc.lt(candKey->value[cv], cr.max)) {
                				doAdd++;
                				break;
                			}
                		}
                	} else {
                		// printf("D\n");
                		doAdd++;
                	}
                	cv++;
                }
                if (doAdd == valueCount) {
                	// printf("E\n");
                	kv_push(SPCompositeScoreSetKey*, candidates, candKey);	
                }
            } else if (candKey == last) {
            	// printf("F\n");
                break;
            }
        }
    }
    if (satisfyCount) {
    	candidates = SPCompGetSatisfaction(&candidates, qp, valueCount);
    }
    RedisModule_Free(value);
    RedisModule_Free(uvalue);
    SPCompProduceRes(&candidates, res, hint);
    kv_destroy(candidates);
}

static khash_t(SIDS) *SPGetCompHint(RedisModuleCtx *ctx, RedisModuleString *name, int *res) {
    size_t len;
    RedisModule_StringPtrLen(name, &len);
    RedisModuleKey *hintKey = NULL;
    SpredisSetCont *hintCont = NULL;
    khash_t(SIDS) *hint = NULL;
    (*res) = REDISMODULE_OK;
    if (len) {
    	hintKey = RedisModule_OpenKey(ctx,name, REDISMODULE_READ);
    	int hintType;
    	if (HASH_NOT_EMPTY_AND_WRONGTYPE_CHECKONLY(hintKey, &hintType ,SPSETTYPE) == 1) {
    		printf("err %d\n", 9);
		    (*res) = REDISMODULE_ERR;
		    return NULL;
    	}
    	hintCont = RedisModule_ModuleTypeGetValue(hintKey);
    	if (hintCont != NULL) {
            hint = hintCont->set;
            if (hint == NULL || kh_size(hint) == 0) {
            	printf("err %d\n", 10);
            	(*res) = REDISMODULE_ERR;
	        }
        } else {
        	printf("err %d\n", 11);
        	(*res) = REDISMODULE_ERR;
        }
        
    }
    return hint;
}

static int SpredisCompStoreRange_RedisCommandT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModuleKey *key;
	SPCompositeScoreCont *cont;
	SPLockContext(ctx);
	int keyOk = SPGetCompKeyValue(ctx, &key, &cont, argv[2], REDISMODULE_READ);
	RedisModuleKey *store = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_WRITE|REDISMODULE_READ);

    int storeType = RedisModule_KeyType(store);
    if (HASH_NOT_EMPTY_AND_WRONGTYPE_CHECKONLY(store, &storeType ,SPSETTYPE) == 1) {;
    	SPUnlockContext(ctx);
    	printf("err %d\n", 12);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
	SpredisSetCont *resCont = _SpredisInitSet();
    khash_t(SIDS) *res = resCont->set;
    int hintRes;
    
    khash_t(SIDS) *hint = SPGetCompHint(ctx, argv[3], &hintRes);
    if (hintRes != REDISMODULE_OK) {
    	return RedisModule_ReplyWithLongLong(ctx,0);
    }
    SpredisSetRedisKeyValueType(store, SPSETTYPE, resCont);
	SPUnlockContext(ctx);

	if (keyOk != REDISMODULE_OK) {
		return keyOk;
	}
	if (cont == NULL) {
		return RedisModule_ReplyWithLongLong(ctx, 0);
	}

	SpredisProtectWriteMap(cont);
	if (cont->compCtx->valueCount == 0) {
		SpredisUnProtectMap(cont);
		return RedisModule_ReplyWithLongLong(ctx, 0);
	}
	//start working magic
	SPCompQueryPart *qp = RedisModule_Calloc(cont->compCtx->valueCount, sizeof(SPCompQueryPart));
	// SPCompQueryComparator *qq = RedisModule_Calloc(cont->compCtx->valueCount, sizeof(SPCompQueryComparator));

	int popRes = SpredisPopulateQueryParts(ctx, cont, qp, argv, argc);
	if (popRes == REDISMODULE_OK) {
		SpredisDoCompositeSearch(cont, res, hint, qp);
	} 
	for (uint8_t i = 0; i < cont->compCtx->valueCount; ++i)
	{
		SPCompQueryPart cqp = qp[i];
		if (cqp.allRanges != NULL) RedisModule_Free(cqp.allRanges);
		if (cqp.comps != NULL) RedisModule_Free(cqp.comps);
	}
	RedisModule_Free(qp);
	// RedisModule_Free(qq);

	//end working magic
	SpredisUnProtectMap(cont);
	RedisModule_ReplyWithLongLong(ctx, kh_size(res));
    return REDISMODULE_OK;
}

int SpredisCompStoreRange_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return SPThreadedWork(ctx, argv, argc, SpredisCompStoreRange_RedisCommandT);
}
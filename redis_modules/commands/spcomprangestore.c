#include "./spcomprangestore.h"
#include <float.h>
#include <float.h>
KHASH_DECLARE_SET(PTR, uint64_t);
KHASH_SET_INIT_INT64(PTR);

typedef struct  _SPCompGeoData {
	SPGeoSearchAreas areas;
	SPGeoHashArea area;
	double lat, lon, radius;
	uint8_t hasFieldQuery;
	SPScoreCont *fieldCont;
	RedisModuleString *unit, *radiusField;
} SPCompGeoData;

typedef struct  _SPCompLexData {
	uint8_t wildcard;
	int minLen, maxLen;
	const char *rawMax;
} SPCompLexData;

typedef struct _SPCompRange {
	SPPtrOrD_t min, max;
	SPCompGeoData geo;
	SPCompLexData lex;
} SPCompRange;

typedef struct _SPCompQueryComparator {
	int (*gt)(SPPtrOrD_t, SPPtrOrD_t, int lenComp);
	int (*lt)(SPPtrOrD_t, SPPtrOrD_t, int lenComp);
} SPCompQueryComparator;


typedef kvec_t(SPCompositeScoreSetKey*) SPKeyVec;
typedef int (*SPCompSatisfier)(SPKeyVec *candidates, void *ptr, khash_t(SIDS) *res, khash_t(SIDS) *hint, SPKeyVec *leavings);

typedef struct _SPCompQueryPart {
	SPCompRange range;
	SPCompRange *allRanges;
	uint32_t rangeCount;
	uint8_t valueIndex, needsSatifying;
	SPCompQueryComparator comp;
	SPCompQueryComparator *comps;
	SPCompSatisfier satisfy;
} SPCompQueryPart;




static inline int SPGeoCompRadiusMatch(SPCompositeScoreSetKey *candKey, uint8_t valueIndex, SPCompGeoData *geo) {
	double lat, lon;
	SPGeoHashDecode(candKey->value[valueIndex].asUInt, &lat, &lon);
    // distance = SPGetDist(geo->lat, geo->lon, lat, lon);
    if (SP_INBOUNDS(lat, lon, geo->area) == 0) return 0;
	return SPGetDist(geo->lat, geo->lon, lat, lon) <= geo->radius ? 1 : 0;
}

static int SpCompGeoSatisfier(SPKeyVec *candidatesPtr, void *ptr, khash_t(SIDS) *res, khash_t(SIDS) *hint, SPKeyVec *leavingsPtr) {
	
	SPCompQueryPart *qp;
	qp = ptr;
	size_t i, count;
	SPKeyVec leavings = leavingsPtr[0];
	SPKeyVec candidates, orig;
	kv_init(candidates);
	orig = candidatesPtr[0];
	SPCompositeScoreSetKey *candKey;
	//happy path
	SPCompGeoData *geo;
	int defer = 0;
	if (!qp->range.geo.radiusField) {
		count = kv_size(orig);
		for (i = 0; i < count; ++i)
		{
			candKey = kv_A(orig, i);
			if (candKey) {
				for (int k = 0; k < count; ++k)
				{
					geo = &(qp->allRanges[k].geo); 
					if (geo->radiusField) {
						defer = 1;
						continue;
					}
					if ( SPGeoCompRadiusMatch(candKey, qp->valueIndex, geo) ) {
						kv_push(SPCompositeScoreSetKey*, candidates, candKey);
						break;
					} else {
						kv_push(SPCompositeScoreSetKey*, leavings, candKey);
					}
				}
			}
		}
		kv_destroy(orig);
		(*candidatesPtr) = candidates;
	}
	return defer;
}
// static int SpCompGeoFieldSatisfier(SPCompositeScoreSetKey *key, void *context, khash_t(SIDS) *hint) {
// 	return 1;
// }

static int SpCompGeoGTE(SPPtrOrD_t a, SPPtrOrD_t b, int lenComp) {
	return a.asUInt >= b.asUInt;
}
static int SpCompGeoLTE(SPPtrOrD_t a, SPPtrOrD_t b, int lenComp) {
	return a.asUInt <= b.asUInt;
}

//if we're searching by field len, or have come across an include all wildcard, always return true as evert thing is game
static int SpCompWildCard(SPPtrOrD_t a, SPPtrOrD_t b, int lenComp) {
	return 1;
}

static int SpCompDoubleGT(SPPtrOrD_t a, SPPtrOrD_t b, int lenComp) {
	return a.asDouble > b.asDouble; 
}
static int SpCompDoubleLT(SPPtrOrD_t a, SPPtrOrD_t b, int lenComp) {
	return a.asDouble < b.asDouble;	
}
static int SpCompDoubleGTE(SPPtrOrD_t a, SPPtrOrD_t b, int lenComp) {
	return a.asDouble >= b.asDouble;
}
static int SpCompDoubleLTE(SPPtrOrD_t a, SPPtrOrD_t b, int lenComp) {
	return a.asDouble <= b.asDouble;
}

static int SpCompLexGTE(SPPtrOrD_t a, SPPtrOrD_t b, int lenComp) {
	return (memcmp(a.asUChar, b.asUChar, lenComp) >= 0) ? 1 : 0;
}
static int SpCompLexLTE(SPPtrOrD_t a, SPPtrOrD_t b, int lenComp) {
	return (memcmp(a.asUChar, b.asUChar, lenComp) <= 0) ? 1 : 0;;
}

static int SpCompLexEQ(SPPtrOrD_t a, SPPtrOrD_t b, int lenComp) {
	// printf("%s == %s == %d\n", a.asChar, b.asChar, strcmp(a.asChar, b.asChar) ? 0 : 1);
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
			SpredisStringToDouble(argv[++idx], &min);
			SpredisStringToDouble(argv[++idx], &max);
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
		SpredisStringToDouble(argv[++idx], &min);
		SpredisStringToDouble(argv[++idx], &max);
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

/*
	this is the most complicated type of single-pass search we can do
*/
static int SpredisBuildGeoQueryPart(RedisModuleCtx *ctx, SPCompQueryPart *qp, RedisModuleString **argv, int count, int idx, int *argIdx) {
	double lat, lon, radius;
	RedisModuleString *unit, *field;
	qp->rangeCount = count;
	/*
		we'll have to do haversine later
	*/
	qp->needsSatifying = 1;

	/*
		here's our base comparator, will be overridden if we run accross a radius by field query
	*/
	qp->comp.gt = SpCompGeoGTE;
	qp->comp.lt = SpCompGeoLTE;
	qp->satisfy = SpCompGeoSatisfier;
	size_t fieldLen;
	qp->allRanges = RedisModule_Calloc(count, sizeof(SPCompRange));
	qp->comps = RedisModule_Calloc(count, sizeof(SPCompQueryComparator));
	SPGeoHashArea area;
	uint64_t geoMin, geoMax;
	for (int i = 0; i < count; ++i)
	{
		SpredisStringToDouble(argv[++idx], &lat);
		SpredisStringToDouble(argv[++idx], &lon);
		SpredisStringToDouble(argv[++idx], &radius);
		unit = argv[++idx];
		field = argv[++idx];
		RedisModule_StringPtrLen(field, &fieldLen);
		if (radius && radius < DBL_MAX && radius > 0) {
			radius = SPConvertToMeters(radius, RedisModule_StringPtrLen(unit, NULL));
		} else {
			radius = DBL_MAX;
		}
		if (i == 0) {
			qp->range.geo.lat = lat;
			qp->range.geo.lon = lon;
			qp->range.geo.radius = radius;
			qp->range.geo.unit = unit;
			qp->range.geo.radiusField = fieldLen ? field : NULL;
		}
		qp->allRanges[i].geo.lat = lat;
		qp->allRanges[i].geo.lon = lon;
		qp->allRanges[i].geo.radius = radius;
		qp->allRanges[i].geo.unit = unit;
		qp->allRanges[i].geo.radiusField = fieldLen ? field : NULL;
		qp->comps[i].gt = fieldLen ? SpCompWildCard : SpCompGeoGTE;
		qp->comps[i].lt = fieldLen ? SpCompWildCard : SpCompGeoLTE;
		// qp->comps[i].satisfy = fieldLen ? SpCompGeoFieldSatisfier : SpCompGeoSatisfier;
		//we've run across a field len, all entries are game
		if (fieldLen) {
			qp->range.geo.hasFieldQuery = 1;
			qp->comp.gt = SpCompWildCard;
			qp->comp.lt = SpCompWildCard;
		} else {
			/*
				get the neighboring bounding boxes
			*/
			SPGetSearchAreas(lat, lon, radius, &qp->allRanges[i].geo.areas, &qp->allRanges[i].geo.area);
			/*
				we ned to iterate through all the neighbors and 
				find the min/max uint64 for each individual query part 
				as wll as the overall min/max
			*/
			for (int gi = 0; gi < 9; ++gi)
			{
				area = qp->allRanges[i].geo.areas.area[gi];
				if (area.hash.bits) {
					geoMin = area.hash.bits << (62 - (area.hash.step * 2));
					geoMax = ++area.hash.bits << (62 - (area.hash.step * 2));
					//we may have hit a zero bit bounding box on previous neighbors (individual)
					if ( qp->allRanges[i].min.asUInt == 0 || geoMin < qp->allRanges[i].min.asUInt) {
						qp->allRanges[i].min.asUInt = geoMin;	
					}
					if ( qp->allRanges[i].max.asUInt == 0 || geoMax > qp->allRanges[i].max.asUInt) {
						qp->allRanges[i].max.asUInt = geoMax;	
					}
					//we may have hit a zero bit bounding box on previous neighbors (overall)
			        if (i == 0 || qp->range.min.asUInt == 0) {
			        	qp->range.min.asUInt = qp->allRanges[i].min.asUInt;
			        }
			        //we may have hit a zero bit bounding box on previous neighbors (overall)
			        if (i == 0 || qp->range.max.asUInt == 0) {
			        	qp->range.max.asUInt = qp->allRanges[i].max.asUInt;
			        }

			        //find min/max for overall query
			        if (i > 0) {
			        	if (qp->allRanges[i].min.asUInt < qp->range.min.asUInt)	{
			        		qp->range.min.asUInt = qp->allRanges[i].min.asUInt;
			        	}
			        	if (qp->allRanges[i].max.asUInt > qp->range.max.asUInt)	{
			        		qp->range.max.asUInt = qp->allRanges[i].max.asUInt;
			        	}
			        }
			        
				}
			}
		}
	}
	for (int i = 0; i < count; ++i)
	{
		if (qp->allRanges[i].geo.radius == DBL_MAX) {
			/* 
				this query will return everything 
			*/
			qp->needsSatifying = 0;
			qp->range.min.asUInt = 0;
			qp->range.max.asUInt = UINT64_MAX;
			qp->comp.gt = SpCompWildCard;
			qp->comp.lt = SpCompWildCard;
			if (qp->allRanges) RedisModule_Free(qp->allRanges);
			if (qp->comps) RedisModule_Free(qp->comps);
			qp->allRanges = NULL;
			qp->comps = NULL;
			break;
		} 
	}
	(*argIdx) = idx;
	return REDISMODULE_OK;
}

static int SpredisBuildLexQueryPart(RedisModuleCtx *ctx, SPCompQueryPart *qp, RedisModuleString **argv, int count, int idx, int *argIdx) {
	const char *min, *max;
	// int minExcl, maxExcl;
	qp->rangeCount = count;
	qp->needsSatifying = 0;
	long long wc;
	int foundWC = 0;
	if (count > 1) {
		qp->allRanges = RedisModule_Calloc(count, sizeof(SPCompRange));
		qp->comps = RedisModule_Calloc(count, sizeof(SPCompQueryComparator));
		for (int i = 0; i < count; ++i)
		{
			min = RedisModule_StringPtrLen(argv[++idx], NULL);
			//this jsu makes the code less confusing
			max = min;
			RedisModule_StringToLongLong(argv[++idx], &wc);
			if (foundWC) continue;

			qp->allRanges[i].min.asChar = min;
			qp->allRanges[i].lex.minLen = strlen(min);
			qp->allRanges[i].lex.wildcard = (wc) ? 1 : 0;

			
			if (qp->allRanges[i].lex.wildcard && qp->allRanges[i].lex.minLen == 0) {
				/*
					we've found a wildcard...everything counts
				*/
				qp->needsSatifying = 0;
				qp->comp.gt = SpCompWildCard;
				qp->comp.lt = SpCompWildCard;
				qp->range.min.asChar = min;
				qp->range.max.asChar = RedisModule_StringPtrLen(RedisModule_CreateStringPrintf(ctx, "%s%c",min, 0xff), NULL);;
				qp->range.lex.maxLen = ++qp->range.lex.minLen;

				if (qp->allRanges) RedisModule_Free(qp->allRanges);
				if (qp->comps) RedisModule_Free(qp->comps);

				qp->allRanges = NULL;
				qp->comps = NULL;
				foundWC = 1;
				continue;
			}
			if (wc) {
				qp->allRanges[i].max.asChar = RedisModule_StringPtrLen(RedisModule_CreateStringPrintf(ctx, "%s%c",max, 0xff), NULL);
				qp->allRanges[i].lex.maxLen = ++qp->range.lex.minLen;
				qp->comps[i].gt = SpCompLexGTE;
				qp->comps[i].lt = SpCompLexLTE;
			} else {
				qp->allRanges[i].max.asChar = min;
				qp->allRanges[i].lex.maxLen = qp->range.lex.minLen;
				qp->comps[i].gt = SpCompLexEQ;
				qp->comps[i].lt = SpCompLexEQ;
			}

			if (i == 0) {
				qp->range.lex.rawMax = max;
				qp->range.min.asChar = min;
				qp->range.lex.minLen = strlen(min);
				qp->range.max.asChar = RedisModule_StringPtrLen(RedisModule_CreateStringPrintf(ctx, "%s%c",max, 0xff), NULL);
				qp->range.lex.maxLen = ++qp->range.lex.minLen;
				qp->comp.gt = SpCompLexGTE;
				qp->comp.lt = SpCompLexLTE;
			} else {
				if (strcmp(qp->allRanges[i].min.asChar, qp->range.min.asChar) < 0) {
					qp->range.min.asChar = qp->allRanges[i].min.asChar;
					qp->range.lex.minLen = strlen(qp->range.min.asChar) + 1;
				}
				if (strcmp(max, qp->range.lex.rawMax) > 0) {
					qp->range.lex.rawMax = max;
					qp->range.max.asChar = RedisModule_StringPtrLen(RedisModule_CreateStringPrintf(ctx, "%s%c",max, 0xff), NULL);
					qp->range.lex.maxLen = strlen(max) + 1;
				}
			}			
		}
	} else {

		min = RedisModule_StringPtrLen(argv[++idx], NULL);
		RedisModule_StringToLongLong(argv[++idx], &wc);
		qp->range.min.asChar = min;
		qp->range.lex.minLen = strlen(min);
		qp->range.lex.wildcard = (wc) ? 1 : 0;

		if (qp->range.lex.wildcard && qp->range.lex.minLen == 0) {
			/*
				we've found a wildcard...everything counts
			*/
			// printf("We have a wildcard\n");
			qp->needsSatifying = 0;
			qp->range.max.asChar = RedisModule_StringPtrLen(RedisModule_CreateStringPrintf(ctx, "%s%c",min, 0xff), NULL);;
			qp->range.lex.maxLen = ++qp->range.lex.minLen;

			qp->comp.gt = SpCompWildCard;
			qp->comp.lt = SpCompWildCard;
			// RedisModule_Free(qp->allRanges);
			// RedisModule_Free(qp->comps);
			qp->allRanges = NULL;
			qp->comps = NULL;
			(*argIdx) = idx;
			return REDISMODULE_OK;
		}
		if (wc) {
			//using auto-memory. thi will be cleaned up for us (++bump auto-memory)
			qp->range.max.asChar = RedisModule_StringPtrLen(RedisModule_CreateStringPrintf(ctx, "%s%c",min, 0xff), NULL);;
			qp->range.lex.maxLen = ++qp->range.lex.minLen;
			qp->comp.gt = SpCompLexGTE;
			qp->comp.lt = SpCompLexLTE;
		} else {
			qp->range.max.asChar = min;
			qp->comp.gt = SpCompLexEQ;
			qp->comp.lt = SpCompLexEQ;
		}
		
	}
	(*argIdx) = idx;
	return REDISMODULE_OK;
}

static int SpredisPopulateQueryParts(RedisModuleCtx *ctx, SPIndexCont *cont, SPCompQueryPart *qp, RedisModuleString **argv, int argc) {
	uint8_t count = cont->compCtx->valueCount;
	int parseRes;
	uint8_t type, i;
	long long ltype, qcount;
	int argIdx = 5;

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
		qp[i].valueIndex = i;
		argIdx++;
	}
	return REDISMODULE_OK;
}

static int SPCompGT(SPPtrOrD_t *val, SPCompQueryPart *qp, uint8_t valueCount) {
	for (uint8_t i = 0; i < valueCount; ++i)
	{
		if (!qp[i].comp.gt(val[i], qp[i].range.min, qp[i].range.lex.minLen)) return 0;
	}
	return 1;
}

static int SPCompLT(SPPtrOrD_t *val, SPCompQueryPart *qp, uint8_t valueCount) {
	for (uint8_t i = 0; i < valueCount; ++i)
	{
		if (!qp[i].comp.lt(val[i], qp[i].range.max, qp[i].range.lex.maxLen)) return 0;
	}
	return 1;
}

static void SPCompProduceRes(SPKeyVec *vecp, khash_t(SIDS) *res, khash_t(SIDS) *hint) {
	SPKeyVec vec = vecp[0];
	if (kv_size(vec) == 0) return;
	SPCompositeScoreSetKey *largest = NULL, *current;
	size_t total = 0;
	//TODO: opportunity for parallel processing here?
	if (hint == NULL) {
		for (int i = 0; i < kv_size(vec); ++i)
		{
			total += kh_size(kv_A(vec,i)->members->set);
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
			current = kv_A(vec,i);
			if (current == NULL || current == largest) continue;
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
	// printf("Total found: %zu, %zu\n", total, kh_size(res));
}
SPKeyVec SPCompGetSatisfaction(
	SPKeyVec *candidates, 
	void *ptr, 
	uint8_t valueCount, 
	khash_t(SIDS) *res, 
	khash_t(SIDS) *hint,
	int *defers,
	SPKeyVec *leftOverCand)
{

	SPCompQueryPart *qpa = ptr;
	SPCompQueryPart qp;
	SPKeyVec leavings, leftovers;
	kv_init(leavings);
	size_t li, leavingsCount;
	int satisfyCount = 0, deferCount = 0;

	for (uint8_t i = 0; i < valueCount; ++i)
	{
		qp = qpa[i];
		if (qp.needsSatifying && qp.satisfy) {
			if (satisfyCount == 0) {
				if (qp.satisfy(candidates, &qp, res, hint, &leavings)) {
					deferCount++;
					// geo by radius field
				} else {
					satisfyCount++;
				}
			} else {
				// keep trimming down the leavings until we are done
				kv_init(leftovers);
				if (qp.satisfy(&leavings, &qp, res, hint, &leftovers)) {
					deferCount++;
					// geo by radius field
				} else {
					satisfyCount++;
					leavingsCount = kv_size(leavings);
					for (li = 0; li < leavingsCount; ++li)
					{
						if (kv_A(leavings, li)) {
							kv_push(SPCompositeScoreSetKey*, *candidates, kv_A(leavings, li));
						}
					}
					kv_destroy(leavings);
					leavings = leftovers;
				}
				
			}
		}
	}

	if (satisfyCount > 1 && deferCount == 0) {
		kv_destroy(leftovers);
	} else if (deferCount) {
		(*leftOverCand) = leftovers;
	}
	if (&leavings != candidates) kv_destroy(leavings);
	(*defers) = deferCount;
	return *candidates;
}

static void SpredisDoCompositeSearch(SPIndexCont *cont, 
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
	kbtree_t(COMPIDX) *btree = cont->index.compTree;
	SPCompositeScoreSetKey *l, *u, *use, *candKey, *last;
	SPCompositeScoreSetKey search = {.compCtx = cont->compCtx, .value = value};
	SPCompositeScoreSetKey usearch = {.compCtx = cont->compCtx, .value = uvalue};


	
	kbitr_t itr;
	
	SPKeyVec candidates, leavings;
	kv_init(candidates);
	SPCompQueryComparator *qcs;
	SPCompQueryComparator cqc;
	SPCompQueryPart cqp, first;
	uint32_t cri;
	SPCompRange cr, *firstRange;
	khash_t(PTR) *ptrHash = kh_init(PTR);
	// khint_t k;
	int absent;

	first = qp[0];
	/* 
		do a pass for each value of the first index. 
		if we have a 'in/or' clause up first where values are far apart, we will generate a lot of misses
		example: make in ['A4', 'X3'] will essentially induce a whole index scan which will be very inefficient,
		if it's later in the index, it's not as big of a problem since thre previous values will have shurnken the number of
		candidates
	*/
	for (int i = 0; i < first.rangeCount; ++i)
	{
		
		firstRange = first.allRanges ? &first.allRanges[i] : &first.range;
		use = NULL;
		last = NULL;
		search.value[0] = firstRange->min;
		usearch.value[0] = firstRange->max;
		first.range.min = firstRange->min;
		first.range.max = firstRange->max;

		kb_intervalp(COMPIDX, btree, &search, &l, &u);
		use = u == NULL ? l : u;
		u = NULL;
		kb_intervalp(COMPIDX, btree, &usearch, &l, &u);
		last = u;
		kb_itr_getp(COMPIDX, btree, use, &itr);

	    for (; kb_itr_valid(&itr); kb_itr_next(COMPIDX, btree, &itr)) { 
	    	candKey = (&kb_itr_key(SPCompositeScoreSetKey, &itr));
			if (candKey == NULL) continue;

			//have we run into this key before? we're going to do multiple passes on the first index value, so it's possible
			// k = kh_get(PTR, ptrHash, (uint64_t)candKey);
			// if (k != kh_end(ptrHash)) continue;
			if (kh_contains(PTR, ptrHash, (uint64_t)candKey)) continue;
			if (SPCompGT(candKey->value, qp, valueCount) && SPCompLT(candKey->value, qp, valueCount)) {
				cv = 1;
				doAdd = 1; //we've already tested the first value.
				while (cv < valueCount) {
					cqp = qp[cv];
					qcs = cqp.comps;
					if (qcs != NULL) {
						cri = 0;
						//this is essentially a  list of or's
						while (cri < cqp.rangeCount) {
							cr = cqp.allRanges[cri];
							cqc = qcs[cri];
							if (cqc.gt(candKey->value[cv], cr.min, cr.lex.minLen) && cqc.lt(candKey->value[cv], cr.max, cr.lex.maxLen)) {
								doAdd++;
								break;
							}
							cri++;
						}
					} else {
						doAdd++;
					}
					cv++;
					if (doAdd != cv) break;
				}
				if (doAdd == valueCount) {
					kh_put(PTR, ptrHash, (uint64_t)candKey, &absent);
					kv_push(SPCompositeScoreSetKey*, candidates, candKey);	
				}
	            if (candKey == last) {
	                break;
	            }
	        }

	    }
	}

	
    int defers = 0;
    if (satisfyCount) {
    	candidates = SPCompGetSatisfaction(&candidates, qp, valueCount, res, hint, &defers, &leavings);
    }
    SPCompProduceRes(&candidates, res, hint);
    if (defers) {
    	kv_destroy(leavings);
    }
    kh_destroy(PTR,ptrHash);
    RedisModule_Free(value);
    RedisModule_Free(uvalue);
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
	SPNamespace *ns = NULL;

	SPLockContext(ctx);

	int keyOk = SPGetNamespaceKeyValue(ctx, &key, &ns, argv[1], REDISMODULE_READ);
	
	SPUnlockContext(ctx);
	if (keyOk != REDISMODULE_OK) {
		return keyOk;
	}
	if (ns == NULL) {
		return RedisModule_ReplyWithLongLong(ctx,0);
	}

	khash_t(SIDS) *res = SPGetTempSet(argv[3]);
    khash_t(SIDS) *hint = SPGetHintSet(argv[4]);
    if (hint != NULL && kh_size(hint) == 0) return RedisModule_ReplyWithLongLong(ctx,0);

	SPReadLock(ns->lock);
	SPIndexCont * cont = SPIndexForFieldName(ns, argv[2]);
	if (cont == NULL || cont->type != SPCompIndexType) {
		SPReadUnlock(ns->lock);
		if (cont != NULL) return RedisModule_ReplyWithError(ctx, "wrong index type");
		return RedisModule_ReplyWithLongLong(ctx,0);	
	}

	SPReadLock(cont->lock);
	if (cont->compCtx->valueCount == 0) {
		SPReadUnlock(ns->lock);
		SPReadUnlock(cont->lock);
		printf("err %d\n", 16);
		return RedisModule_ReplyWithLongLong(ctx, 0);
	}
	//start working magic
	SPCompQueryPart *qp = RedisModule_Calloc(cont->compCtx->valueCount, sizeof(SPCompQueryPart));
	int popRes = SpredisPopulateQueryParts(ctx, cont, qp, argv, argc);
	if (popRes == REDISMODULE_OK) {
		SpredisDoCompositeSearch(cont, res, hint, qp);
	} else {
		printf("err %d\n", 17);
	}
	for (uint8_t i = 0; i < cont->compCtx->valueCount; ++i)
	{
		SPCompQueryPart cqp = qp[i];
		if (cqp.allRanges != NULL) RedisModule_Free(cqp.allRanges);
		if (cqp.comps != NULL) RedisModule_Free(cqp.comps);
	}
	RedisModule_Free(qp);
	//end working magic
	SPReadUnlock(cont->lock);
	SPReadUnlock(ns->lock);
	
	RedisModule_ReplyWithLongLong(ctx, kh_size(res));
    return REDISMODULE_OK;
}

int SpredisCompStoreRange_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return SPThreadedWork(ctx, argv, argc, SpredisCompStoreRange_RedisCommandT);
}
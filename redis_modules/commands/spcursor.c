#include "spcursor.h"



static SPPtrOrD_t NULL_STR;
static SPPtrOrD_t NULL_NUM;
static SPPtrOrD_t NULL_GEO;

KHASH_DECLARE(CURSORS, const char*, SPCursor*);
KHASH_MAP_INIT_STR(CURSORS, SPCursor*);

typedef struct _SPCursors {
    SPLock lock;
    khash_t(CURSORS) *cursors;
} SPCursors;

// typedef SPItem* SPItemPtr;

SPItem *SPSetToItems(khash_t(SIDS) *set, size_t *size, SPPtrOrD_t *sortDatas, int sortCount) {
	SPItem *a = RedisModule_Calloc( kh_size(set), sizeof(SPItem) );
	spid_t id;
	SPRecordId rid;
	size_t pos = 0;
	kh_foreach_key(set, id, {
		rid.id = id;
		if (rid.record->exists) {
			a[pos].record = rid.record;
			// printf("%s %d\n", a[pos].record->sid, sortDatas == NULL);
			a[pos].sortData = sortDatas ? &sortDatas[pos * sortCount] : NULL;
			pos++;
		}
	});
	(*size) = pos;
	return a;
}

static SPCursors *CursorHash;
typedef struct _SPItemFieldSort SPItemFieldSort;

typedef void (*SPItemResolver)(SPItem*, SPItemFieldSort*);
typedef int (*SPItemComp)(SPPtrOrD_t a,SPPtrOrD_t b);

int SPDoubleLTASC(SPPtrOrD_t a,SPPtrOrD_t b) {
	return (a.asDouble) < (b.asDouble) ? -1 : (a.asDouble) > (b.asDouble);
	// (((b.asDouble) < (a.asDouble)) - ((a.asDouble) < (b.asDouble)));;
}

int SPDoubleLTDESC(SPPtrOrD_t a,SPPtrOrD_t b) {
	return (b.asDouble) < (a.asDouble) ? -1 : (b.asDouble) > (a.asDouble);
	// return (((a.asDouble) < (b.asDouble)) - ((b.asDouble) < (a.asDouble)));
}

int SPLexLTASC(SPPtrOrD_t a,SPPtrOrD_t b) {
	return a.asChar != b.asChar ? strcmp(a.asChar, b.asChar) : 0;
}

int SPLexLTDESC(SPPtrOrD_t a,SPPtrOrD_t b) {
	return a.asChar != b.asChar ? strcmp(b.asChar, a.asChar) : 0;
}

struct _SPItemFieldSort {
	const char *field;
	long long type;
	int order, fieldOrder, resIndex;
	double lat,lon;
};

typedef struct _SPItemSortCtx {
	SPItemFieldSort *sorts;
	SPItemResolver *resolvers;
	SPItemComp *comps;
	int count;

} SPItemSortCtx;

static inline int SPCursorMCLTCompare(SPItem a, SPItem b, SPItemSortCtx *ctx) {
	int i = 0;
	// printf("SPCursorLTCompare %d\n", ctx->count);
	int res;
	while (i < ctx->count) {
		res = ctx->comps[i]( a.sortData[i], b.sortData[i]); 
		// if ( ctx->comps[i]( a.sortData[i], b.sortData[i] )) {
			// printf("SPCursorLTCompare 1 %d, %d\n", ctx->count, i);
		if (res) return (res < 0);
		// if (res > 0) return 0;
		// }
		// printf("SPCursorLTCompare 2 %d, %d\n", ctx->count, i);
		i++;
	}
	return 0; //(uint64_t)a.record < (uint64_t)b.record;
}

static inline int SPCursorSCLTCompare(SPItem a, SPItem b, SPItemSortCtx *ctx) {
	return ((*ctx->comps)( *a.sortData, *b.sortData) < 0);
}

SPREDIS_SORT_STRUCT_INIT(MCItem, SPItem, SPItemSortCtx*, SPCursorMCLTCompare);
SPREDIS_SORT_STRUCT_INIT(SCItem, SPItem, SPItemSortCtx*, SPCursorSCLTCompare);

void SpredisCursorInit() {
	CursorHash = RedisModule_Calloc(1, sizeof(SPCursors));
	SPLockInit(CursorHash->lock);
	CursorHash->cursors = kh_init(CURSORS);

	NULL_STR = (SPPtrOrD_t){.asChar = ""}; //will need this for sorting
	NULL_NUM = (SPPtrOrD_t){.asDouble = DBL_MIN};  //will need this for sorting
	NULL_GEO = (SPPtrOrD_t){.asUInt = 0}; 
}

int SpredisDeleteCursor_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	int i = 1;
	int res = 0;
	khint_t k;
	const char *name;
	while (i < argc) {
		name = RedisModule_StringPtrLen(argv[i++], NULL);
		SPWriteLock(CursorHash->lock);
		k = kh_get(CURSORS, CursorHash->cursors, name);
		if (k != kh_end(CursorHash->cursors)) {
			if (kh_value(CursorHash->cursors, k)->items) RedisModule_Free(kh_value(CursorHash->cursors, k)->items);
			// if (kh_value(CursorHash->cursors, k)->sortData) RedisModule_Free(kh_value(CursorHash->cursors, k)->sortData);
			name = kh_key(CursorHash->cursors, k);
			kh_del(CURSORS, CursorHash->cursors, k);
			RedisModule_Free( (char *) name);
			RedisModule_Free(kh_value(CursorHash->cursors, k));
			res += 1;
		}
		SPWriteUnlock(CursorHash->lock);
	}
	RedisModule_ReplyWithLongLong(ctx, res);
	return REDISMODULE_OK;
}

void SPDestroyItemSortCtxContents(SPItemSortCtx *sctx) {
	// printf("sort count %d\n", sctx->count);
	for (int i = 0; i < sctx->count; ++i)
	{
		if (sctx->sorts[i].field) RedisModule_Free((char *)sctx->sorts[i].field);
	}
	if (sctx->sorts) RedisModule_Free(sctx->sorts);
	if (sctx->comps) RedisModule_Free(sctx->comps);
	if (sctx->resolvers) RedisModule_Free(sctx->resolvers);

}

void SPGeoSortResolver(SPItem *item, SPItemFieldSort *fs) {
	if (item->record->exists) {
		SPFieldData *vals = &item->record->fields[ fs->fieldOrder ];
		SPPtrOrD_t distance = NULL_NUM;
		double lat, lon;
		if (vals->ilen && vals->iv) {
			SPGeoHashDecode(vals->iv[0].asUInt, &lat, &lon);
			distance.asDouble = SPGetDist(fs->lat, fs->lon, lat, lon);
		}
		item->sortData[ fs->resIndex ] = distance;
	} else {
		item->sortData[ fs->resIndex ] = NULL_GEO;
	}
}

void SPValueSortResolver(SPItem *item, SPItemFieldSort *fs) {
	SPPtrOrD_t val = NULL_NUM;
	if (item->record->exists) {
		SPFieldData *vals = &item->record->fields[fs->fieldOrder];
		if (vals->ilen && vals->iv) {
			val = vals->iv[0];
		}
	}
	item->sortData[ fs->resIndex ] = val;
}

void SPLexSortResolver(SPItem *item, SPItemFieldSort *fs) {
	SPPtrOrD_t val = NULL_STR;
	if (item->record->exists) {
		SPFieldData *vals = &item->record->fields[fs->fieldOrder];
		if (vals->alen && vals->av) {
			val = vals->av[0];
		} else if (vals->ilen && vals->iv) {
			val = vals->iv[0];
		}	
	}
	
	item->sortData[ fs->resIndex ] = val;
}

int SPPrepareSortData(RedisModuleCtx *ctx, RedisModuleString **argv, int sortCount, int argLimit, SPItemSortCtx *sctx, SPNamespace *ns) {
	

	sctx->sorts = RedisModule_Calloc(sortCount, sizeof(SPItemFieldSort));
	sctx->comps = RedisModule_Calloc(sortCount, sizeof(SPItemComp));
	sctx->resolvers = RedisModule_Calloc(sortCount, sizeof(SPItemResolver));

	sctx->count = sortCount;
	// sctx->resolverCount = 0;
	// int exprResIndex = 0;
	int idx = 0;
	// long long type;
	SPFieldDef *fd;
	SPItemFieldSort *fs;
	for (int i = 0; i < sortCount; ++i)
	{
		fs = &sctx->sorts[i];
		RedisModule_StringToLongLong(argv[idx++], &fs->type);
		// fs->exprResIndex = -1;
		fs->resIndex = i;
		fs->field =  RedisModule_Strdup( RedisModule_StringPtrLen( argv[idx++], NULL) );
		fs->order = strcasecmp("asc", RedisModule_StringPtrLen(argv[idx++], NULL)) ? 1 : 0; 
		//0 ascending, 1 descending
		fd = SPFieldDefForCharName(ns, fs->field);

		if (fd->fieldType == SPLexPart) {
			sctx->comps[i] = fs->order ? SPLexLTDESC : SPLexLTASC;
		} else {
			sctx->comps[i] = fs->order ? SPDoubleLTDESC : SPDoubleLTASC;
		}
		fs->fieldOrder = SPFieldIndex(ns, fs->field);


		if (fs->fieldOrder < 0) return REDISMODULE_ERR;
		if (fs->type == 1) {
			//distance sort
			sctx->resolvers[i] = SPGeoSortResolver;
			RedisModule_StringToDouble(argv[idx++], &fs->lat);
			RedisModule_StringToDouble(argv[idx++], &fs->lon);
		} else {
			sctx->resolvers[i] = fd->fieldType == SPLexPart ? SPLexSortResolver : SPValueSortResolver;
		}
		// printf("name:%s fieldOrder:%d resIndex:%d order:%d fieldType:%d\n", fs->field, fs->fieldOrder, fs->resIndex, fs->order, fd->fieldType);
	}
	

	return REDISMODULE_OK;
}
typedef struct _SPItemResolveArg {
	size_t start, end;
	SPItem *items;
	SPItemSortCtx *sctx;	
} SPItemResolveArg;

static void DOSPResolveMCSortValues(void *arg) {
	SPItemResolveArg *ra = arg;
	int scnt = ra->sctx->count;
	// printf("scnt %d\n", scnt);
	SPItem *item, *items = ra->items;;
	SPItemFieldSort *sorts = ra->sctx->sorts;
	SPItemResolver *resolvers = ra->sctx->resolvers;
	size_t start = ra->start, end = ra->end;
	int k;
	while (start < end)
	{
		item = &items[start++];
		for (k = 0; k < scnt; ++k)
		{
			resolvers[k](item, &sorts[k]);
		}
	}
}

static void DOSPResolveMCResumeSortValues(void *arg) {
	SPItemResolveArg *ra = arg;
	int scnt = ra->sctx->count;
	// printf("scnt %d\n", scnt);
	SPItem *item, *items = ra->items;;
	SPItemFieldSort *sorts = ra->sctx->sorts;
	SPItemResolver *resolvers = ra->sctx->resolvers;
	size_t start = ra->start, end = ra->end;
	int k;
	while (start < end)
	{
		item = &items[start++];
		for (k = 1; k < scnt; ++k)
		{
			resolvers[k](item, &sorts[k]);
		}
	}
}

static void DOSPResolveSCSortValues(void *arg) {
	SPItemResolveArg *ra = arg;
	// int scnt = ra->sctx->count;
	// printf("scnt %d\n", scnt);
	SPItem *item, *items = ra->items;;
	SPItemFieldSort *sorts = ra->sctx->sorts;
	SPItemResolver *resolvers = ra->sctx->resolvers;
	size_t start = ra->start, end = ra->end;

	while (start < end)
	{
		item = &items[start++];
		(*resolvers)(item, sorts);
	}
}
static void SPResolveSortValues(size_t count, SPItem *items, SPItemSortCtx *sctx, void (*resolve)(void*)) {
	// printf("count: %zu\n", count);
	if (count < SP_PTHRESHOLD) {
	// if (count < 100000000) {
		SPItemResolveArg ra = {.start=0, .end=count, .items = items, .sctx = sctx};
		resolve(&ra);
		// if (sctx->count > 1) {
		// 	DOSPResolveMCSortValues(&ra);
		// } else {
		// 	DOSPResolveSCSortValues(&ra);
		// }
	} else {
		//Let's do some parallel processing
		size_t start = 0;
        size_t incr = count / SP_PQ_TCOUNT_SIZE;
        SPItemResolveArg *arg;

        // SPItemResolveArg **pargs = RedisModule_Calloc(SP_PQ_TCOUNT_SIZE, sizeof(SPItemResolveArg*));
        SPItemResolveArg *pargs[SP_PQ_TCOUNT_SIZE]; 
        void (*func[SP_PQ_TCOUNT_SIZE])(void*);
        for (int j = 0; j < SP_PQ_TCOUNT_SIZE; ++j)
        {
            func[j] = resolve;//(sctx->count > 1) ? DOSPResolveMCSortValues : DOSPResolveSCSortValues;
            arg = RedisModule_Calloc(1, sizeof(SPItemResolveArg));
            pargs[j] = arg;
            arg->start = start;
            arg->items = items;
            arg->sctx = sctx;
            if (j == (SP_PQ_TCOUNT_SIZE - 1)) {
                arg->end = count;
            } else {
                start += incr;
                arg->end = start;
            }
        }
        SPDoWorkInParallel(func,(void **)pargs,SP_PQ_TCOUNT_SIZE);
        for (int j = 0; j < SP_PQ_TCOUNT_SIZE; ++j)
        {
        	RedisModule_Free(pargs[j]);
        }
        // RedisModule_Free(pargs);
	}
}


int SpredisPrepareCursor_RedisCommandT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

	RedisModuleKey *key;
    SPNamespace *ns = NULL;
    SPLockContext(ctx);
    int keyOk = SPGetNamespaceKeyValue(ctx, &key, &ns, argv[1], REDISMODULE_READ);
    SPUnlockContext(ctx);
    if (keyOk != REDISMODULE_OK) {
        return keyOk;
    }
    if (ns == NULL) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    }
    khash_t(SIDS) *result = SPGetTempSet(argv[3]);

    // unused at the moment
    RedisModuleString *lang;
	lang =  argv[4];

	long long read;
	int sortCount = 0, start = 0, end = 0, idsOnly = 0, parseOk, exprCount;

	parseOk = RedisModule_StringToLongLong(argv[5], &read);
	sortCount = parseOk == REDISMODULE_OK ? (int)read : 0;

	parseOk = RedisModule_StringToLongLong(argv[6], &read);
	exprCount = parseOk == REDISMODULE_OK ? (int)read : 0;

	parseOk = RedisModule_StringToLongLong(argv[7], &read);
	start = parseOk == REDISMODULE_OK ? (int)read : 0;
	parseOk = RedisModule_StringToLongLong(argv[8], &read);
	end = parseOk == REDISMODULE_OK ? (int)read : 0;
	parseOk = RedisModule_StringToLongLong(argv[9], &read);
	idsOnly = parseOk == REDISMODULE_OK ? (int)read : 0;

	if (sortCount > SP_MAX_SORT_FIELDS) {
		char buff[256];
		sprintf(buff, "Sort columns exceeded limit of %d", SP_MAX_SORT_FIELDS);
		return RedisModule_ReplyWithError(ctx, buff);
	}
	// printf("E\n");
	SPCursor *cursor = RedisModule_Calloc(1, sizeof(SPCursor));
    
    const char *cursorName = RedisModule_StringPtrLen(argv[2], NULL);

    SPReadLock(ns->lock);
    // SPReadLock(ns->rs->lock);
    // SPReadLock(ns->rs->deleteLock);
    // printf("F\n");
    
    // printf("Preparing sort data\n");
    // long long tstart = RedisModule_Milliseconds();
    SPPtrOrD_t *sd = sortCount ? RedisModule_Alloc(kh_size(result) * sortCount * sizeof(SPSortData)) : NULL;
    size_t len;
    SPReadLock(ns->rs->lock);
    SPReadLock(ns->rs->deleteLock);
    SPItem *items = SPSetToItems( result, &len, sd , sortCount);
    SPReadUnlock(ns->rs->deleteLock);
    SPReadUnlock(ns->rs->lock);
    // printf("Alloc and s2a took %llu\n", RedisModule_Milliseconds() - tstart);
    // printf("Done with result %s, %zu\n", RedisModule_StringPtrLen(argv[3], NULL), kh_size(result));
    cursor->items = items;
    cursor->count = len;
    cursor->created = RedisModule_Milliseconds();
    // printf("cursor %s: %zu\n", cursorName, cursor->count);

    SPWriteLock(CursorHash->lock);
    // printf("Saving cursor %s, %zu\n", cursorName, kh_size(result));

    int absent;
	khint_t k = kh_put(CURSORS, CursorHash->cursors, cursorName, &absent);
	if (absent) {
		kh_key(CursorHash->cursors, k) = RedisModule_Strdup(cursorName);
	} else {
		RedisModule_Free( kh_value(CursorHash->cursors, k)->items ) ;
		RedisModule_Free( kh_value(CursorHash->cursors, k) ) ;
	}
	kh_value(CursorHash->cursors, k) = cursor;
	SPWriteUnlock(CursorHash->lock);
	

	int pres = REDISMODULE_OK;

	if (sortCount) {
		// printf("Preparing sort data, %d\n", sortCount);
		SPItemSortCtx sctx;
		pres = SPPrepareSortData(ctx, argv + 10, sortCount, argc - 10, &sctx, ns);
		sctx.count = sortCount;
		if (pres == REDISMODULE_OK) {
			// if (sctx.resolverCount) {
			// 	//ResolveExpressionValues
			// }
			size_t newStart = start;
			size_t newEnd = end + start;
			if (newEnd > cursor->count) newEnd = cursor->count;
			// tstart = RedisModule_Milliseconds();

			if (sortCount > 1) {
				SPItem a, b;
				SPItemComp comp = sctx.comps[0];
				if (cursor->count < 128 || newEnd >= cursor->count) { //small list have little effect, let's keep it simple
					
					SPReadLock(ns->rs->deleteLock);
					SPResolveSortValues(cursor->count, cursor->items, &sctx, DOSPResolveMCSortValues);	
					SPReadUnlock(ns->rs->deleteLock);

					SpredisMCItemSort(cursor->count, cursor->items, &sctx);	
				} else {
					/*
						at minimum, we need to sort evrything by the first column,
						but we only need to sort by the rest of the columns for the items that the 
						client has asked for.

						we're going to sort the entire list but the first column and then get our bearings

						if we decide to do persistent cursors down the road, we'll have to save the state 
						of the sort, as well as the sort data itself, 
						then do incremental sort for each page requested
							in this scenario we're doing a bit more work than if we just sorted everything from
							the get go, but we'll spread the cpu cycles out over time
							most searches never get past the first page or 2 anyway
					*/
					SPReadLock(ns->rs->deleteLock);
					SPResolveSortValues(cursor->count, cursor->items, &sctx, DOSPResolveSCSortValues);
					SPReadUnlock(ns->rs->deleteLock);

					SpredisSCItemSort(cursor->count, cursor->items, &sctx);

					/*
						we need to find the beginning of the series of the first value in the result
						as values may move up/down in the list based off the secondary sort columns
					*/
					while (newStart) {
						a = cursor->items[newStart];
						b = cursor->items[newStart - 1];
						if (!comp(*a.sortData, *b.sortData)) break;
						newStart--;
					}
					/*
						we need to find the end of the series of the last value in the result
						as values may move up/down in the list based off the secondary sort columns
				 	*/
					while (newEnd < cursor->count - 1) {
						a = cursor->items[newEnd];
						b = cursor->items[newEnd + 1];
						if (!comp(*a.sortData, *b.sortData)) break;
						newEnd++;
					}
					// make sure we grab the rest of the values we need to sort off of
					SPReadLock(ns->rs->deleteLock);
					SPResolveSortValues(newEnd - newStart, cursor->items + newStart, &sctx, DOSPResolveMCResumeSortValues);
					SPReadUnlock(ns->rs->deleteLock);
					/*
						now resort the section, we need to keep the first column 
						as the section likely contains multiple series
							if we only sort off the extra columns, we'll lose our initial order
						note* 	it might be better to do a merge sort here 
								as the values are partially sorted already,
								but this is already complicated enough
								there are also memory concerns with a merge sort
					*/
					SpredisMCItemSort(newEnd - newStart, cursor->items + newStart, &sctx);
				}
				
			} else {
				SPReadLock(ns->rs->deleteLock);
				SPResolveSortValues(cursor->count, cursor->items, &sctx, DOSPResolveSCSortValues);	
				SPReadUnlock(ns->rs->deleteLock);

				SpredisSCItemSort(cursor->count, cursor->items, &sctx);
			}
			

			// printf("Sort took %llu\n", RedisModule_Milliseconds() - tstart);
		}
		SPDestroyItemSortCtxContents(&sctx);
	}

	// SPReadLock(ns->rs->lock);
	SPReadLock(ns->rs->deleteLock);
	if (pres == REDISMODULE_OK) {
		RedisModule_ReplyWithArray(ctx, 2);
		RedisModule_ReplyWithLongLong(ctx, cursor->count);
		int replyLen = 0;

		if (end) {
			end += start;
			RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
			SPItem *item;
			RedisModuleString *tmp;
			SPRecordId rid;
			SPRecord *record;
			// SPPackContId pid;
			// SPPackCont *pc;
			// int absent;
			char *doc;
			if (idsOnly) {
				while (start < end && start < cursor->count) {
					item = &cursor->items[start++];
					record = item->record;
					if (!record->exists) continue;
					tmp = RedisModule_CreateString(ctx, record->sid, strlen(record->sid));
					RedisModule_ReplyWithString(ctx, tmp);
					RedisModule_FreeString(ctx, tmp);
					replyLen++;
				}
			} else {
				while (start < end && start < cursor->count) {
					item = &cursor->items[start++];
					record = item->record;
					if (!record->exists) continue;
					rid.record = record;

					// pid.pc = record->pc;
					doc = SP_UNPACK_DOC(ns->rs, rid, 1);
					// kh_put(SIDS, packs, pid.id, &absent);
					tmp = RedisModule_CreateString(ctx, doc, strlen(doc));
					RedisModule_ReplyWithString(ctx, tmp);
					RedisModule_FreeString(ctx, tmp);
					RedisModule_Free(doc);
					replyLen++;
				}

			}
			RedisModule_ReplySetArrayLength(ctx, replyLen);
		} else {
			RedisModule_ReplyWithNull(ctx);
		}
	}
    SPReadUnlock(ns->rs->deleteLock);
    // SPReadUnlock(ns->rs->lock);
    
    SPReadUnlock(ns->lock);
    // printf("I\n");
    if (sd) {
		RedisModule_Free(sd);
	}

    if (pres != REDISMODULE_OK) {
    	return RedisModule_ReplyWithError(ctx, "Bad field name in sorts");
    }
    return REDISMODULE_OK;
	
	
}

SPCursor *SPGetCursor(RedisModuleString *tname) {
	const char *name = RedisModule_StringPtrLen(tname, NULL);
	SPReadLock(CursorHash->lock);
	khint_t k = kh_get(CURSORS, CursorHash->cursors, name);
	SPCursor *c = NULL;
	if (k != kh_end(CursorHash->cursors)) {
		c = kh_value(CursorHash->cursors, k);
	}
	SPReadUnlock(CursorHash->lock);
	return c;
}

long long SPOldestCursorTime() {
	long long time = LLONG_MAX;
	SPReadLock(CursorHash->lock);
	const char *id;
	SPCursor *c;
	kh_foreach(CursorHash->cursors, id, c, {
		time = c->created < time ? c->created : time;
	});
	SPReadUnlock(CursorHash->lock);
	return time;
}

int SpredisPrepareCursor_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	// return RedisModule_ReplyWithLongLong(ctx, 0);
	return SPThreadedWork(ctx, argv, argc, SpredisPrepareCursor_RedisCommandT);
}
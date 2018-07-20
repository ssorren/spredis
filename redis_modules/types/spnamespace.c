#include "../spredis.h"
#include "../lib/khsort.h"

KSORT_INIT_STR;

static SPTmpSets *TempHash;
// static SPCursors *CursorHash;

#define SP_FD_ARG_CNT 6
static const char *INDEXTYPE_DESC[3] = {"btree", "composite-btree", "id-hash"};
static const char *FIELDTYPE_DESC[5] = {"composite", "geo", "number", "lex", "id"};

#define SPGeoPart 1
#define SPDoublePart 2
#define SPLexPart 3
#define SPIDPart 4

void SPSaveFieldToRDB(RedisModuleCtx *ctx, RedisModuleIO *io, SPFieldDef *fd);
SPFieldDef *SPReadFieldFromRDB(RedisModuleCtx *ctx, RedisModuleIO *io, SPNamespace *ns);
// static const char *EMPTY_STRING = "";

static inline const char *SPNSUniqStr(SPNamespace *ns, const char *str) {
    if (str == NULL) return NULL;
    khint_t k = kh_get(STR, ns->uniq, str);
    if (k == kh_end(ns->uniq)) {
        int a;
        const char *key = RedisModule_Strdup(str);
        kh_put(STR, ns->uniq, key, &a);
        return key;
    }
    return kh_key(ns->uniq, k);
}

RedisModuleString** SPDoubleRewriteArgs(RedisModuleCtx *ctx,SPRecordId rid, SPFieldData* data, SPFieldDef* fd, int *resLen) {
	int len = 2 + data->ilen;
	RedisModuleString **args = RedisModule_Alloc(len * sizeof(RedisModuleString*));
	args[0] = RedisModule_CreateString(ctx, fd->name, strlen(fd->name));
	args[1] = RedisModule_CreateStringPrintf(ctx, "%d", data->ilen);
	for (int i = 2, idx = 0; i < len; ++i, ++idx)
	{
		args[i] = RedisModule_CreateStringPrintf(ctx, "%1.17g", data->iv[idx].asDouble);
	}
	(*resLen) = len;
	return args;
}

RedisModuleString** SPGeoRewriteArgs(RedisModuleCtx *ctx,SPRecordId rid, SPFieldData* data, SPFieldDef* fd, int *resLen) {
	int len = 2 + (data->ilen * 2);
	RedisModuleString **args = RedisModule_Alloc(len * sizeof(RedisModuleString*));
	args[0] = RedisModule_CreateString(ctx, fd->name, strlen(fd->name));
	args[1] = RedisModule_CreateStringPrintf(ctx, "%d", data->ilen);
	double lat, lon;
	int i = 2;
	for (int idx = 0; idx < data->ilen; ++idx)
	{
		SPGeoHashDecode(data->iv[idx].asUInt, &lat, &lon);
		args[i++] = RedisModule_CreateStringPrintf(ctx, "%1.17g", lat);
		args[i++] = RedisModule_CreateStringPrintf(ctx, "%1.17g", lon);
	}
	(*resLen) = len;
	return args;
}

RedisModuleString** SPLexRewriteArgs(RedisModuleCtx *ctx,SPRecordId rid, SPFieldData* data, SPFieldDef* fd, int *resLen) {
	int len = 2;
	int valLen = 0;
	//each value takes 2 args - index value and actual value (for facets)
	if (fd->suffix && data->ilen) {
		// we have suffixes, ilen is already doubled
		len += data->ilen;
		// suffix values are not sent over the wire...we only want to look at the first 1/2 of the values
		valLen = (data->ilen / 2);
	} else {
		// a non suffixed field, arg len *= 2
		len += (data->ilen * 2);
		valLen = data->ilen;
	}

	RedisModuleString **args = RedisModule_Alloc(len * sizeof(RedisModuleString*));
	args[0] = RedisModule_CreateString(ctx, fd->name, strlen(fd->name));
	args[1] = RedisModule_CreateStringPrintf(ctx, "%d", data->ilen);
	
	int i = 2;
	for (int idx = 0; idx < valLen; ++idx)
	{
		//index values
		args[i++] = RedisModule_CreateStringPrintf(ctx, data->iv[idx].asChar, strlen(data->iv[idx].asChar));
	}
	for (int idx = 0; idx < valLen; ++idx)
	{
		//actual values
		if (data->av && idx < data->alen) {
			args[i++] = RedisModule_CreateStringPrintf(ctx, data->av[idx].asChar, strlen(data->av[idx].asChar));
		} else {
			// if we're not tracking actual values (full text fields), send an empty string
			args[i++] = RedisModule_CreateString(ctx, "", 0);
		}
	}
	(*resLen) = len;
	return args;
}

int SPFieldIndex(SPNamespace *ns, const char *name) {
	for (int i = 0; i < kv_size(ns->fields); ++i)
	{
		if (!strcmp(name, kv_A(ns->fields, i))) return i;
	}
	return -1;
}

int SPDoubleArgTx(RedisModuleCtx *ctx, int valueCount, RedisModuleString **args, SPFieldData *data, SPFieldDef *fd, SPNamespace *ns) {
	data->iv = RedisModule_Calloc(valueCount, sizeof(SPPtrOrD_t));
	data->av = NULL;
	data->ilen = valueCount;
	data->alen = 0;
	double val;
	for (int i = 0; i < valueCount; ++i)
	{
		if (SpredisStringToDouble(args[i], &val)) {
			val = DBL_MIN;
		}
		data->iv[i].asDouble = val;
	}
	return 0;
}
int SPGeoArgTx(RedisModuleCtx *ctx, int valueCount, RedisModuleString **args, SPFieldData *data, SPFieldDef *fd,  SPNamespace *ns) {
	data->iv = RedisModule_Calloc(valueCount, sizeof(SPPtrOrD_t));
	data->av = NULL;
	data->ilen = valueCount;
	data->alen = 0;
	double lat, lon;
	valueCount *= 2;
	int idx = 0;
	for (int i = 0; i < valueCount; ++i, ++idx)
	{
		if (SpredisStringToDouble(args[i++], &lat)) {
			lat = 0;
		}
		if (SpredisStringToDouble(args[i], &lon)) {
			lon = 0;
		}
		data->iv[idx].asUInt = SPGeoHashEncode(lat, lon);
	}
	return 0;
}

int SPLexArgTx(RedisModuleCtx *ctx,int valueCount, RedisModuleString **args, SPFieldData *data, SPFieldDef *fd,  SPNamespace *ns) {
	data->iv = RedisModule_Calloc(valueCount * (fd->suffix ? 2 : 1), sizeof(SPPtrOrD_t));
	data->av = fd->fullText ? NULL : RedisModule_Calloc(valueCount, sizeof(SPPtrOrD_t));
	data->ilen = valueCount * (fd->suffix ? 2 : 1);
	data->alen = fd->fullText ? 0 : valueCount;
	int count = valueCount * 2;
	int idx = 0;
	
	for (int i = 0; i < count; ++i, ++idx)
	{
		data->iv[idx].asChar  = SPNSUniqStr(ns,  RedisModule_StringPtrLen(args[i++], NULL) );
		// printf("%s = %s : %llu\n", fd->name, data->iv[idx].asChar, data->iv[idx].asUInt);
		if (data->av) data->av[idx].asChar = SPNSUniqStr(ns,  RedisModule_StringPtrLen(args[i], NULL) );
	}
	if (fd->suffix) {
		for (int i = 0; i < count; ++i, ++idx)
		{
			char *suffix = SPToSuffix(ctx, RedisModule_StringPtrLen(args[i++], NULL));
			data->iv[idx].asChar  = SPNSUniqStr(ns,  suffix );
			RedisModule_Free(suffix);
		}
	}
	return 0;
}


#define SP_WRITE_FD_COUNTS(io, data) RedisModule_SaveUnsigned(io, data->ilen); RedisModule_SaveUnsigned(io, data->alen)

int SPWriteDoubleFieldData(RedisModuleCtx *ctx, RedisModuleIO *io, SPRecord *record, SPFieldDef *fd, SPNamespace *ns, int fieldIndex) {
	SPFieldData *data = &record->fields[fieldIndex];

	SP_WRITE_FD_COUNTS(io, data);
	for (uint16_t k = 0; k < data->ilen; ++k)
	{
		RedisModule_SaveDouble(io, data->iv[k].asDouble);
	}
	for (uint16_t k = 0; k < data->alen; ++k)
	{
		RedisModule_SaveDouble(io, data->av[k].asDouble);
	}
	
	return 0;
}
int SPReadDoubleFieldData(RedisModuleCtx *ctx, RedisModuleIO *io, SPRecord *record, SPFieldDef *fd, SPNamespace *ns, int fieldIndex) {
	SPFieldData *data = &record->fields[fieldIndex];
	data->ilen = RedisModule_LoadUnsigned(io);
	data->alen = RedisModule_LoadUnsigned(io);
	data->iv = data->ilen ? RedisModule_Calloc(data->ilen, sizeof(SPPtrOrD_t)) : NULL;
	data->av = data->alen ? RedisModule_Calloc(data->alen, sizeof(SPPtrOrD_t)) : NULL;
	// printf("double lens %d, %d\n", data->ilen, data->alen );
	for (uint16_t k = 0; k < data->ilen; ++k)
	{
		
		data->iv[k].asDouble = RedisModule_LoadDouble(io);
		// printf("%f\n", data->iv[k].asDouble);
	}

	for (uint16_t k = 0; k < data->alen; ++k)
	{
		data->av[k].asDouble = RedisModule_LoadDouble(io);
	}


	return 0;
}

int SPWriteGeoFieldData(RedisModuleCtx *ctx, RedisModuleIO *io, SPRecord *record, SPFieldDef *fd, SPNamespace *ns, int fieldIndex) {
	SPFieldData *data = &record->fields[fieldIndex];

	SP_WRITE_FD_COUNTS(io, data);

	for (uint16_t k = 0; k < data->ilen; ++k)
	{
		RedisModule_SaveUnsigned(io, data->iv[k].asUInt);
	}
	for (uint16_t k = 0; k < data->alen; ++k)
	{
		RedisModule_SaveUnsigned(io, data->av[k].asUInt);
	}
	return 0;
}
int SPReadGeoFieldData(RedisModuleCtx *ctx, RedisModuleIO *io, SPRecord *record, SPFieldDef *fd, SPNamespace *ns, int fieldIndex) {
	SPFieldData *data = &record->fields[fieldIndex];
	data->ilen = RedisModule_LoadUnsigned(io);
	data->alen = RedisModule_LoadUnsigned(io);
	data->iv = data->ilen ? RedisModule_Calloc(data->ilen, sizeof(SPPtrOrD_t)) : NULL;
	data->av = data->alen ? RedisModule_Calloc(data->alen, sizeof(SPPtrOrD_t)) : NULL;
	
	// printf("geo lens %d, %d\n", data->ilen, data->alen );
	for (uint16_t k = 0; k < data->ilen; ++k)
	{
		data->iv[k].asUInt = RedisModule_LoadUnsigned(io);
	}

	for (uint16_t k = 0; k < data->alen; ++k)
	{
		data->av[k].asUInt = RedisModule_LoadUnsigned(io);
	}

	return 0;
}

int SPWriteLexFieldData(RedisModuleCtx *ctx, RedisModuleIO *io, SPRecord *record, SPFieldDef *fd, SPNamespace *ns, int fieldIndex) {
	SPFieldData *data = &record->fields[fieldIndex];
	
	SP_WRITE_FD_COUNTS(io, data);
	for (uint16_t k = 0; k < data->ilen; ++k)
	{
		RedisModule_SaveStringBuffer(io, data->iv[k].asChar, strlen(data->iv[k].asChar));
	}
	for (uint16_t k = 0; k < data->alen; ++k)
	{
		RedisModule_SaveStringBuffer(io, data->av[k].asChar, strlen(data->av[k].asChar));
	}
	
	return 0;
}
int SPReadLexFieldData(RedisModuleCtx *ctx, RedisModuleIO *io, SPRecord *record, SPFieldDef *fd, SPNamespace *ns, int fieldIndex) {
	SPFieldData *data = &record->fields[fieldIndex];
	data->ilen = RedisModule_LoadUnsigned(io);
	data->alen = RedisModule_LoadUnsigned(io);
	data->iv = data->ilen ? RedisModule_Calloc(data->ilen, sizeof(SPPtrOrD_t)) : NULL;
	data->av = data->alen ? RedisModule_Calloc(data->alen, sizeof(SPPtrOrD_t)) : NULL;
	RedisModuleString *tmp;
	// printf("lex lens %d, %d\n", data->ilen, data->alen );
	for (uint16_t k = 0; k < data->ilen; ++k)
	{
		tmp = RedisModule_LoadString(io);
		data->iv[k].asChar = SPNSUniqStr(ns,  RedisModule_StringPtrLen(tmp, NULL) );
		// printf("%s\n", data->iv[k].asChar);
		RedisModule_FreeString(ctx, tmp);
		
	}

	for (uint16_t k = 0; k < data->alen; ++k)
	{
		tmp = RedisModule_LoadString(io);
		data->av[k].asChar = SPNSUniqStr(ns,  RedisModule_StringPtrLen(tmp, NULL) );
		// printf("%s\n", tmp);
		RedisModule_FreeString(ctx, tmp);
	}
	
	return 0;
}



void SPAddDoubleToIndex(SPRecordId rid, SPFieldData *data, SPFieldDef *fd, SPNamespace *ns) {
	if (!fd->index) return;
	if (data == NULL) {printf("NULL data\n"); return;}
	SPIndexCont *cont =  SPIndexForFieldCharName(ns, fd->name);
	if (cont == NULL) {printf("NULL cont\n"); return;};
	SPWriteLock(cont->lock);
	for (int i = 0; i < data->ilen; ++i)
	{
		SPAddScoreToSet(cont->index.btree, NULL, rid.id, data->iv[i]);
	}

	SPWriteUnlock(cont->lock);
}

void SPRemDoubleFromIndex(SPRecordId rid, SPFieldData *data, SPFieldDef *fd, SPNamespace *ns) {
	if (!fd->index) return;
	if (data == NULL) {printf("NULL data\n"); return;}
	SPIndexCont *cont =  SPIndexForFieldCharName(ns, fd->name);
	if (cont == NULL) {printf("NULL cont\n"); return;};
	SPWriteLock(cont->lock);
	for (int i = 0; i < data->ilen; ++i)
	{
		SPRemScoreFromSet(cont->index.btree, NULL, rid.id, data->iv[i]);
	}
	SPWriteUnlock(cont->lock);
}

void SPAddGeoToIndex(SPRecordId rid, SPFieldData *data, SPFieldDef *fd, SPNamespace *ns) {
	if (!fd->index) return;
	if (data == NULL) {printf("NULL data\n"); return;}
	SPIndexCont *cont =  SPIndexForFieldCharName(ns, fd->name);
	if (cont == NULL) {printf("NULL cont\n"); return;};
	SPWriteLock(cont->lock);
	for (int i = 0; i < data->ilen; ++i)
	{
		SPAddGeoScoreToSet(cont->index.btree, NULL, rid.id, data->iv[i]);
	}
	
	SPWriteUnlock(cont->lock);
}

void SPRemGeoFromIndex(SPRecordId rid, SPFieldData *data, SPFieldDef *fd, SPNamespace *ns) {
	if (!fd->index) return;
	if (data == NULL) {printf("NULL data\n"); return;}
	SPIndexCont *cont =  SPIndexForFieldCharName(ns, fd->name);
	if (cont == NULL) {printf("NULL cont\n"); return;};
	SPWriteLock(cont->lock);
	for (int i = 0; i < data->ilen; ++i)
	{
		SPRemGeoScoreFromSet(cont->index.btree, NULL, rid.id, data->iv[i]);
	}
	
	SPWriteUnlock(cont->lock);
}

void SPAddLexToIndex(SPRecordId rid, SPFieldData *data, SPFieldDef *fd, SPNamespace *ns) {
	if (!fd->index) return;
	if (data == NULL) {printf("NULL data\n"); return;}
	SPIndexCont *cont =  SPIndexForFieldCharName(ns, fd->name);
	if (cont == NULL) {printf("NULL cont\n"); return;};
	SPWriteLock(cont->lock);
	for (int i = 0; i < data->ilen; ++i)
	{
		
		if (data->iv) {
			SPAddLexScoreToSet(cont->index.btree, NULL, rid.id, data->iv[i], NULL);
		} else {
			printf("WTF2\n");
		}
	}
	
	SPWriteUnlock(cont->lock);
}

void SPRemLexFromIndex(SPRecordId rid, SPFieldData *data, SPFieldDef *fd, SPNamespace *ns) {
	if (!fd->index) return;
	if (data == NULL) {printf("NULL data\n"); return;}
	SPIndexCont *cont =  SPIndexForFieldCharName(ns, fd->name);
	if (cont == NULL) {printf("NULL cont\n"); return;};
	SPWriteLock(cont->lock);
	for (int i = 0; i < data->ilen; ++i)
	{
		SPRemLexScoreFromSet(cont->index.btree, NULL, rid.id, data->iv[i]);
	}
	
	SPWriteUnlock(cont->lock);
}

static inline void SPAssignExtras(SPFieldDef *fd) {
	switch (fd->fieldType) {
		case SPGeoPart:
			// indexCont->compCtx->compare[i] = SPGeoPartCompare;
			fd->argCount = 2;
			fd->argTx = SPGeoArgTx;
			fd->dbWrite = SPWriteGeoFieldData;
		    fd->dbRead = SPReadGeoFieldData;
		    fd->addIndex = SPAddGeoToIndex;
		    fd->remIndex = SPRemGeoFromIndex;
		    fd->rewrite = SPGeoRewriteArgs;
			break;
		case SPDoublePart:
			// indexCont->compCtx->compare[i] = SPDoublePartCompare;
			fd->argCount = 1;
			fd->argTx = SPDoubleArgTx;
			fd->dbWrite = SPWriteDoubleFieldData;
		    fd->dbRead = SPReadDoubleFieldData;

		    fd->addIndex = SPAddDoubleToIndex;
		    fd->remIndex = SPRemDoubleFromIndex;
		    fd->rewrite = SPDoubleRewriteArgs;
			break;
		case SPLexPart:
			fd->argCount = 2;
			fd->argTx = SPLexArgTx;
			fd->dbWrite = SPWriteLexFieldData;
		    fd->dbRead = SPReadLexFieldData;

		    fd->addIndex = SPAddLexToIndex;
		    fd->remIndex = SPRemLexFromIndex;
		    fd->rewrite = SPLexRewriteArgs;
			// indexCont->compCtx->compare[i] = SPLexPartCompare;
			break;
		//ignoring id types for now
	}
}
void SpredisInitMaster() {
	TempHash = RedisModule_Calloc(1, sizeof(SPTmpSets));
	SPLockInit(TempHash->lock);
	TempHash->sets =  kh_init(TMPSETS);
}

khash_t(SIDS) *SPGetTempSet(RedisModuleString *tname) {
	const char *name = RedisModule_StringPtrLen(tname, NULL);
	SPUpReadLock(TempHash->lock);
	khint_t k = kh_get(TMPSETS, TempHash->sets, name);
	if (k == kh_end(TempHash->sets)) {
		SPUpgradeLock(TempHash->lock);
		int absent;
		k = kh_put(TMPSETS, TempHash->sets, RedisModule_Strdup(name), &absent);
		kh_value(TempHash->sets, k) = kh_init(SIDS);
	}
	SPWriteUnlock(TempHash->lock);
	return kh_value(TempHash->sets, k);
}

khash_t(SIDS) *SPCreateTempSet(RedisModuleString *tname, khash_t(SIDS) *res) {
	const char *name = RedisModule_StringPtrLen(tname, NULL);
	SPWriteLock(TempHash->lock);
	int absent;
	khint_t k = kh_put(TMPSETS, TempHash->sets, name, &absent);
	if (absent) {
		kh_key(TempHash->sets, k) = RedisModule_Strdup(name);
		kh_value(TempHash->sets, k) = res;
	} else {
		if (res != kh_value(TempHash->sets, k)) kh_destroy(SIDS, kh_value(TempHash->sets, k));
		kh_value(TempHash->sets, k) = res;
	}
	SPWriteUnlock(TempHash->lock);
	return res;
}

khash_t(SIDS) *SPGetHintSet(RedisModuleString *tname) {
	size_t len;
	const char *name = RedisModule_StringPtrLen(tname, &len);
	if (len == 0) return NULL;
	SPReadLock(TempHash->lock);
	khint_t k = kh_get(TMPSETS, TempHash->sets, name);
	if (k == kh_end(TempHash->sets)) {
		SPReadUnlock(TempHash->lock);	
		return SPGetTempSet(tname);
	}
	SPReadUnlock(TempHash->lock);
	return kh_value(TempHash->sets, k);
}

int SPDestroyTempSet(RedisModuleString *tname) {
	const char *name = RedisModule_StringPtrLen(tname, NULL);
	SPWriteLock(TempHash->lock);
	khint_t k = kh_get(TMPSETS, TempHash->sets, name);
	int res = 0;
	if (k != kh_end(TempHash->sets)) {
		kh_destroy(SIDS, kh_value(TempHash->sets, k));
		name = kh_key(TempHash->sets, k);
		kh_del(TMPSETS, TempHash->sets, k);
		RedisModule_Free( (char *) name);
		res = 1;
	}
	SPWriteUnlock(TempHash->lock);
	return res;
}

int SpredisDeleteTempSet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	int i = 1;
	int res = 0;
	while (i < argc) {
		// printf("Deleteing result %s\n", RedisModule_StringPtrLen(argv[i], NULL));
		res += SPDestroyTempSet(argv[i++]);
	}
	RedisModule_ReplyWithLongLong(ctx, res);
	return REDISMODULE_OK;
}

SPNamespace *SPInitNamespace() {
	SPNamespace *ns = RedisModule_Calloc(1, sizeof(SPNamespace));
	SPLockInit(ns->lock);
	SPLockInit(ns->indexLock);
	// SPLockInit(ns->temps.lock);
	kv_init(ns->fields);
	kv_init(ns->composites);

	ns->uniq = kh_init(STR);

	ns->indeces = kh_init(INDECES);
	ns->defs = kh_init(FIELDS);
	ns->rs = SPCreateRecordSet(ns);
	// ns->temps.sets = kh_init(TMPSETS);
	return ns;
}

void SPCompDestroy (kbtree_t(COMPIDX) *btree) {
	if (!btree) {
		printf("NULL btree\n");
		return;
	}
	kbitr_t itr;
	kb_itr_first(COMPIDX, btree, &itr);
	SPCompositeScoreSetKey *p;
	for (; kb_itr_valid(&itr); kb_itr_next(COMPIDX, btree, &itr)) { // move on
        p = &kb_itr_key(SPCompositeScoreSetKey, &itr);
        if (p && p->members) {
        	if (p->members->set) kh_destroy(SIDS, p->members->set);
        	RedisModule_Free(p->members);
        }
        if (p && p->value) RedisModule_Free(p->value);
    }
    kb_destroy(COMPIDX, btree);
}

void SPDestroyNamespace(SPNamespace *ns) {
	SPWriteLock(ns->lock);
	SPWriteLock(ns->indexLock);
	const char *fid;
	SPIndexCont *indexCont;
	kh_foreach(ns->indeces, fid, indexCont, {
		SPWriteLock(indexCont->lock);
		switch(indexCont->type) {
			case SPTreeIndexType:
				SPDestroyScoreSet(indexCont->index.btree);
				break;
			case SPCompIndexType:
				SPCompDestroy(indexCont->index.compTree);
				break;
			case SPIdIndexType:
				kh_destroy(IDTYPE, indexCont->index.hash);
				break;
		}
		SPWriteUnlock(indexCont->lock);
		SPLockDestroy(indexCont->lock);
		if (indexCont->compCtx) {
			RedisModule_Free(indexCont->compCtx->types);
		    RedisModule_Free(indexCont->compCtx->compare);
		    RedisModule_Free(indexCont->compCtx);
		}
		RedisModule_Free(indexCont);
	});

	SPDestroyRecordSet(ns->rs);
	// SP_TWORK(SPDestroyRecordSet, ns->rs, {});
			
	kh_destroy(INDECES, ns->indeces);
	SPWriteUnlock(ns->lock);
	SPWriteUnlock(ns->indexLock);
	SPLockDestroy(ns->lock);
	SPLockDestroy(ns->indexLock);
	kv_destroy(ns->fields);
	// kv_destroy(ns->types);
	kv_destroy(ns->composites);
	// printf("Destroying namespace %d\n", 18);
	for (int i = 0; i < ns->rewriteLen; ++i)
	{
		RedisModule_Free( ns->rewrite[i] );
	}
	RedisModule_Free(ns->rewrite);
	for (khint_t k = 0; k < kh_end(ns->uniq); ++k) {
		if (kh_exist(ns->uniq, k)) {
			RedisModule_Free((char*)kh_key(ns->uniq, k));
		}
	}
        
    kh_destroy(STR, ns->uniq);

 //    SPWriteLock(ns->temps.lock);

 //    for (khint_t k = 0; k < kh_end(ns->temps.sets); ++k) {
	// 	if (kh_exist(ns->temps.sets, k)) {
	// 		kh_destroy(SIDS, kh_value(ns->temps.sets, k));
	// 		RedisModule_Free((char *)kh_key(ns->temps.sets, k));
	// 	}
	// }
	// kh_destroy(TMPSETS, ns->temps.sets);

	// SPWriteUnlock(ns->temps.lock);
	// SPLockDestroy(ns->temps.lock);
	RedisModule_Free(ns);
}

SPIndexCont *SPGetIndexForFd(SPNamespace *ns, SPFieldDef *fd, int *typeMismatch) {
	khint_t k;
	SPIndexCont *index = NULL;
	*typeMismatch = 0;
	SPReadLock(ns->indexLock);
	k = kh_get(INDECES, ns->indeces, fd->name);
	if (k != kh_end(ns->indeces)) {
		index = kh_value(ns->indeces, k);
		if (index->type != fd->indexType) *typeMismatch = 1;
	}
	SPReadUnlock(ns->indexLock);
	return index;
}

SPIndexCont *SPCreateIndexForFd(SPFieldDef *fd, SPNamespace *ns) {
	SPIndexCont *indexCont = RedisModule_Calloc(1, sizeof(SPIndexCont));
	SPLockInit(indexCont->lock);
	indexCont->type = fd->indexType;
	switch(indexCont->type) {
		case SPTreeIndexType:
			indexCont->index.btree = kb_init(SCORESET, SP_DEFAULT_TREE_SIZE);
			break;
		case SPCompIndexType:
			indexCont->index.compTree = kb_init(COMPIDX, SP_DEFAULT_TREE_SIZE);
			//TODO: create compCtx
			size_t valueCount = kv_size(fd->fieldPlaceHolders);
			SPFieldDef *cfd;
			indexCont->compCtx = RedisModule_Calloc(1, sizeof(SPCompositeCompCtx));
			indexCont->compCtx->valueCount = valueCount;
			indexCont->compCtx->types = RedisModule_Calloc(valueCount, sizeof(uint8_t));
			indexCont->compCtx->compare = RedisModule_Calloc(valueCount, sizeof(SPCompositeComp));
			const char *fname;
			khint_t k;
			for (int i = 0; i < valueCount; ++i)
			{
				fname = kv_A(fd->fieldPlaceHolders, i).name;
				k = kh_get(FIELDS, ns->defs, fname);
				cfd = kh_value(ns->defs, k);
				indexCont->compCtx->types[i] = cfd->fieldType;
				switch (cfd->fieldType) {
					case SPGeoPart:
						indexCont->compCtx->compare[i] = SPGeoPartCompare;
						break;
					case SPDoublePart:
						indexCont->compCtx->compare[i] = SPDoublePartCompare;
						break;
					case SPLexPart:
						indexCont->compCtx->compare[i] = SPLexPartCompare;
						break;
					//ignoring id types for now
				}
			}
			break;
		case SPIdIndexType:
			indexCont->index.hash = kh_init(IDTYPE);
			break;
	}
	return indexCont;
}

void SPSaveNamespaceToRDB(RedisModuleCtx *ctx, RedisModuleIO *io, SPNamespace *ns) {
	SPReadLock(ns->lock);
	RedisModuleString *tmp = RedisModule_CreateString(ctx, ns->name, strlen(ns->name));
	RedisModule_SaveString(io, tmp);
	RedisModule_FreeString(ctx, tmp);

	RedisModule_SaveUnsigned(io, ns->rewriteLen);
	for (int i = 0; i < ns->rewriteLen; ++i)
	{
		tmp = RedisModule_CreateString(ctx, ns->rewrite[i], strlen(ns->rewrite[i]));
		RedisModule_SaveString(io, tmp);
		RedisModule_FreeString(ctx, tmp);
	}

	tmp = RedisModule_CreateString(ctx, ns->defaultLang, strlen(ns->defaultLang));
	RedisModule_SaveString(io, tmp);
	RedisModule_FreeString(ctx, tmp);
	RedisModule_SaveUnsigned(io, kv_size(ns->fields));
	const char *fname;
	SPFieldDef *def;
	khint_t k;
	for (size_t i = 0; i < kv_size(ns->fields); ++i)
	{
		fname = kv_A(ns->fields, i);
		k = kh_get(FIELDS, ns->defs, fname);
		def = kh_value(ns->defs, k);
		SPSaveFieldToRDB(ctx, io, def);
	}

	RedisModule_SaveUnsigned(io, kv_size(ns->composites));
	for (size_t i = 0; i < kv_size(ns->composites); ++i)
	{
		fname = kv_A(ns->composites, i);
		k = kh_get(FIELDS, ns->defs, fname);
		def = kh_value(ns->defs, k);
		SPSaveFieldToRDB(ctx, io, def);
	}

	SPSaveRecordSetToRDB(ctx, io, ns->rs, ns);
	SPReadUnlock(ns->lock);
}

SPNamespace *SPReadNamespaceFromRDB(RedisModuleCtx *ctx, RedisModuleIO *io) {
	SPNamespace *ns = SPInitNamespace();

	SPWriteLock(ns->lock);

	RedisModuleString *tmp = RedisModule_LoadString(io);
	ns->name = SPNSUniqStr(ns,  RedisModule_StringPtrLen(tmp, NULL) );
	RedisModule_FreeString(ctx, tmp);

	ns->rewriteLen = RedisModule_LoadUnsigned(io);
	ns->rewrite = RedisModule_Alloc(ns->rewriteLen * sizeof(char*));
	for (int i = 0; i < ns->rewriteLen; ++i)
	{
		tmp = RedisModule_LoadString(io);
		ns->rewrite[i] = RedisModule_Strdup( RedisModule_StringPtrLen(tmp, NULL) );// RedisModule_LoadStringBuffer(io, NULL);
		RedisModule_FreeString(ctx, tmp);
		// RedisModule_SaveStringBuffer(io, ns->rewrite[i], strlen(ns->rewrite[i]));
	}

	tmp = RedisModule_LoadString(io);
	ns->defaultLang = SPNSUniqStr(ns,  RedisModule_StringPtrLen(tmp, NULL) );
	RedisModule_FreeString(ctx, tmp);

	size_t count = RedisModule_LoadUnsigned(io);
	SPFieldDef *def;
	khint_t k;
	uint16_t order = 0;
	int absent;
	const char *cname;
	for (size_t i = 0; i < count; ++i)
	{
		def = SPReadFieldFromRDB(ctx, io, ns);
		if (def) {
			def->fieldOrder = order++;
			kv_push(const char*, ns->fields, SPNSUniqStr(ns, def->name));
			k = kh_put(FIELDS, ns->defs, def->name, &absent);
			kh_value(ns->defs, k) = def;
			ns->fieldCount++;
		}
	}
	ks_introsort(str, kv_size(ns->fields), ns->fields.a);
	
	count = RedisModule_LoadUnsigned(io);
	for (size_t i = 0; i < count; ++i)
	{
		def = SPReadFieldFromRDB(ctx, io, ns);
		if (def) {
			kv_push(const char*, ns->composites, SPNSUniqStr(ns, def->name));
			k = kh_put(FIELDS, ns->defs, def->name, &absent);
			kh_value(ns->defs, k) = def;
			ns->compositeCount++;
		}
	}
	ks_introsort(str, kv_size(ns->composites), ns->composites.a);
	// we have field defs loaded now create the empty indeces
	
	SPIndexCont *ic;

	SPWriteLock(ns->indexLock);

	kh_foreach(ns->defs, cname, def, {
		if (def->index) {
			ic = SPCreateIndexForFd(def, ns);
			k = kh_put(INDECES, ns->indeces, cname, &absent);
			kh_value(ns->indeces, k) = ic;	
		}
	});
	

	SPReadRecordSetFromRDB(ctx, io, ns);

	SPWriteUnlock(ns->indexLock);
	SPWriteUnlock(ns->lock);
	return ns;
}

void SPSaveFieldToRDB(RedisModuleCtx *ctx, RedisModuleIO *io, SPFieldDef *fd) {
	RedisModuleString *tmp = RedisModule_CreateString(ctx, fd->name, strlen(fd->name));
	RedisModule_SaveString(io, tmp);
	RedisModule_FreeString(ctx, tmp);


	RedisModule_SaveUnsigned(io, fd->indexType);
	RedisModule_SaveUnsigned(io, fd->fieldType);
	RedisModule_SaveUnsigned(io, fd->index);

	RedisModule_SaveUnsigned(io, fd->prefix);
	RedisModule_SaveUnsigned(io, fd->suffix);
	RedisModule_SaveUnsigned(io, fd->fullText);

	if (fd->indexType == SPCompIndexType) {
		SPFieldPH ph;
		RedisModule_SaveUnsigned(io, kv_size(fd->fieldPlaceHolders));
		for (size_t i = 0; i < kv_size(fd->fieldPlaceHolders); ++i)
		{
			ph = kv_A(fd->fieldPlaceHolders, i);
			RedisModule_SaveUnsigned(io, ph.fieldType);
			tmp = RedisModule_CreateString(ctx, ph.name, strlen(ph.name));
			RedisModule_SaveString(io, tmp);
			RedisModule_FreeString(ctx, tmp);
		}
	}
}

SPFieldDef *SPInitFieldDef() {
	SPFieldDef *fd = RedisModule_Calloc(1, sizeof(SPFieldDef));
	kv_init(fd->fieldPlaceHolders);
	return fd;
}

void SPDestroyFieldDef(SPFieldDef *fd) {
	kv_destroy(fd->fieldPlaceHolders);
	// kv_destroy(fd->fields);
	RedisModule_Free(fd);
}

SPFieldDef *SPFieldDefForCharName(SPNamespace *ns, const char *fname) {
	khint_t k = kh_get(FIELDS, ns->defs, fname);
	if (k != kh_end(ns->defs)) {
		return kh_value(ns->defs, k);
	}
	return NULL;
}
SPFieldDef *SPFieldDefForName(SPNamespace *ns, RedisModuleString *fname) {
	return SPFieldDefForCharName(ns, RedisModule_StringPtrLen(fname, NULL));
}

SPFieldDef *SPCopyFieldDef(SPFieldDef *src) {
	SPFieldDef *fd = SPInitFieldDef();

	fd->name = src->name;
	fd->indexType = src->indexType;
	fd->fieldType = src->fieldType;
	fd->index = src->index;

	fd->prefix = src->prefix;
	fd->suffix = src->suffix;
	fd->fullText = src->fullText;
	SPAssignExtras(fd);
	if (fd->indexType == SPCompIndexType) {
		SPFieldPH ph;
		for (size_t i = 0; i < kv_size(src->fieldPlaceHolders); ++i)
		{
			ph = kv_A(src->fieldPlaceHolders, i);
			SPFieldPH nph;
			nph.fieldType = ph.fieldType;
			nph.name = ph.name;	
			kv_push(SPFieldPH, fd->fieldPlaceHolders, nph);
		}
	}
	return fd;
}

int SPFieldDefsEqual(SPFieldDef *a, SPFieldDef *b) {
	if (strcmp(a->name, b->name)) return 0;
	if (a->indexType != b->indexType) return 0;
	if (a->fieldType != b->fieldType) return 0;
	
	/*

	// for our purposes, we don't actually car about these flags as they do not effect the recordset,
	// we'll track these separately.
	// these are considered small changes that we can implement without modifyinng the recordset itself
	if (a->index != b->index) return 0;
	if (a->prefix != b->prefix) return 0;
	if (a->suffix != b->suffix) return 0;
	if (a->fullText != b->fullText) return 0;

	*/

	if (a->indexType == SPCompIndexType) {
		SPFieldPH aph, bph;
		if (kv_size(a->fieldPlaceHolders) != kv_size(a->fieldPlaceHolders)) return 0;
		for (size_t i = 0; i < kv_size(a->fieldPlaceHolders); ++i)
		{
			aph = kv_A(a->fieldPlaceHolders, i);
			bph = kv_A(b->fieldPlaceHolders, i);
			if (aph.fieldType != bph.fieldType) return 0;
			if (strcmp(aph.name, bph.name)) return 0;
		}
	}

	return 1;
}


int SPFieldDefHasSmallChanges(SPFieldDef *a, SPFieldDef *b) {
	if (a->index != b->index) return 1; // can be auto-reindexed

	if (a->prefix != b->prefix) return 1; // no action required
	if (a->suffix != b->suffix) return 1; // can be auto-reindexed
	if (a->fullText != b->fullText) return 1; // will require manual re-indexing
	return 0;
}


SPFieldDef *SPReadFieldFromRDB(RedisModuleCtx *ctx, RedisModuleIO *io, SPNamespace *ns) {
	SPFieldDef *fd = SPInitFieldDef();
	RedisModuleString *tmp = RedisModule_LoadString(io);
	fd->name = SPNSUniqStr(ns,  RedisModule_StringPtrLen(tmp, NULL) );
	RedisModule_FreeString(ctx, tmp);
	fd->indexType = (uint8_t)RedisModule_LoadUnsigned(io);
	fd->fieldType = (uint8_t)RedisModule_LoadUnsigned(io);
	fd->index = (uint8_t)RedisModule_LoadUnsigned(io);
	fd->prefix = (uint8_t)RedisModule_LoadUnsigned(io);
	fd->suffix = (uint8_t)RedisModule_LoadUnsigned(io);
	fd->fullText = (uint8_t)RedisModule_LoadUnsigned(io);
	SPAssignExtras(fd);
	if (fd->indexType == SPCompIndexType) {
		size_t count = RedisModule_LoadUnsigned(io);
		for (size_t i = 0; i < count; ++i)
		{
			uint8_t fpht = (uint8_t)RedisModule_LoadUnsigned(io);
			tmp = RedisModule_LoadString(io);
			const char *fphn = SPNSUniqStr(ns,  RedisModule_StringPtrLen(tmp, NULL) );
			RedisModule_FreeString(ctx, tmp);
			SPFieldPH ph = {.fieldType = fpht, .name = fphn};
			kv_push(SPFieldPH, fd->fieldPlaceHolders, ph);
		}
	}
	return fd;
}

void SpredisNSSave(RedisModuleIO *io, void *ptr) {
	RedisModuleCtx *ctx = RedisModule_GetContextFromIO(io);
	RedisModule_AutoMemory(ctx);
	SPSaveNamespaceToRDB(ctx, io, ptr);
}

void SpredisNSRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value) {
	printf("REWRITING NS\n");
	SPNamespace *ns = value;
	SPReadLock(ns->lock);
	RedisModuleCtx *ctx = RedisModule_GetContextFromIO(aof);
	RedisModuleString **vargs = RedisModule_Alloc(ns->rewriteLen * sizeof(RedisModuleString *));
	for (int i = 0; i < ns->rewriteLen; ++i)
	{
		vargs[i] = RedisModule_CreateString(ctx, ns->rewrite[i], strlen(ns->rewrite[i]));
	}
	RedisModule_EmitAOF(aof, "spredis.definenamespace", "v", vargs, ns->rewriteLen);
	for (int i = 0; i < ns->rewriteLen; ++i)
	{
		RedisModule_FreeString(ctx, vargs[i]);
	}
	RedisModule_Free(vargs);


	SPRewriteRecordSetToAOF(ctx, key, aof, ns->rs, ns);

	SPReadUnlock(ns->lock);
}

void SpredisNSFree(void *ns) {
	SPDestroyNamespace(ns);
}

void *SpredisNSLoad(RedisModuleIO *io, int encver) {
	RedisModuleCtx *ctx = RedisModule_GetContextFromIO(io);
	RedisModule_AutoMemory(ctx);
	return SPReadNamespaceFromRDB(ctx, io);
}

int SPGetNamespaceKeyValue(RedisModuleCtx *ctx, RedisModuleKey **key, SPNamespace **cont, RedisModuleString *name, int mode) {
	(*key) = RedisModule_OpenKey(ctx,name,
            mode);
    int keyType;

    if (HASH_NOT_EMPTY_AND_WRONGTYPE((*key), &keyType, SPNSTYPE) != 0) {
    	(*cont) = NULL;
        RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
    	(*cont) = NULL;
        return REDISMODULE_OK;
    }
    (*cont) = RedisModule_ModuleTypeGetValue((*key));
    return REDISMODULE_OK;
}

uint8_t SPSmallIntFromString(RedisModuleString *str) {
	long long val = 0;
	RedisModule_StringToLongLong(str, &val);
	return (uint8_t)val;
}

SPFieldDef *SPReadFieldFromARG(RedisModuleCtx *ctx, RedisModuleString **argv, uint8_t type, SPNamespace *ns) {
	SPFieldDef *fd = SPInitFieldDef();
/*
#define SPGeoPart 1
#define SPDoublePart 2
#define SPLexPart 3
#define SPIDPart 4
*/
	fd->name = SPNSUniqStr(ns,  RedisModule_StringPtrLen(argv[0], NULL) );
	fd->fieldType = SPSmallIntFromString(argv[1]);
	fd->index = SPSmallIntFromString(argv[2]) ? 1 : 0;
	fd->prefix = SPSmallIntFromString(argv[3]) ? 1 : 0;
	fd->suffix = SPSmallIntFromString(argv[4]) ? 1 : 0;
	fd->fullText = SPSmallIntFromString(argv[5]) ? 1 : 0;
	fd->indexType = type;
	SPAssignExtras(fd);

	if (fd->fieldType == 0) {
		SPDestroyFieldDef(fd);
		fd = NULL;
	}
	return fd;
}


SPFieldDef *SPReadCompFieldFromARG(RedisModuleCtx *ctx, RedisModuleString **argv, khash_t(FIELDS) *newDefs, int idx, int *idxPtr, int argc, SPNamespace *ns) {
	SPFieldDef *fd = SPInitFieldDef();
/*
#define SPGeoPart 1
#define SPDoublePart 2
#define SPLexPart 3
#define SPIDPart 4
*/
	fd->name = SPNSUniqStr(ns,  RedisModule_StringPtrLen(argv[++idx], NULL) );
	uint8_t fieldCount = SPSmallIntFromString(argv[++idx]);
	fd->indexType = SPCompIndexType;
	fd->fieldType = 0;
	fd->index = 1;
	// fd->indexType = type;
	const char *fname;
	SPFieldDef *f;
	khint_t k;
	while (fieldCount--) {
		fname = SPNSUniqStr(ns,  RedisModule_StringPtrLen(argv[++idx], NULL) );
		SPFieldPH ph;
		ph.name = fname;
		k = kh_get(FIELDS, newDefs, fname);
		if (k != kh_end(newDefs)) {
			f = kh_value(newDefs, k);
			ph.fieldType = f->fieldType;
			kv_push(SPFieldPH, fd->fieldPlaceHolders, ph);
		} else {
			// a referenced field does not exist
			SPDestroyFieldDef(fd);
			fd = NULL;
			break;
		}
	}
	*idxPtr = idx;
	return fd;
}

void SPHandleDeletes(SPFieldDefVec deletes, SPNamespace *ns) {
	SPIndexCont *ic;
	SPFieldDef *fd;
	khint_t k;
	for (size_t i = 0; i < kv_size(deletes); ++i) {
		fd = kv_A(deletes, i);
		k = kh_get(INDECES, ns->indeces, fd->name);
		if (k != kh_end(ns->indeces)) {
			ic = kh_value(ns->indeces, k);
			kh_del(INDECES, ns->indeces, k);
			switch(ic->type) {
				case SPTreeIndexType:
					// SPDestroyScoreSet(ic->index.btree);
					SP_TWORK(SPDestroyScoreSet, ic->index.btree, {});
					// SPDestroyScoreSet(ic->index.btree);
					break;
				case SPCompIndexType:

					SP_TWORK(SPCompDestroy, ic->index.compTree, {});
					// SPCompDestroy(ic->index.compTree);
					break;
				case SPIdIndexType:
					kh_destroy(IDTYPE, ic->index.hash);
					break;
			}
			RedisModule_Free(ic);
		}
	}
}

void SPHandleAdds(SPFieldDefVec adds, SPNamespace *ns) {
	SPIndexCont *ic;
	SPFieldDef *fd;
	khint_t k;
	int absent;
	for (size_t i = 0; i < kv_size(adds); ++i) {
		fd = kv_A(adds, i);
		if (fd->index) {
			ic = SPCreateIndexForFd(fd, ns);
			k = kh_put(INDECES, ns->indeces, fd->name, &absent);
			kh_value(ns->indeces, k) = ic;
		}
	}
}


int SPDefineNamespace(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	return REDISMODULE_OK;
}

int SpredisDefineNamespace_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);

	RedisModuleKey *key = NULL;
	SPNamespace *ns;
	RedisModuleString *keyName = argv[1];
	int isNew = 0;

	const char *defaultLang = RedisModule_StringPtrLen(argv[2], NULL);

	int rewriteLen = argc - 1;
	char **rewrite = RedisModule_Alloc((rewriteLen) * sizeof(char*));
	for (int i = 1; i < argc; ++i)
	{
		rewrite[ i - 1 ] = RedisModule_Strdup( RedisModule_StringPtrLen(argv[i], NULL) );
	}

	int res = SPGetNamespaceKeyValue(ctx, &key, &ns, keyName, REDISMODULE_WRITE);
	if (res != REDISMODULE_OK) {
		return res;
	}
	if (ns == NULL) {
		isNew = 1;
		ns = SPInitNamespace();
		ns->name = SPNSUniqStr(ns,  RedisModule_StringPtrLen(keyName, NULL) );
		ns->defaultLang = SPNSUniqStr(ns,  defaultLang );
		
		SpredisSetRedisKeyValueType(key,SPNSTYPE,ns);
	} else {
		for (int i = 0; i < ns->rewriteLen; ++i)
		{
			// printf("%s\n", ns->rewrite[i]);
			RedisModule_Free( ns->rewrite[i] );
		}
		RedisModule_Free(ns->rewrite);
	}
	ns->rewrite = rewrite;
	ns->rewriteLen = rewriteLen;

	SPUpReadLock(ns->lock);
	SPUpReadLock(ns->indexLock);

	long long fieldCountArg = 0, compCountArg = 0;
	RedisModule_StringToLongLong(argv[3], &fieldCountArg);
	RedisModule_StringToLongLong(argv[4], &compCountArg);
	uint16_t fieldCount, compCount;
	fieldCount = (uint16_t)fieldCountArg;
	compCount = (uint16_t)compCountArg;
	SPFieldDef *fd, *ofd;
	kvec_t(SPFieldDef*) newDefs, newCompDefs;
	kv_init(newDefs);
	kv_init(newCompDefs);
	int idx = 4;
	int hasError = 0;
	khash_t(FIELDS) *newFields = kh_init(FIELDS);
	khint_t k;
	int absent;
	for (uint16_t i = 0; i < fieldCount; ++i)
	{
		if ((idx + 1 + SP_FD_ARG_CNT) > argc) {
			hasError = 1;
			break;
		}
		fd = SPReadFieldFromARG(ctx, (argv + (idx + 1)), SPTreeIndexType, ns);
		if (fd == NULL) {
			hasError = 2;
			break;
		}
		kv_push(SPFieldDef*, newDefs, fd);
		k = kh_put(FIELDS, newFields, fd->name, &absent);
		kh_value(newFields, k) = fd;
		idx += SP_FD_ARG_CNT;
	}

	for (uint16_t i = 0; i < compCount; ++i)
	{
		fd = SPReadCompFieldFromARG(ctx,argv, newFields, idx, &idx, argc, ns);
		if (fd == NULL) {
			hasError = 3;
			break;
		}
		kv_push(SPFieldDef*, newCompDefs, fd);
		k = kh_put(FIELDS, newFields, fd->name, &absent);
		kh_value(newFields, k) = fd;
	}
	
	if (hasError) {
		SPWriteUnlockRP(ns->lock);
		kh_destroy(FIELDS, newFields);
		kv_destroy(newDefs);
		kv_destroy(newCompDefs);
		if (isNew) RedisModule_DeleteKey(key);
		return RedisModule_ReplyWithError(ctx, "ERR- argument count mismatch");
	}

	SPUpgradeLock(ns->lock);
	SPUpgradeLock(ns->indexLock);
	if (isNew) {
		for (size_t i = 0; i < kv_size(newDefs); ++i)
		{
			fd = kv_A(newDefs, i);
			fd->fieldOrder = (uint16_t)i;
			kv_push(const char*, ns->fields, fd->name);
		}
		for (size_t i = 0; i < kv_size(newCompDefs); ++i)
		{
			fd = kv_A(newCompDefs, i);
			kv_push(const char*, ns->composites, fd->name);
		}
		ks_introsort(str, kv_size(ns->fields), ns->fields.a);
		ks_introsort(str, kv_size(ns->composites), ns->composites.a);

		// for (size_t i = 0; i < kv_size(ns->fields); ++i) {
		// 	printf("CreateDef %s\n", kv_A(ns->fields, i));
		// }
		kh_destroy(FIELDS, ns->defs);
		ns->defs = newFields;
		RedisModule_ReplyWithLongLong(ctx, isNew);
		SPFieldDef *def;
		const char *cname;
		SPIndexCont *ic;
		kh_foreach(ns->defs, cname, def, {
			if (def->index) {
				ic = SPCreateIndexForFd(def, ns);
				k = kh_put(INDECES, ns->indeces, cname, &absent);
				kh_value(ns->indeces, k) = ic;
			}
		});
	} else {
		SPFieldDefVec adds, updates, deletes, same;
		kv_init(adds), kv_init(updates), kv_init(deletes), kv_init(same);

		// printf("New fields %zu\n", kv_size(newDefs));
		// printf("New composites %zu\n", kv_size(newCompDefs));

		for (size_t i = 0; i < kv_size(newDefs); ++i)
		{
			fd = kv_A(newDefs, i);
			fd->fieldOrder = (uint16_t)i;
			k = kh_get(FIELDS, ns->defs, fd->name);
			if (k != kh_end(ns->defs)) {
				ofd = kh_value(ns->defs, k);
				if (SPFieldDefsEqual(fd, ofd)) {
					kv_push(SPFieldDef*, same, SPCopyFieldDef(ofd));
				} else {
					kv_push(SPFieldDef*, updates, SPCopyFieldDef(ofd));
				}
			} else {
				kv_push(SPFieldDef*, adds, fd);
			}
		}

		for (size_t i = 0; i < kv_size(newCompDefs); ++i)
		{
			fd = kv_A(newCompDefs, i);
			k = kh_get(FIELDS, ns->defs, fd->name);
			if (k != kh_end(ns->defs)) {
				ofd = kh_value(ns->defs, k);
				if (SPFieldDefsEqual(fd, ofd)) {
					kv_push(SPFieldDef*, same, SPCopyFieldDef(ofd));
				} else {
					kv_push(SPFieldDef*, updates, SPCopyFieldDef(ofd));
				}
			} else {
				kv_push(SPFieldDef*, adds, fd);
			}
		}

		const char *fname;
		for (size_t i = 0; i < kv_size(ns->fields); ++i) {
			fname = kv_A(ns->fields, i);
			k = kh_get(FIELDS, newFields, fname);
			if (k == kh_end(newFields)) {
				k = kh_get(FIELDS, ns->defs, fname);
				kv_push(SPFieldDef*, deletes, SPCopyFieldDef(kh_value(ns->defs, k)));
			}
		}

		for (size_t i = 0; i < kv_size(ns->composites); ++i) {
			fname = kv_A(ns->composites, i);
			k = kh_get(FIELDS, newFields, fname);
			if (k == kh_end(newFields)) {
				k = kh_get(FIELDS, ns->defs, fname);
				kv_push(SPFieldDef*, deletes, SPCopyFieldDef(kh_value(ns->defs, k)));
			}
		}

		SPHandleDeletes(deletes, ns);
		SPHandleAdds(adds, ns);

		khash_t(FIELDS) *oldDefs = ns->defs;
		ns->defs = newFields;

		kv_destroy(ns->fields);
		kv_init(ns->fields);

		for (size_t i = 0; i < kv_size(newDefs); ++i)
		{
			fd = kv_A(newDefs, i);
			kv_push(const char*, ns->fields, fd->name);
		}
		kv_destroy(ns->composites);
		kv_init(ns->composites);

		for (size_t i = 0; i < kv_size(newCompDefs); ++i)
		{
			fd = kv_A(newCompDefs, i);
			kv_push(const char*, ns->composites, fd->name);
		}
		
		ks_introsort(str, kv_size(ns->fields), ns->fields.a);
		ks_introsort(str, kv_size(ns->composites), ns->composites.a);

		// for (size_t i = 0; i < kv_size(ns->fields); ++i) {
		// 	printf("CreateDef %s\n", kv_A(ns->fields, i));
		// }
		//TODO: manage index changes
		RedisModule_ReplyWithString(ctx, RedisModule_CreateStringPrintf(ctx, "{\"adds\":%zu, \"deletes\":%zu, \"updates\":%zu, \"same\":%zu}", kv_size(adds), kv_size(deletes), kv_size(updates), kv_size(same)));

		kh_foreach(oldDefs, fname, fd, {
			SPDestroyFieldDef(fd);
		});

		kh_destroy(FIELDS, oldDefs);

		kv_destroy(newDefs), kv_destroy(newCompDefs), kv_destroy(adds), kv_destroy(updates), kv_destroy(deletes), kv_init(same);
	}
	/*
	fd->name = 0
	fd->fieldType 1
	fd->index = 2
	fd->prefix =3
	fd->suffix = 4
	fd->fullText = 5

	spredis.definenamespace TEST:DB 3 0 make 3 1 1 0 0 model 3 1 1 0 0 year 2 1 0 0 0
	spredis.definenamespace TEST:DB 2 0 make 3 1 1 0 0 model 3 1 1 0 0
	spredis.definenamespace TEST:DB 3 1 make 3 1 1 0 0 model 3 1 1 0 0 year 2 1 0 0 0 ymm 3 model make year
	spredis.definenamespace TEST:DB 3 1 make 3 1 1 0 0 model 3 1 1 0 0 year 3 1 0 0 0 ymm 3 model make year
	*/
	SPWriteUnlockRP(ns->lock);
	SPWriteUnlockRP(ns->indexLock);
	RedisModule_ReplicateVerbatim(ctx);
	return REDISMODULE_OK;
}

int SpredisDescribeNamespace_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);

	RedisModuleKey *key = NULL;
	SPNamespace *ns;
	RedisModuleString *keyName = argv[1];
	int res = SPGetNamespaceKeyValue(ctx, &key, &ns, keyName, REDISMODULE_WRITE);
	if (res != REDISMODULE_OK) {
		return res;
	}
	if (ns == NULL) {
		return RedisModule_ReplyWithNull(ctx);
	}
	SPReadLock(ns->lock);
	RedisModule_ReplyWithArray(ctx, kv_size(ns->fields) + kv_size(ns->composites) + 1);
	const char *fname;
	khint_t k;
	SPFieldDef *fd;
	for (size_t i = 0; i < kv_size(ns->fields); ++i)
	{
		fname = kv_A(ns->fields, i);
		k = kh_get(FIELDS, ns->defs, fname);
		fd = kh_value(ns->defs, k);
		RedisModule_ReplyWithString(ctx, RedisModule_CreateStringPrintf(ctx, "name=%s, type=%s indexType=%s, index=%d, prefix=%d, suffix=%d, fullText=%d", 
			fd->name, FIELDTYPE_DESC[fd->fieldType], INDEXTYPE_DESC[fd->indexType],
			fd->index, fd->prefix,
			fd->suffix, fd->fullText) );
	}
	for (size_t i = 0; i < kv_size(ns->composites); ++i)
	{
		fname = kv_A(ns->composites, i);
		k = kh_get(FIELDS, ns->defs, fname);
		fd = kh_value(ns->defs, k);
		// printf("fieldPlaceHolders=%zu\n", kv_size(fd->fieldPlaceHolders));
		RedisModuleString *tmp = RedisModule_CreateStringPrintf(ctx, "fields=%s:%s", kv_A(fd->fieldPlaceHolders, 0).name, FIELDTYPE_DESC[kv_A(fd->fieldPlaceHolders, 0).fieldType]);
		const char *buffer;
		size_t bufferLen;
		for (size_t ii = 1; ii < kv_size(fd->fieldPlaceHolders); ++ii)
		{
			SPFieldPH tph = kv_A(fd->fieldPlaceHolders, ii);
			RedisModuleString *tmp2 = RedisModule_CreateStringPrintf(ctx, ", %s:%s", tph.name, FIELDTYPE_DESC[tph.fieldType]);
			buffer = RedisModule_StringPtrLen(tmp2, &bufferLen); // sprintf(buffer, ", %s:%s", tph.name, FIELDTYPE_DESC[tph.fieldType]);
			RedisModule_StringAppendBuffer(ctx, tmp, buffer, bufferLen);
		}
		RedisModule_ReplyWithString(ctx, RedisModule_CreateStringPrintf(ctx, "name=%s, type=%s indexType=%s, index=%d, fieldCount=%zu, %s", 
			fd->name, FIELDTYPE_DESC[fd->fieldType], INDEXTYPE_DESC[fd->indexType],
			fd->index, kv_size(fd->fieldPlaceHolders), RedisModule_StringPtrLen(tmp, NULL)) );
	}
	RedisModule_ReplyWithString(ctx, RedisModule_CreateStringPrintf(ctx, "record count = %zu", kh_size(ns->rs->docs))), 
	SPReadUnlock(ns->lock);
	return REDISMODULE_OK;
}

SPIndexCont *SPIndexForFieldCharName(SPNamespace *ns, const char *name) {
	khint_t k;
	k = kh_get(INDECES, ns->indeces, name);
	if (k != kh_end(ns->indeces)) return kh_value(ns->indeces, k);
	return NULL;
}

SPIndexCont *SPIndexForFieldName(SPNamespace *ns, RedisModuleString *name) {
	return SPIndexForFieldCharName(ns, RedisModule_StringPtrLen(name, NULL));
}


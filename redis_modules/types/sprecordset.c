#include "../spredis.h"

#include <float.h>
#include "../lib/lz4.h"
// #include "../lib/utstring.h"
#define SP_ACCELERATION_FACTOR 2
#define SP_MAX_DICT_SIZE 2048
#define SP_PC_LEN 10
#define SP_UNPACK_BUFF_SZ (1 <<20)

#ifndef SP_RS_DEL_INTERVAL
#define SP_RS_DEL_INTERVAL 10
#endif

// (64 * (1 <<10))

//(64 * (1 <<10))


static kvec_t(SPRecordId) DeallocList;
// static LZ4_stream_t* COMPRESSOR_STREAM;
// static char *LZW_DICT;
// static int LZW_DICT_SIZE;
typedef struct _SPUnindexArg {
	SPRecordId rid;
	SPNamespace *ns;
} SPUnindexArg;



static inline int SPPackContFull(SPPackCont *pc) {
	return kv_size(pc->records) >= SP_PC_LEN;
}
static inline void SPDoIndexFields(SPNamespace *ns, SPRecordId rid, SPFieldData *fields);
static inline void SPDoUnIndexFields(SPNamespace *ns, SPRecordId rid, SPFieldData *fields);
static inline void SPReIndexFields(SPNamespace *ns, SPRecordId rid, SPFieldData *oldFields, SPFieldData *newFields);

SPFieldData *SPFieldDataHashToArray(khash_t(FIELDDATA) *hash, SPNamespace *ns) {
	// SPStrVec fields = ns->fields;
	SPFieldData *a = RedisModule_Calloc(kv_size(ns->fields), sizeof(SPFieldData));
	khint_t k;
	// SPFieldData t;
	// SPFieldDef *fd;
	for (int i = 0; i < kv_size(ns->fields); ++i)
	{
		k = kh_get(FIELDDATA, hash, kv_A(ns->fields, i));

		if (k != kh_end(hash)) {
			a[i] = kh_value(hash, k);
		}
	}
	return a;
}

void SpredisInitRecordSet() {
	//not sure if we need this yet

	// NULL_STR_VAL = &NULL_STR; //will need this for sorting
	// NULL_NUM_VAL = &NULL_NUM; //will need this for sorting
	// NULL_GEO_VAL = &NULL_GEO; //not sure if we need this yet
	
	// COMPRESSOR_STREAM = LZ4_createStream();
	// LZW_DICT_SIZE = 0;
	// LZW_DICT = RedisModule_Calloc(SP_DICT_SIZE, sizeof(char));
	kv_init(DeallocList);
}
#define sp_foreach_record(i, pc, raw, rid, code) \
SPRawDoc *(raw); \
SPRecordId rid; \
for (size_t i = 0; i < kv_size( (pc->records) ) ; ++i) \
{ \
	rid = kv_A(pc->records, i); \
	raw = &rid.record->rawDoc; \
	code \
}

#define sp_foreach_record_aux(i, pc, raw, rid, code) \
for (size_t i = 0; i < kv_size( (pc->records) ) ; ++i) \
{ \
	rid = kv_A(pc->records, i); \
	raw = &rid.record->rawDoc; \
	code \
}

static inline void SP_CLEAR_UNPACKED(SPPackCont *pc) {
	sp_foreach_record(i, pc, raw, rid, {
		RedisModule_Free(raw->unpacked);
		raw->unpacked = NULL;
	});
}

static inline void SP_UNPACK_ALL(SPRecordSet *rs, SPPackCont *pc, SPRawDoc *subst) {
	if (!pc->packed && subst == NULL) return;

	size_t curs = 0;
	char *subunpacked;
	

	if (!pc->packed && subst) {

		sp_foreach_record(i, pc, raw, rid, {
			subst[i].oSize = raw->oSize;
			subunpacked = subst[i].unpacked = RedisModule_Realloc(subst[i].unpacked, (raw->oSize + 1) * sizeof(char));
			subunpacked[raw->oSize] = 0;
			memcpy(subunpacked, raw->unpacked, raw->oSize);
		});

		return;
	}

	char *unpacked = RedisModule_Alloc(pc->oSize * sizeof(char));
	// rs->unpackBuff = RedisModule_Alloc( SP_UNPACK_BUFF_SZ * sizeof(char) )
	// size_t maxSize = SP_UNPACK_BUFF_SZ;
	// if (pc->oSize > maxSize) {
	// 	rs->unpackBuff = RedisModule_Realloc(unpacked, pc->oSize * sizeof(char));
	// }
	// LZ4_setStreamDecode(rs->decompressStream, NULL,0);
	// LZ4_decompress_fast_continue(rs->decompressStream, pc->bytes, unpacked, pc->oSize);
	LZ4_decompress_fast(pc->bytes, unpacked, pc->oSize);
	if (subst == NULL) {
		sp_foreach_record(i, pc, raw, rid, {
			raw->unpacked = RedisModule_Alloc((raw->oSize + 1) * sizeof(char));
			raw->unpacked[raw->oSize] = 0;
			memcpy(raw->unpacked, unpacked + curs, raw->oSize);
			curs += raw->oSize;
		});
		pc->packed = 0;
		RedisModule_Free(pc->bytes);
		pc->bytes = NULL;

		pc->cSize = 0;
		pc->oSize = 0;	
	} else {
		sp_foreach_record(i, pc, raw, rid, {
			subst[i].oSize = raw->oSize;
			subunpacked = subst[i].unpacked = RedisModule_Realloc(subst[i].unpacked, (raw->oSize + 1) * sizeof(char));
			subunpacked[raw->oSize] = 0;
			memcpy(subunpacked, unpacked + curs, raw->oSize);
			curs += raw->oSize;
		});
	}
	// if (pc->oSize > maxSize) {
	// 	rs->unpackBuff = RedisModule_Realloc(rs->unpackBuff, maxSize * sizeof(char));
	// }
	RedisModule_Free(unpacked);
}



static inline char *SP_UNPACK_RID(SPRecordSet *rs, SPPackCont *pc, SPRecordId rid) {
	char *unpacked = RedisModule_Alloc(pc->oSize * sizeof(char));

	LZ4_decompress_fast(pc->bytes, unpacked, pc->oSize);
	size_t curs = 0;
	char *res = RedisModule_Alloc((rid.record->rawDoc.oSize + 1) * sizeof(char));
	res[rid.record->rawDoc.oSize] = 0;
	sp_foreach_record(i, pc, raw, trid, {
		if (trid.id == rid.id) {
			memcpy(res, unpacked + curs, raw->oSize);
			break;
		} else {
			curs += raw->oSize;
		}
	});

	RedisModule_Free(unpacked);
	return res;
}
static inline void SP_PACK_CONTAINER(SPRecordSet *rs, SPPackCont *pc, int lock) {
	if (lock) SPWriteLock(rs->lzwLock);
	if (!SPPackContFull(pc)) {
		if (lock) SPWriteUnlock(rs->lzwLock);
		return;
	}
	
	size_t count = kv_size(pc->records);
	SPRawDoc *raw;
	SPRecordId rid;
	int dstCapacity;
	pc->oSize = 0;
	// UT_string *tmp;
	// utstring_new(tmp);
	char *buff = NULL;
	// int bufflen = 0;
	// RedisModuleString *tmp = RedisModule_CreateString(ctx, "", 0);
	for (size_t i = 0; i < count; ++i)
	{
		
		rid = kv_A(pc->records, i);
		raw = &rid.record->rawDoc;
		buff = RedisModule_Realloc(buff, (pc->oSize + raw->oSize) * sizeof(char));
		memcpy(buff + pc->oSize, raw->unpacked, raw->oSize);
		// if (i == 0) {
		// 	utstring_print(tmp, raw->unpacked);
		// } else {
		// utstring_bincpy(tmp, raw->unpacked, raw->oSize - 1);	
		// }
		// RedisModule_StringAppendBuffer(ctx, tmp, raw->unpacked, raw->oSize);
		pc->oSize += raw->oSize;
	}

	// char *bytes = utstring_body(tmp);// RedisModule_StringPtrLen(tmp, NULL);
	dstCapacity = LZ4_COMPRESSBOUND(pc->oSize);
	if (pc->bytes) {
		pc->bytes = RedisModule_Realloc(pc->bytes, dstCapacity * sizeof(char));	
	} else {
		pc->bytes = RedisModule_Alloc(dstCapacity * sizeof(char));	
	}
	
	pc->cSize = LZ4_compress_fast(buff, pc->bytes, pc->oSize, dstCapacity, 2);
	pc->bytes = RedisModule_Realloc(pc->bytes, pc->cSize * sizeof(char));

	for (size_t i = 0; i < count; ++i)
	{
		rid = kv_A(pc->records, i);
		raw = &rid.record->rawDoc;
		RedisModule_Free(raw->unpacked);
		raw->unpacked = NULL;
	}
	pc->packed = 1;
	if (lock) SPWriteUnlock(rs->lzwLock);
	// utstring_free(tmp);
	// RedisModule_FreeString(ctx, tmp);
	RedisModule_Free(buff);
}

static inline void SP_REPLACE_RID(SPRecordSet *rs, SPRecordId rid, const char *doc, size_t doclen) {
	SPPackCont *pc = rid.record->pc;
	SPRawDoc *raw = &rid.record->rawDoc;
	SP_UNPACK_ALL(rs, pc, NULL);
	if (raw->unpacked) RedisModule_Free(raw->unpacked);
	raw->unpacked = RedisModule_Strdup(doc);
	raw->oSize = doclen;
	SP_PACK_CONTAINER(rs, pc, 0);
}

static inline void SP_PACK_DOC(SPRecordSet *rs, const char *doc, size_t doclen, SPRecordId rid, int lock) {
    if (doc == NULL) return;
    // SPRawDoc lzw = rid.record->rawDoc;

    if (lock) SPWriteLock(rs->lzwLock);
    int full = SPAcquirePackCont(rs, rid);
    if (full && !rid.record->pc->packed) {
    	//we've just filled a pack container
    	if (rid.record->rawDoc.unpacked) {
    		RedisModule_Free(rid.record->rawDoc.unpacked);
    	}
    	rid.record->rawDoc.unpacked = RedisModule_Strdup(doc);
	    rid.record->rawDoc.oSize = doclen;	
	    //pack all
	    SP_PACK_CONTAINER(rs, rid.record->pc, 0);
    } else if (full && rid.record->pc->packed) {
    	// this is a pre-existing record that has been packed.
    	// repack for this doc
    	SP_REPLACE_RID(rs, rid, doc, doclen);
    } else {
    	// pack container is not full yet
    	if (rid.record->rawDoc.unpacked) {
    		RedisModule_Free(rid.record->rawDoc.unpacked);
    	}
    	rid.record->rawDoc.unpacked = RedisModule_Strdup(doc);
	    rid.record->rawDoc.oSize = doclen;
	    //wait for pack cont to be full
    }
 	if (lock) SPWriteUnlock(rs->lzwLock);
}

char *SP_UNPACK_DOC(SPRecordSet *rs, SPRecordId rid, int lock) {
	SPRawDoc lzw = rid.record->rawDoc;

    // char *res = RedisModule_Alloc(lzw.oSize + 1);
    // res[lzw.oSize] = 0; //null the last char
    
    if (lock) SPWriteLock(rs->lzwLock);
    SPPackCont *pc = rid.record->pc;
    char *res;
    if (!pc->packed) {
    	res = RedisModule_Strdup(lzw.unpacked);	
    } else {
    	res = SP_UNPACK_RID(rs, pc, rid);
    }

    // if (!LZ4_decompress_fast(lzw.packed, res, lzw.oSize)) {
    //     RedisModule_Free(res);
    //     return NULL;
    // }
    if (lock) SPWriteUnlock(rs->lzwLock);
    return res;
}

static inline int SPAcquirePackCont(SPRecordSet *rs, SPRecordId rid) {
	if (rid.record->pc) return SPPackContFull(rid.record->pc);
	if (kv_size(rs->packStack) == 0) {
		rid.record->pc = RedisModule_Calloc(1, sizeof(SPPackCont));
	} else {
		rid.record->pc = kv_pop(rs->packStack);
	}
	kv_push(SPRecordId, rid.record->pc->records, rid);
	if (SPPackContFull(rid.record->pc)) {
		return 1;
	}
	kv_push(SPPackCont*, rs->packStack, rid.record->pc);
	return 0;
}

static inline void SPReleasePackCont(RedisModuleCtx *ctx, SPRecordSet *rs, SPRecordId rid) {
	SPPackCont *pc = rid.record->pc;
	if (!SPPackContFull(pc)) {
		SPPRecordIdVec newVec, oldVec;
		kv_init(newVec);
		oldVec = rid.record->pc->records;
		sp_foreach_record(i, pc, raw, trid, {
			if (trid.id != rid.id) {
				kv_push(SPRecordId, newVec, trid);
			}
		});
		kv_destroy(oldVec);
		rid.record->pc->records = newVec;
		rid.record->pc = NULL;
	} else {
		/*
			we're going to destroy this pack container 
			and use any partially full containers on the stack (rs->packStack)
		*/
		SP_UNPACK_ALL(rs, pc, NULL);
		sp_foreach_record(i, pc, raw, trid, {
			trid.record->pc = NULL; //!important!
			if (trid.id != rid.id) { //ignore the record to be deleted
				if (SPAcquirePackCont(rs, trid)) {
					SP_PACK_CONTAINER(rs, trid.record->pc, 0);
				}
			}
		});
		kv_destroy(pc->records);
		if (pc->bytes) RedisModule_Free(pc->bytes);
		RedisModule_Free(pc);
	}
}

/*
	deleteing unused record objects.
	the reaosn we don't jsut detroy them right away is becuase the ptr
	may actually be referenced by a cursor
	this was the only obvious way to allow cursors on multiple threads
	without locking the entire record set for their lifespan
*/
void *SPDeleteThread(void *arg) {
	SPRecordSet *rs = arg;
	int run = 1;
	while (run) {
		sleep(SP_RS_DEL_INTERVAL);
		// printf("Grabbin read lock\n");
		SPReadLock(rs->lock);
		// printf("Grabbed read lock\n");
		run = rs->delrun;
		SPReadUnlock(rs->lock);
		if (run) {
			long long oldestCursor = SPOldestCursorTime();
			// printf("Grabbin del lock\n");
			SPWriteLock(rs->deleteLock);
			// printf("Grabbed del lock\n");
			SPDeletedRecordVec ndrvec;
			SPDeletedRecordVec old = rs->deleted;
			kv_init(ndrvec);
			SPDeletedRecord cand;
			
			while(kv_size(old)) {
				cand = kv_pop(old);
				if (cand.date < oldestCursor) {
					SPDestroyRecord(cand.rid, rs);
				} else {
					kv_push(SPDeletedRecord, ndrvec, cand);
				}
			}
			rs->deleted = ndrvec;
			kv_destroy(old);
			SPWriteUnlock(rs->deleteLock);
		}
		
	}
	return NULL;
}

SPRecordSet *SPCreateRecordSet(SPNamespace *ns) {
	SPRecordSet *rs = RedisModule_Calloc(1, sizeof(SPRecordSet));
	rs->docs = kh_init(MSTDOC);
	kv_init(rs->deleted);
	// rs->deleted = kh_init(SIDS);
	SPLockInit(rs->lock);
	SPLockInit(rs->deleteLock);
	SPLockInit(rs->lzwLock);
	kv_init(rs->packStack);
	rs->delrun = 1;

	pthread_create(&rs->deleteThread, NULL, SPDeleteThread, rs);
	pthread_detach(rs->deleteThread);
	// rs->unpackBuff = RedisModule_Alloc( SP_UNPACK_BUFF_SZ * sizeof(char) );
	// rs->decompressStream = LZ4_createStreamDecode();
 //    rs->compressStream = LZ4_createStream();
	return rs;
}

SPRecordId *SPRecordSetToArray(SPRecordSet *rs, size_t *len) {
	// SPReadLock(rs->lock);
	size_t pos = 0;
	SPRecordId *a = RedisModule_Alloc(sizeof(SPRecordId) * kh_size(rs->docs));
	const char *id;
	SPRecordId rid;
	kh_foreach(rs->docs, id, rid, {
		// SPReadLock(rid.record->lock);
		if (rid.record->exists) a[pos++] = rid;
		// SPReadUnlock(rid.record->lock);
	});
	(*len) = pos;
	return a;
	// SPReadUnlock(rs->lock);
}


SPRecordId *SPResultSetToArray(khash_t(SIDS) *set, size_t *len) {
	// SPReadLock(rs->lock);
	size_t pos = 0;
	SPRecordId *a = RedisModule_Alloc(sizeof(SPRecordId) * kh_size(set));
	// const char *id;
	spid_t id;
	SPRecordId rid;
	kh_foreach_key(set, id, {
		rid.id = id;
		// SPReadLock(rid.record->lock);
		if (rid.record->exists) a[pos++] = rid;
		// SPReadUnlock(rid.record->lock);
	});
	(*len) = pos;
	return a;
	// SPReadUnlock(rs->lock);
}

void SPDestroyRecordSet(SPRecordSet *rs) {
	SPWriteLock(rs->lock);
	rs->delrun = 0;
	SPWriteLock(rs->lzwLock);
	SPWriteLock(rs->deleteLock);
	
	pthread_cancel(rs->deleteThread);

	for (khint_t k = 0; k < kh_end(rs->docs); ++k) {
        if (kh_exist(rs->docs, k)) {
            SPDestroyRecord(kh_value(rs->docs, k), rs);
        }
    }
	kh_destroy(MSTDOC,rs->docs);

	while(kv_size(rs->deleted)) {
		SPDestroyRecord(kv_pop(rs->deleted).rid, rs);
	}
	kv_destroy(rs->deleted);


	SPWriteUnlock(rs->deleteLock);
	SPWriteUnlock(rs->lzwLock);
	SPWriteUnlock(rs->lock);

	SPLockDestroy(rs->lock);
	SPLockDestroy(rs->lzwLock);
	SPLockDestroy(rs->deleteLock);
	RedisModule_Free(rs);
}

void SPUnindexRecord(SPUnindexArg *ua) {
	SPReadLock(ua->ns->lock);
	SPRecord *rec = ua->rid.record;

	// SPWriteLock(rec->lock);
	rec->rawDoc.oSize = 0;
	// if (rec->rawDoc.packed) {
	// 	// RedisModule_Free(rec->rawDoc.packed);
	// 	// rec->rawDoc.packed = NULL;
	// 	rec->rawDoc.oSize = 0;
	// }
	// SPWriteUnlock(rec->lock);

	SPReadUnlock(ua->ns->lock);
	RedisModule_Free(ua);
}

int SPDeleteRecord(RedisModuleCtx *ctx, SPNamespace *ns, const char *id) {
	// SPReadLock(ns->lock);
	SPRecordSet *rs = ns->rs;
	SPUpReadLock(rs->lock);
	khint_t k;
	SPRecordId rid;
	SPRecord* rec;
	int res = 0;
	// int absent;

	k = kh_get(MSTDOC, rs->docs, id);
	if (k != kh_end(rs->docs)) {
		rid = kh_value(rs->docs, k);
		rec = rid.record;

		if (rec->exists) {
			res = 1;
			SPUpgradeLock(rs->lock);

			SPWriteLock(rs->deleteLock);

			SPWriteLock(rs->lzwLock);
			SPReleasePackCont(ctx, rs, rid);
			SPWriteUnlock(rs->lzwLock);

			

			if (rid.record->rawDoc.unpacked) {
				// printf("Freeing doc\n");
				RedisModule_Free(rid.record->rawDoc.unpacked);
				rid.record->rawDoc.unpacked = NULL;
			}
			rid.record->rawDoc.oSize = 0;
			rid.record->pc = NULL;
			rec->exists = 0;
			kh_del(MSTDOC, rs->docs, k);
			SPDoUnIndexFields(ns, rid, rid.record->fields);
			RedisModule_Free(rid.record->fields);
			rid.record->fields = NULL;
			
			// SPDestroyRecord(rid, rs);
			SPDeletedRecord dr = {.rid = rid, .date = RedisModule_Milliseconds()};
			kv_push(SPDeletedRecord, rs->deleted, dr);
			// kh_put(SIDS, rs->deleted, rid.id, &absent);
			SPWriteUnlock(rs->deleteLock);
			

		}
	}
	SPWriteUnlockRP(rs->lock);
	
	
	// SPReadUnlock(ns->lock);
	return res;
}

SPRecordId SPInitRecord(const char *id, uint8_t fieldCount) {
	/* 
		Pretty fancy...
		The record id is the address of the record itself.
		Only works if we never re-alloc the record itself.
		This also means that we can't store set data in the file system, only record data.
		This is fine as it will actuall shrink the size on the file system
	*/

	SPRecordId rid = {.record = RedisModule_Calloc(1, sizeof(SPRecord))};
	if (id != NULL) rid.record->sid = RedisModule_Strdup(id);
	rid.record->exists = 1;
	// rid.record->fc = fieldCount;
	return rid;
}


static void SPDestroyFieldData(SPFieldData *fd) {
	if (fd->iv) RedisModule_Free(fd->iv);
	if (fd->av) RedisModule_Free(fd->av);	
}

void SPDestroyRecord(SPRecordId rid, SPRecordSet *rs) {
	SPRecord *record = rid.record;
	if (record->fields) {
		for (uint16_t i = 0; i < rs->fc; ++i)
		{
			SPDestroyFieldData(&record->fields[i]);
		}
		RedisModule_Free(record->fields);
	}
	if (record->rawDoc.unpacked) RedisModule_Free(record->rawDoc.unpacked);
	if (record->sid) RedisModule_Free(record->sid);
	RedisModule_Free(record);
}

void SPReadRecordSetFromRDB(RedisModuleCtx *ctx, RedisModuleIO *io, SPNamespace *ns) {
	// printf("1\n");
	SPRecordSet *rs = ns->rs;

	SPWriteLock(rs->lock);
	SPWriteLock(rs->deleteLock);

	size_t count = RedisModule_LoadUnsigned(io);
	RedisModuleString *tmp1, *tmp2;
	SPRecordId rid;
	SPFieldDef *fd;
	khint_t k;
	int absent;
	size_t doclen;
	const char *sid, *doc;
	// printf("3\n");

	// int read = 0;
	// 

	for (size_t i = 0; i < count; ++i)
	{
		tmp1 = RedisModule_LoadString(io);
		// printf("3\n");
		tmp2 = RedisModule_LoadString(io);
		// printf("4\n");
		sid = RedisModule_StringPtrLen(tmp1, NULL);
		doc = RedisModule_StringPtrLen(tmp2, &doclen);
		// printf("%s\n%s\n", sid, doc);
		k = kh_get(MSTDOC, rs->docs, sid);
		// printf("5\n");
		if (k != kh_end(rs->docs)) {
			printf("Existing? %s\n", sid);
			rid = kh_value(rs->docs, k);
		} else {
			rid = SPInitRecord(sid, rs->fc);
			k = kh_put(MSTDOC, rs->docs, rid.record->sid, &absent);
			kh_value(rs->docs, k) = rid;
		}
		// printf("6\n");
		// rid.record->kson = kson_parse(doc, ns);

		SPWriteLock(rs->lzwLock);
		SP_PACK_DOC(rs, doc, doclen, rid, 0);
		SPWriteUnlock(rs->lzwLock);

		RedisModule_FreeString(ctx, tmp1);
		RedisModule_FreeString(ctx, tmp2);

		size_t fieldCount = RedisModule_LoadUnsigned(io);
		// printf("fieldCount %zu\n", fieldCount);
		rid.record->fields = RedisModule_Calloc(fieldCount, sizeof(SPFieldData));

		// printf("7, %d, %d\n", fieldCount, read);
		for (size_t ii = 0; ii < fieldCount; ++ii)
		{
			
			fd = SPFieldDefForCharName(ns, kv_A(ns->fields, ii));
			fd->dbRead(ctx, io, rid.record, fd, ns, ii);
		}
		// read++;
		// printf("8\n");

		SPDoIndexFields(ns, rid, rid.record->fields);

		// printf("9\n");
		// RedisModule_Free(doc);
	}
	// kh_foreach(rs->docs, sid, rid, {
	// 	SPDoIndexFields(ns, rid, rid.record->fields);
	// });

	SPWriteUnlock(rs->deleteLock);
	SPWriteUnlock(rs->lock);
	
}

void SPSaveRecordSetToRDB(RedisModuleCtx *ctx, RedisModuleIO *io, SPRecordSet *rs, SPNamespace *ns) {

	SPReadLock(ns->lock);
	SPReadLock(rs->lock);
	size_t recLen, pos = 0;
	SPRecordId *rids = SPRecordSetToArray(rs, &recLen);
	SPReadUnlock(rs->lock);
	RedisModule_SaveUnsigned(io, recLen);

	khash_t(SIDS) *packs = kh_init(SIDS);

	const char *id;
	SPRecordId rid;
	SPRecord *record;
	// SPPackCont *pc;
	SPFieldDef *fd;
	SPPackContId pcid = {.id = 0};
	khint_t pk;
	int absent;
	SPRawDoc *raw;
	
	// printf("Size on start save = %zu\n", kh_size(rs->docs));
	long long start = RedisModule_Milliseconds();
	SPRawDoc *subst = RedisModule_Alloc(SP_PC_LEN * sizeof(SPRawDoc));
	for (int i = 0; i < SP_PC_LEN; ++i)
	{
		subst[i].unpacked = RedisModule_Alloc(1);
	}
	// kh_foreach(rs->docs, id, rid, {
	// printf("E\n");
	
	// printf("F\n");
	while (pos < recLen) {
		rid = rids[pos++];
	
		// we're going tp lock.unlock eeach iteration so ass not to completely stop indexing
		// expensive but necessary
		SPReadLock(rs->lzwLock);
		SPReadLock(rs->deleteLock);
		
		if (rid.record->pc == NULL || !rid.record->exists) continue;
		pcid.pc = rid.record->pc;

		//we only want to unpack once per pack container
		pk = kh_get(SIDS, packs, pcid.id);
		if (pk == kh_end(packs)) {
			
			SP_UNPACK_ALL(rs, pcid.pc, subst);
			sp_foreach_record_aux(i, pcid.pc, raw, rid, {
				record = rid.record;
				id = record->sid;
				
				RedisModule_SaveStringBuffer(io, id, strlen(id));
				RedisModule_SaveStringBuffer(io, subst[i].unpacked, subst[i].oSize);
				RedisModule_SaveUnsigned(io, kv_size(ns->fields));

				for (int i = 0; i < kv_size(ns->fields); ++i)
				{
					fd = SPFieldDefForCharName(ns, kv_A(ns->fields, i));
					fd->dbWrite(ctx, io, record, fd, ns, i);
				}
			});
			kh_put(SIDS, packs, pcid.id, &absent);
			
		}
		SPReadUnlock(rs->deleteLock);
		SPReadUnlock(rs->lzwLock);
	};
	// );
	// printf("Size on end save = %zu\n", kh_size(rs->docs));
	// printf("G\n");
	SPReadUnlock(ns->lock);
	// printf("H\n");
	kh_destroy(SIDS, packs);
	for (int i = 0; i < SP_PC_LEN; ++i)
	{
		RedisModule_Free(subst[i].unpacked);
	}
	RedisModule_Free(subst);
	RedisModule_Free(rids);
	printf("Save recordset took %llums\n", RedisModule_Milliseconds() - start);
}



void SPRewriteRecordSetToAOF(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleIO *aof, SPRecordSet *rs, SPNamespace *ns) {
	SPReadLock(rs->lock);
	// SPReadLock(rs->lzwLock);
	

	khash_t(SIDS) *packs = kh_init(SIDS);

	const char *id;
	SPRecordId rid;
	SPRecord *record;
	SPPackCont *pc;
	SPFieldDef *fd;
	SPPackContId pcid = {.id = 0};
	khint_t pk;
	int absent;
	SPRawDoc *raw;
	SPReadLock(rs->lzwLock);
	// printf("Size on start save = %zu\n", kh_size(rs->docs));
	long long start = RedisModule_Milliseconds();
	SPRawDoc *subst = RedisModule_Alloc(SP_PC_LEN * sizeof(SPRawDoc));
	for (int i = 0; i < SP_PC_LEN; ++i)
	{
		subst[i].unpacked = RedisModule_Alloc(1);
	}
	
	kh_foreach(rs->docs, id, rid, {
		pcid.pc = rid.record->pc;
		//we only want to unpack once per pack container
		pk = kh_get(SIDS, packs, pcid.id);
		if (pk == kh_end(packs)) {
			
			pc = pcid.pc;
			SP_UNPACK_ALL(rs, pc, subst);
			RedisModuleString **allArgs;
			RedisModuleString **fArgs;
			int curs;
			// int allLen;
			int fArgLen;
			sp_foreach_record_aux(i, pc, raw, rid, {
				curs = 0;
				record = rid.record;
				id = record->sid;

				allArgs = RedisModule_Alloc(3 * sizeof(RedisModuleString*));
				allArgs[curs++] = key;
				allArgs[curs++] = RedisModule_CreateString(ctx, id, strlen(id));
				allArgs[curs++] = RedisModule_CreateStringPrintf(ctx, "%d", kv_size(ns->fields));

				
				for (int i = 0; i < kv_size(ns->fields); ++i)
				{
					fd = SPFieldDefForCharName(ns, kv_A(ns->fields, i));
					// fd->dbWrite(ctx, io, record, fd, ns, i);
					// SPDoubleRewriteArgs(RedisModuleCtx *ctx,SPRecordId rid, SPFieldData* data, SPFieldDef* fd, int *resLen)
					fArgs = fd->rewrite(ctx, rid, &record->fields[i], fd, &fArgLen);
					allArgs = RedisModule_Realloc(allArgs, (curs + fArgLen) * sizeof(RedisModuleString*));
					memmove(&allArgs[curs], fArgs, sizeof(RedisModuleString*) * fArgLen);
					curs += fArgLen;
				}

				RedisModule_EmitAOF(aof, "spredis.addrecord", "vc", allArgs, curs, subst[i].unpacked);

				for (int i = 1; i < curs; ++i)
				{
					RedisModule_Free(allArgs[i]);
				}
				RedisModule_Free(allArgs);
			});
			kh_put(SIDS, packs, pcid.id, &absent);
		}
	});
	// printf("Size on end save = %zu\n", kh_size(rs->docs));
	SPReadUnlock(rs->lzwLock);
	SPReadUnlock(rs->lock);
	kh_destroy(SIDS, packs);
	for (int i = 0; i < SP_PC_LEN; ++i)
	{
		RedisModule_Free(subst[i].unpacked);
	}
	RedisModule_Free(subst);
	printf("Rewrite recordset took %llums\n", RedisModule_Milliseconds() - start);
}


static inline void SPDoIndexFields(SPNamespace *ns, SPRecordId rid, SPFieldData *fields) {
	SPFieldDef *fd;
	SPPerms perms;
	for (int i = 0; i < kv_size(ns->fields); ++i)
	{
		fd = SPFieldDefForCharName(ns, kv_A(ns->fields, i));
		fd->addIndex(rid, &fields[i], fd, ns);
	}
	
	for (int i = 0; i < kv_size(ns->composites); ++i)
	{
		fd = SPFieldDefForCharName(ns, kv_A(ns->composites, i));
		if (fd) {
			perms = SPCompositePermutationsForRecord(ns, rid.record, fd, fields);
			SPIndexCont *ic = SPIndexForFieldCharName(ns, fd->name);
			while (kv_size(perms)) {
				SPPtrOrD_t *a = kv_pop(perms);
				SPAddCompScoreToSet(ic, rid.id, a);
			}

			kv_destroy(perms);
		}
	}
}


static inline int SPCompFieldData(SPFieldData *a, SPFieldData *b) {
	if (a->ilen != b->ilen) return 1;
	for (int i = 0; i < a->ilen; ++i)
	{
		if (a->iv[i].asUInt != b->iv[i].asUInt) return 1;
	}
	return 0;
}

static inline int SPSTrVecContains(SPStrVec vec, const char *name) {
	for (int i = 0; i < kv_size(vec); ++i)
	{
		if (strcmp(name, kv_A(vec, i))) return 1;
	}
	return 0;
}


static inline void SPReIndexFields(SPNamespace *ns, SPRecordId rid, SPFieldData *oldFields, SPFieldData *newFields) {
	SPFieldDef *fd;
	SPPerms operms;
	SPPerms perms;
	SPStrVec dirties;
	kv_init(dirties);
	for (int i = 0; i < kv_size(ns->fields); ++i)
	{
		fd = SPFieldDefForCharName(ns, kv_A(ns->fields, i));
		if (SPCompFieldData(oldFields, newFields)) {
			kv_push(const char*, dirties, fd->name);
			fd->remIndex(rid, &oldFields[i], fd, ns);
			fd->addIndex(rid, &newFields[i], fd, ns);
		}
	}
	for (int i = 0; i < kv_size(ns->composites); ++i)
	{
		fd = SPFieldDefForCharName(ns, kv_A(ns->composites, i));
		if (fd) {
			
			int dirty = 0;
			for (int k = 0; k < kv_size(fd->fieldPlaceHolders); ++k)
			{
				if (SPSTrVecContains(dirties, kv_A(fd->fieldPlaceHolders, i).name)) {
					dirty = 1;
					break;
				}
			}
			if (dirty) {
				SPIndexCont *ic = SPIndexForFieldCharName(ns, fd->name);
				operms = SPCompositePermutationsForRecord(ns, rid.record, fd, oldFields);
				perms = SPCompositePermutationsForRecord(ns, rid.record, fd, newFields);
				while (kv_size(operms)) {
					SPPtrOrD_t *a = kv_pop(operms);
					SPRemCompScoreFromSet(ic, rid.id, a);
				}
				while (kv_size(perms)) {
					SPPtrOrD_t *a = kv_pop(perms);
					SPAddCompScoreToSet(ic, rid.id, a);
				}
				kv_destroy(perms);
				kv_destroy(operms);
			}
		}
	}


	kv_destroy(dirties);
}

static inline void SPDoUnIndexFields(SPNamespace *ns, SPRecordId rid, SPFieldData *fields) {
	SPFieldDef *fd;
	SPPerms perms;
	for (int i = 0; i < kv_size(ns->fields); ++i)
	{
		fd = SPFieldDefForCharName(ns, kv_A(ns->fields, i));
		fd->remIndex(rid, &fields[i], fd, ns);
	}
	for (int i = 0; i < kv_size(ns->composites); ++i)
	{
		fd = SPFieldDefForCharName(ns, kv_A(ns->composites, i));
		if (fd) {
			perms = SPCompositePermutationsForRecord(ns, rid.record, fd, fields);
			SPIndexCont *ic = SPIndexForFieldCharName(ns, fd->name);
			while (kv_size(perms)) {
				SPPtrOrD_t *a = kv_pop(perms);
				SPRemCompScoreFromSet(ic, rid.id, a);
			}
			kv_destroy(perms);
		}
	}

	for (int i = 0; i < kv_size(ns->fields); ++i)
	{
		SPDestroyFieldData(&rid.record->fields[i]);
	}
}
typedef struct {
	SPNamespace *ns;
	char *doc;
	char *sid;
	size_t doclen;
	khash_t(FIELDDATA) *tfdata;
	// SPRecordSet *rs;
} SPIndexThreadArg;


void SPDoIndexingWork(SPIndexThreadArg *arg) {
	SPNamespace *ns = arg->ns;
	SPRecordSet *rs = ns->rs;
	SPReadLock(ns->lock);
	SPWriteLock(rs->lock);

	char *doc, *sid;
	doc = arg->doc;
	sid = arg->sid;//  RedisModule_StringPtrLen(argv[idx++], &doclen);
	SPRecordId rid;
	khint_t k;
	int absent;

	k = kh_get(MSTDOC, rs->docs, sid);
	if (k != kh_end(rs->docs)) {
		rid = kh_value(rs->docs, k);
		// oldvals = rid.record->fields;

		if (rid.record->fields) {
			SPFieldData *newFields = SPFieldDataHashToArray(arg->tfdata, ns);
			SPFieldData *oldFields = rid.record->fields;
			/*
				this delete lock is for preventing the changing of data
				while facets or sorting are taking place
				-	we could make yet another update lock
					but since those processes are already locking on delete
					just use the delete lock to keep things as simple as possible
			*/
			SPWriteLock(rs->deleteLock);
			rid.record->fields = newFields;
			SPWriteUnlock(rs->deleteLock);

			SPReIndexFields(ns, rid, rid.record->fields, newFields);
			for (int i = 0; i < kv_size(ns->fields); ++i)
			{
				SPDestroyFieldData(&oldFields[i]);
			}
			RedisModule_Free(oldFields);

		} else {
			// this is not likely to ever be called, but just in case my logic changes in the future
			SPWriteLock(rs->deleteLock);
			rid.record->fields = SPFieldDataHashToArray(arg->tfdata, ns);
			SPWriteUnlock(rs->deleteLock);
			SPDoIndexFields(ns, rid, rid.record->fields);	
		}
	} else {
		rid = SPInitRecord(sid, rs->fc);
		k = kh_put(MSTDOC, rs->docs, rid.record->sid, &absent);
		kh_value(rs->docs, k) = rid;
		
		rid.record->fields = SPFieldDataHashToArray(arg->tfdata, ns);
		SPDoIndexFields(ns, rid, rid.record->fields);
	}

	// rid.record->kson = kson_parse(doc, ns);
	// SPWriteLock(rs->deleteLock);

	SP_PACK_DOC(rs, doc, arg->doclen, rid, 1);

	// SPWriteUnlock(rs->deleteLock);

	SPWriteUnlock(rs->lock);
	SPReadUnlock(ns->lock);

	RedisModule_Free(arg->sid);
	RedisModule_Free(arg->doc);
	kh_destroy(FIELDDATA, arg->tfdata);
	RedisModule_Free(arg);
}

int SpredisAddRecord_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);

	RedisModuleKey *key = NULL;
	SPNamespace *ns;
	RedisModuleString *keyName = argv[1];
	// SPLockContext(ctx);
	int res = SPGetNamespaceKeyValue(ctx, &key, &ns, keyName, REDISMODULE_WRITE);
	// SPUnlockContext(ctx);
	if (res != REDISMODULE_OK) {
		return res;
	}
	if (ns == NULL) {
		return RedisModule_ReplyWithNull(ctx);
	}
	// size_t doclen;
	// SPRecordSet *rs = ns->rs;
	SPReadLock(ns->lock);
	// SPWriteLock(rs->lock);
	const char *sid = RedisModule_StringPtrLen(argv[2], NULL);
	

	long long fc = 0;
	RedisModule_StringToLongLong(argv[3], &fc);
	int idx = 4;
	
	RedisModuleString *fname;
	long long valueCount;
	SPFieldDef *fd;
	RedisModuleString *badFieldDef = NULL;
	SPFieldData data;
	


	khash_t(FIELDDATA) *tfdata = kh_init(FIELDDATA);
	int absent;
	khint_t k;
	for (long long i = 0; i < fc; ++i)
	{
		fname = argv[idx++];
		fd = SPFieldDefForName(ns, fname);
		RedisModule_StringToLongLong(argv[idx++], &valueCount);
		if (fd) {
			fd->argTx(ctx, (int)valueCount, argv + idx, &data, fd, ns);
			idx += valueCount * fd->argCount;
			k = kh_put(FIELDDATA, tfdata, fd->name, &absent);
			kh_value(tfdata, k) = data;
		} else {
			printf("badFieldDef: %s\n", RedisModule_StringPtrLen(fname, NULL));
			SPFieldData data;
			const char *name;
			kh_foreach(tfdata, name, data, {
				SPDestroyFieldData(&data);
			});
			badFieldDef = fname;
			kh_destroy(FIELDDATA, tfdata);
			break;
		}
	}

	if (!badFieldDef) {

		SPIndexThreadArg *arg = RedisModule_Alloc(sizeof(SPIndexThreadArg));
		arg->doc = RedisModule_Strdup(RedisModule_StringPtrLen(argv[idx++], &arg->doclen));
		arg->sid = RedisModule_Strdup(sid);
		arg->tfdata = tfdata;
		arg->ns = ns;
		// SPReadLock(ns->lock);
		SPDoWorkInThreadPoolAndWaitForStart(SPDoIndexingWork, arg);
		
	} else {
		kh_destroy(FIELDDATA, tfdata);
	}


	SPReadUnlock(ns->lock);
	RedisModule_ReplicateVerbatim(ctx);

	if (badFieldDef) {
		return RedisModule_ReplyWithError(ctx, RedisModule_StringPtrLen(RedisModule_CreateStringPrintf(ctx, "Bad field Definition: %s", RedisModule_StringPtrLen(badFieldDef, NULL)), NULL));
	}
	RedisModule_ReplyWithSimpleString(ctx, "OK");
	return REDISMODULE_OK;
}

// int SpredisAddRecord_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
// 	RedisModule_ReplicateVerbatim(ctx);
// 	return SPThreadedWork(ctx, argv, argc, SpredisAddRecord_RedisCommandT);
// }

int SpredisDeleteRecord_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
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
	const char *sid = RedisModule_StringPtrLen(argv[2], NULL);
	SPReadLock(ns->lock);
	// printf("Deleteing record: %s\n", sid);
	SPDeleteRecord(ctx, ns, sid);
	SPReadUnlock(ns->lock);
	RedisModule_ReplyWithSimpleString(ctx, "OK");
	RedisModule_ReplicateVerbatim(ctx);
	return REDISMODULE_OK;
}


SPPerms SPEveryPermValue(SPPerms perms, SPFieldData *data, int iter) {
	SPPtrOrD_t *a, *b;
	SPPerms newPerms;
	if (iter == 0) {
		//first iteration, we need to seed perms with the first value
		for (int i = 0; i < data->ilen; ++i)
		{
			a = RedisModule_Calloc(iter + 1, sizeof(SPPtrOrD_t));
			a[iter] = data->iv[i];
			kv_push(SPPtrOrD_t*, perms, a);
		}
		return perms;
	}
	kv_init(newPerms);
	while (kv_size(perms)) {
		b = kv_pop(perms);
		for (int i = 0; i < data->ilen; ++i)
		{
			a = RedisModule_Calloc(iter + 1, sizeof(SPPtrOrD_t));
			memcpy(a, b, iter * sizeof(SPPtrOrD_t));
			a[iter] = data->iv[i];
			kv_push(SPPtrOrD_t*, newPerms, a);
		}
		if (b) RedisModule_Free(b);
	}
	kv_destroy(perms);
	return newPerms;
}

SPPerms SPCompositePermutationsForRecord(SPNamespace *ns, SPRecord *record, SPFieldDef *fd, SPFieldData *fields) {
	SPFieldData *data;
	SPPerms perms;
	kv_init(perms);
	int idx;
	for (int i = 0; i < kv_size(fd->fieldPlaceHolders); ++i)
	{
		idx = SPFieldIndex(ns, kv_A(fd->fieldPlaceHolders, i).name);
		if (idx < 0) break;

		data = &fields[idx];
		// if (data->ilen == 0) {
			//TODO: maybe replcae data with default null value by type
		// }
		perms = SPEveryPermValue(perms, data, i);
	}

	if (idx < 0) {
		printf("Bad field placeholder\n");
		while (kv_size(perms)) {
			SPPtrOrD_t *a = kv_pop(perms);
			if (a) RedisModule_Free(a);
		}
	}
	return perms;
}




int SpredisStoreRecords_RedisCommandT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModuleKey *key = NULL;
	SPNamespace *ns;

	RedisModuleString *keyName = argv[1];
	SPLockContext(ctx);
	int res = SPGetNamespaceKeyValue(ctx, &key, &ns, keyName, REDISMODULE_WRITE);
	SPUnlockContext(ctx);

	if (res != REDISMODULE_OK) {
		return res;
	}
	if (ns == NULL) {
		return RedisModule_ReplyWithLongLong(ctx, 0);
	}

	khash_t(SIDS) *store = SPGetTempSet(argv[2]);
    khash_t(SIDS) *hint = SPGetHintSet(argv[3]);
    if (hint != NULL && kh_size(hint) == 0) return RedisModule_ReplyWithLongLong(ctx,0);

	SPReadLock(ns->lock);

	SPRecordSet *rs = ns->rs;

	SPReadLock(rs->lock);
	khint_t k;
	SPRecordId rid;
	int absent;
	for (int i = 4; i < argc; ++i)
	{
		k = kh_get(MSTDOC, rs->docs, RedisModule_StringPtrLen( argv[i], NULL ));
		if ( k != kh_end(rs->docs)) {
			rid = kh_value(rs->docs, k);
			if (hint == NULL || kh_contains(SIDS, hint, rid.id)) kh_put(SIDS, store, rid.id, &absent);
		}
	}

    SPReadUnlock(rs->lock);
    SPReadUnlock(ns->lock);
	
	RedisModule_ReplyWithLongLong(ctx, kh_size(store));
	return REDISMODULE_OK;
}

int SpredisStoreRecords_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	return SPThreadedWork(ctx, argv, argc, SpredisStoreRecords_RedisCommandT);
}
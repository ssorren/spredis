#ifndef __SPREDIS_SCORE_SET
#define __SPREDIS_SCORE_SET


#include "../spredis.h"


#define sp_scoreset_each(type, ss, kvar, idvar, code) {  \
	kbitr_t ___itr; \
    SPScoreSetKey *___p; \
    kb_itr_first(type, (ss), &___itr); \
    for (; kb_itr_valid(&___itr); kb_itr_next(type, (ss), &___itr)) { \
        ___p = &kb_itr_key(SPScoreSetKey, &___itr); \
        kvar = ___p->value; \
        kh_foreach_key(___p->members->set, idvar, { \
            code \
        }); \
    } \
}


#define SPAddAllToSet(svar, kvar, hintvar) { \
	spid_t ___id; \
    int ___absent; \
    if ((hintvar) != NULL) { \
	    khash_t(SIDS) *___smaller, *___larger; \
	    if (kh_size(hintvar) < kh_size( ((kvar)->members->set) )) { \
	        ___smaller = (hintvar); \
	        ___larger = ((kvar)->members->set); \
	    } else { \
	        ___smaller = ((kvar)->members->set); \
	        ___larger = (hintvar); \
	    } \
	    kh_foreach_key(___smaller,___id, { \
	        if (kh_contains(SIDS, ___larger, ___id)) kh_put(SIDS, (svar), ___id, &___absent); \
	    });    \
    } else if (kh_size((svar)) == 0 && kh_size((kvar)->members->set)) { \
    	kh_dup_set(spid_t, (svar), (kvar)->members->set); \
    } else { \
    	if ((svar)->n_buckets < (kvar)->members->set->n_buckets) kh_resize(SIDS, (svar), (kvar)->members->set->n_buckets);\
	    kh_foreach_key( ((kvar)->members->set) ,___id, { \
	        kh_put(SIDS, (svar), ___id, &___absent); \
	    }); 	\
    } \
}




void SPAddScoreToSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value);
void SPAddLexScoreToSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value);
void SPAddGeoScoreToSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value);
void SPRemScoreFromSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value);
void SPRemLexScoreFromSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value);
void SPRemGeoScoreFromSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value);
// void SPAddAllToSet(khash_t(SIDS) *set, SPScoreSetKey *key, khash_t(SIDS) *hint);

void SPWriteScoreSetToRDB(RedisModuleIO *io, kbtree_t(SCORESET) *ss);
void SPWriteLexSetToRDB(RedisModuleIO *io, kbtree_t(SCORESET) *ss);
void SPWriteGeoSetToRDB(RedisModuleIO *io, kbtree_t(SCORESET) *ss);

void SPReadScoreSetFromRDB(RedisModuleIO *io, kbtree_t(SCORESET) *ss);
void SPReadLexSetFromRDB(RedisModuleIO *io, kbtree_t(SCORESET) *ss);
void SPReadGeoSetFromRDB(RedisModuleIO *io, kbtree_t(SCORESET) *ss);

void SPDestroyScoreSet(kbtree_t(SCORESET) *ss);
void SPDestroyLexScoreSet(kbtree_t(SCORESET) *ss);
void SPDestroyGeoScoreSet(kbtree_t(SCORESET) *ss);

#endif
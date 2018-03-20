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

void SPAddScoreToSet(kbtree_t(SCORESET) *ss, khash_t(SORTTRACK) *st, spid_t id, SPPtrOrD_t value);
void SPAddLexScoreToSet(kbtree_t(SCORESET) *ss, khash_t(SORTTRACK) *st, spid_t id, SPPtrOrD_t value);
void SPAddGeoScoreToSet(kbtree_t(SCORESET) *ss, khash_t(SORTTRACK) *st, spid_t id, SPPtrOrD_t value);
void SPRemScoreFromSet(kbtree_t(SCORESET) *ss, khash_t(SORTTRACK) *st, spid_t id, SPPtrOrD_t value);
void SPRemLexScoreFromSet(kbtree_t(SCORESET) *ss, khash_t(SORTTRACK) *st, spid_t id, SPPtrOrD_t value);
void SPRemGeoScoreFromSet(kbtree_t(SCORESET) *ss, khash_t(SORTTRACK) *st, spid_t id, SPPtrOrD_t value);
void SPAddAllToSet(khash_t(SIDS) *set, SPScoreSetKey *key, khash_t(SIDS) *hint);

void SPWriteScoreSetToRDB(RedisModuleIO *io, kbtree_t(SCORESET) *ss);
void SPWriteLexSetToRDB(RedisModuleIO *io, kbtree_t(SCORESET) *ss);
void SPWriteGeoSetToRDB(RedisModuleIO *io, kbtree_t(SCORESET) *ss);

void SPReadScoreSetFromRDB(RedisModuleIO *io, kbtree_t(SCORESET) *ss, khash_t(SORTTRACK) *st);
void SPReadLexSetFromRDB(RedisModuleIO *io, kbtree_t(SCORESET) *ss, khash_t(SORTTRACK) *st);
void SPReadGeoSetFromRDB(RedisModuleIO *io, kbtree_t(SCORESET) *ss, khash_t(SORTTRACK) *st);

void SPDestroyScoreSet(kbtree_t(SCORESET) *ss);
void SPDestroyLexScoreSet(kbtree_t(SCORESET) *ss);
void SPDestroyGeoScoreSet(kbtree_t(SCORESET) *ss);

#endif
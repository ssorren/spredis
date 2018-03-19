#ifndef __SPREDIS_SCORE_SET
#define __SPREDIS_SCORE_SET


#include "../spredis.h"

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
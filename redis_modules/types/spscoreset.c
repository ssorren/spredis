#include "./spscoreset.h"

static inline SPScoreSetKey * SP_CREATE_SCORESET_KEY() {
	SPScoreSetKey *key = RedisModule_Calloc(1, sizeof(SPScoreSetKey));
	key->scoreSet = RedisModule_Calloc(1, sizeof(SPScoreSetCont));
	key->scoreSet->set = kh_init(SIDS);
	return key;
}

static inline void SP_FREE_SCORESET_KEY(SPScoreSetKey *key) {
	if (key->scoreSet->set) kh_destroy(SIDS, key->scoreSet->set);
	RedisModule_Free(key->scoreSet);
	RedisModule_Free(key);
}

static inline void SP_FREE_LEXSET_KEY(SPScoreSetKey *key) {
	if (key->value) RedisModule_Free((char *)key->value);
	SP_FREE_SCORESET_KEY(key);
}

static inline void SP_FREE_GEOSET_KEY(SPScoreSetKey *key) {
	SP_FREE_SCORESET_KEY(key);
}

static void SPAddScoreToSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value)
{
	SP_DOADD_SCORESET(SCORESET, ss, id, value);
}

static void SPAddLexScoreToSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value)
{
	SP_DOADD_LEXSET(LEXSET, ss, id, value);
}

static void SPAddGeoScoreToSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value)
{
	SP_DOADD_SCORESET(GEOSET, ss, id, value);
}

static void SPRemScoreFromSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value)
{
	SP_DOREM_SCORESET(SCORESET, ss, id, value);
}

static void SPRemLexScoreLexFromSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value)
{
	SP_DOREM_SCORESET(LEXSET, ss, id, value);
}

static void SPRemGeoScoreFromSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value)
{
	SP_DOREM_SCORESET(GEOSET, ss, id, value);
}



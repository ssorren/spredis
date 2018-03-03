#include "./spscoreset.h"

static inline SPScoreSetKey * SP_CREATE_SCORE_SET_KEY() {
	SPScoreSetKey *key = RedisModule_Calloc(1, sizeof(SPScoreSetKey));
	key->scoreSet = RedisModule_Calloc(1, sizeof(SPScoreSetCont));
	return key;
}





static void SPAddScoreToSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value)
{
	SP_DOADD_SCORESET(SCORESET, ss, id, value);
}

static void SPAddLexScoreToSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value)
{
	SP_DOADD_SCORESET(LEXSET, ss, id, value);
}

static void SPAddGeoScoreToSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value)
{
	SP_DOADD_SCORESET(GEOSET, ss, id, value);
}
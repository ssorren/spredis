#include "./spscoreset.h"

static inline SPScoreSetKey * SP_CREATE_SCORESET_KEY() {
	SPScoreSetKey *key = RedisModule_Calloc(1, sizeof(SPScoreSetKey));
	key->scoreSet = RedisModule_Calloc(1, sizeof(SPScoreSetCont));
	key->scoreSet->set = kh_init(SIDS);
	return key;
}

#define  SP_FREE_SCORESET_KEY(key)  \
    if ((key)->scoreSet->set) kh_destroy(SIDS, (key)->scoreSet->set); \
    RedisModule_Free((key)->scoreSet); \
    RedisModule_Free(key)

#define SP_FREE_LEXSET_KEY(key)  \
    if (key->value) RedisModule_Free((char *)key->value); \
    SP_FREE_SCORESET_KEY(key)

#define SP_FREE_GEOSET_KEY(key)  \
    SP_FREE_SCORESET_KEY(key)


#define SP_DOADD_SCORESET_AUX(type, ss, id, value, dup) { \
    SPScoreSetKey ___search = {.value = (SPPtrOrD_t)(value)}; \
    SPScoreSetKey *___key = kb_getp(type, ss, &___search); \
    int absent; \
    if (___key == NULL) { \
        ___key = SP_CREATE_SCORESET_KEY(); \
       	if (dup) { \
        	___key->value = (SPPtrOrD_t)RedisModule_Strdup((char *)(value)); \
	    } else { \
	    	___key->value = (value); \
	    } \
        kb_putp(type, ss, ___key); \
    } \
    if (___key->scoreSet->singleId && ___key->scoreSet->singleId != id) { \
	    kh_put(SIDS, ___key->scoreSet->set, id, &absent); \
	} else { \
		___key->scoreSet->singleId = id; \
	} \
}


#define SP_DOADD_SCORESET(type, ss, id, value) SP_DOADD_SCORESET_AUX(type, ss, id, value, 0)
#define SP_DOADD_LEXSET(type, ss, id, value) SP_DOADD_SCORESET_AUX(type, ss, id, value, 1)

#define SP_DOREM_SCORESET(type, ss, id, value) { \
    SPScoreSetKey ___search = {.value = (SPPtrOrD_t)(value)}; \
    SPScoreSetKey *___key = kb_getp(type, ss, &___search); \
    if (___key != NULL) { \
    	if (___key->scoreSet->singleId == (id)) ___key->scoreSet->singleId = 0; \
    	khint_t ___k = kh_get(SIDS, ___key->scoreSet->set , (id)); \
    	if (kh_exist(___key->scoreSet->set, ___k)) { \
	        kh_del(SIDS, ___key->scoreSet->set, ___k);    \
        } \
        if (kh_size(___key->scoreSet->set) == 0 && ___key->scoreSet->singleId == 0) { \
        	kb_delp(type, ss, &___search); \
        	SP_FREE_##type##_KEY(___key); \
        }\
    } \
}

void SPAddAllToSet(khash_t(SIDS) *set, SPScoreSetKey *key) {
	spid_t id;
	int absent;
	khash_t(SIDS) *res = key->scoreSet->set;
	if (key->scoreSet->singleId) kh_put(SIDS, (set), (key)->scoreSet->singleId, &absent);
	kh_foreach_key(res,id, {
		kh_put(SIDS, set, id, &absent);
	});
}

void SPAddScoreToSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value)
{
	SP_DOADD_SCORESET(SCORESET, ss, id, value);
}

void SPAddLexScoreToSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value)
{
	SP_DOADD_LEXSET(LEXSET, ss, id, value);
}

void SPAddGeoScoreToSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value)
{
	SP_DOADD_SCORESET(GEOSET, ss, id, value);
}

void SPRemScoreFromSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value)
{
	SP_DOREM_SCORESET(SCORESET, ss, id, value);
}

void SPRemLexScoreLexFromSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value)
{
	SP_DOREM_SCORESET(LEXSET, ss, id, value);
}

void SPRemGeoScoreFromSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value)
{
	SP_DOREM_SCORESET(GEOSET, ss, id, value);
}



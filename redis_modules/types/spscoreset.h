#ifndef __SPREDIS_SCORE_SET
#define __SPREDIS_SCORE_SET

#ifndef SP_DEFAULT_TREE_SIZE
#define SP_DEFAULT_TREE_SIZE 2048
#endif

#include "../spredis.h"
static inline SPScoreSetKey * SP_CREATE_SCORESET_KEY();
static inline void SP_FREE_SCORESET_KEY(SPScoreSetKey *key);
static inline void SP_FREE_LEXSET_KEY(SPScoreSetKey *key);
static inline void SP_FREE_GEOSET_KEY(SPScoreSetKey *key);

static void SPAddScoreToSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value);
static void SPAddLexScoreToSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value);
static void SPAddGeoScoreToSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value);

static void SPRemScoreFromSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value);
static void SPRemLexScoreLexFromSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value);
static void SPRemGeoScoreFromSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value);

#define SP_DOADD_SCORESET_AUX(type, ss, id, value, dup) { \
    SPScoreSetKey ___search = {.value = (SPPtrOrD_t)value}; \
    SPScoreSetKey *___key = kb_getp(type, ss, &___search); \
    int absent; \
    if (___key == NULL) { \
        ___key = SP_CREATE_SCORESET_KEY(); \
       	if (dup) { \
        	___key->value = (SPPtrOrD_t)RedisModule_Strdup((char *)value); \
	    } else { \
	    	___key->value = value; \
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
    SPScoreSetKey ___search = {.value = (SPPtrOrD_t)value}; \
    SPScoreSetKey *___key = kb_getp(type, ss, &___search); \
    if (___key != NULL) { \
    	if (___key->scoreSet->singleId == id) ___key->scoreSet->singleId = 0; \
    	khint_t ___k = kh_get(SIDS, ___key->scoreSet->set , id); \
    	if (kh_exist(___key->scoreSet->set, ___k)) { \
	        kh_del(SIDS, ___key->scoreSet->set, ___k);    \
        } \
        if (kh_size(___key->scoreSet->set) == 0 && ___key->scoreSet->singleId == 0) { \
        	kb_delp(type, ss, &___search); \
        	SP_FREE_##type##_KEY(___key); \
        }\
    } \
}


#define SPAddAllToSet(set, ___key) { \
	spid_t ___id; \
	int ___absent; \
	khash_t(SIDS) *___res = key->scoreSet->set; \
	if (___key->scoreSet->singleId) kh_put(SIDS, ___res, ___key->scoreSet->singleId, &___absent); \
	kh_foreach_key(res, ___id, { \
		kh_put(SIDS, ___res, ___id, &___absent); \
	}); \
}

#endif
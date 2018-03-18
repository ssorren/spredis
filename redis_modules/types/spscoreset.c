#include "./spscoreset.h"

static inline SPScoreSetKey * SP_CREATE_SCORESET_KEY() {
	SPScoreSetKey *key = RedisModule_Calloc(1, sizeof(SPScoreSetKey));
	key->scoreSet = RedisModule_Calloc(1, sizeof(SPScoreSetMembers));
	// key->scoreSet->set = kh_init(SIDS);
    key->scoreSet->set = NULL;
	return key;
}

#define  SP_FREE_SCORESET_KEY(key)  \
    if ((key)->scoreSet->set) kh_destroy(SIDS, (key)->scoreSet->set); \
    RedisModule_Free( (key) )

#define SP_FREE_LEXSET_KEY(key)  \
    if (key->value) RedisModule_Free((char *)key->value); \
    SP_FREE_SCORESET_KEY(key)

#define SP_FREE_GEOSET_KEY(key)  \
    SP_FREE_SCORESET_KEY(key)


#define SP_DOADD_SCORESET_AUX(type, ss, st, id, value, dup) { \
    SPScoreSetKey ___search = {.value = (SPPtrOrD_t)(value)}; \
    SPScoreSetKey *___key = kb_getp(type, ss, &___search); \
    int absent; \
    if (___key == NULL) { \
        ___key = SP_CREATE_SCORESET_KEY(); \
       	if (dup) { \
        	___key->value = (SPPtrOrD_t)RedisModule_Strdup((char *)(value)); \
            ___key->scoreSet->score = 0; /* score will be added later on apply sort */ \
	    } else { \
	    	___key->value = (value); \
            ___key->scoreSet->score = (value); \
	    } \
        kb_putp(type, ss, ___key); \
    } \
    if (___key->scoreSet->singleId && ___key->scoreSet->singleId != (id)) { \
        if (___key->scoreSet->set == NULL) ___key->scoreSet->set = kh_init(SIDS);\
	    kh_put(SIDS, ___key->scoreSet->set, (id), &absent); \
	} else { \
		___key->scoreSet->singleId = (id); \
	} \
    if (st != NULL) { \
        khint_t ___k = kh_put(SORTTRACK, (st), (id), &absent);\
        kh_value((st), ___k) = ___key->scoreSet; \
    } \
}


#define SP_DOADD_SCORESET(type, ss, st, id, value) SP_DOADD_SCORESET_AUX(type, ss, st, id, value, 0)
#define SP_DOADD_LEXSET(type, ss, st, id, value) SP_DOADD_SCORESET_AUX(type, ss, st, id, value, 1)

#define SP_DOREM_SCORESET(type, ss, st, id, value) { \
    SPScoreSetKey ___search = {.value = (SPPtrOrD_t)(value)}; \
    SPScoreSetKey *___key = kb_getp(type, ss, &___search); \
    if (___key != NULL) { \
    	if (___key->scoreSet->singleId == (id)) ___key->scoreSet->singleId = 0; \
        if (___key->scoreSet->set != NULL) { \
        	khint_t ___k = kh_get(SIDS, ___key->scoreSet->set , (id)); \
        	if (___k != kh_end(___key->scoreSet->set)) { \
    	        kh_del(SIDS, ___key->scoreSet->set, ___k);    \
            } \
        } \
        if ((___key->scoreSet->set == NULL || kh_size(___key->scoreSet->set) == 0) && ___key->scoreSet->singleId == 0) { \
            /*printf("deleting: %s\n", (char *)(___key->value));*/  \
            kb_delp(type, ss, ___key); \
            SP_FREE_##type##_KEY(___key); \
        }\
    } \
}

void _SPAddAllToSetWithHint(khash_t(SIDS) *set, SPScoreSetKey *key, khash_t(SIDS) *hint) {
    spid_t id;
    int absent;
    khash_t(SIDS) *res = key->scoreSet->set;
    if (key->scoreSet->singleId) kh_put(SIDS, (set), (key)->scoreSet->singleId, &absent);
    if (res != NULL) {
        kh_foreach_key(res,id, {
            if (kh_contains(SIDS, hint, id)) kh_put(SIDS, set, id, &absent);
        });    
    }
}

void SPAddAllToSet(khash_t(SIDS) *set, SPScoreSetKey *key, khash_t(SIDS) *hint) {
    if (hint != NULL) {
       _SPAddAllToSetWithHint(set, key, hint);
       return; 
    }
	spid_t id;
	int absent;
	khash_t(SIDS) *res = key->scoreSet->set;
	if (key->scoreSet->singleId) kh_put(SIDS, (set), (key)->scoreSet->singleId, &absent);
    if (res != NULL) {
        kh_foreach_key(res,id, {
            kh_put(SIDS, set, id, &absent);
        });    
    }
}

void SPAddScoreToSet(kbtree_t(SCORESET) *ss, khash_t(SORTTRACK) *st, spid_t id, SPPtrOrD_t value)
{
	SP_DOADD_SCORESET(SCORESET, ss, st, id, value);
}

void SPAddLexScoreToSet(kbtree_t(SCORESET) *ss, khash_t(SORTTRACK) *st, spid_t id, SPPtrOrD_t value)
{
	SP_DOADD_LEXSET(LEXSET, ss, st, id, value);
}

void SPAddGeoScoreToSet(kbtree_t(SCORESET) *ss, khash_t(SORTTRACK) *st, spid_t id, SPPtrOrD_t value)
{
	SP_DOADD_SCORESET(GEOSET, ss, st, id, value);
}

void SPRemScoreFromSet(kbtree_t(SCORESET) *ss, khash_t(SORTTRACK) *st, spid_t id, SPPtrOrD_t value)
{
	SP_DOREM_SCORESET(SCORESET, ss, st, id, value);
}

void SPRemLexScoreFromSet(kbtree_t(SCORESET) *ss, khash_t(SORTTRACK) *st, spid_t id, SPPtrOrD_t value)
{
	SP_DOREM_SCORESET(LEXSET, ss, st, id, value);
}

void SPRemGeoScoreFromSet(kbtree_t(SCORESET) *ss, khash_t(SORTTRACK) *st, spid_t id, SPPtrOrD_t value)
{
	SP_DOREM_SCORESET(GEOSET, ss, st, id, value);
}


void SPDestroyScoreSet(kbtree_t(SCORESET) *ss)
{
    kbitr_t itr;
    SPScoreSetKey *p;
    kb_itr_first(SCORESET, ss, &itr); // get an iterator pointing to the first
    for (; kb_itr_valid(&itr); kb_itr_next(SCORESET, ss, &itr)) { // move on
        p = &kb_itr_key(SPScoreSetKey, &itr);
        RedisModule_Free(p);
    }
    kb_destroy(SCORESET, ss);
}

void SPDestroyLexScoreSet(kbtree_t(SCORESET) *ss)
{
    kbitr_t itr;
    SPScoreSetKey *p;
    kb_itr_first(SCORESET, ss, &itr); // get an iterator pointing to the first
    for (; kb_itr_valid(&itr); kb_itr_next(SCORESET, ss, &itr)) { // move on
        p = &kb_itr_key(SPScoreSetKey, &itr);
        if (p->value) RedisModule_Free((char *)p->value);
        RedisModule_Free(p);
    }
    kb_destroy(SCORESET, ss);
}

void SPDestroyGeoScoreSet(kbtree_t(SCORESET) *ss)
{
    SPDestroyScoreSet(ss);
}



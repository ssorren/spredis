#include "./spscoreset.h"

// static inline SPScoreSetKey * SP_CREATE_SCORESET_KEY() {
// 	SPScoreSetKey *key = RedisModule_Calloc(1, sizeof(SPScoreSetKey));
// 	key->members = RedisModule_Calloc(1, sizeof(SPScoreSetMembers));
// 	// key->members->set = kh_init(SIDS);
//     key->members->set = NULL;
// 	return key;
// }

#define  SP_FREE_SCORESET_KEY(key)  \
    if ((key)->members->set) kh_destroy(SIDS, (key)->members->set);

#define SP_FREE_LEXSET_KEY(key)  \
    if (key->value) RedisModule_Free((char *)key->value); \
    SP_FREE_SCORESET_KEY(key)

#define SP_FREE_GEOSET_KEY(key)  \
    SP_FREE_SCORESET_KEY(key)


#define SP_DOADD_SCORESET_AUX(type, ss, st, id, value, dup) { \
    SPScoreSetKey ___search = {.value = (SPPtrOrD_t)(value)}; \
    SPScoreSetKey *___key = kb_getp(type, ss, &___search); \
    SPScoreSetKey __create; \
    int absent; \
    if (___key == NULL) { \
       	if (dup) { \
            const char *__lexVal = RedisModule_Strdup((char *)(value)); \
            __create.value = (SPPtrOrD_t)__lexVal; \
            __create.members = RedisModule_Calloc(1, sizeof(SPScoreSetMembers)); \
            __create.members->set = kh_init(SIDS); \
            ___key = &__create; \
            ___key->members->score = 0; /* score will be added later on apply sort */ \
	    } else { \
            __create.value = (value); \
            __create.members = RedisModule_Calloc(1, sizeof(SPScoreSetMembers)); \
            __create.members->set = kh_init(SIDS); \
            ___key = &__create; \
            ___key->members->score = (value); \
	    } \
        kb_putp(type, ss, ___key); \
    } \
    kh_put(SIDS, ___key->members->set, (id), &absent); \
    if (st != NULL) { \
        khint_t ___k = kh_put(SORTTRACK, (st), (id), &absent);\
        kh_value((st), ___k) = ___key->members; \
    } \
}


#define SP_DOADD_SCORESET(type, ss, st, id, value) SP_DOADD_SCORESET_AUX(type, ss, st, id, value, 0)
#define SP_DOADD_LEXSET(type, ss, st, id, value) SP_DOADD_SCORESET_AUX(type, ss, st, id, value, 1)

#define SP_DEL_SCORESET_KEY(key, search) \
        SPScoreSetMembers *___mems = (key)->members; \
        kb_delp(SCORESET, ss, (search)); \
        if (___mems) { \
            kh_destroy(SIDS, ___mems->set); \
            RedisModule_Free(___mems); \
        }

#define SP_DEL_LEXSET_KEY(key, search) \
        void *___ptr = (void *)(key)->value; \
        SPScoreSetMembers *___mems = ___key->members; \
        kb_delp(LEXSET, ss, (search)); \
        if (___ptr) RedisModule_Free(___ptr); \
        if (___mems) { \
            kh_destroy(SIDS, ___mems->set); \
            RedisModule_Free(___mems); \
        }


#define SP_DEL_GEOSET_KEY(key, search) \
        SPScoreSetMembers *___mems = (key)->members; \
        kb_delp(GEOSET, ss, (search)); \
        if (___mems) { \
            if (___mems->set) kh_destroy(SIDS, ___mems->set); \
            RedisModule_Free(___mems); \
        }

#define SP_DOREM_SCORESET(type, ss, st, id, value) { \
    SPScoreSetKey ___search = {.value = (SPPtrOrD_t)(value)}; \
    SPScoreSetKey *___key = kb_getp(type, ss, &___search); \
    if (___key != NULL) { \
    	/*if (___key->members->singleId == (id)) ___key->members->singleId = 0;*/ \
    	khint_t ___k = kh_get(SIDS, ___key->members->set , (id)); \
    	if (___k != kh_end(___key->members->set)) { \
	        kh_del(SIDS, ___key->members->set, ___k);    \
        } \
        if (kh_size(___key->members->set) == 0 /*&& ___key->members->singleId == 0*/) { \
            SP_DEL_##type##_KEY(___key, &___search); \
        }\
    } \
}

void _SPAddAllToSetWithHint(khash_t(SIDS) *set, SPScoreSetKey *key, khash_t(SIDS) *hint) {
    spid_t id;
    int absent;
    // if (key->members->singleId && kh_contains(SIDS, hint, key->members->singleId))
    //     kh_put(SIDS, (set), (key)->members->singleId, &absent);
    khash_t(SIDS) *smaller, *larger;
    if (kh_size(hint) < kh_size(key->members->set)) {
        smaller = hint;
        larger = key->members->set;
    } else {
        smaller = key->members->set;
        larger = hint;
    }
    kh_foreach_key(smaller,id, {
        if (kh_contains(SIDS, larger, id)) kh_put(SIDS, set, id, &absent);
    });    
}

void SPAddAllToSet(khash_t(SIDS) *set, SPScoreSetKey *key, khash_t(SIDS) *hint) {
    if (hint != NULL) {
       _SPAddAllToSetWithHint(set, key, hint);
       return; 
    }
	spid_t id;
	int absent;
    kh_foreach_key(key->members->set,id, {
        kh_put(SIDS, set, id, &absent);
    });    
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
        if (p->members) {
            kh_destroy(SIDS, p->members->set); \
            RedisModule_Free(p->members); \
        }
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
        if (p->members) {
            kh_destroy(SIDS, p->members->set); \
            RedisModule_Free(p->members); \
        }
    }
    kb_destroy(SCORESET, ss);
}



void SPDestroyGeoScoreSet(kbtree_t(SCORESET) *ss)
{
    SPDestroyScoreSet(ss);
}



void SPWriteScoreSetToRDB(RedisModuleIO *io, kbtree_t(SCORESET) *ss) {
    RedisModule_SaveUnsigned(io, kb_size(ss));
    kbitr_t itr;
    SPScoreSetKey *p;
    kb_itr_first(SCORESET, ss, &itr); // get an iterator pointing to the first
    for (; kb_itr_valid(&itr); kb_itr_next(SCORESET, ss, &itr)) { // move on
        p = &kb_itr_key(SPScoreSetKey, &itr);
        RedisModule_SaveDouble(io, (double)p->value);
        size_t count = 0;
        // if (p->members->singleId) count++;
        if (p->members->set) count += kh_size(p->members->set);
        RedisModule_SaveUnsigned(io, count);
        // if (p->members->singleId) RedisModule_SaveUnsigned(io, p->members->singleId);
        if (p->members->set) {
            spid_t id;
            kh_foreach_key(p->members->set, id, {
                RedisModule_SaveUnsigned(io, id);
            }); 
        }
    }
}

void SPWriteLexSetToRDB(RedisModuleIO *io, kbtree_t(SCORESET) *ss) {
    RedisModule_SaveUnsigned(io, kb_size(ss));
    kbitr_t itr;
    SPScoreSetKey *p;
    kb_itr_first(LEXSET, ss, &itr); // get an iterator pointing to the first
    for (; kb_itr_valid(&itr); kb_itr_next(LEXSET, ss, &itr)) { // move on
        p = &kb_itr_key(SPScoreSetKey, &itr);
        const char *key = (char *)p->value;
        RedisModule_SaveStringBuffer(io, key, strlen(key));
        size_t count = 0;
        // if (p->members->singleId) count++;
        if (p->members->set) count += kh_size(p->members->set);
        RedisModule_SaveUnsigned(io, count);
        // if (p->members->singleId) RedisModule_SaveUnsigned(io, p->members->singleId);
        if (p->members->set) {
            spid_t id;
            kh_foreach_key(p->members->set, id, {
                RedisModule_SaveUnsigned(io, id);
            }); 
        }
    }
}

void SPWriteGeoSetToRDB(RedisModuleIO *io, kbtree_t(SCORESET) *ss) {
    SPWriteScoreSetToRDB(io, ss);
}


void SPReadScoreSetFromRDB(RedisModuleIO *io, kbtree_t(SCORESET) *ss, khash_t(SORTTRACK) *st) {
    size_t count = RedisModule_LoadUnsigned(io);
    for (size_t i = 0; i < count; ++i)
    {
        SPPtrOrD_t key = (SPPtrOrD_t)RedisModule_LoadDouble(io);
        size_t keyCount = RedisModule_LoadUnsigned(io);
        for (size_t k = 0; k < keyCount; ++k)
        {
            spid_t id = RedisModule_LoadUnsigned(io);
            SPAddScoreToSet(ss, st, id, key);
        }
    }
}

void SPReadLexSetFromRDB(RedisModuleIO *io, kbtree_t(SCORESET) *ss, khash_t(SORTTRACK) *st) {
    size_t count = RedisModule_LoadUnsigned(io);
    for (size_t i = 0; i < count; ++i)
    {
        RedisModuleString *s = RedisModule_LoadString(io);
        SPPtrOrD_t key = (SPPtrOrD_t)RedisModule_StringPtrLen(s, NULL);
        size_t keyCount = RedisModule_LoadUnsigned(io);
        for (size_t k = 0; k < keyCount; ++k)
        {
            spid_t id = RedisModule_LoadUnsigned(io);
            SPAddLexScoreToSet(ss, st, id, key);
        }
        RedisModule_FreeString(RedisModule_GetContextFromIO(io),s);
    }
}

void SPReadGeoSetFromRDB(RedisModuleIO *io, kbtree_t(SCORESET) *ss, khash_t(SORTTRACK) *st) {
    SPReadScoreSetFromRDB(io, ss, st);
}








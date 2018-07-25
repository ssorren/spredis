#include "./spscoreset.h"
#include <float.h>

#define  SP_FREE_SCORESET_KEY(key)  \
    if ((key)->members->set) kh_destroy(SIDS, (key)->members->set);

#define SP_FREE_LEXSET_KEY(key)  \
    if (key->value) RedisModule_Free((char *)key->value); \
    SP_FREE_SCORESET_KEY(key)

#define SP_FREE_GEOSET_KEY(key)  \
    SP_FREE_SCORESET_KEY(key)


#define SP_DOADD_SCORESET_PTR(type, ss, id, value) { \
    SPScoreSetKey ___search = {.value = (value)}; \
    SPScoreSetKey *___key = kb_getp(type, ss, &___search); \
    int absent; \
    if (___key == NULL) { \
        SPScoreSetKey __create; \
        /*__create.value.asChar = (char *)RedisModule_Strdup((value).asChar);*/ \
        __create.value.asChar = SPUniqStr((value).asChar); \
        __create.members = RedisModule_Calloc(1, sizeof(SPScoreSetMembers)); \
        __create.members->set = kh_init(SIDS); \
        __create.members->score.asDouble = DBL_MIN; /* score will be added later on apply sort */ \
        ___key = kb_putp(type, ss, &__create); \
    } else if (resort != NULL) { *(resort) = 0; } \
    kh_put(SIDS, ___key->members->set, (id), &absent); \
}

#define SP_DOADD_SCORESET_AUX(type, ss, id, value) { \
    SPScoreSetKey ___search = {.value = (value)}; \
    SPScoreSetKey *___key = kb_getp(type, ss, &___search); \
    int absent; \
    if (___key == NULL) { \
        ___key = kb_putp(type, ss, &___search); \
        ___key->members = RedisModule_Calloc(1, sizeof(SPScoreSetMembers)); \
        ___key->members->set = kh_init(SIDS); \
        ___key->members->score = (value); \
    } \
    kh_put(SIDS, ___key->members->set, (id), &absent); \
}



#define SP_DOADD_SCORESET(type, ss, id, value) SP_DOADD_SCORESET_AUX(type, ss, id, value)
// #define SP_DOADD_LEXSET(type, ss, st, id, value, vresort) SP_DOADD_SCORESET_PTR(type, ss, st, id, value, (int *)vresort)
#define SP_DOADD_LEXSET(type, ss, id, value) SP_DOADD_SCORESET_AUX(type, ss, id, value)

#define SP_DEL_SCORESET_KEY(key, search) \
        SPScoreSetMembers *___mems = (key)->members; \
        kb_delp(SCORESET, ss, (search)); \
        if (___mems) { \
            kh_destroy(SIDS, ___mems->set); \
            RedisModule_Free(___mems); \
        }

#define SP_DEL_LEXSET_KEY(key, search) \
        /*void *___ptr = (key)->value.asChar;*/ \
        SPScoreSetMembers *___mems = ___key->members; \
        kb_delp(LEXSET, ss, (search)); \
        /*if (___ptr) RedisModule_Free(___ptr);*/ \
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

#define SP_DOREM_SCORESET(type, ss, id, value) { \
    SPScoreSetKey ___search = {.value = (value)}; \
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

void SPRemLexScoreFromSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value)
{
	SP_DOREM_SCORESET(LEXSET, ss, id, value);
}

void SPRemGeoScoreFromSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value)
{
	SP_DOREM_SCORESET(GEOSET, ss, id, value);
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
        /*using SPUniqStr, don't free anymore*/
        // if (p->value.asChar) RedisModule_Free(p->value.asChar);
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
        RedisModule_SaveDouble(io, p->value.asDouble);
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
        const char *key = p->value.asChar;
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


void SPReadScoreSetFromRDB(RedisModuleIO *io, kbtree_t(SCORESET) *ss) {
    size_t count = RedisModule_LoadUnsigned(io);
    for (size_t i = 0; i < count; ++i)
    {
        SPPtrOrD_t key = {.asDouble = RedisModule_LoadDouble(io)};
        size_t keyCount = RedisModule_LoadUnsigned(io);
        for (size_t k = 0; k < keyCount; ++k)
        {
            spid_t id = RedisModule_LoadUnsigned(io);
            SPAddScoreToSet(ss, id, key);
        }
    }
}

void SPReadLexSetFromRDB(RedisModuleIO *io, kbtree_t(SCORESET) *ss) {
    size_t count = RedisModule_LoadUnsigned(io);
    for (size_t i = 0; i < count; ++i)
    {
        RedisModuleString *s = RedisModule_LoadString(io);
        SPPtrOrD_t key;
        key.asChar = RedisModule_StringPtrLen(s, NULL);
        size_t keyCount = RedisModule_LoadUnsigned(io);
        for (size_t k = 0; k < keyCount; ++k)
        {
            spid_t id = RedisModule_LoadUnsigned(io);
            SPAddLexScoreToSet(ss, id, key);
        }
        RedisModule_FreeString(RedisModule_GetContextFromIO(io),s);
    }
}

void SPReadGeoSetFromRDB(RedisModuleIO *io, kbtree_t(SCORESET) *ss) {
    SPReadScoreSetFromRDB(io, ss);
}








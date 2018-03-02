#ifndef _SP_SHARED_TYPES
#define _SP_SHARED_TYPES

#include <stdlib.h>
#include "../lib/khash.h"
#include "../lib/sp_kbtree.h"
#include "../lib/kvec.h"


#define SPScoreComp(a,b) (((double)(a).score < (double)(b).score) ? -1 : (((double)(b).score < (double)(a).score) ? 1 : kb_generic_cmp((a).id, (b).id)))
#define SPIntScoreComp(a,b) (((uint64_t)(a).score < (uint64_t)(b).score) ? -1 : (((uint64_t)(b).score < (uint64_t)(a).score) ? 1 : kb_generic_cmp((a).id, (b).id)))

#define SPScoreSetComp(a,b) kb_generic_cmp((a).score, (b).score)
#define SPGeoSetComp(a,b)  kb_generic_cmp((a).score, (b).score)
#define SPLexSetComp(a,b)  kb_str_cmp((a).lex, (b).lex)

typedef uint8_t SPHashValueType;
typedef uint8_t SPExpResolverType;
typedef uint64_t SPPtrOrD_t;
typedef kvec_t(SPPtrOrD_t) SPPtrOrD;


// typedef struct _SPGeo {
//     spid_t id;
//     double lat, lon;
//     char *lex;
//     struct _SPGeo *next, *prev;
// } SPGeo;

// typedef struct _SPRevGeo {
//     spid_t id;
//     double lat, lon;
//     double radius;
//     char *lex;
//     struct _SPGeo *next, *prev;
// } SPRevGeo;

typedef struct _SPScore {
    spid_t id;
    double score;
    char *lex;
    struct _SPScore *next, *prev;
} SPScore;


typedef struct _SPScoreKey {
	spid_t id;
    SPPtrOrD_t score;
    void *value;
} SPScoreKey;


#define SPGeoExpType 0
#define SPMathExpType 1

#define SPHashStringType 0
#define SPHashDoubleType 1

typedef struct _SPHashValue {
	spid_t id;
	SPHashValueType type;
	kvec_t(char) used;
    SPPtrOrD values;
    struct _SPHashValue *next, *prev;
} SPHashValue;

// extern int SPLexScoreComp(SPScoreKey a, SPScoreKey b);

static inline int SPLexScoreComp(SPScoreKey a, SPScoreKey b) {
    int res = strcmp((char *)a.score,(char *)b.score);
    return  res ? res : (((a).id < (b).id) ? -1 : (((b).id < (a).id) ? 1 : 0 ));
}




static inline int SPGeoScoreComp(SPScoreKey a, SPScoreKey b) {
    int res = memcmp(&a.score,&b.score,8);
    return  res ? res : (((a).id < (b).id) ? -1 : (((b).id < (a).id) ? 1 : 0 ));
}


KB_TYPE(SCORE);
// KB_TYPE(GEO);
typedef kbtree_t(SCORE) kbtree_t(LEX);
typedef kbtree_t(SCORE) kbtree_t(GEO);
// typedef kbtree_t(SCORE) kbtree_t(REVGEO);

KHASH_DECLARE_SET(SIDS, spid_t);
KHASH_DECLARE(SCORE, spid_t, SPScore*);


typedef struct _SPLexSetKey
{
    char *lex;
    khash_t(SIDS) *set;
} SPLexSetKey;

typedef struct _SPScoreSetKey
{
    double score;
    khash_t(SIDS) *set;
} SPScoreSetKey;

typedef struct _SPGeoSetKey
{
    uint64_t score;
    khash_t(SIDS) *set;
} SPGeoSetKey;

// KHASH_DECLARE(LEX, spid_t, SPScore*);
typedef khash_t(SCORE) khash_t(LEX);

KHASH_DECLARE(HASH, spid_t, SPHashValue*);
KHASH_MAP_INIT_INT64(HASH, SPHashValue*);
KHASH_SET_INIT_INT64(SIDS);
KHASH_MAP_INIT_INT64(SCORE, SPScore*);
KHASH_MAP_INIT_INT64(LEX, SPScore*);

// KHASH_DECLARE(GEO, spid_t, SPGeo*);
// KHASH_MAP_INIT_INT64(GEO, SPGeo*);

// KHASH_DECLARE(REVGEO, spid_t, SPRevGeo*);
// KHASH_MAP_INIT_INT64(REVGEO, SPRevGeo*);

KBTREE_INIT(SCORE, SPScoreKey, SPScoreComp);
KBTREE_INIT(LEX, SPScoreKey, SPLexScoreComp);
KBTREE_INIT(GEO, SPScoreKey, SPIntScoreComp);

KB_TYPE(SCORESET);
KB_TYPE(LEXSET);
KB_TYPE(GEOSET);
KBTREE_INIT(SCORESET, SPScoreSetKey, SPScoreSetComp);
KBTREE_INIT(LEXSET, SPLexSetKey, SPLexSetComp);
KBTREE_INIT(GEOSET, SPGeoSetKey, SPGeoSetComp);
// KBTREE_INIT(REVGEO, SPScoreKey, SPGeoScoreComp);

// SPSCORE_BTREE_INIT(SCORE);
// SPLEX_BTREE_INIT(LEX);

#endif
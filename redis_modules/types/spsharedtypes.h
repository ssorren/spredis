#ifndef _SP_SHARED_TYPES
#define _SP_SHARED_TYPES

#include <stdlib.h>
#include "../lib/khash.h"
#include "../lib/sp_kbtree.h"
#include "../lib/kvec.h"


#define SPScoreComp(a,b) (((double)(a).score < (double)(b).score) ? -1 : (((double)(b).score < (double)(a).score) ? 1 : kb_generic_cmp((a).id, (b).id)))


typedef uint8_t SPHashValueType;
typedef uint64_t SPPtrOrD_t;
typedef kvec_t(SPPtrOrD_t) SPPtrOrD;


typedef struct _SPGeo {
    spid_t id;
    double lat, lon;
    char *lex;
    struct _SPGeo *next, *prev;
} SPGeo;

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




#define SPHashStringType 0
#define SPHashDoubleType 1

typedef struct _SPHashValue {
	spid_t id;
	SPHashValueType type;
	kvec_t(char) used;
    SPPtrOrD values;
    struct _SPHashValue *next, *prev;
} SPHashValue;


extern int SPLexScoreComp(SPScoreKey a, SPScoreKey b);


KB_TYPE(SCORE);
typedef kbtree_t(SCORE) kbtree_t(LEX);

KHASH_DECLARE_SET(SIDS, spid_t);
KHASH_DECLARE(SCORE, spid_t, SPScore*);
KHASH_DECLARE(HASH, spid_t, SPHashValue*);

typedef khash_t(SCORE) khash_t(LEX);
KHASH_DECLARE(GEO, spid_t, SPGeo*);



#define SPSCORE_BTREE_INIT(name) KBTREE_INIT(name, SPScoreKey, SPScoreComp)
#define SPLEX_BTREE_INIT(name) KBTREE_INIT(name, SPScoreKey, SPLexScoreComp)
#endif
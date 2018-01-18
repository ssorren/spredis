#ifndef _SP_SHARED_TYPES
#define _SP_SHARED_TYPES

#include <stdlib.h>
#include "../lib/khash.h"
#include "../lib/sp_kbtree.h"

#define SPScoreComp(a,b) (((a).score < (b).score) ? -1 : (((b).score < (a).score) ? 1 : kb_generic_cmp((a).id, (b).id)))



typedef struct _SPGeo {
    uint32_t id;
    double lat, lon;
    struct _SPGeo *next, *prev;
} SPGeo;


typedef struct _SPScore {
    uint32_t id;//!order is important
    double score;//!order is important
    char *lex;
    struct _SPScore *next, *prev;
} SPScore;

// typedef struct _SPLexScore {
//     uint32_t id; //!order is important
//     double score;//!order is important
//     struct _SPLexScore *next, *prev;
    
// } SPLexScore;

extern int SPLexScoreComp(SPScore a, SPScore b);

/* tree stuff */
/*typedef struct {
	int32_t is_internal:1, n:31;
} kbnode_t;

typedef struct {
	kbnode_t *x;
	int i;
} kbpos_t;

typedef struct {
	kbpos_t stack[KB_MAX_DEPTH], *p;
} kbitr_t;*/


KB_TYPE(SCORE);
typedef kbtree_t(SCORE) kbtree_t(LEX);
// KB_TYPE(LEX);

// KHASH_MAP_INIT_INT(LEX, SPLexScore*)
KHASH_DECLARE_SET(SIDS, uint32_t);
KHASH_DECLARE(SCORE, uint32_t, SPScore*);

typedef khash_t(SCORE) khash_t(LEX);
KHASH_DECLARE(GEO, uint32_t, SPGeo*);
// KHASH_MAP_INIT_INT(GEO, SPGeo*)

#define SPSCORE_BTREE_INIT(name) KBTREE_INIT(name, SPScore, SPScoreComp)
#define SPLEX_BTREE_INIT(name) KBTREE_INIT(name, SPScore, SPLexScoreComp)
#endif
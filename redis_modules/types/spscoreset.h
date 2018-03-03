#ifndef __SPREDIS_SCORE_SET
#define __SPREDIS_SCORE_SET

#ifndef SP_DEFAULT_TREE_SIZE
#define SP_DEFAULT_TREE_SIZE 2048
#endif

#include "../spredis.h"

static void SPAddScoreToSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value);
static void SPAddLexScoreToSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value);
static void SPAddGeoScoreToSet(kbtree_t(SCORESET) *ss, spid_t id, SPPtrOrD_t value);

#endif
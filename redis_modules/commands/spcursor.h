#ifndef __SPREDIS_CURSOR
#define __SPREDIS_CURSOR

#include "../spredis.h"
#define SP_MAX_SORT_FIELDS 16

typedef struct _SPSortData {
	SPPtrOrD_t data[SP_MAX_SORT_FIELDS];
} SPSortData;

typedef struct _SPItem {
    SPRecord *record;
    SPPtrOrD_t *sortData;
} SPItem;

typedef struct _SPCursor {
    SPItem *items;
    long long created;
    size_t count;
} SPCursor;

void SpredisCursorInit();

int SpredisDeleteCursor_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisPrepareCursor_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
SPCursor *SPGetCursor(RedisModuleString *tname);
long long SPOldestCursorTime();
#endif
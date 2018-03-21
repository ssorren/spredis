#ifndef __SPREDIS_FACETRANGE
#define __SPREDIS_FACETRANGE

#include "../spredis.h"

int SpredisFacetRange_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif
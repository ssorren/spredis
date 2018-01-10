#ifndef __SPREDIS_FACET
#define __SPREDIS_FACET

#include "../spredis.h"

void SpredisFacetInit();
int SpredisFacets_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif
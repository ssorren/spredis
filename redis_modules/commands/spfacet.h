#ifndef __SPREDIS_FACET
#define __SPREDIS_FACET

#include "../spredis.h"

int SpredisFacets_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif
#ifndef __SPREDIS_COMPRANGESTORE
#define __SPREDIS_COMPRANGESTORE

#include "../spredis.h"

// void SPStoreRangeInit();
int SpredisCompStoreRange_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif
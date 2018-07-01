#ifndef __SPREDIS_RANGESTORE
#define __SPREDIS_RANGESTORE

#include "../spredis.h"
#include "../types/spscore.h"

// void SpredisStoreRangeByRadius_Thread(SPStoreRadiusTarg *targ);
double SPConvertToMeters(double val, const char * unit);
void SPStoreRangeInit();
int SpredisStoreLexRange_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisStoreRangeByScore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisStoreRangeByRadius_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif
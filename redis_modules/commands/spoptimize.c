#include "../spredis.h"

int SpredisOptimizeIndex_RedisCommandT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	
	return REDISMODULE_OK;
}

int SpredisOptimizeIndex_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_ReplicateVerbatim(ctx);
	return SPThreadedWork(ctx, argv, argc, SpredisOptimizeIndex_RedisCommandT);
}

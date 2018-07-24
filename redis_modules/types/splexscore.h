#ifndef __SPREDIS_LEXSCORE
#define __SPREDIS_LEXSCORE

#ifndef SP_DEFAULT_TREE_SIZE
#define SP_DEFAULT_TREE_SIZE 2048
#endif

#include "../spredis.h"
#include <pthread.h>


int SpredisZLexLinkSet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif
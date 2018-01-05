#ifndef __SPREDIS_SORT
#define __SPREDIS_SORT

#include "../spredis.h"


typedef struct SpredisSortData {
  int id;
  RedisModuleString *key;
  double *scores;
} SpredisSortData;

typedef struct SpredisColumnData {
    SpredisDMapCont **cols;
    int *orders;
    int colCount;
} SpredisColumnData;


// static inline int SpredisSortDataCompareLT(SpredisSortData *a, SpredisSortData *b, SpredisColumnData *mcd);

// static inline void __ks_insertsort_Spredis(SpredisSortData** s, SpredisSortData** t, SpredisColumnData *mcd);
// static inline void ks_combsort_Spredis(size_t n, SpredisSortData** a, SpredisColumnData *mcd);

// static inline void ks_introsort_Spredis(size_t n, SpredisSortData** a, SpredisColumnData *mcd);

// int SpredisMergeSortZset(RedisModuleCtx *ctx, RedisModuleKey *key, unsigned int count, SpredisColumnData *mcd, long long replyStart, long long replyCount, RedisModuleString *dest);
// int SpredisFetchColumnsAndOrders(RedisModuleCtx *ctx, RedisModuleString **argv, SpredisColumnData *mcd, int argOffset);

int SpredisZsetMultiKeySort_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisStoreLexRange_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisStoreRangeByScore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif
#ifndef __SPREDIS_GEO
#define __SPREDIS_GEO

#include "../spredis.h"
#include "./spscore.h"
#include <pthread.h>



// typedef kbitr_t SPScoreIter;
// typedef struct _SPGeoScoreCont {
// 	khash_t(SCORE) *set;
// 	pthread_rwlock_t mutex;
// 	kbtree_t(GEO) *btree;
// } SPGeoScoreCont;

typedef struct _SPGeoSearchAreas {
	SPGeoHashArea area[9];
} SPGeoSearchAreas;

// int SPLexScoreComp(SPScoreKey a, SPScoreKey b);

// SPScoreCont *SPGeoScoreContInit();
// void SPGeoScoreContDestroy(void *cont);
// void SpredisZGeoSetRDBSave(RedisModuleIO *io, void *ptr);
// void SpredisZGeoSetRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value);
// void *SpredisZGeoSetRDBLoad(RedisModuleIO *io, int encver);
// void SpredisZGeoSetFreeCallback(void *value);
// int SPGeoScorePutValue(SPScoreCont *cont, spid_t id, uint16_t pos, double lat, double lon);
// int SPGeoScoreDel(SPScoreCont *cont, spid_t id, double lat, double lon);
// int SpredisZGeoSetAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
// int SpredisZGeoSetScore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
// int SpredisZGeoSetRem_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
// int SpredisZGeoSetCard_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);


uint64_t SPGeoHashEncode(double lat, double lng);
uint64_t SPGeoHashEncodeForRadius(double lat, double lng, double radiusInMeters, SPGeoHashBits *theHash);
// void SPGetSearchAreas(double lat, double lon, double radiusInMeters, SPGeoHashArea *area, SPGeoHashNeighbors *neighbors);
void SPGetSearchAreas(double lat, double lon, double radiusInMeters, SPGeoSearchAreas *areas, SPGeoHashArea *bounds);
void SPGeoHashDecode(uint64_t score, double *lat, double *lon);

#endif

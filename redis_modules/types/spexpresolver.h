#ifndef __SPREDIS_EXPR
#define __SPREDIS_EXPR

#include "../spredis.h"


typedef struct _SpExpResolverCont {
	khash_t(SCORE) *set;
	pthread_rwlock_t mutex;	
	SPExpResolverType type;
	char *keyName;
	void *keyData;
	void *input;
	void (*resolve)(double, void *, double *);
} SpExpResolverCont;

void SPResolveGeoScore(double, void *, double *);
void SpredisExpRslvrDBSave(RedisModuleIO *io, void *ptr);
void SpredisExpRslvrRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value);
void *SpredisExpRslvrRDBLoad(RedisModuleIO *io, int encver);
void SpredisExpRslvrFreeCallback(void *value);


int SpredisSetGeoResolver_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
#endif
#ifndef __SPREDIS_SCORE
#define __SPREDIS_SCORE

#ifndef SP_DEFAULT_TREE_SIZE
#define SP_DEFAULT_TREE_SIZE 2048
#endif

#include "../spredis.h"
#include <pthread.h>

// int SPScoreComp(SPScore a, SPScore b);

typedef kbitr_t SPScoreIter;
// typedef struct _SPScoreCont {
// 	khash_t(SCORE) *set;
// 	pthread_rwlock_t mutex;
// 	kbtree_t(SCORE) *btree;
// } SPScoreCont;


typedef struct _SPScoreCont {
	// khash_t(SCORE) *set;
	int sort;
	pthread_rwlock_t mutex;
	kbtree_t(SCORESET) *btree;
	khash_t(SORTTRACK) *st;
} SPScoreCont;


SPScoreCont *SPScoreContInit();
void SPScoreContDestroy(SPScoreCont *cont);
void SpredisZSetRDBSave(RedisModuleIO *io, void *ptr);
void SpredisZSetRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value);
void *SpredisZSetRDBLoad(RedisModuleIO *io, int encver);
void SpredisZSetFreeCallback(void *value);
int SPScorePutValue(SPScoreCont *cont, spid_t id, double val);
int SPScoreDel(SPScoreCont *cont, spid_t id, double val);
int SpredisZSetAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisZScoreLinkSet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisZSetRem_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisZSetCard_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
#endif
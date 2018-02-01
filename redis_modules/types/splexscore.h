#ifndef __SPREDIS_LEXSCORE
#define __SPREDIS_LEXSCORE

#ifndef SP_DEFAULT_TREE_SIZE
#define SP_DEFAULT_TREE_SIZE 2048
#endif

#include "../spredis.h"
#include <pthread.h>

// int SPScoreComp(SPScore a, SPScore b);

// typedef kbitr_t SPScoreIter;
// typedef struct _SPLexScoreCont {
// 	khash_t(LEX) *set;
// 	pthread_rwlock_t mutex;
// 	kbtree_t(LEX) *btree;
// } SPLexScoreCont;

// int SPLexScoreComp(SPScoreKey a, SPScoreKey b);

SPScoreCont *SPLexScoreContInit();
void SPLexScoreContDestroy(SPScoreCont *cont);
void SpredisZLexSetRDBSave(RedisModuleIO *io, void *ptr);
void SpredisZLexSetRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value);
void *SpredisZLexSetRDBLoad(RedisModuleIO *io, int encver);
void SpredisZLexSetFreeCallback(void *value);
int SPLexScorePutValue(SPScoreCont *cont, spid_t id, const char *lexValue, double val);
int SPLexScoreDel(SPScoreCont *cont, spid_t id);
int SpredisZLexSetAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisZLexSetScore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisZLexSetRem_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisZLexSetCard_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisZLexSetApplySortScores_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
#endif
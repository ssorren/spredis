#ifndef __SPREDIS_MASTER
#define __SPREDIS_MASTER

#include "../spredis.h"

void SpredisInitMaster();

SPNamespace *SPInitNamespace();
void SPDestroyNamespace(SPNamespace *ns);

SPFieldDef *SPInitFieldDef();
void SPDestroyFieldDef(SPFieldDef *fd);
SPFieldDef *SPCopyFieldDef(SPFieldDef *fd);
int SPFieldDefsEqual(SPFieldDef *a, SPFieldDef *b);
SPFieldDef *SPFieldDefForName(SPNamespace *ns, RedisModuleString *fname);
SPFieldDef *SPFieldDefForCharName(SPNamespace *ns, const char *fname);

int SpredisDefineNamespace_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisDescribeNamespace_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SPGetNamespaceKeyValue(RedisModuleCtx *ctx, RedisModuleKey **key, SPNamespace **cont, RedisModuleString *name, int mode);
void SpredisNSSave(RedisModuleIO *io, void *ptr);
void SpredisNSRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value);
void *SpredisNSLoad(RedisModuleIO *io, int encver);
void SpredisNSFree(void *ns);
SPIndexCont *SPIndexForFieldName(SPNamespace *ns, RedisModuleString *name);
SPIndexCont *SPIndexForFieldCharName(SPNamespace *ns, const char *name);
int SPFieldIndex(SPNamespace *ns, const char *name);
// const char *SPNSUniqStr(SPNamespace *ns, const char *str);
// const char *SPNSUniqStrWithFree(SPNamespace *ns, const char *str, void (*freeit)(void*));

int SpredisDeleteTempSet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
khash_t(SIDS) *SPGetTempSet(RedisModuleString *tname);
khash_t(SIDS) *SPCreateTempSet(RedisModuleString *tname, khash_t(SIDS) *res);
int SPDestroyTempSet(RedisModuleString *tname);
khash_t(SIDS) *SPGetHintSet(RedisModuleString *tname);
#endif



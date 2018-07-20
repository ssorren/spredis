#ifndef __SPREDIS_RECSET
#define __SPREDIS_RECSET
#include "../spredis.h"


void SpredisInitRecordSet();
SPRecordId SPInitRecord(const char *id, uint8_t fieldCount);
void SPDestroyRecord(SPRecordId id, SPRecordSet *rs);
SPRecordSet *SPCreateRecordSet(SPNamespace *ns);
void SPDestroyRecordSet(SPRecordSet *rs);
SPRecordId *SPRecordSetToArray(SPRecordSet *rs, int *len);
SPRecordId *SPResultSetToArray(khash_t(SIDS) *set, int *len);
int SPDeleteRecord(RedisModuleCtx *ctx, SPNamespace *ns, const char *id);

static inline int SPAcquirePackCont(SPRecordSet *rs, SPRecordId rid);
static inline void SPReleasePackCont(RedisModuleCtx *ctx, SPRecordSet *rs, SPRecordId rid);

void SPReadRecordSetFromRDB(RedisModuleCtx *ctx, RedisModuleIO *io, SPNamespace *ns);
void SPSaveRecordSetToRDB(RedisModuleCtx *ctx, RedisModuleIO *io, SPRecordSet *rs, SPNamespace *ns);
void SPRewriteRecordSetToAOF(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleIO *io, SPRecordSet *rs, SPNamespace *ns);

int SpredisAddRecord_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpredisDeleteRecord_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
SPPerms SPCompositePermutationsForRecord(SPNamespace *ns, SPRecord *record, SPFieldDef *fd, SPFieldData *fields);

char *SP_UNPACK_DOC(SPRecordSet *rs, SPRecordId rid, int lock);
void SP_PACK_CONTAINER(RedisModuleCtx *ctx, SPRecordSet *rs, SPPackCont *pc, int lock);
#endif
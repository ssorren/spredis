#ifndef __SPREDIS_STRING_TMPRESULT
#define __SPREDIS_STRING_TMPRESULT

#include "../spredis.h"

void SpredisTMPResDBSave(RedisModuleIO *io, void *ptr);
void SpredisTMPResRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value);
void *SpredisTMPResRDBLoad(RedisModuleIO *io, int encver);
void SpredisTMPResFreeCallback(void *value);

#endif
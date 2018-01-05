#ifndef __SPREDIS_HASH
#define __SPREDIS_HASH

#define SPREDISDHASH_ENCODING_VERSION 0

#include "redismodule.h"
// #define RSTRING RedisModuleString

#define SPREDIS_ORDER(order)  (strcasecmp("asc", order) ? 0 : 1)


#define kcalloc(N,Z) RedisModule_Calloc(N,Z)
#define kmalloc(Z) RedisModule_Alloc(Z)
#define krealloc(P,Z) RedisModule_Realloc(P,Z)
#define kfree(P) RedisModule_Free(P)
#include "khash.h"
#include "klist.h"

#define TOCHARKEY(k) RedisModule_StringPtrLen(k,NULL)
#define TOINTKEY(k) (int)strtol(RedisModule_StringPtrLen(k,NULL), NULL, 16)
#define TOINTKEY_PTRLEN(k,len) (int)strtol(RedisModule_StringPtrLen(k,len), NULL, 16)
#define TOINTKEY10(k) (int)strtol(RedisModule_StringPtrLen(k,NULL), NULL, 10)
#define TOINTFROMCHAR(k) (int)strtol(k, NULL, 16)
// #define TOSTRINGFROMCHAR(k) (int)strtol(k, NULL, 16)


// KHASH_MAP_INIT_INT(SPREDISD, double);
// KHASH_MAP_INIT_INT(SPREDISS, RedisModuleString*);

// #define DHash_t khash_t(SPREDISD)
// #define SHash_t khash_t(SPREDISS)

// #define DHashInit() kh_init(SPREDISD)
// #define SHashInit() kh_init(SPREDISS)

// #define DHashGet(hash, id) kh_get(SPREDISD, hash, id)
// #define SHashGet(hash, id) kh_get(SPREDISS, hash, id)

// #define DHashPut(hash, id, res) kh_put(SPREDISD, hash, id, res)
// #define SHashPut(hash, id, res) kh_put(SPREDISS, hash, id, res)

#define SPDBLTYPE 0
#define SPSTRINGTYPE 1
#define SPTMPRESTYPE 2

#define SET_REDIS_KEY_VALUE_TYPE(key, type, value) RedisModule_ModuleTypeSetValue(key, SPSTRINGTYPE ,dhash);

int HASH_NOT_EMPTY_AND_WRONGTYPE(RedisModuleKey *key, int *type, int targetType);
int HASH_EMPTY_OR_WRONGTYPE(RedisModuleKey *key, int *type, int targetType);

int SpredisSetRedisKeyValueType(RedisModuleKey *key, int type, void *value);

#include "spredisutil.h"
#include "types/spstringtype.h"
#include "types/spdoubletype.h"
#include "types/sptempresult.h"
#include "commands/spfacet.h"
#include "commands/spmcsort.h"

#endif
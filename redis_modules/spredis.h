#ifndef __SPREDIS_HASH
#define __SPREDIS_HASH
#include <pthread.h>
#include <inttypes.h>
#include "lib/thpool.h"

#define SP_FAST_TPOOL_SIZE 8
#define SP_SLOW_TPOOL_SIZE 12
#define SP_RLYSLOW_TPOOL_SIZE 16

#define SP_SLOW_THRESHOLD 1000
#define SP_REALLY_SLOW_THRESHOLD 75000


#define SPREDISDHASH_ENCODING_VERSION 0
#define REDISMODULE_EXPERIMENTAL_API
#include "lib/redismodule.h"
// #define RSTRING RedisModuleString

#define SPREDIS_ORDER(order)  (strcasecmp("asc", order) ? 0 : 1)
#define SPREDIS_ORDER_INT(order)  (strcasecmp("asc", order) ? -1 : 1)


#define uthash_malloc(sz) RedisModule_Alloc(sz)      /* malloc fcn                      */
#define uthash_free(ptr,sz) RedisModule_Free(ptr)     /* free fcn                        */
#include "lib/uthash.h"

#define kcalloc(N,Z) RedisModule_Calloc(N,Z)
#define kmalloc(Z) RedisModule_Alloc(Z)
#define krealloc(P,Z) RedisModule_Realloc(P,Z)
#define kfree(P) RedisModule_Free(P)
#include "lib/spredisutil.h"
#include "lib/khash.h"
#include "lib/sp_kbtree.h"
#include "lib/klist.h"
// #include "lib/kbtree.h"
// #include "klist.h"

#define TOCHARKEY(k) RedisModule_StringPtrLen(k,NULL)
#define TOINTKEY(k) strtol(RedisModule_StringPtrLen(k,NULL), NULL, 16)
#define TOINTKEY_PTRLEN(k,len) strtol(RedisModule_StringPtrLen(k,len), NULL, 16)
#define TOINTKEY10(k) strtol(RedisModule_StringPtrLen(k,NULL), NULL, 10)

#define TOINTID(k,base) strtol(RedisModule_StringPtrLen(k,NULL), NULL, base)

#define TOSIZE_TKEY(k) (size_t)strtol(RedisModule_StringPtrLen(k,NULL), NULL, 10)
#define TOINTFROMCHAR(k) strtol(k, NULL, 16)
// #define TOSTRINGFROMCHAR(k) (int)strtol(k, NULL, 16)

#define SPDBLTYPE 0
#define SPSTRINGTYPE 1
#define SPTMPRESTYPE 2
#define SPSETTYPE 3
#define SPZSETTYPE 4
#define SPZLSETTYPE 5

#define SET_REDIS_KEY_VALUE_TYPE(key, type, value) RedisModule_ModuleTypeSetValue(key, SPSTRINGTYPE ,dhash);

int HASH_NOT_EMPTY_AND_WRONGTYPE(RedisModuleKey *key, int *type, int targetType);
int HASH_NOT_EMPTY_AND_WRONGTYPE_CHECKONLY(RedisModuleKey *key, int *type, int targetType);
int HASH_EMPTY_OR_WRONGTYPE(RedisModuleKey *key, int *type, int targetType);

int SpredisSetRedisKeyValueType(RedisModuleKey *key, int type, void *value);

// static threadpool SPLazyPool;
// #define SP_LAZY_CALL(a, obj) thpool_add_work(SPLazyPool, (void*)a, obj)


#include "types/spset.h"
#include "types/spscore.h"
#include "types/splexscore.h"
#include "types/spstringtype.h"
#include "types/spdoubletype.h"
#include "commands/spmcsort.h"
#include "commands/spfacet.h"
#include "types/sptempresult.h"




#endif
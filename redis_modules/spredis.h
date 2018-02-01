#ifndef __SPREDIS_MAIN
#define __SPREDIS_MAIN
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
#include "lib/kvec.h"
#include "lib/kexpr.h"
// #include "lib/kbtree.h"
// #include "klist.h"

typedef khint64_t spid_t;

#define TOCHARKEY(k) RedisModule_StringPtrLen(k,NULL)
#define TOINTKEY(k) strtoull(RedisModule_StringPtrLen(k,NULL), NULL, 16)
#define TOINTKEY_PTRLEN(k,len) strtoull(RedisModule_StringPtrLen(k,len), NULL, 16)
#define TOINTKEY10(k) strtoull(RedisModule_StringPtrLen(k,NULL), NULL, 10)

#define TOINTID(k,base) strtoull(RedisModule_StringPtrLen(k,NULL), NULL, base)

#define TOSIZE_TKEY(k) (size_t)strtoull(RedisModule_StringPtrLen(k,NULL), NULL, 10)
#define TOINTFROMCHAR(k) strtoull(k, NULL, 16)
// #define TOSTRINGFROMCHAR(k) (int)strtol(k, NULL, 16)

#define SPTMPRESTYPE 0
#define SPSETTYPE 1
#define SPZSETTYPE 2
#define SPZLSETTYPE 3
#define SPHASHTYPE 4
#define SPGEOTYPE 5

#define SET_REDIS_KEY_VALUE_TYPE(key, type, value) RedisModule_ModuleTypeSetValue(key, SPSTRINGTYPE ,dhash);

int HASH_NOT_EMPTY_AND_WRONGTYPE(RedisModuleKey *key, int *type, int targetType);
int HASH_NOT_EMPTY_AND_WRONGTYPE_CHECKONLY(RedisModuleKey *key, int *type, int targetType);
int HASH_EMPTY_OR_WRONGTYPE(RedisModuleKey *key, int *type, int targetType);

int SpredisSetRedisKeyValueType(RedisModuleKey *key, int type, void *value);

#define MAX_LAT             90.0
#define MIN_LAT             -90.0

#define MAX_LONG            180.0
#define MIN_LONG            -180.0


#include "lib/geohash.h"
#include "types/spsharedtypes.h"
#include "types/spgeo.h"
#include "types/spset.h"
#include "types/spscore.h"
#include "types/splexscore.h"
#include "types/sphash.h"
#include "commands/spmcsort.h"
#include "commands/sprangestore.h"
#include "commands/spfacet.h"
#include "types/sptempresult.h"



#endif
#ifndef __SPREDIS_MAIN
#define __SPREDIS_MAIN
#include <pthread.h>
#include <inttypes.h>
#include <stdarg.h>
#include <float.h>
#include "lib/thpool.h"
#define REDISMODULE_EXPERIMENTAL_API
#include "lib/redismodule.h"
#include "lib/sp_parallelqueue.h"

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

pthread_t SPMainThread();
int SP_WRITE_LOCK();
int SP_READ_LOCK();
int SP_UNLOCK();

#define kcalloc(N,Z) RedisModule_Calloc(N,Z)
#define kmalloc(Z) RedisModule_Alloc(Z)
#define krealloc(P,Z) RedisModule_Realloc(P,Z)
#define kfree(P) RedisModule_Free(P)
#include "lib/spredisutil.h"
#include "lib/khash.h"
#include "lib/sp_kbtree.h"
#include "lib/kvec.h"
#include "lib/kexpr.h"
#include "lib/thpool.h"
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

#define SPNOTYPE 0
#define SPTMPRESTYPE 1
#define SPSETTYPE 2
#define SPZSETTYPE 3
#define SPZLSETTYPE 4
#define SPHASHTYPE 5
#define SPGEOTYPE 6
#define SPEXPRTYPE 7
#define SPDOCTYPE 8
#define SPCOMPTYPE 9
#define SPNSTYPE 10
#define SPRECORDTYPE 11
#define SPMAXTYPE 12

#define SET_REDIS_KEY_VALUE_TYPE(key, type, value) RedisModule_ModuleTypeSetValue(key, SPSTRINGTYPE ,dhash);

#define SPLockContext(ctx) RedisModule_ThreadSafeContextLock(ctx)
#define SPUnlockContext(ctx) RedisModule_ThreadSafeContextUnlock(ctx)

int SpredisStringToDouble(RedisModuleString *str, double *val);
void kt_for(int n_threads, void (*func)(void*,long,int), void *data, long n);

int HASH_NOT_EMPTY_AND_WRONGTYPE(RedisModuleKey *key, int *type, int targetType);
int HASH_NOT_EMPTY_AND_WRONGTYPE_CHECKONLY(RedisModuleKey *key, int *type, int targetType);
int HASH_NOT_EMPTY(RedisModuleKey *key);
int HASH_EMPTY_OR_WRONGTYPE(RedisModuleKey *key, int *type, int targetType);
void SP_GET_KEYTYPES(RedisModuleKey *key, int *type, int *spType);

int SpredisSetRedisKeyValueType(RedisModuleKey *key, int type, void *value);
int SPDoWorkInThreadPool(void *func, void *arg);
int SPDoWorkInThreadPoolAndWaitForStart(void *func, void *arg);
void SPDoWorkInParallel(void (**func)(void*), void **arg, int jobCount);
int SPThreadedWork(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int (*command)(RedisModuleCtx*, RedisModuleString**, int));

void SpredisDebug(RedisModuleCtx *ctx, const char *fmt,...);
void SpredisLog(RedisModuleCtx *ctx, const char *fmt,...);
void SpredisWarn(RedisModuleCtx *ctx, const char *fmt,...);
const char *SPUniqStr(const char *str);

#define SP_TWORK(f,a, code) {if (SPDoWorkInThreadPool(f,a)) code}
#define SP_TWORK_WAIT(f,a, code) {if (SPDoWorkInThreadPoolAndWaitForStart(f,a)) code}
// void SP_TWORK(void *func, void *arg) {
//     if () {
//         printf("%s\n", "THREAD LAUNCH ERROR!!!!!");
//     }
// }

#define SP_GEN_TPOOL_SIZE 32
#define SP_PQ_POOL_SIZE 24
#define SP_PQ_TCOUNT_SIZE 4
#define SP_PTHRESHOLD 1024


#define MAX_LAT             90.0
#define MIN_LAT             -90.0

#define MAX_LONG            180.0
#define MIN_LONG            -180.0


#include "lib/geohash.h"

#include "types/spsharedtypes.h"
#include "types/spcomposite.h"
#include "types/spscoreset.h"
#include "types/spgeo.h"
#include "types/spset.h"
#include "types/spscore.h"
#include "types/splexscore.h"
#include "types/sphash.h"
#include "types/spexpresolver.h"
#include "commands/spcomprangestore.h"
#include "commands/spmcsort.h"
#include "commands/sprangestore.h"
#include "commands/spfacet.h"
#include "commands/spfacetrange.h"
#include "types/sptempresult.h"
#include "types/spdocument.h"


#include "types/spnamespace.h"
#include "types/sprecordset.h"
#include "commands/spcursor.h"


#include "lib/spsortapplicator.h"



#endif
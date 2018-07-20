#include "../spredis.h"


// typedef struct {
//     RedisModuleBlockedClient *bc;
//     int argc, stripEmpties, includeStore;
//     RedisModuleString **argv;
//     SpredisSetCont * (*command)(SpredisSetCont **, int);
// } SPSetTarg;

void * _SpredisInitSet() {

    SpredisSetCont *cont = RedisModule_Calloc(1, sizeof(SpredisSetCont));
    cont->mutex = (pthread_rwlock_t)PTHREAD_RWLOCK_INITIALIZER;
    pthread_rwlock_init ( &(cont->mutex),NULL );
    cont->linkedSet = 0;
    cont->set = kh_init(SIDS);
    return cont;
}

void * _SpredisInitWithLinkedSet(khash_t(SIDS) *s, pthread_rwlock_t mutex) {

    SpredisSetCont *cont = RedisModule_Calloc(1, sizeof(SpredisSetCont));
    cont->mutex = mutex;
    cont->linkedSet = 1;
    cont->set = s;
    return cont;
}

// int SPThreadedSetReply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
// {
//     return REDISMODULE_OK;
// }

// void SPThreadedSetReplyFree(void *arg)
// {
//     RedisModule_Free(arg);
// }

typedef khash_t(SIDS) SPSortableSet;
static inline int SpredisSetLenCompareLT(SPSortableSet *a, SPSortableSet *b, void *mcd) {
    return kh_size(a) < kh_size(b);
}

SPREDIS_SORT_INIT(SPSortableSet, void, SpredisSetLenCompareLT)

static khash_t(SIDS) *SpredisSIntersect(khash_t(SIDS) **cas, int count) {
    // printf("calling intersect %d\n", count);
    if (count == 0) return NULL;

    khash_t(SIDS) *res = kh_init(SIDS);

    SpredisSPSortableSetSort(count, cas, NULL);
    spid_t id;
    // khint_t bk;
    int absent, add;
    khash_t(SIDS) *comp, *small;
    small = cas[0];
    // printf("%s\n", small == comp[]);
    kh_foreach_key(small, id, {
        add = 1;
        for (int j = 1; j < count; ++j)
        {
            comp = cas[j];
            // bk = kh_get(SIDS, comp, id);
            if (!kh_contains(SIDS, comp, id)) {
                add = 0;
                break;
            }
        }
        if (add) {
            kh_put(SIDS,res, id, &absent);
        }
    });
    
    return res;
}


static khash_t(SIDS) *SpredisSDifference(khash_t(SIDS) **cas, int count) {
    
    
    khash_t(SIDS) *res = kh_init(SIDS);
    if (count == 0) return res;
    khash_t(SIDS) *ca = cas[0];
    if (ca == NULL || kh_size(ca) == 0) return res;
    if (count > 2) {
        khash_t(SIDS) **cbs = cas + 1;
        SpredisSPSortableSetSort(count - 1, cbs, NULL);
    }

    khash_t(SIDS) *comp;
    spid_t id;
    // khint_t bk;
    int absent, add;
    res = _SpredisInitSet();
    
    kh_foreach_key(ca, id, {
        add = 1;
        for (int j = 1; j < count; ++j)
        {
            comp = cas[j];
            // bk = kh_get(SIDS, comp, id);
            if (kh_contains(SIDS, comp, id)) {
                add = 0;
                break;
            }
        }
        if (add) {
            kh_put(SIDS,res,id, &absent);
        }
    });
    
    // if (kh_size(product) == 0) {
    //  _SpredisDestroySet(res);
    //  return NULL;
    // }
    // printf("differnce took %lldms\n", RedisModule_Milliseconds() - startTimer);
    return res;
}

static khash_t(SIDS) *SpredisSUnion(khash_t(SIDS) **cas, int count) {
    khash_t(SIDS) *set, *largest;
    khash_t(SIDS) *res = kh_init(SIDS);
    int absent;
    spid_t id;
    SpredisSPSortableSetSort(count, cas, NULL);
    largest = cas[--count];
    kh_dup_set(spid_t, res, largest); //do a striaght copy on the largest set, no need to iterate through it.
    while (count) {
        set = cas[--count];
        if (set != NULL) {
            kh_foreach_key(set, id, {
                kh_put(SIDS,res, id, &absent);
            });
        }
    }
    return res;
}


static khash_t(SIDS) *SpredisSAddAll(khash_t(SIDS) **cas, int count) {
    khash_t(SIDS) *set;
    khash_t(SIDS) *res = cas[0];
    int absent;
    spid_t id;
    while (count > 1) {
        set = cas[--count];
        if (set != NULL) {
            kh_foreach_key(set, id, {
                kh_put(SIDS, res, id, &absent);
            });
        }
    }
    return res;
}

void SPSetCommandInit() {
    // SPSetCommandPool = thpool_init(SP_GEN_TPOOL_SIZE);
}

static int SpredisSTempBase_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, khash_t(SIDS) *(*command)(khash_t(SIDS)**, int), int stripEmpties, int includeStore) {

    // for (int i = 0; i < argc; ++i)
    // {
    //     printf("%s ", RedisModule_StringPtrLen(argv[i], NULL) );
    // }
    // printf("\n");

    // SPLockContext(ctx);
    int keyCount = argc - 2;

    RedisModuleString **keyNames = argv + 2;
    RedisModuleString *storeName = argv[1];

    khash_t(SIDS) **sets = RedisModule_Calloc((keyCount + includeStore), sizeof(khash_t(SIDS)*));
    khash_t(SIDS) *set, *store;

    // printf("A");
    int actualKeyCount = includeStore;

    for (int i = 0; i < keyCount; ++i)
    {
        set = SPGetTempSet(keyNames[i]);
        if (set != NULL) {
            sets[actualKeyCount++] = set;
        }
    }
    // printf("B");
    if (includeStore) {
        // printf("B.1");
        sets[0] = SPGetTempSet(storeName);
    }

    // for (int i = 0; i < actualKeyCount; ++i)
    // {
    //     printf("set size= %zu\n",  kh_size(sets[i]));
    // }
    
    // printf("C");
    khash_t(SIDS) *result = command(sets, actualKeyCount);
    // printf("D");
    if (result != NULL) {
        store = SPCreateTempSet(storeName, result);    
    } else {
        store = SPGetTempSet(storeName);
    }
    // printf("E");
    RedisModule_ReplyWithLongLong(ctx, kh_size(store));
    RedisModule_Free(sets);

    return REDISMODULE_OK;
    
}



static int SpredisSTempInterstore_RedisCommandT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    return SpredisSTempBase_RedisCommand(ctx, argv, argc, SpredisSIntersect, 0, 0);
}

static int SpredisSTempDifference_RedisCommandT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return SpredisSTempBase_RedisCommand(ctx, argv, argc, SpredisSDifference, 1, 0);
}

static int SpredisSTempUnion_RedisCommandT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return SpredisSTempBase_RedisCommand(ctx, argv, argc, SpredisSUnion, 1, 0);
}

static int SpredisSTempAddAll_RedisCommandT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return SpredisSTempBase_RedisCommand(ctx, argv, argc, SpredisSAddAll, 1, 1);   
}

int SpredisSTempInterstore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return SPThreadedWork(ctx, argv, argc, SpredisSTempInterstore_RedisCommandT);
}

int SpredisSTempDifference_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return SPThreadedWork(ctx, argv, argc, SpredisSTempDifference_RedisCommandT);
}

int SpredisSTempUnion_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return SPThreadedWork(ctx, argv, argc, SpredisSTempUnion_RedisCommandT);
}

int SpredisSTempAddAll_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return SPThreadedWork(ctx, argv, argc, SpredisSTempAddAll_RedisCommandT);
}

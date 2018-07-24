// #pragma GCC diagnostic ignored "-Wunused-function"
#include "../spredis.h"


int SpredisZScoreLinkSet_RedisCommandT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    RedisModuleKey *key;
    SPNamespace *ns = NULL;
    SPLockContext(ctx);

    int keyOk = SPGetNamespaceKeyValue(ctx, &key, &ns, argv[1], REDISMODULE_READ);
    
    SPUnlockContext(ctx);
    if (keyOk != REDISMODULE_OK) {
        return keyOk;
    }
    if (ns == NULL) {
        return RedisModule_ReplyWithLongLong(ctx,0);
    }
    
    SPReadLock(ns->lock);
    SPIndexCont * cont = SPIndexForFieldName(ns, argv[2]);
    if (cont == NULL || cont->type != SPTreeIndexType) {
        SPReadUnlock(ns->lock);
        if (cont != NULL) return RedisModule_ReplyWithError(ctx, "wrong index type");
        return RedisModule_ReplyWithLongLong(ctx,0);    
    }
    khash_t(SIDS) *res = SPGetTempSet(argv[3]);

    double value;
    int scoreRes = SpredisStringToDouble(argv[4], &value);

    if (scoreRes != REDISMODULE_OK) {
        SPReadUnlock(ns->lock);
        return RedisModule_ReplyWithError(ctx, "ERR Could not convert score to double");
    }

    SPScoreSetKey *p;
    SPScoreSetKey search = {.value.asDouble = value};
    SPReadLock(cont->lock);
    p = kb_getp(LEXSET, cont->index.btree, &search);

    if (p) {
        kh_dup_set(spid_t, res, p->members->set);
    }
    SPReadUnlock(cont->lock);
    SPReadUnlock(ns->lock);
    RedisModule_ReplyWithLongLong(ctx, kh_size(res));
    
    return REDISMODULE_OK;
}

int SpredisZScoreLinkSet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 5) return RedisModule_WrongArity(ctx);
    return SPThreadedWork(ctx, argv, argc, SpredisZScoreLinkSet_RedisCommandT);
}



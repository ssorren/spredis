// #pragma GCC diagnostic ignored "-Wunused-function"
#include "../spredis.h"


// KBTREE_INIT(SCORE, SPScore, SPScoreComp);
// KHASH_MAP_INIT_INT(LEX, SPLexScore*)

// typedef struct _SPScoreCont {
// 	khash_t(SCORE) *set;
// 	pthread_rwlock_t mutex;
// 	void *tree;
// } SPScoreCont;
// static const char* SPSCRTYPEptr = ;
SPScoreCont *SPScoreContInit() {
	SPScoreCont *cont = RedisModule_Alloc(sizeof(SPScoreCont));
	// cont->set = kh_init(SCORE);
	cont->btree = kb_init(SCORESET, SP_DEFAULT_TREE_SIZE);
    // cont->mutex = {PTHREAD_RWLOCK_INITIALIZER};
    cont->mutex = (pthread_rwlock_t)PTHREAD_RWLOCK_INITIALIZER;
	pthread_rwlock_init ( &cont->mutex, NULL);
    // pthread_rwlockattr_setkind_np(NULL);
    // pthread_mutex_init(&(cont->sortlock), NULL);
    cont->st = kh_init(SORTTRACK);
	return cont;
}


void SPScoreContDestroy(SPScoreCont *cont) {
	SpredisProtectWriteMap(cont);//, "SPScoreContDestroy");
    SPDestroyScoreSet(cont->btree);
	// kbtree_t(SCORESET)* tree = cont->btree;	
 //    kb_destroy(SCORESET, tree);
    // SPScore *score;
    // kh_foreach_value(cont->set, score, {
    // 	RedisModule_Free(score);
    // });
    kh_destroy(SORTTRACK, cont->st);
    SPRWUnlock(cont);//, "SPScoreContDestroy");
    pthread_rwlock_destroy(&cont->mutex);
    // pthread_mutex_destroy(&(cont->sortlock));
    RedisModule_Free(cont);
}



void SpredisZSetRDBSave(RedisModuleIO *io, void *ptr) {
    SPScoreCont *cont = ptr;
    // SpredisProtectReadMap(cont);//,"SpredisZSetRDBSave");
    SPWriteScoreSetToRDB(io, cont->btree);
    // SPRWUnlock(cont);//,"SpredisZSetRDBSave");
}


void SpredisZSetRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    SPScoreCont *cont = value;
    SPPtrOrD_t val;
    spid_t id;
    sp_scoreset_each(SCORESET, cont->btree, val, id, {
        char ress[32];
        sprintf(ress, "%" PRIx64, id);
        char dbl[50];
        sprintf(dbl, "%1.17g" , val.asDouble);
        RedisModule_EmitAOF(aof,"spredis.zadd","scc", key, ress, dbl);
    });
}



void *SpredisZSetRDBLoad(RedisModuleIO *io, int encver) {
    if (encver != SPREDISDHASH_ENCODING_VERSION) {
        /* We should actually log an error here, or try to implement
           the ability to load older versions of our data structure. */
        return NULL;
    }
    
    SPScoreCont *cont = SPScoreContInit();
    SpredisProtectWriteMap(cont);//, "SpredisZSetRDBLoad");
    SPReadScoreSetFromRDB(io, cont->btree, cont->st);
    SPRWUnlock(cont);//, "SpredisZSetRDBLoad");
    return cont;
}


void SpredisZSetFreeCallback(void *value) {
    if (value == NULL) return;
    SPScoreCont *dhash = value;
    // SPScoreContDestroy(dhash);
    SP_TWORK(SPScoreContDestroy, dhash, {
        //do nothing
    });
}


int SPScorePutValue(SPScoreCont *cont, spid_t id, double val) {
	SpredisProtectWriteMap(cont);//, "SPScorePutValue");
    SPPtrOrD_t value = {.asDouble = val};
    SPAddScoreToSet(cont->btree, cont->st, id, value);
    SPRWUnlock(cont);//, "SPScorePutValue");
	return 1;
}

int SPScoreDel(SPScoreCont *cont, spid_t id, double val) {
	SpredisProtectWriteMap(cont);//, "SPScoreDel");
    SPPtrOrD_t value = {.asDouble = val};
    SPRemScoreFromSet(cont->btree, cont->st, id, value);
	
    SPRWUnlock(cont);//, "SPScoreDel");
	return 1;
}


int SpredisZSetAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) return RedisModule_WrongArity(ctx);
    int argOffset = 2;
    if ( ((argc - argOffset) % 2) != 0 ) return RedisModule_WrongArity(ctx);
    int keyCount = (argc - argOffset) / 2;
    int setCount = 0;
    int argIndex = argOffset;
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_WRITE);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPZSETTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }

    double scores[keyCount]; 
    for (int i = 0; i < keyCount; ++i)
    {
        argIndex++;
        int scoreRes = SpredisStringToDouble(argv[argIndex++], &(scores[i]));
        if (scoreRes != REDISMODULE_OK) {
        	// RedisModule_CloseKey(key);
            return RedisModule_ReplyWithError(ctx, "ERR Could not convert score to double");
        }
    }

    SPScoreCont *dhash;
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        dhash = SPScoreContInit();
        SpredisSetRedisKeyValueType(key,SPZSETTYPE,dhash);
    } else {
        dhash = RedisModule_ModuleTypeGetValue(key);
    }
    
    argIndex = argOffset;
    for (int i = 0; i < keyCount; ++i)
    {
        spid_t id = strtoll(RedisModule_StringPtrLen(argv[argIndex++],NULL), NULL, 16);//TOINTKEY(argv[2]);
        setCount += SPScorePutValue(dhash, id, scores[i]);
        argIndex++;
    }
   
    // int found = hashmap_get(dhash, id, (void **)&d);

    /* if we've aleady seen this id, just set the score */
    // RedisModule_CloseKey(key);
    RedisModule_ReplyWithLongLong(ctx, setCount);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}


int SpredisZSetScore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_READ);
    int keyType;

    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPZSETTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithNull(ctx);
        // RedisModule_CloseKey(key);
        return REDISMODULE_OK;
    }
 //    SPScoreCont *cont = RedisModule_ModuleTypeGetValue(key);
 //    SpredisProtectReadMap(cont);
 //    SPScore *score;
 //    spid_t id = TOINTKEY(argv[2]);
 //    khint_t k = kh_get(SCORE, cont->set, id);
	// if (kh_exist(cont->set, k)) {
	// 	score = kh_value(cont->set, k);
		RedisModule_ReplyWithDouble(ctx, 0);
	// } else {
	// 	RedisModule_ReplyWithNull(ctx);
	// }
    // RedisModule_CloseKey(key);
    // SPRWUnlock(cont);
    return REDISMODULE_OK;
}


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

int SpredisZSetCard_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	// printf("Getting %d\n", TOINTKEY(argv[2]));
    RedisModule_AutoMemory(ctx);
    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1], REDISMODULE_READ);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPZSETTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        // RedisModule_CloseKey(key);
        return REDISMODULE_OK;
    }

    SPScoreCont *dhash = RedisModule_ModuleTypeGetValue(key);
    // SpredisProtectReadMap(dhash);//, "SpredisZSetCard_RedisCommand");
    RedisModule_ReplyWithLongLong(ctx, kb_size(dhash->btree));
    // SPRWUnlock(dhash);//, "SpredisZSetCard_RedisCommand");
    // RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

int SpredisZSetRem_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 4) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_WRITE|REDISMODULE_READ);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPZSETTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx,0);
        // RedisModule_CloseKey(key);
        return REDISMODULE_OK;
    }


    SPScoreCont *cont = RedisModule_ModuleTypeGetValue(key);
    spid_t id = TOINTID(argv[2],16);
    double score;
    // int scoreRes;
    // scoreRes = 
    SpredisStringToDouble(argv[3], &score);

    // int res = 

    RedisModule_ReplyWithLongLong(ctx,SPScoreDel(cont, id, score));

    
    // khint_t k = kh_get(SIDS, dhash->set , id);
    // if (kh_exist(dhash->set, k)) {
    // 	kh_del(SIDS, dhash->set, k);	
    	
    // } else {
    // 	RedisModule_ReplyWithLongLong(ctx,0);
    // }
    // 
    // SpredisProtectReadMap(cont);//, "SpredisZSetRem_RedisCommand");
    if (kb_size(cont->btree) == 0) {
        RedisModule_DeleteKey(key);
    }
    // SPRWUnlock(cont);//, "SpredisZSetRem_RedisCommand");

    // RedisModule_CloseKey(key);
    // RedisModule_ReplyWithLongLong(ctx, (remRes == MAP_OK) ? 1 : 0);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}


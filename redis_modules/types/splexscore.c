// #pragma GCC diagnostic ignored "-Wunused-function"
#include "../spredis.h"


// KBTREE_INIT(kbtree_LEX_t, SPScore, SPLexScoreComp);
// KHASH_DECLARE(LEX, SPScore*);
// typedef struct _SPScoreCont {
// 	khash_t(LEX) *set;
// 	pthread_rwlock_t mutex;
// 	void *tree;
// } SPLexScoreCont;


SPScoreCont *SPLexScoreContInit() {
    return SPScoreContInit();
}


void SPLexScoreContDestroy(SPScoreCont *cont) {
	

    if (cont->sort) {
        SpredisDQSort(cont);
    }
    //!!!IMPORTANT - do not grab write lock until you own the sort lock
    SpredisProtectWriteMap(cont);//, "SPLexScoreContDestroy");
    SPDestroyLexScoreSet(cont->btree);
    // kbtree_t(SCORESET)* tree = cont->btree;  
 //    kb_destroy(SCORESET, tree);
    // SPScore *score;
    // kh_foreach_value(cont->set, score, {
    //  RedisModule_Free(score);
    // });
    kh_destroy(SORTTRACK, cont->st);
    SpredisUnProtectMap(cont);//, "SPLexScoreContDestroy");
    pthread_rwlock_destroy(&cont->mutex);
    // pthread_mutex_destroy(&(cont->sortlock));
    RedisModule_Free(cont);
}



void SpredisZLexSetRDBSave(RedisModuleIO *io, void *ptr) {
    SPScoreCont *cont = ptr;
    // SpredisProtectReadMap(cont);//, "SpredisZLexSetRDBSave");
    RedisModule_SaveSigned(io, cont->sort);
    SPWriteLexSetToRDB(io, cont->btree);
    // SpredisUnProtectMap(cont);//, "SpredisZLexSetRDBSave");
}


void SpredisZLexSetRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value) {
	SPScoreCont *cont = value;
    SPPtrOrD_t val;
    spid_t id;
    sp_scoreset_each(LEXSET, cont->btree, val, id, {
        const char *vkey =  (const char *)val.asChar;
        char ress[32];
        sprintf(ress, "%" PRIx64, id);
        RedisModule_EmitAOF(aof,"spredis.zladd","sclc", key, ress, cont->sort, vkey);
    });
}


void *SpredisZLexSetRDBLoad(RedisModuleIO *io, int encver) {
    if (encver != SPREDISDHASH_ENCODING_VERSION) {
        /* We should actually log an error here, or try to implement
           the ability to load older versions of our data structure. */
        return NULL;
    }
    
    SPScoreCont *cont = SPLexScoreContInit();
    SpredisProtectWriteMap(cont);//, "SpredisZLexSetRDBLoad");
    cont->sort = RedisModule_LoadSigned(io);
    SPReadLexSetFromRDB(io, cont->btree, (cont->sort ? cont->st : NULL));
    SpredisUnProtectMap(cont);
    if (cont->sort) {
        SpredisQSort(cont);
    }
    //, "SpredisZLexSetRDBLoad");
    return cont;
}

void SpredisZLexSetFreeCallback(void *value) {
    if (value == NULL) return;


    SPScoreCont *dhash = value;

    // SPLexScoreContDestroy(dhash);

    SP_TWORK(SPLexScoreContDestroy, dhash, {
        //do nothing
    });
}


int SPLexScorePutValue(SPScoreCont *cont, spid_t id, const char *lexValue, int sort) {
	SpredisProtectWriteMap(cont);//, "SPLexScorePutValue");
    if (sort) cont->sort = sort;
    int resort;
    SPPtrOrD_t val = {.asChar = lexValue};
    SPAddLexScoreToSet(cont->btree, (sort ? cont->st : NULL), id, val, sort ? &resort : NULL);
    SpredisUnProtectMap(cont);
    if (sort && resort) {
        SpredisQSort(cont);
    }
	int res = 1;
    //, "SPLexScorePutValue");
	return res;
}

int SPLexScoreDel(SPScoreCont *cont, spid_t id, const char *lexValue) {
	SpredisProtectWriteMap(cont);//, "SPLexScoreDel");
    SPPtrOrD_t val = {.asChar = lexValue};
    // val.asChar = (char *)lexValue;
    SPRemLexScoreFromSet(cont->btree, (cont->sort ? cont->st : NULL), id, val);
	int res = 0;
	SpredisUnProtectMap(cont);//, "SPLexScoreDel");
	return res;
}


int SpredisZLexSetAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 5) return RedisModule_WrongArity(ctx);
    if ( ((argc - 2) % 3) != 0 ) return RedisModule_WrongArity(ctx);
    int argOffset = 2;
    int keyCount = (argc - argOffset) / 3;
    int setCount = 0;
    int argIndex = argOffset;
    // SPLockContext(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_WRITE);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPZLSETTYPE) != 0) {
        // SPUnlockContext(ctx);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    long long sorts[keyCount]; 
    for (int i = 0; i < keyCount; ++i)
    {
        argIndex++;
        int scoreRes = RedisModule_StringToLongLong(argv[argIndex++], &(sorts[i]));
        if (scoreRes != REDISMODULE_OK) {
        	// RedisModule_CloseKey(key);
            // SPUnlockContext(ctx);
            return RedisModule_ReplyWithError(ctx, "ERR Could not convert score to long long");
        }
        argIndex++;
    }

    SPScoreCont *dhash;
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        dhash = SPLexScoreContInit();
        SpredisSetRedisKeyValueType(key,SPZLSETTYPE,dhash);
    } else {
        dhash = RedisModule_ModuleTypeGetValue(key);
    }
    // SPUnlockContext(ctx);
    argIndex = argOffset;
    for (int i = 0; i < keyCount; ++i)
    {
        spid_t id = strtoll(RedisModule_StringPtrLen(argv[argIndex++],NULL), NULL, 16);//TOINTKEY(argv[2]);
        
        argIndex++;
        setCount += SPLexScorePutValue(dhash, id, RedisModule_StringPtrLen(argv[argIndex++], NULL) , (int)sorts[i]);	
        
    }
    /* if we've aleady seen this id, just set the score */
    // RedisModule_CloseKey(key);
    RedisModule_ReplyWithLongLong(ctx, setCount);
    // SPLockContext(ctx);
    RedisModule_ReplicateVerbatim(ctx);
    // SPUnlockContext(ctx);
    return REDISMODULE_OK;
}

// int SpredisZLexSetAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
//     if (argc < 5) return RedisModule_WrongArity(ctx);
//     if ( ((argc - 2) % 3) != 0 ) return RedisModule_WrongArity(ctx);
//     return SPThreadedWork(ctx, argv, argc, SpredisZLexSetAdd_RedisCommandT);
// }


int SpredisZLexLinkSet_RedisCommandT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // RedisModule_AutoMemory(ctx);
    SPLockContext(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_READ);

    RedisModuleKey *store = RedisModule_OpenKey(ctx,argv[2],
            REDISMODULE_WRITE);

    const char * value = RedisModule_StringPtrLen(argv[3], NULL);

    int keyType;

    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPZLSETTYPE) != 0) {
        SPUnlockContext(ctx);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        SPUnlockContext(ctx);
        RedisModule_ReplyWithNull(ctx);
        return REDISMODULE_OK;
    }

    if (HASH_NOT_EMPTY(store)) {
        SPUnlockContext(ctx);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }

    SPScoreCont *cont = RedisModule_ModuleTypeGetValue(key);
    SPUnlockContext(ctx);
    // printf("Linking\n");
    SpredisProtectReadMap(cont, "SpredisZLexLinkSet_RedisCommand");
    SPScoreSetKey *p;
    SPScoreSetKey search = {.value.asChar = value};
    // search.value.asChar = (char *)value;
    SpredisSetCont *result;
    p = kb_getp(LEXSET, cont->btree, &search);
    // if (p) {
    //     result = _SpredisInitWithLinkedSet(p->members->set, cont->mutex);
    // } else {
    
    // }
    result = _SpredisInitSet();
    if (p) {
        // if (kh_size(p->members->set)) {
        //     kh_resize(SIDS, result->set, p->members->set->n_buckets);
        // }
        kh_dup_set(spid_t, result->set, p->members->set);
        // SPAddAllToSet(result->set, p, (khash_t(SIDS)*)NULL);
    }
    
    SpredisUnProtectMap(cont);//, "SpredisZLexLinkSet_RedisCommand");

    SPLockContext(ctx);
    SpredisSetRedisKeyValueType(store, SPSETTYPE, result);
    SPUnlockContext(ctx);
    RedisModule_ReplyWithLongLong(ctx, kh_size(result->set));
	
    return REDISMODULE_OK;
}
int SpredisZLexLinkSet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 4) return RedisModule_WrongArity(ctx);
    return SPThreadedWork(ctx, argv, argc, SpredisZLexLinkSet_RedisCommandT);
}

int SpredisZLexSetApplySortScores_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_WRITE);
    int keyType;

    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPZLSETTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithNull(ctx);
        // RedisModule_CloseKey(key);
        return REDISMODULE_OK;
    }
    SPScoreCont *cont = RedisModule_ModuleTypeGetValue(key);
    SpredisProtectWriteMap(cont);//, "SpredisZLexSetApplySortScores_RedisCommand");

  //   double newScore = 0;
  //   SPScoreSetKey *skey;
  //   kbitr_t itr;
  //   kb_itr_first(LEXSET, cont->btree, &itr); // get an iterator pointing to the first
  //   while (kb_itr_valid(&itr)) { // move on
  //       skey = &kb_itr_key(SPScoreSetKey, &itr);
  //       if (skey != NULL && skey->members != NULL) {
  //       	skey->members->score = ++newScore;        	
  //       }
		// kb_itr_next(LEXSET, cont->btree, &itr);
  //   }
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    // RedisModule_CloseKey(key);
    SpredisUnProtectMap(cont);//, "SpredisZLexSetApplySortScores_RedisCommand");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}


int SpredisZLexSetCard_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1], REDISMODULE_READ);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPZLSETTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        // RedisModule_CloseKey(key);
        return REDISMODULE_OK;
    }

    SPScoreCont *dhash = RedisModule_ModuleTypeGetValue(key);
    // SpredisProtectReadMap(dhash);//, "SpredisZLexSetCard_RedisCommand");
    RedisModule_ReplyWithLongLong(ctx,kb_size(dhash->btree));
    // SpredisUnProtectMap(dhash);//, "SpredisZLexSetCard_RedisCommand");
    // RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

int SpredisZLexSetRem_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 4) return RedisModule_WrongArity(ctx);
    // SPLockContext(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_WRITE|REDISMODULE_READ);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPZLSETTYPE) != 0) {
        // SPUnlockContext(ctx);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        // SPUnlockContext(ctx);
        RedisModule_ReplyWithLongLong(ctx,0);
        // RedisModule_CloseKey(key);
        return REDISMODULE_OK;
    }


    SPScoreCont *cont = RedisModule_ModuleTypeGetValue(key);
    // SPUnlockContext(ctx);
    spid_t id = TOINTID(argv[2],16);

    RedisModule_ReplyWithLongLong(ctx,SPLexScoreDel(cont, id, RedisModule_StringPtrLen(argv[3], NULL) ));

    if (kb_size(cont->btree) == 0) {
        printf("Deleting key\n");
        RedisModule_DeleteKey(key);
    }

    // RedisModule_CloseKey(key);
    // SPLockContext(ctx);
    RedisModule_ReplicateVerbatim(ctx);
    // SPUnlockContext(ctx);
    return REDISMODULE_OK;
}

// int SpredisZLexSetRem_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
//     if (argc != 4) return RedisModule_WrongArity(ctx);
//     return SPThreadedWork(ctx, argv, argc, SpredisZLexSetRem_RedisCommandT);
// }



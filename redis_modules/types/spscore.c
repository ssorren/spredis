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
	pthread_rwlock_init ( &cont->mutex, NULL);
    cont->st = kh_init(SORTTRACK);
	return cont;
}


void SPScoreContDestroy(SPScoreCont *cont) {
	SpredisProtectWriteMap(cont);
    SPDestroyScoreSet(cont->btree);
	// kbtree_t(SCORESET)* tree = cont->btree;	
 //    kb_destroy(SCORESET, tree);
    // SPScore *score;
    // kh_foreach_value(cont->set, score, {
    // 	RedisModule_Free(score);
    // });
    kh_destroy(SORTTRACK, cont->st);
    SpredisUnProtectMap(cont);
    pthread_rwlock_destroy(&cont->mutex);
    RedisModule_Free(cont);
}



void SpredisZSetRDBSave(RedisModuleIO *io, void *ptr) {
    SPScoreCont *cont = ptr;
    SpredisProtectReadMap(cont);
    SPWriteScoreSetToRDB(io, cont->btree);
    SpredisUnProtectMap(cont);
}


void SpredisZSetRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value) {
	SPScoreCont *cont = value;
    // SpredisProtectReadMap(cont);
    // SPWriteScoreSetToRDB(io, cont->btree);
    // SpredisUnProtectMap(cont);
}



void *SpredisZSetRDBLoad(RedisModuleIO *io, int encver) {
    if (encver != SPREDISDHASH_ENCODING_VERSION) {
        /* We should actually log an error here, or try to implement
           the ability to load older versions of our data structure. */
        return NULL;
    }
    
    SPScoreCont *cont = SPScoreContInit();
    SpredisProtectWriteMap(cont);
    SPReadScoreSetFromRDB(io, cont->btree, NULL);
    SpredisUnProtectMap(cont);
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
	SpredisProtectWriteMap(cont);

    SPAddScoreToSet(cont->btree, NULL, id, (SPPtrOrD_t)val);
    SpredisUnProtectMap(cont);
	return 1;
}

int SPScoreDel(SPScoreCont *cont, spid_t id, double val) {
	SpredisProtectWriteMap(cont);
    SPRemScoreFromSet(cont->btree, NULL, id, (SPPtrOrD_t)val);
	
    SpredisUnProtectMap(cont);
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
        int scoreRes = RedisModule_StringToDouble(argv[argIndex++], &(scores[i]));
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
    // int keyType;

 //    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPZSETTYPE) != 0) {
 //        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
 //    }
 //    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
 //        RedisModule_ReplyWithNull(ctx);
 //        // RedisModule_CloseKey(key);
 //        return REDISMODULE_OK;
 //    }
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
    // SpredisUnProtectMap(cont);
    return REDISMODULE_OK;
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
    SpredisProtectReadMap(dhash);
    RedisModule_ReplyWithLongLong(ctx, kb_size(dhash->btree));
    SpredisUnProtectMap(dhash);
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
    int scoreRes;
    scoreRes = RedisModule_StringToDouble(argv[3], &score);

    // int res = 

    RedisModule_ReplyWithLongLong(ctx,SPScoreDel(cont, id, score));

    
    // khint_t k = kh_get(SIDS, dhash->set , id);
    // if (kh_exist(dhash->set, k)) {
    // 	kh_del(SIDS, dhash->set, k);	
    	
    // } else {
    // 	RedisModule_ReplyWithLongLong(ctx,0);
    // }
    // 
    SpredisProtectReadMap(cont);
    if (kb_size(cont->btree) == 0) {
        RedisModule_DeleteKey(key);
    }
    SpredisUnProtectMap(cont);

    // RedisModule_CloseKey(key);
    // RedisModule_ReplyWithLongLong(ctx, (remRes == MAP_OK) ? 1 : 0);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}


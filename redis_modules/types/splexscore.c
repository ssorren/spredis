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
	SpredisProtectWriteMap(cont);
    SPDestroyLexScoreSet(cont->btree);
    // kbtree_t(SCORESET)* tree = cont->btree;  
 //    kb_destroy(SCORESET, tree);
    // SPScore *score;
    // kh_foreach_value(cont->set, score, {
    //  RedisModule_Free(score);
    // });
    kh_destroy(SORTTRACK, cont->st);
    SpredisUnProtectMap(cont);
    pthread_rwlock_destroy(&cont->mutex);
    RedisModule_Free(cont);
}



void SpredisZLexSetRDBSave(RedisModuleIO *io, void *ptr) {
    SPScoreCont *cont = ptr;
    SpredisProtectReadMap(cont);
    RedisModule_SaveSigned(io, cont->sort);
    SPWriteLexSetToRDB(io, cont->btree);
    SpredisUnProtectMap(cont);
}


void SpredisZLexSetRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value) {
	SPScoreCont *cont = value;
    SPPtrOrD_t val;
    spid_t id;
    sp_scoreset_each(LEXSET, cont->btree, val, id, {
        const char *vkey =  (const char *)val;
        char ress[32];
        sprintf(ress, "%" PRIx64, (unsigned long long)id);
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
    SpredisProtectWriteMap(cont);
    cont->sort = RedisModule_LoadSigned(io);
    SPReadLexSetFromRDB(io, cont->btree, (cont->sort ? cont->st : NULL));
    SpredisUnProtectMap(cont);
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
	SpredisProtectWriteMap(cont);
    if (sort) cont->sort = sort;
    SPAddLexScoreToSet(cont->btree, (sort ? cont->st : NULL), id, (SPPtrOrD_t)lexValue);
	// SPScore *score;
	// khint_t k;
	// int absent;
	int res = 1;

	// SPScoreKey search;
	// k = kh_put(LEX, cont->set, id, &absent);
 //    if (absent) {
 //    	score = RedisModule_Calloc(1, sizeof(SPScore));
	// 	score->id = id;
	// 	score->lex = RedisModule_Strdup(lexValue);
	// 	score->score = val;
	// 	kh_put_value(cont->set, k, score);

	// 	SPScoreKey key = {.id = score->id, .score = (SPPtrOrD_t)score->lex, .value = score};
	// 	kb_putp(LEX, cont->btree, &key);
        
 //    } else {
 //    	score = kh_value(cont->set, k);
 //    	if (strcmp(score->lex, lexValue ) != 0) {
 //    		search.id = score->id;
 //    		search.score = (SPPtrOrD_t)score->lex;
 //            kb_delp(LEX, cont->btree, &search);
 //    		RedisModule_Free(score->lex);
 //    		score->lex = RedisModule_Strdup(lexValue);
	//     	score->score = val;
	//     	SPScoreKey key = {.id = score->id, .score = (SPPtrOrD_t)score->lex, .value = score};
	//     	kb_putp(LEX, cont->btree, &key);
 //    	} else {
 //    		res = 0;
 //    	}
 //    }
    SpredisUnProtectMap(cont);
	return res;
}

int SPLexScoreDel(SPScoreCont *cont, spid_t id, const char *lexValue) {
	SpredisProtectWriteMap(cont);
    SPRemLexScoreFromSet(cont->btree, (cont->sort ? cont->st : NULL), id, (SPPtrOrD_t)lexValue);
	// SPScore *score;
	// khint_t k;
	int res = 0;
	// k = kh_get(LEX, cont->set, id);
	// if (k != kh_end(cont->set)) {
	// 	score = kh_value(cont->set, k);
 //        if (score != NULL) {
 //            SPScoreKey search = {.id=score->id, .score=(SPPtrOrD_t)score->lex};
 //            kb_delp(LEX, cont->btree, &search);
 //            if (score->lex != NULL) RedisModule_Free(score->lex);
 //            kh_del_key_value(LEX, cont->set, k, score, 1);
 //            res = 1;    
 //        }
	// }
	SpredisUnProtectMap(cont);
	return res;
}


int SpredisZLexSetAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 5) return RedisModule_WrongArity(ctx);
    int argOffset = 2;
    if ( ((argc - argOffset) % 3) != 0 ) return RedisModule_WrongArity(ctx);
    int keyCount = (argc - argOffset) / 3;
    int setCount = 0;
    int argIndex = argOffset;
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_WRITE);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPZLSETTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    long long sorts[keyCount]; 
    for (int i = 0; i < keyCount; ++i)
    {
        argIndex++;
        int scoreRes = RedisModule_StringToLongLong(argv[argIndex++], &(sorts[i]));
        if (scoreRes != REDISMODULE_OK) {
        	// RedisModule_CloseKey(key);
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
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}


int SpredisZLexLinkSet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 4) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_READ);

    RedisModuleKey *store = RedisModule_OpenKey(ctx,argv[2],
            REDISMODULE_WRITE);

    const char * value = RedisModule_StringPtrLen(argv[3], NULL);

    int keyType;

    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPZLSETTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithNull(ctx);
        return REDISMODULE_OK;
    }

    if (HASH_NOT_EMPTY(store)) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }

    SPScoreCont *cont = RedisModule_ModuleTypeGetValue(key);
    SpredisProtectReadMap(cont);
    SPScoreSetKey *p;
    SPScoreSetKey search = {.value = (SPPtrOrD_t)value};
    SpredisSetCont *result;
    p = kb_getp(LEXSET, cont->btree, &search);
    if (p) {
        result = _SpredisInitWithLinkedSet(p->members->set, cont->mutex);
    } else {
        result = _SpredisInitSet();
    }
    SpredisSetRedisKeyValueType(store, SPSETTYPE, result);
	RedisModule_ReplyWithLongLong(ctx, kh_size(result->set));
    SpredisUnProtectMap(cont);
    return REDISMODULE_OK;
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
    SpredisProtectWriteMap(cont);

    double newScore = 0;
    SPScoreSetKey *skey;
    kbitr_t itr;
    kb_itr_first(LEXSET, cont->btree, &itr); // get an iterator pointing to the first
    while (kb_itr_valid(&itr)) { // move on
        skey = &kb_itr_key(SPScoreSetKey, &itr);
        if (skey != NULL && skey->members != NULL) {
        	skey->members->score = ++newScore;        	
        }
		kb_itr_next(LEXSET, cont->btree, &itr);
    }
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    // RedisModule_CloseKey(key);
    SpredisUnProtectMap(cont);
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
    SpredisProtectReadMap(dhash);
    RedisModule_ReplyWithLongLong(ctx,kb_size(dhash->btree));
    SpredisUnProtectMap(dhash);
    // RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

int SpredisZLexSetRem_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 4) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_WRITE|REDISMODULE_READ);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPZLSETTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx,0);
        // RedisModule_CloseKey(key);
        return REDISMODULE_OK;
    }


    SPScoreCont *cont = RedisModule_ModuleTypeGetValue(key);
    spid_t id = TOINTID(argv[2],16);

    RedisModule_ReplyWithLongLong(ctx,SPLexScoreDel(cont, id, RedisModule_StringPtrLen(argv[3], NULL) ));

    if (kb_size(cont->btree) == 0) {
        printf("Deleting key\n");
        RedisModule_DeleteKey(key);
    }

    // RedisModule_CloseKey(key);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}


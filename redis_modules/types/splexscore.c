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
	SPScoreCont *cont = RedisModule_Alloc(sizeof(SPScoreCont));
	cont->set = kh_init(LEX);
	cont->btree = kb_init(LEX, SP_DEFAULT_TREE_SIZE);
	pthread_rwlock_init ( &cont->mutex, NULL );
	return cont;
}


void SPLexScoreContDestroy(SPScoreCont *cont) {
	SpredisProtectWriteMap(cont);
    kb_destroy(LEX, cont->btree);
    SPScore *score;
    kh_foreach_value(cont->set, score, {
    	if (score->lex) RedisModule_Free(score->lex);
    	RedisModule_Free(score);
    });
    kh_destroy(LEX, cont->set);
    SpredisUnProtectMap(cont);
    pthread_rwlock_destroy(&cont->mutex);
    RedisModule_Free(cont);
}



void SpredisZLexSetRDBSave(RedisModuleIO *io, void *ptr) {
    SPScoreCont *dhash = ptr;
    SpredisProtectReadMap(dhash);
    RedisModule_SaveUnsigned(io, kh_size(dhash->set));
    SPScore *s = NULL;

    kh_foreach_value(dhash->set, s, {

    	RedisModule_SaveUnsigned(io, s->id);
        RedisModule_SaveDouble(io, s->score);
    	RedisModule_SaveStringBuffer(io, s->lex, strlen(s->lex));
        
    });
    SpredisUnProtectMap(dhash);
}


void SpredisZLexSetRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value) {
	SPScoreCont *dhash = value;
    SPScore *s;
    kh_foreach_value(dhash->set, s, {
    	char ress[32];
        sprintf(ress, "%" PRIx64, (unsigned long long)s->id);
        char score[50];
        sprintf(score, "%1.17g" ,s->score);
        RedisModule_EmitAOF(aof,"spredis.zladd","sccc", key, ress, score, s->lex);
    });
}



void *SpredisZLexSetRDBLoad(RedisModuleIO *io, int encver) {
    if (encver != SPREDISDHASH_ENCODING_VERSION) {
        /* We should actually log an error here, or try to implement
           the ability to load older versions of our data structure. */
        return NULL;
    }
    
    SPScoreCont *dhash = SPLexScoreContInit();
    spid_t valueCount = RedisModule_LoadUnsigned(io);
    for (spid_t i = 0; i < valueCount; ++i)
    {
        spid_t id = RedisModule_LoadUnsigned(io);
        double score = RedisModule_LoadDouble(io);
        RedisModuleString *lex = RedisModule_LoadString(io);
        SPLexScorePutValue(dhash, id, RedisModule_StringPtrLen(lex, NULL), score);
        RedisModule_Free(lex);
    }
    return dhash;
}

void SpredisZLexSetFreeCallback(void *value) {
    if (value == NULL) return;


    SPScoreCont *dhash = value;

    // SPLexScoreContDestroy(dhash);

    SP_TWORK(SPLexScoreContDestroy, dhash, {
        //do nothing
    });
}


int SPLexScorePutValue(SPScoreCont *cont, spid_t id, const char *lexValue, double val) {
	SpredisProtectWriteMap(cont);
	SPScore *score;
	khint_t k;
	int absent;
	int res = 1;

	SPScoreKey search, *oldKey;
	k = kh_put(LEX, cont->set, id, &absent);
    if (absent) {
    	score = RedisModule_Calloc(1, sizeof(SPScore));
		score->id = id;
		score->lex = RedisModule_Strdup(lexValue);
		score->score = val;
		kh_put_value(cont->set, k, score);

		SPScoreKey key = {.id = score->id, .score = (SPPtrOrD_t)score->lex, .value = score};
		kb_putp(LEX, cont->btree, &key);
        
    } else {
    	score = kh_value(cont->set, k);
    	if (strcmp(score->lex, lexValue ) != 0) {
    		search.id = score->id;
    		search.score = (uint64_t)score->lex;
    		oldKey = kb_getp(LEX, cont->btree, &search);
    		if (oldKey) {
				kb_delp(LEX, cont->btree, oldKey);
			}
    		RedisModule_Free(score->lex);
    		score->lex = RedisModule_Strdup(lexValue);
	    	score->score = val;
	    	SPScoreKey key = {.id = score->id, .score = (SPPtrOrD_t)score->lex, .value = score};
	    	kb_putp(LEX, cont->btree, &key);
    	} else {
    		res = 0;
    	}
    }
    SpredisUnProtectMap(cont);
	return res;
}

int SPLexScoreDel(SPScoreCont *cont, spid_t id) {
	SpredisProtectWriteMap(cont);
	SPScore *score;
	SPScoreKey *key = NULL;
	// kbtree_t(LEX)* tree = cont->btree;;
	khint_t k;
	int res = 0;
	k = kh_get(LEX, cont->set, id);
	if (k != kh_end(cont->set)) {
		score = kh_value(cont->set, k);
		SPScoreKey search = {.id=score->id, .score=(uint64_t)score->lex};
		key = kb_getp(LEX, cont->btree, &search);		
		if (key) {
			kb_delp(LEX, cont->btree, key);
		}
		if (score->lex) RedisModule_Free(score->lex);
		kh_del_key_value(LEX, cont->set, k, score, 1);
		res = 1;
	}
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
    double scores[keyCount]; 
    for (int i = 0; i < keyCount; ++i)
    {
        argIndex++;
        int scoreRes = RedisModule_StringToDouble(argv[argIndex++], &(scores[i]));
        if (scoreRes != REDISMODULE_OK) {
        	// RedisModule_CloseKey(key);
            return RedisModule_ReplyWithError(ctx, "ERR Could not convert score to double");
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
        setCount += SPLexScorePutValue(dhash, id, RedisModule_StringPtrLen(argv[argIndex++], NULL) , scores[i]);	
        
    }
    /* if we've aleady seen this id, just set the score */
    // RedisModule_CloseKey(key);
    RedisModule_ReplyWithLongLong(ctx, setCount);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}


int SpredisZLexSetScore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_READ);
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
    SpredisProtectReadMap(cont);
    SPScore *score;
    spid_t id = TOINTKEY(argv[2]);
    khint_t k = kh_get(LEX, cont->set, id);
	if (kh_exist(cont->set, k)) {
		score = kh_value(cont->set, k);
		RedisModule_ReplyWithDouble(ctx, score->score);
	} else {
		RedisModule_ReplyWithNull(ctx);
	}
    // RedisModule_CloseKey(key);
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
    SPScoreKey *skey;
    SPScore *score;
    kbitr_t itr;
    kb_itr_first(LEX, cont->btree, &itr); // get an iterator pointing to the first
    while (kb_itr_valid(&itr)) { // move on
        skey = &kb_itr_key(SPScoreKey, &itr);
        if (skey != NULL && skey->value != NULL) {
        	score = skey->value;
        	score->score = ++newScore;        	
        }
		kb_itr_next(LEX, cont->btree, &itr);
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
    RedisModule_ReplyWithLongLong(ctx,kh_size(dhash->set));
    SpredisUnProtectMap(dhash);
    // RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

int SpredisZLexSetRem_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) return RedisModule_WrongArity(ctx);
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

    RedisModule_ReplyWithLongLong(ctx,SPLexScoreDel(cont, id));

    if (kh_size(cont->set) == 0) {
        RedisModule_DeleteKey(key);
    }

    // RedisModule_CloseKey(key);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}


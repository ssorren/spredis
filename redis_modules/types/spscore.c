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
	cont->set = kh_init(SCORE);
	cont->btree = kb_init(SCORE, SP_DEFAULT_TREE_SIZE);
	pthread_rwlock_init ( &cont->mutex, NULL );
	return cont;
}


void SPScoreContDestroy(SPScoreCont *cont) {
	SpredisProtectWriteMap(cont);
	kbtree_t(SCORE)* tree = cont->btree;	
    kb_destroy(SCORE, tree);
    SPScore *score;
    kh_foreach_value(cont->set, score, {
    	RedisModule_Free(score);
    });
    kh_destroy(SCORE, cont->set);
    SpredisUnProtectMap(cont);
    pthread_rwlock_destroy(&cont->mutex);
    RedisModule_Free(cont);
}



void SpredisZSetRDBSave(RedisModuleIO *io, void *ptr) {
    SPScoreCont *dhash = ptr;
    // kbtree_t(SCORE)* tree = dhash->btree;
    // kbtree_t(SCORE)* tree = cont->btree;
    RedisModule_SaveUnsigned(io, kh_size(dhash->set));
    // spid_t id;
    SPScore *s;
    kh_foreach_value(dhash->set, s, {
    	RedisModule_SaveUnsigned(io, s->id);
        RedisModule_SaveDouble(io, s->score);
    });
}


void SpredisZSetRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value) {
	SPScoreCont *dhash = value;
    SPScore *s;
    kh_foreach_value(dhash->set, s, {
    	char ress[32];
        sprintf(ress, "%" PRIx64, (unsigned long long)s->id);
        char score[50];
        sprintf(score, "%1.17g" ,s->score);
        RedisModule_EmitAOF(aof,"spredis.zadd","sclc", key, ress, score);
    });

    // kh_foreach_key(dhash->set, id, {
    // 	char ress[32];
    //     sprintf(ress, "%" PRIx32, id);
    //     // RedisModule_CreateStringPrintf(ctx, "%" PRIx32, id);
    // 	RedisModule_EmitAOF(aof,"spredis.sadd","sc", key, ress);
    // });
}



void *SpredisZSetRDBLoad(RedisModuleIO *io, int encver) {
    if (encver != SPREDISDHASH_ENCODING_VERSION) {
        /* We should actually log an error here, or try to implement
           the ability to load older versions of our data structure. */
        return NULL;
    }
    
    SPScoreCont *dhash = SPScoreContInit();
    spid_t valueCount = RedisModule_LoadUnsigned(io);
    for (spid_t i = 0; i < valueCount; ++i)
    {
        spid_t id = RedisModule_LoadUnsigned(io);
        double score = RedisModule_LoadDouble(io);
        SPScorePutValue(dhash, id, score);
    }
    // kbtree_t(SCORE)* tree = dhash->btree;
    // printf("Loaded zset, count =%u, %d\n", kh_size(dhash->set), (tree == 0));
    // printf("Loaded tree, count =%u", tree->n_keys);
    return dhash;
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
	SPScore *score;
	kbtree_t(SCORE) *tree = cont->btree;;
	khint_t k;
	int absent;
	int res = 1;
	k = kh_put(SCORE, cont->set, id, &absent);

    if (absent) {
    	score = RedisModule_Calloc(1, sizeof(SPScore));
		score->id = id;
		score->score = val;
        kh_put_value(cont->set, k, score);
        SPScoreKey key = {.id=score->id, .score=(uint64_t)score->score, .value = score};
        kb_putp(SCORE, tree, &key);
    } else {
    	score = kh_value(cont->set, k);
    	if (kh_value(cont->set, k)->score != val) {
    		SPScoreKey search = {.id=score->id, .score=(uint64_t)score->score};
    		kb_delp(SCORE, tree, &search);
	    	kh_value(cont->set, k)->score = val;
	    	SPScoreKey key = {.id=score->id, .score=(uint64_t)score->score, .value = score};
	    	kb_putp(SCORE, tree, &key);	
    	} else {
    		res = 0;
    	}
    }
    SpredisUnProtectMap(cont);
	return res;
}

int SPScoreDel(SPScoreCont *cont, spid_t id) {
	SpredisProtectWriteMap(cont);
	SPScore *score;
	kbtree_t(SCORE)* tree = cont->btree;;
	khint_t k;
	int res = 0;
	k = kh_get(SCORE, cont->set, id);
	if (k != kh_end(cont->set)) {
		score = kh_value(cont->set, k);
		SPScoreKey search = {.id=score->id, .score=(uint64_t)score->score};
		kb_delp(SCORE, tree, &search);
		kh_del_key_value(SCORE, cont->set, k, score, 1)
		res = 1;
	}
	SpredisUnProtectMap(cont);
	return res;
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
    int keyType;

    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPZSETTYPE) != 0) {
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
    khint_t k = kh_get(SCORE, cont->set, id);
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
    RedisModule_ReplyWithLongLong(ctx,kh_size(dhash->set));
    SpredisUnProtectMap(dhash);
    // RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

int SpredisZSetRem_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) return RedisModule_WrongArity(ctx);
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


    // int res = 

    RedisModule_ReplyWithLongLong(ctx,SPScoreDel(cont, id));

    // SpredisProtectWriteMap(dhash);
    // khint_t k = kh_get(SIDS, dhash->set , id);
    // if (kh_exist(dhash->set, k)) {
    // 	kh_del(SIDS, dhash->set, k);	
    	
    // } else {
    // 	RedisModule_ReplyWithLongLong(ctx,0);
    // }
    // SpredisUnProtectMap(dhash);

    if (kh_size(cont->set) == 0) {
        RedisModule_DeleteKey(key);
    }

    // RedisModule_CloseKey(key);
    // RedisModule_ReplyWithLongLong(ctx, (remRes == MAP_OK) ? 1 : 0);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

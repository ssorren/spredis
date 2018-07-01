#include "../spredis.h"



SPHashCont *SpHashContInit(SPHashValueType valueType) {
	SPHashCont *cont = RedisModule_Calloc(1, sizeof(SPHashCont));
	cont->set = kh_init(HASH);
    cont->mutex = (pthread_rwlock_t)PTHREAD_RWLOCK_INITIALIZER;
	pthread_rwlock_init ( &cont->mutex,NULL );
	cont->valueType = valueType;
	return cont;
}

SPHashValue * SPHashInitValue(SPHashValueType valueType) {
	SPHashValue *hashVal =  RedisModule_Calloc(1, sizeof(SPHashValue));
	hashVal->type = valueType;
	kv_init(hashVal->used);
	kv_init(hashVal->values);
	return hashVal;
}


int SPHashPutValue(SPHashCont *cont, spid_t id, uint16_t pos, SPPtrOrD_t val) {
	khint_t k;
    int absent;
    SPHashValue *hashVal;
	k = kh_put(HASH, cont->set, id, &absent);
	if (absent) {
		hashVal = SPHashInitValue(cont->valueType);
		hashVal-> id = id;
		kh_put_value(cont->set, k, hashVal);
	} else {
		hashVal = kh_value(cont->set, k);
	}
    /* don't free anymore, using SPUniqStr*/
    kv_set_value(SPPtrOrD_t, hashVal->used, hashVal->values, val, pos, 0);
	// kv_set_value(SPPtrOrD_t, hashVal->used, hashVal->values, val, pos, (hashVal->type == SPHashStringType));
	return 1;
}

void SpredisHashRDBSave(RedisModuleIO *io, void *ptr) {
	if (ptr == NULL) return;
	SPHashCont *cont = ptr;
	// SpredisProtectReadMap(cont);//,"SpredisHashRDBSave");
	SPHashValue *hv = NULL;
	RedisModule_SaveUnsigned(io, kh_size(cont->set));
	RedisModule_SaveUnsigned(io, cont->valueType);
	uint16_t count = 0, pos = 0;
	SPPtrOrD_t val;
	const char *sVal;
    kh_foreach_value(cont->set, hv, {
    	count = 0;
    	RedisModule_SaveUnsigned(io, hv->id);
		kv_used_count(hv->used, &count);
		RedisModule_SaveUnsigned(io, count);
		kv_foreach_value(hv->used, hv->values, &val, &pos, {
			RedisModule_SaveUnsigned(io, pos);
			if (cont->valueType == SPHashStringType) {
				sVal = val.asChar;
				RedisModule_SaveStringBuffer(io, sVal, strlen(sVal));
			} else {
				RedisModule_SaveDouble(io, val.asDouble);
			}
		});
    });
	// SpredisUnProtectMap(cont);//,"SpredisHashRDBSave");
}

void SpredisHashRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value) {
	if (value == NULL) return;
	SPHashCont *cont = value;
	// SpredisProtectReadMap(cont);//, "SpredisHashRewriteFunc");
	SPHashValue *hv = NULL;
	uint16_t pos = 0;
	SPPtrOrD_t val;
	const char *sVal;
    kh_foreach_value(cont->set, hv, {
    	char ress[32];
        sprintf(ress, "%" PRIx64, (uint64_t)hv->id);
		kv_foreach_value(hv->used, hv->values, &val, &pos, {
			if (cont->valueType == SPHashStringType) {
				sVal = val.asChar;
				RedisModule_EmitAOF(aof,"spredis.hsetstr","sclc", key, ress, pos, sVal);
			} else {
				char score[50];
		        sprintf(score, "%1.17g" ,val.asDouble);
		        RedisModule_EmitAOF(aof,"spredis.hsetdbl","sclc", key, ress, pos, score);
			}
		});
    });
	// SpredisUnProtectMap(cont);//, "SpredisHashRewriteFunc");
}

void *SpredisHashRDBLoad(RedisModuleIO *io, int encver) {
	if (encver != SPREDISDHASH_ENCODING_VERSION) {
        /* We should actually log an error here, or try to implement
           the ability to load older versions of our data structure. */
        return NULL;
    }
	size_t valueCount = RedisModule_LoadUnsigned(io);
	uint8_t valueType = (uint8_t)RedisModule_LoadUnsigned(io);
	SPHashCont *cont = SpHashContInit(valueType);
	SPPtrOrD_t val;
	for (size_t i = 0; i < valueCount; ++i)
    {
    	spid_t id = RedisModule_LoadUnsigned(io);
    	uint16_t icnt = RedisModule_LoadUnsigned(io);
    	for (uint16_t i = 0; i < icnt; ++i)
    	{
    		uint16_t pos = RedisModule_LoadUnsigned(io);
    		if (cont->valueType == SPHashStringType) {
    			RedisModuleString *s = RedisModule_LoadString(io);

                val.asChar = SPUniqStr( RedisModule_StringPtrLen(s, NULL));
				// val.asChar = RedisModule_Strdup( RedisModule_StringPtrLen(s, NULL));

                RedisModule_FreeString(RedisModule_GetContextFromIO(io),s);
			} else {
				val.asDouble = RedisModule_LoadDouble(io);
			}
    		SPHashPutValue(cont, id, pos, val);
    	}
    }
	return cont;
}

void SpredisHashDestroy(void *value) {
	if (value == NULL) return;
	SPHashCont *cont = value;
	// SPPtrOrD_t t;
	SPHashValue *hv;
	// uint16_t pos = 0;
	SpredisProtectWriteMap(cont);//, "SpredisHashDestroy");
	kh_foreach_value(cont->set, hv, {
		// if (hv->type == SPHashStringType) {
		// 	kv_foreach_value(hv->used, hv->values, &t, &pos, {
		// 		RedisModule_Free(t.asChar);
		// 	});
		// }
		kv_destroy(hv->used);
		kv_destroy(hv->values);
		RedisModule_Free(hv);
    });
    kh_destroy(HASH, cont->set);
    SpredisUnProtectMap(cont);//, "SpredisHashDestroy");
    pthread_rwlock_destroy(&cont->mutex);
    RedisModule_Free(cont);
}


void SpredisHashFreeCallback(void *value) {
    if (value == NULL) return;
    SP_TWORK(SpredisHashDestroy, value, {
        //do nothing
    });

}

int SpredisHashSet(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, SPHashValueType valueType) {
	if (argc < 5) return RedisModule_WrongArity(ctx);
    int argOffset = 2;
    if ( ((argc - argOffset) % 3) != 0 ) return RedisModule_WrongArity(ctx);
    int keyCount = (argc - argOffset) / 3;
    int setCount = 0;
    SPHashCont *cont;
    // int argIndex = argOffset;
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_WRITE);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPHASHTYPE) != 0) {
    	// RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }

    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
    	cont = SpHashContInit(valueType);
    	SpredisSetRedisKeyValueType(key,SPHASHTYPE,cont);
    } else {
    	cont = RedisModule_ModuleTypeGetValue(key);
    	if (cont->valueType != valueType) {
    		// RedisModule_CloseKey(key);
    		return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    	}
    }

    kvec_t(spid_t) ids;
    kvec_t(uint16_t) poss;
    kvec_t(SPPtrOrD_t) ptrs;
    kv_init(ids);
    kv_init(poss);
    kv_init(ptrs);
    int errorCondition = 0;
    RedisModuleString *valArg;
    double doubleVal;
    const char *strVal;
    for (int i = argOffset; i < argc; )
    {
    	kv_push(spid_t, ids, TOINTKEY(argv[i++]));
    	kv_push(uint16_t, poss, (uint16_t)TOINTKEY10(argv[i++]));
    	valArg = argv[i++];
    	if (valueType == SPHashDoubleType) {
    		if (SpredisStringToDouble(valArg, &doubleVal) == REDISMODULE_ERR) {
    			errorCondition = 1;
                // printf("!!! %s\n", RedisModule_StringPtrLen( valArg, NULL ));
    			RedisModule_ReplyWithError(ctx, "ERR Could not parse double value");
    			break;
    		} else {
                SPPtrOrD_t dt = {.asDouble = doubleVal};
    			kv_push(SPPtrOrD_t, ptrs, dt);
    		}
    	} else {
            
            strVal = SPUniqStr(RedisModule_StringPtrLen(valArg , NULL));
    		// strVal = RedisModule_Strdup(RedisModule_StringPtrLen(valArg , NULL));
            SPPtrOrD_t dt = {.asChar = strVal};
    		kv_push(SPPtrOrD_t, ptrs, dt);
    	}
    }
    if (!errorCondition) {
        SpredisProtectWriteMap(cont);//, "SpredisHashSet");
        for (int i = 0; i < keyCount; ++i)
        {
        	spid_t id = kv_A(ids, i);
        	uint16_t pos = kv_A(poss, i);
        	SPPtrOrD_t val = kv_A(ptrs, i);
        	// if (valueType != SPHashStringType || strlen((char*)val) > 0) {
        		setCount += SPHashPutValue(cont, id, pos, val);	
        	// }
        	
        }
        SpredisUnProtectMap(cont);//, "SpredisHashSet");
    }
    kv_destroy(ids);
    kv_destroy(poss);
    kv_destroy(ptrs);
    // RedisModule_CloseKey(key);
    if (!errorCondition) RedisModule_ReplyWithLongLong(ctx, setCount);
	return REDISMODULE_OK;
}

int SpredisHashGet(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, SPHashValueType valueType) {

	RedisModule_ReplyWithNull(ctx);
	return REDISMODULE_OK;
}



int SpredisHashSetStr_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);

	int res = SpredisHashSet(ctx, argv, argc, SPHashStringType);
	if (res == REDISMODULE_OK) RedisModule_ReplicateVerbatim(ctx);
	return res;
}
int SpredisHashGetStr_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);
	return SpredisHashGet(ctx, argv, argc, SPHashStringType);
}

int SpredisHashSetDouble_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);

	int res = SpredisHashSet(ctx, argv, argc, SPHashDoubleType);
	if (res == REDISMODULE_OK) RedisModule_ReplicateVerbatim(ctx);
	return res;
}

int SpredisHashGetDouble_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);
	return SpredisHashGet(ctx, argv, argc, SPHashDoubleType);;
}

int SpredisHashDel_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);
	if (argc < 4) return RedisModule_WrongArity(ctx);
    int argOffset = 2;
    if ( ((argc - argOffset) % 2) != 0 ) return RedisModule_WrongArity(ctx);
    int keyCount = (argc - argOffset) / 2;
    int delCount = 0;
    SPHashCont *cont;
    // int argIndex = argOffset;
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_WRITE);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPHASHTYPE) != 0) {
    	// RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }

    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
    	return RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
    	cont = RedisModule_ModuleTypeGetValue(key);
    }

    kvec_t(spid_t) ids;
    kvec_t(uint16_t) poss;
    kv_init(ids);
    kv_init(poss);
    for (int i = argOffset; i < argc; )
    {
    	kv_push(spid_t, ids, TOINTKEY(argv[i++]));
    	kv_push(uint16_t, poss, (uint16_t)TOINTKEY10(argv[i++]));
    }
    SpredisProtectWriteMap(cont);//, "SpredisHashDel_RedisCommand");
    spid_t id;
    uint16_t pos;
    khint_t k;
    SPHashValue *hv;
    for (int i = 0; i < keyCount; ++i)
    {
    	id = kv_A(ids, i);
    	pos = kv_A(poss, i);
    	k = kh_get(HASH, cont->set, id);
    	if (k != kh_end(cont->set) && kh_exist(cont->set, k)) {
    		hv = kh_value(cont->set, k);
    		kv_del_value(hv->used, hv->values, pos, 0, &delCount);
    		int hasValues = 0;
	    	kv_used_count(hv->used, &hasValues);
	    	if (!hasValues) {
	    		kv_destroy(hv->used);
	    		kv_destroy(hv->values);
	    		kh_del_key_value(HASH, cont->set, k, hv, 1);
	    	}
    	}
    }
    SpredisUnProtectMap(cont);//, "SpredisHashDel_RedisCommand");
    if (kh_size(cont->set) == 0) {
        RedisModule_DeleteKey(key);
    }
    kv_destroy(ids);
    kv_destroy(poss);
    // RedisModule_CloseKey(key);
	RedisModule_ReplyWithLongLong(ctx, delCount);
	RedisModule_ReplicateVerbatim(ctx);
	return REDISMODULE_OK;
}

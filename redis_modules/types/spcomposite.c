#include "./spcomposite.h"

#define SPGeoPartType "GEO"
#define SPDoublePartType "DBL"
#define SPLexPartType "LEX"

int SPCompositeScoreComp(SPCompositeScoreSetKey a,SPCompositeScoreSetKey b) {
	int i = 0, res = 0;
	while (res == 0 && a.compCtx->valueCount > i) {
		res = (a.compCtx->compare[i])(a.value[i], b.value[i]);
		i++;
	}
	return res;
}

int SPGeoPartCompare(SPPtrOrD_t a, SPPtrOrD_t b) {
	return kb_generic_cmp(((a).asUInt), ((b).asUInt));
}

int SPDoublePartCompare(SPPtrOrD_t a, SPPtrOrD_t b) {
	return kb_generic_cmp(((a).asDouble), ((b).asDouble));
}

int SPLexPartCompare(SPPtrOrD_t a, SPPtrOrD_t b) {
	return kb_str_cmp(((a).asChar), ((b).asChar));
}

SPCompositeScoreCont *SPInitCompCont(int valueCount) {
	SPCompositeScoreCont *cont = RedisModule_Calloc(1, sizeof(SPCompositeScoreCont));
	cont->compCtx = RedisModule_Calloc(1, sizeof(SPCompositeCompCtx));
	cont->compCtx->valueCount = valueCount;
	cont->compCtx->types = RedisModule_Calloc(valueCount, sizeof(uint8_t));
	cont->compCtx->compare = RedisModule_Calloc(valueCount, sizeof(SPCompositeComp));
	cont->btree = kb_init(COMPIDX, SP_DEFAULT_TREE_SIZE);
	cont->mutex = (pthread_rwlock_t)PTHREAD_RWLOCK_INITIALIZER;
	pthread_rwlock_init ( &cont->mutex, NULL);
	return cont;
}

SPPtrOrD_t SPGetPartValue(const char *ctype, RedisModuleString *arg1, RedisModuleString *arg2) {
	SPPtrOrD_t res;
	// const char *ctype = RedisModule_StringPtrLen(type, NULL);
	double lat, lon;
	if (!strcmp(SPGeoPartType, ctype)) {
		SpredisStringToDouble(arg1, &lat);
		SpredisStringToDouble(arg2, &lon);
		res.asUInt = SPGeoHashEncode(lat, lon);
	} else if (!strcmp(SPDoublePartType, ctype)) {
		SpredisStringToDouble(arg1, &lat);
		res.asDouble = lat;
	} else {
		res.asChar = SPUniqStr(RedisModule_StringPtrLen(arg1, NULL));
	} 
	return res;
}


void SpredisCompSetRDBSave(RedisModuleIO *io, void *ptr) {

	RedisModuleCtx *ctx = RedisModule_GetContextFromIO(io);
	RedisModule_AutoMemory(ctx);
	SPCompositeScoreCont *cont = ptr;
	// SpredisProtectReadMap(cont, "SpredisCompSetRDBSave");
	SPCompositeCompCtx *compCtx = cont->compCtx;
	RedisModule_SaveUnsigned(io, compCtx->valueCount);
	RedisModule_SaveUnsigned(io, kb_size(cont->btree));
	for (uint8_t i = 0; i < compCtx->valueCount; ++i)
	{
		RedisModule_SaveUnsigned(io, compCtx->types[i]);
	}
	kbitr_t itr;
	size_t writeCount = 0;
    SPCompositeScoreSetKey *p;
    kb_itr_first(COMPIDX, cont->btree, &itr);
    RedisModuleString *tmp;
	for (; kb_itr_valid(&itr); kb_itr_next(COMPIDX, cont->btree, &itr)) { // move on
		writeCount++;
        p = &kb_itr_key(SPCompositeScoreSetKey, &itr);

        for (uint8_t i = 0; i < compCtx->valueCount; ++i)
		{
			if (compCtx->types[i] == SPGeoPart) {
				RedisModule_SaveUnsigned(io, p->value[i].asUInt);
			} else if (compCtx->types[i] == SPDoublePart) {
				RedisModule_SaveDouble(io, p->value[i].asDouble);
			} else {
				tmp = RedisModule_CreateString(ctx, p->value[i].asChar, strlen(p->value[i].asChar));
				RedisModule_SaveString(io, tmp);
				RedisModule_FreeString(ctx, tmp);
			}
		}
        size_t count = 0;
        if (p->members->set) count = kh_size(p->members->set);
        RedisModule_SaveUnsigned(io, count);
        if (p->members->set) {
            spid_t id;
            kh_foreach_key(p->members->set, id, {
                RedisModule_SaveUnsigned(io, id);
            }); 
        }
    }
    
    // SpredisUnProtectMap(cont);
}

void SpredisCompSetRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value) {
	SPCompositeScoreCont *cont = value;
	// SpredisProtectReadMap(cont, "SpredisCompSetRewriteFunc");
	SPCompositeCompCtx *compCtx = cont->compCtx;
	RedisModuleCtx *ctx = RedisModule_GetContextFromIO(aof);


	RedisModuleString *geo, *dbl, *lex, **vargs, **tofree, *a, *b;
	geo = RedisModule_CreateString(ctx, "GEO", 3);
	dbl = RedisModule_CreateString(ctx, "DBL", 3);
	lex = RedisModule_CreateString(ctx, "LEX", 3);
	int vargcount = 0, vargidx = 0, tofreeidx = 0;
	for (uint8_t i = 0; i < compCtx->valueCount; ++i)
	{
		vargcount += ((compCtx->types[i] == SPGeoPart) ? 3 : 2);
	}
	vargs = RedisModule_Calloc(vargcount, sizeof(RedisModuleString *));
	tofree = RedisModule_Calloc(vargcount, sizeof(RedisModuleString *));

	kbitr_t itr;
	spid_t id;
	char ress[32];
	double dlat, dlon;
    SPCompositeScoreSetKey *p;
    kb_itr_first(COMPIDX, cont->btree, &itr);
	for (; kb_itr_valid(&itr); kb_itr_next(COMPIDX, cont->btree, &itr)) { // move on

        p = &kb_itr_key(SPCompositeScoreSetKey, &itr);
        vargidx = 0;
        tofreeidx = 0;
        for (uint8_t i = 0; i < compCtx->valueCount; ++i)
		{
			if (compCtx->types[i] == SPGeoPart) {
				vargs[vargidx++] = geo;
				tofree[tofreeidx++] = NULL;
				SPGeoHashDecode(p->value[i].asUInt, &dlat, &dlon);
				a = RedisModule_CreateStringPrintf(ctx, "%1.17g", dlat);
				b = RedisModule_CreateStringPrintf(ctx, "%1.17g", dlon);
				vargs[vargidx++] = a;
				tofree[tofreeidx++] = a;
				vargs[vargidx++] = b;
				tofree[tofreeidx++] = b;
			} else if (compCtx->types[i] == SPDoublePart) {
				vargs[vargidx++] = dbl;
				tofree[tofreeidx++] = NULL;
				a = RedisModule_CreateStringPrintf(ctx, "%1.17g", p->value[i].asDouble);
				vargs[vargidx++] = a;
				tofree[tofreeidx++] = a;
			} else {
				vargs[vargidx++] = lex;
				tofree[tofreeidx++] = NULL;
				a = RedisModule_CreateString(ctx, p->value[i].asChar, strlen(p->value[i].asChar));
				vargs[vargidx++] = a;
				tofree[tofreeidx++] = a;
			}
		}
        if (p->members->set) {
            
            kh_foreach_key(p->members->set, id, {
		        sprintf(ress, "%" PRIx64, id);
                RedisModule_EmitAOF(aof, "spredis.compadd", "sclv", key, ress, compCtx->valueCount, vargs, vargcount);
            }); 
        }
        for (int i = 0; i < vargcount; ++i)
        {
        	a = tofree[i];
        	if (a != NULL) {
        		RedisModule_Free(a);
        		tofree[i] = NULL;
        	}
        }
    }
    // SpredisUnProtectMap(cont);
    RedisModule_FreeString(ctx, geo);
    RedisModule_FreeString(ctx, dbl);
    RedisModule_FreeString(ctx, lex);
    RedisModule_Free(vargs);
    RedisModule_Free(tofree);
}

void *SpredisCompSetRDBLoad(RedisModuleIO *io, int encver) {
	RedisModuleCtx *ctx = RedisModule_GetContextFromIO(io);
	RedisModule_AutoMemory(ctx);

	uint8_t valueCount = (uint8_t)RedisModule_LoadUnsigned(io);
	SPCompositeScoreCont *cont = SPInitCompCont(valueCount);
	SPCompositeCompCtx *compCtx = cont->compCtx;
	RedisModuleString *tmp;

	size_t count = RedisModule_LoadUnsigned(io);
	// char *s;
	spid_t id;
	int absent;
	SPPtrOrD_t *value;
	for (uint8_t i = 0; i < compCtx->valueCount; ++i)
	{
		compCtx->types[i] = (uint8_t)RedisModule_LoadUnsigned(io);
		if (compCtx->types[i] == SPGeoPart) compCtx->compare[i] = SPGeoPartCompare;
		else if (compCtx->types[i] == SPDoublePart) compCtx->compare[i] = SPDoublePartCompare;
		else compCtx->compare[i] = SPLexPartCompare;
	}
	for (size_t i = 0; i < count; ++i)
	{
		value = RedisModule_Calloc(compCtx->valueCount, sizeof(SPPtrOrD_t));
		SPCompositeScoreSetKey p = {.compCtx = compCtx, .value = value, .members = RedisModule_Calloc(1, sizeof(SPScoreSetMembers))};
		p.members->set = kh_init(SIDS);
		for (uint8_t ii = 0; ii < compCtx->valueCount; ++ii)
		{
			if (compCtx->types[ii] == SPGeoPart) {
				value[ii].asUInt = RedisModule_LoadUnsigned(io);
			} else if (compCtx->types[ii] == SPDoublePart) {
				value[ii].asDouble = RedisModule_LoadDouble(io);
			} else {
				tmp = RedisModule_LoadString(io);
				value[ii].asChar = SPUniqStr( RedisModule_StringPtrLen(tmp, NULL) );
				RedisModule_FreeString(ctx, tmp);
			}
		}
		size_t scount = RedisModule_LoadUnsigned(io);
		if (scount) {
			while(scount--) {
				id = RedisModule_LoadUnsigned(io);
				kh_put(SIDS, p.members->set, id, &absent);
			}
		}
		kb_putp(COMPIDX, cont->btree, &p);
	}
	return cont;
}

void SPCompScoreContDestroy (SPCompositeScoreCont *cont) {
	

	SpredisProtectWriteMap(cont);
	kbitr_t itr;
	kb_itr_first(COMPIDX, cont->btree, &itr);
	SPCompositeScoreSetKey *p;
	// printf("A\n");
	for (; kb_itr_valid(&itr); kb_itr_next(COMPIDX, cont->btree, &itr)) { // move on
        p = &kb_itr_key(SPCompositeScoreSetKey, &itr);
        if (p->members) {
        	if (p->members->set) kh_destroy(SIDS, p->members->set);
        	RedisModule_Free(p->members);
        }
        if (p->value) RedisModule_Free(p->value);
    }
    kb_destroy(COMPIDX, cont->btree);
    // printf("B\n");
    RedisModule_Free(cont->compCtx->types);
    RedisModule_Free(cont->compCtx->compare);
    RedisModule_Free(cont->compCtx);
    // printf("C\n");
	SpredisUnProtectMap(cont);
	pthread_rwlock_destroy(&cont->mutex);
	// printf("D\n");
	RedisModule_Free(cont);
	// printf("E\n");
}

void SpredisCompSetFreeCallback(void *value) {
	if (value == NULL) return;

    SPCompositeScoreCont *cont = value;
    SPCompScoreContDestroy(cont);
    // SP_TWORK(SPCompScoreContDestroy, cont, {
    //     //do nothing
    // });
}

void SPCompPopulateValues(int argIdx, long long valCount, SPCompositeScoreCont *cont, SPPtrOrD_t *value, RedisModuleString **argv) {
	int idx = 0;
	RedisModuleString *a, *b;
	while (idx < valCount) {
		const char *vtype = RedisModule_StringPtrLen(argv[argIdx++], NULL);
		if (!strcmp(vtype, SPGeoPartType)) {
			a = argv[argIdx++];
			b = argv[argIdx++];
			value[idx] = SPGetPartValue(vtype, a, b);
			if (cont) {
				cont->compCtx->compare[idx] = SPGeoPartCompare;
				cont->compCtx->types[idx] = SPGeoPart;	
			}
			
		} else if (!strcmp(vtype, SPDoublePartType)) {
			a = argv[argIdx++];
			b = NULL;
			value[idx] = SPGetPartValue(vtype, a, b);
			if (cont) {
				cont->compCtx->compare[idx] = SPDoublePartCompare;
				cont->compCtx->types[idx] = SPDoublePart;
			}
		} else {
			a = argv[argIdx++];
			b = NULL;
			value[idx] = SPGetPartValue(vtype, a, b);
			if (cont) {
				cont->compCtx->compare[idx] = SPLexPartCompare;
				cont->compCtx->types[idx] = SPLexPart;
			}
		}
		idx++;
	}
}

int SpredisCompSetAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);
	RedisModuleKey *key;
	SPCompositeScoreCont *cont;
	// SPLockContext(ctx);
	int res = SPGetCompKeyValue(ctx, &key, &cont, argv[1], REDISMODULE_WRITE);
	if (res != REDISMODULE_OK) {
		// SPUnlockContext(ctx);
		return res;
	}
	spid_t id = TOINTID(argv[2],16);
	long long valCount = 0;
	RedisModule_StringToLongLong(argv[3], &valCount);
	int contWasNull = (cont == NULL) ? 1 : 0;
	if (cont == NULL) {
		cont = SPInitCompCont((int)valCount);
		SpredisSetRedisKeyValueType(key,SPCOMPTYPE,cont);
	}
	
	// SPUnlockContext(ctx);
	SpredisProtectWriteMap(cont);
	SPPtrOrD_t *value = RedisModule_Calloc(valCount, sizeof(SPPtrOrD_t));
	
	SPCompPopulateValues(4, valCount, (contWasNull ? cont : NULL), value, argv);

	SPCompositeScoreSetKey search = {.compCtx = cont->compCtx, .value = value};
	

    SPCompositeScoreSetKey *compkey = kb_getp(COMPIDX, cont->btree, &search);
    int absent;
    if (compkey == NULL) {
    	search.members = RedisModule_Calloc(1, sizeof(SPScoreSetMembers));
    	search.members->set = kh_init(SIDS);
        compkey = kb_putp(COMPIDX, cont->btree, &search);
    } else {
    	RedisModule_Free(value);
    }
    kh_put(SIDS, compkey->members->set, (id), &absent);
	SpredisUnProtectMap(cont);
	RedisModule_ReplyWithLongLong(ctx, absent);
	RedisModule_ReplicateVerbatim(ctx);
	return REDISMODULE_OK;
}

int SpredisCompSetRem_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);
	RedisModuleKey *key;
	SPCompositeScoreCont *cont;
	// SPLockContext(ctx);
	int res = SPGetCompKeyValue(ctx, &key, &cont, argv[1], REDISMODULE_WRITE);
	if (res != REDISMODULE_OK) {
		// SPUnlockContext(ctx);
		return res;
	}
	if (cont == NULL) {
		// SPUnlockContext(ctx);
		return RedisModule_ReplyWithLongLong(ctx, 0);
	}
	// SPUnlockContext(ctx);
	SpredisProtectWriteMap(cont);
	// 

	long long valCount = 0;
	spid_t id = TOINTID(argv[2],16);
	RedisModule_StringToLongLong(argv[3], &valCount);
	SPPtrOrD_t *value = RedisModule_Calloc(valCount, sizeof(SPPtrOrD_t));
	SPCompositeScoreSetKey search = {.compCtx = cont->compCtx, .value = value};
	SPCompPopulateValues(4, valCount, NULL, value, argv);

	SPCompositeScoreSetKey *compkey = kb_getp(COMPIDX, cont->btree, &search);
	if (compkey == NULL) {
		RedisModule_ReplyWithLongLong(ctx, 0);
	} else {
		khint_t k = kh_get(SIDS, compkey->members->set, id);
    	if (k != kh_end(compkey->members->set)) {
	        kh_del(SIDS, compkey->members->set, k);
	        RedisModule_ReplyWithLongLong(ctx, 1);
        } else {
        	RedisModule_ReplyWithLongLong(ctx, 0);
        }
        if (kh_size(compkey->members->set) == 0 ) {
        	SPScoreSetMembers *mems = compkey->members;
        	SPPtrOrD_t *value = compkey->value;
	        kb_delp(COMPIDX, cont->btree, &search);
	        if (mems) {
	            if (mems->set) kh_destroy(SIDS, mems->set);
	            RedisModule_Free(mems);
	        }
	        RedisModule_Free(value);
        }
    }

    //TODO: delete if nothing in btree

	RedisModule_Free(value);
	SpredisUnProtectMap(cont);
	RedisModule_ReplicateVerbatim(ctx);
	return REDISMODULE_OK;
}

int SpredisCompSetCard_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);
	RedisModuleKey *key;
	SPCompositeScoreCont *cont;
	// SPLockContext(ctx);
	int res = SPGetCompKeyValue(ctx, &key, &cont, argv[1], REDISMODULE_READ);
	if (res != REDISMODULE_OK) {
		// SPUnlockContext(ctx);
		return res;
	}
	if (cont == NULL) {
		// SPUnlockContext(ctx);
		return RedisModule_ReplyWithLongLong(ctx, 0);
	}
	// SpredisProtectReadMap(cont);
	// SPUnlockContext(ctx);

	RedisModule_ReplyWithLongLong(ctx, kb_size(cont->btree));

	// SpredisUnProtectMap(cont);
	return REDISMODULE_OK;
}


// int SpredisCompSetAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
// 	RedisModule_ReplicateVerbatim(ctx);
// 	return SPThreadedWork(ctx, argv, argc, SpredisCompSetAdd_RedisCommandT);
// }
// int SpredisCompSetRem_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
// 	RedisModule_ReplicateVerbatim(ctx);
// 	return SPThreadedWork(ctx, argv, argc, SpredisCompSetRem_RedisCommandT);
// }
// int SpredisCompSetCard_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
// 	return SPThreadedWork(ctx, argv, argc, SpredisCompSetCard_RedisCommandT);
// }

int SPGetCompKeyValue(RedisModuleCtx *ctx, RedisModuleKey **key, SPCompositeScoreCont **cont, RedisModuleString *name, int mode)
{
	(*key) = RedisModule_OpenKey(ctx,name,
            mode);
    int keyType;

    if (HASH_NOT_EMPTY_AND_WRONGTYPE((*key), &keyType, SPCOMPTYPE) != 0) {
    	(*cont) = NULL;
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
    	(*cont) = NULL;
        return REDISMODULE_OK;
    }
    (*cont) = RedisModule_ModuleTypeGetValue((*key));
    return REDISMODULE_OK;
}


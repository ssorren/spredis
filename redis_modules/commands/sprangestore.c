#include "../spredis.h"

#define SP_INBOUNDS(lat, lon, bounds)  (lon >= bounds.longitude.min && lon <= bounds.longitude.max && lat >= bounds.latitude.min && lat <= bounds.latitude.max)

double SPConvertToMeters(double val, const char * unit) {
    if (unit == NULL) return val;
    if (strcasecmp(unit, "km") == 0) return val * 1000;
    if (strcasecmp(unit, "mi") == 0) return val * 1609.344;
    if (strcasecmp(unit, "ft") == 0) return val * 0.3048;
    //assume meters
    return val;
}

typedef struct _SPBlockedClientReply {
    RedisModuleBlockedClient *bc;
    uint64_t reply;
} SPBlockedClientReply;

typedef struct {
    RedisModuleBlockedClient *bc;
    uint64_t reply;
    double slat, slon, radius;
    SPGeoScoreCont *geoCont;
    SpredisSetCont *hintCont;
    SpredisSetCont *resCont;
} SPStoreRadiusTarg;

int SPThreadedGenericLongReply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    SPStoreRadiusTarg *targ = RedisModule_GetBlockedClientPrivateData(ctx);
    // if (targ->geoCont) SpredisUnProtectMap(targ->geoCont);
    // if (targ->hintCont) SpredisUnProtectMap(targ->hintCont);
    // if (targ->resCont) SpredisUnProtectMap(targ->resCont);
    return RedisModule_ReplyWithLongLong(ctx,targ->reply);
}

void SPStoreRangeInit() {
    // SPRangePool = thpool_init(SP_GEN_TPOOL_SIZE);
}

int SPThreadedGenericTimeoutReply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    
    // SPStoreRadiusTarg *targ = RedisModule_GetBlockedClientPrivateData(ctx);
    // if (targ->geoCont) SpredisUnProtectMap(targ->geoCont);
    // if (targ->hintCont) SpredisUnProtectMap(targ->hintCont);
    // if (targ->resCont) SpredisUnProtectMap(targ->resCont);
    return RedisModule_ReplyWithNull(ctx);
}

void SPFreeSPStoreRadiusTarg(void *arg) {
    RedisModule_Free(arg);
}

void SpredisStoreRangeByRadius_Thread(SPStoreRadiusTarg *targ) {
    // SPStoreRadiusTarg *targ = arg;
    SpredisProtectReadMap(targ->geoCont);
    if (targ->hintCont) SpredisProtectReadMap(targ->hintCont);
    SpredisProtectReadMap(targ->resCont);

    double slat = targ->slat;
    double slon = targ->slon;
    double radius = targ->radius;
    SPGeoScoreCont *geoCont = targ->geoCont;
    SpredisSetCont *resCont = targ->resCont;
    khash_t(SIDS) *hint = NULL;
    SpredisSetCont *hintCont = targ->hintCont;
    if (hintCont!= NULL) hint = hintCont->set;

    double lat, lon;
    SPGeoHashArea area, bounds;
    SPGeoSearchAreas areas;
    uint64_t start, stop;// = SPGeoHashEncodeForRadius(slat, slon, radius, &ghash);
    SPGetSearchAreas(slat, slon, radius, &areas, &bounds);
    kbtree_t(GEO) *geoTree = geoCont->btree;
    khash_t(SCORE) *geoSet = geoCont->set;
    SPScoreKey *l, *u, *use, *candKey;
    SPScore *cand;
    
    khash_t(SIDS) *res = resCont->set;
    
    int absent;
    kbitr_t itr;

    int shouldIntersore = 0;
    if (hint != NULL && (kh_size(hint) < (kb_size(geoTree) / 2))) { //at some point it is not faster to intersore first
        shouldIntersore = 1;
    }

    if (shouldIntersore) {
        uint32_t id;
        khint_t k;
        kh_foreach_key(hint, id, {
            k = kh_get(SCORE, geoSet, id);
            if (k != kh_end(geoSet)) {
                cand = kh_value(geoSet, k);
                if (cand && cand->score) {
                    SPGeoHashDecode(cand->score, &lat, &lon);
                    if (SP_INBOUNDS(lat, lon, bounds) && SPGetDist(slat, slon, lat, lon) <= radius) {
                        kh_put(SIDS, res, id, &absent);
                    }
                }
            }
        });
    } else {
        for (int i = 0; i < 9; ++i)
        {
            /* code */
            area = areas.area[i];
            if (!area.hash.bits) continue;
            start = area.hash.bits << (62 - (area.hash.step * 2));
            stop = ++area.hash.bits << (62 - (area.hash.step * 2));
            SPScoreKey t = {
                .id = 0,
                .score = start
            };
            // afound = 0;
            kb_intervalp(SCORE, geoTree, &t, &l, &u);
            use = u == NULL ? l : u;
            kb_itr_getp(GEO, geoTree, use, &itr);
            for (; kb_itr_valid(&itr); kb_itr_next(SCORE, geoTree, &itr)) { // move on
                candKey = (&kb_itr_key(SPScoreKey, &itr));
                if (candKey) {
                    if (candKey->score >= start) {
                        if (candKey->score >= stop) break;
                        SPGeoHashDecode(candKey->score, &lat, &lon);
                        if (SP_INBOUNDS(lat, lon, bounds) && SPGetDist(slat, slon, lat, lon) <= radius
                                && (hint == NULL || kh_contains(SIDS, hint, candKey->id)) ) {
                            kh_put(SIDS, res, candKey->id, &absent);
                        }
                    }
                }
            }
        }
    }
    // RedisModule_ZsetRangeStop(key);
    targ->reply = kh_size(res);
    SpredisUnProtectMap(geoCont);
    if (hintCont) SpredisUnProtectMap(hintCont);
    SpredisUnProtectMap(resCont);

    RedisModule_UnblockClient(targ->bc,targ);
}



int SpredisStoreRangeByRadius_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */
    if (argc != 8) return RedisModule_WrongArity(ctx);
    

    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[2], REDISMODULE_READ);

    int keyType;
    
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType ,SPGEOTYPE) == 1) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx,0);
        return REDISMODULE_OK;
    }

    RedisModuleKey *store = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_WRITE|REDISMODULE_READ);

    int storeType = RedisModule_KeyType(store);
    if (HASH_NOT_EMPTY_AND_WRONGTYPE_CHECKONLY(store, &storeType ,SPSETTYPE) == 1) {;
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    SPGeoScoreCont *geoCont = RedisModule_ModuleTypeGetValue(key);

    double slat, slon, radius;
    int pres;
    pres = RedisModule_StringToDouble(argv[4], &slat);
    if (pres != REDISMODULE_OK) return RedisModule_ReplyWithError(ctx, "ERR could not parse latitude");
    pres = RedisModule_StringToDouble(argv[5], &slon);
    if (pres != REDISMODULE_OK) return RedisModule_ReplyWithError(ctx, "ERR could not parse longitude");
    pres = RedisModule_StringToDouble(argv[6], &radius);
    if (pres != REDISMODULE_OK) return RedisModule_ReplyWithError(ctx, "ERR could not parse radius");

    const char * units = RedisModule_StringPtrLen(argv[7], NULL);
    radius = SPConvertToMeters(radius, units);
   

    RedisModuleString *hintName = argv[3];
    size_t len;
    RedisModule_StringPtrLen(hintName, &len);
    RedisModuleKey *hintKey = NULL;
    SpredisSetCont *hintCont = NULL;
    // khash_t(SIDS) *hint = NULL;
    if (len) {
        hintKey = RedisModule_OpenKey(ctx,argv[3], REDISMODULE_READ);
        int hintType;
        if (HASH_NOT_EMPTY_AND_WRONGTYPE_CHECKONLY(hintKey, &hintType ,SPSETTYPE) == 1) {
            // RedisModule_CloseKey(key);
            // RedisModule_CloseKey(store);
            // RedisModule_CloseKey(hintKey);
            return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        hintCont = RedisModule_ModuleTypeGetValue(hintKey);
        if (hintCont == NULL) {
            // SPUnlockContext(ctx);
            RedisModule_ReplyWithLongLong(ctx,0);
            return REDISMODULE_OK;
        }
        // if (hintCont != NULL) {
        //     hint = hintCont->set;
        // }
    }
    

    SPStoreRadiusTarg *targ = RedisModule_Calloc(1,sizeof(SPStoreRadiusTarg));

    RedisModuleBlockedClient *bc =
        RedisModule_BlockClient(ctx, SPThreadedGenericLongReply, SPThreadedGenericTimeoutReply, SPFreeSPStoreRadiusTarg ,0);

    SpredisSetCont *resCont = _SpredisInitSet();
    SpredisSetRedisKeyValueType(store, SPSETTYPE, resCont);
    // targ->set = cont;
    targ->bc = bc;
    targ->reply = 0;
    targ->slat = slat;
    targ->slon = slon;
    targ->radius = radius;
    targ->geoCont = geoCont;

    targ->hintCont = hintCont;

    targ->resCont = resCont;
    SpredisProtectReadMap(geoCont);
    if (hintCont) SpredisProtectReadMap(hintCont);
    SpredisProtectReadMap(resCont);

    // RedisModule_CloseKey(key);
    // RedisModule_CloseKey(store);
    // RedisModule_CloseKey(hintKey);
    SP_TWORK(SpredisStoreRangeByRadius_Thread, targ, {
        // printf("%s\n", "coudln't do work");
        RedisModule_AbortBlock(bc);
        RedisModule_ReplyWithError(ctx, "ERR Could not launch thread, there something is something seriously wrong, restart REDIS!");
    });

    SpredisUnProtectMap(geoCont);
    if (hintCont) SpredisUnProtectMap(hintCont);
    SpredisUnProtectMap(resCont);
    return REDISMODULE_OK;
}


int SpredisStoreRangeByScore_RedisCommandT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // RedisModule_AutoMemory(ctx); /* Use automatic memory management. */
    if (argc != 6) return RedisModule_WrongArity(ctx);

    SPLockContext(ctx);
    RedisModuleKey *store = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_WRITE);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[2],
        REDISMODULE_READ|REDISMODULE_WRITE);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType ,SPZSETTYPE) == 1) {
        // RedisModule_CloseKey(key);
        SPUnlockContext(ctx);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        SPUnlockContext(ctx);
        RedisModule_ReplyWithLongLong(ctx,0);
        return REDISMODULE_OK;
    }
    int storeType = RedisModule_KeyType(store);
    if (HASH_NOT_EMPTY_AND_WRONGTYPE_CHECKONLY(store, &storeType ,SPSETTYPE) == 1) {
        // RedisModule_CloseKey(key);
        // RedisModule_CloseKey(store);
        SPUnlockContext(ctx);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    RedisModuleString *hintName = argv[3];
    size_t len;
    RedisModule_StringPtrLen(hintName, &len);
    RedisModuleKey *hintKey = NULL;
    SpredisSetCont *hintCont = NULL;
    khash_t(SIDS) *hint = NULL;

    if (len) {
    	hintKey = RedisModule_OpenKey(ctx,argv[3], REDISMODULE_READ);
    	int hintType;
    	if (HASH_NOT_EMPTY_AND_WRONGTYPE_CHECKONLY(hintKey, &hintType ,SPSETTYPE) == 1) {
    		// RedisModule_CloseKey(key);
		    // RedisModule_CloseKey(store);
		    // RedisModule_CloseKey(hintKey);
            SPUnlockContext(ctx);
		    return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    	}
    	hintCont = RedisModule_ModuleTypeGetValue(hintKey);
    	if (hintCont != NULL) {
            hint = hintCont->set;
        } else {
            SPUnlockContext(ctx);
            RedisModule_ReplyWithLongLong(ctx,0);
            return REDISMODULE_OK;
        }
    }

    SPScoreCont *testScoreCont = RedisModule_ModuleTypeGetValue(key);
    kbtree_t(SCORE) *testScore = testScoreCont->btree;

    const char * gtCmp = RedisModule_StringPtrLen(argv[4], NULL);
    const char * ltCmp = RedisModule_StringPtrLen(argv[5], NULL);

    int (*GT)(double,double) = SP_SCRGTCMP(gtCmp);
    int (*LT)(double,double) = SP_SCRLTCMP(ltCmp);

    gtCmp = SP_ARG_MINUS_INC_EXC(gtCmp);
    ltCmp = SP_ARG_MINUS_INC_EXC(ltCmp);

    double min;
    double max;
    RedisModule_StringToDouble(RedisModule_CreateString(ctx, gtCmp, strlen(gtCmp)), &min);
    RedisModule_StringToDouble(RedisModule_CreateString(ctx, ltCmp, strlen(ltCmp)), &max);
    SPScoreKey *l, *u;
    SPScore *cand;
    SPScoreKey t = {
        .id = 0,
        .score = min
    };
    kb_intervalp(SCORE, testScore, &t, &l, &u);
    // if (l == NULL) {
    //     RedisModule_ReplyWithLongLong(ctx,0);
    //     // RedisModule_CloseKey(key);
    //     // RedisModule_CloseKey(store);
    //     // RedisModule_CloseKey(hintKey);
    //     SPUnlockContext(ctx);
    //     return REDISMODULE_OK;
    // }
    int shouldIntersore = 0;
    if (hint != NULL && (min == -HUGE_VAL || max == HUGE_VAL || kh_size(hint) < kb_size(testScore) / 5)) { //at some point it is not faster to intersore first
    	shouldIntersore = 1;
    }

    SpredisSetCont *resCont = _SpredisInitSet();
	SpredisSetRedisKeyValueType(store, SPSETTYPE, resCont);  
    SPUnlockContext(ctx);


    khash_t(SIDS) *res = resCont->set;
    int absent;
    int reached = 0;
    uint32_t id;
    
    kbitr_t itr;
    SpredisProtectReadMap(testScoreCont);
    if (hintCont) SpredisProtectReadMap(hintCont);
    if (shouldIntersore) {
        khint_t k;
        kh_foreach_key(hint, id, {
            k = kh_get(SCORE, testScoreCont->set, id);
            if (k != kh_end(testScoreCont->set)) {
                cand = kh_value(testScoreCont->set, k);
                if (cand && GT( cand->score, min ) && LT(cand->score, max)) {
                    kh_put(SIDS, res, id, &absent);
                }
            }
        });
    	// int RedisModule_ZsetScore(RedisModuleKey *key, RedisModuleString *ele, double *score)


    } else if (hint == NULL) { //more wordy, but also more efficient to do one if statement rather tht 1 per iteration
        if (l != NULL) {
            kb_itr_getp(SCORE, testScore, l, &itr);
        } else {
            kb_itr_first(SCORE, testScore, &itr);
        }
        for (; kb_itr_valid(&itr); kb_itr_next(SCORE, testScore, &itr)) { // move on
            cand = (&kb_itr_key(SPScoreKey, &itr))->value;
            if (cand) {
                if (reached || GT(cand->score, min)) {
                    if (LT(cand->score, max)) {
                        reached = 1;
                        kh_put(SIDS, res, cand->id, &absent);
                    } else {
                        break;
                    }
                }
            }
        }		
   	} else {
   		// khint_t k;
        if (l != NULL) {
            kb_itr_getp(SCORE, testScore, l, &itr);
        } else {
            kb_itr_first(SCORE, testScore, &itr);
        }
        for (; kb_itr_valid(&itr); kb_itr_next(SCORE, testScore, &itr)) { // move on
            cand = (&kb_itr_key(SPScoreKey, &itr))->value;
            if (cand) {
                if (reached || GT(cand->score, min)) {
                    if (LT(cand->score, max)) {
                        reached = 1;
                        if (kh_contains(SIDS, hint, cand->id)) kh_put(SIDS, res, cand->id, &absent);
                    } else {
                        break;
                    }
                }
            }
        }
   	}
    SpredisUnProtectMap(testScoreCont);
    if (hintCont) SpredisUnProtectMap(hintCont);
    RedisModule_ReplyWithLongLong(ctx,kh_size(res));
    // RedisModule_CloseKey(key);
    // RedisModule_CloseKey(store);
    // RedisModule_CloseKey(hintKey);
    return REDISMODULE_OK;
}

int SpredisStoreRangeByScore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return SPThreadedWork(ctx, argv, argc, SpredisStoreRangeByScore_RedisCommandT);
}

int SpredisStoreLexRange_RedisCommandT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // RedisModule_AutoMemory(ctx); /* Use automatic memory management. */
    // const char DELIM = ';';
    // long long arraylen = 0;

    if (argc != 6) return RedisModule_WrongArity(ctx);
    SPLockContext(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[2], REDISMODULE_READ);

    int keyType;
    
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType ,SPZLSETTYPE) == 1) {
        // RedisModule_CloseKey(key);
        SPUnlockContext(ctx);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        SPUnlockContext(ctx);
        RedisModule_ReplyWithLongLong(ctx,0);
        return REDISMODULE_OK;
    }

    RedisModuleKey *store = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_WRITE|REDISMODULE_READ);

    int storeType = RedisModule_KeyType(store);
    if (HASH_NOT_EMPTY_AND_WRONGTYPE_CHECKONLY(store, &storeType ,SPSETTYPE) == 1) {;
        SPUnlockContext(ctx);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    const unsigned char * gtCmp = (const unsigned char *)RedisModule_StringPtrLen(argv[4], NULL);
    const unsigned char * ltCmp = (const unsigned char *)RedisModule_StringPtrLen(argv[5], NULL);

    int (*GT)(int) = SP_LEXGTCMP(gtCmp);
    int (*LT)(int) = SP_LEXLTCMP(ltCmp);

    gtCmp = SP_ARG_MINUS_INC_EXC(gtCmp);
    ltCmp = SP_ARG_MINUS_INC_EXC(ltCmp);

    size_t gtLen = strlen((const char *)gtCmp);
    size_t ltLen = strlen((const char *)ltCmp);

    if ( ltLen > 0 && ( (unsigned char)(ltCmp[ ltLen - 1 ]) - 0xff) < 0) {
        ltLen += 1;
        gtLen += 1;
    }

    RedisModuleString *hintName = argv[3];
    size_t len;
    RedisModule_StringPtrLen(hintName, &len);
    RedisModuleKey *hintKey = NULL;
    SpredisSetCont *hintCont = NULL;
    khash_t(SIDS) *hint = NULL;
    if (len) {
        hintKey = RedisModule_OpenKey(ctx,argv[3], REDISMODULE_READ);
        int hintType;
        if (HASH_NOT_EMPTY_AND_WRONGTYPE_CHECKONLY(hintKey, &hintType ,SPSETTYPE) == 1) {
            // RedisModule_CloseKey(key);
            // RedisModule_CloseKey(store);
            // RedisModule_CloseKey(hintKey);
            SPUnlockContext(ctx);
            return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        hintCont = RedisModule_ModuleTypeGetValue(hintKey);
        if (hintCont != NULL) {
            hint = hintCont->set;
        } else {
            SPUnlockContext(ctx);
            RedisModule_ReplyWithLongLong(ctx,0);
            return REDISMODULE_OK;
        }
    }

    SPScoreCont *testLexCont = RedisModule_ModuleTypeGetValue(key);
    kbtree_t(LEX) *testLex = testLexCont->btree;

    SPScoreKey *l, *u;
    SPScore *cand;
    SpredisSetCont *resCont = _SpredisInitSet();
    khash_t(SIDS) *res = resCont->set;


    SpredisSetRedisKeyValueType(store, SPSETTYPE, resCont);
    int absent;
    kbitr_t itr;

    SPScoreKey t = {
        .id = 0,
        .score = (uint64_t)gtCmp
    };
    SpredisProtectReadMap(testLexCont);
    if (hintCont) SpredisProtectReadMap(hintCont);

    kb_intervalp(LEX, testLex, &t, &l, &u);
    SPUnlockContext(ctx);

    if (l == NULL) {
        RedisModule_ReplyWithLongLong(ctx,0);
        // RedisModule_CloseKey(key);
        // RedisModule_CloseKey(store);
        // RedisModule_CloseKey(hintKey);
        return REDISMODULE_OK;
    }

    int shouldIntersore = 0;
    if (hint != NULL && ltLen < 3 && (kh_size(hint) < (kb_size(testLex) / 2))) { //at some point it is not faster to intersore first
        shouldIntersore = 1;
    }
    
    if (shouldIntersore) {
        uint32_t id;
        khint_t k;
        kh_foreach_key(hint, id, {
            k = kh_get(LEX, testLexCont->set, id);
            if (k != kh_end(testLexCont->set)) {
                cand = kh_value(testLexCont->set, k);
                if (cand && cand->lex && GT(memcmp(gtCmp, cand->lex, gtLen )) && LT(memcmp(cand->lex,ltCmp , ltLen ))) {
                    kh_put(SIDS, res, id, &absent);
                }
            }
        });
    } else {
        int reached = 0;
        kb_itr_getp(LEX, testLex, l, &itr); // get an iterator pointing to the first
        if (hint == NULL) { //wordy to do it this way bu way more efficient
            for (; kb_itr_valid(&itr); kb_itr_next(LEX, testLex, &itr)) { // move on
                cand = (&kb_itr_key(SPScoreKey, &itr))->value;
                if (cand && cand->lex ) {
                    // printf("%s,%u,, %d, %d\n", cand->lex, cand->id, memcmp(gtCmp, cand->lex, gtLen ),  memcmp(cand->lex, ltCmp, ltLen ));
                    if (reached || GT(memcmp(gtCmp, cand->lex, gtLen ))) {
                        if (LT(memcmp(cand->lex,ltCmp , ltLen ))) {
                            reached = 1;
                            kh_put(SIDS, res, cand->id, &absent);
                        } else {
                            break;
                        }
                    }
                }
            }
        } else {
            for (; kb_itr_valid(&itr); kb_itr_next(LEX, testLex, &itr)) { // move on
                cand = (&kb_itr_key(SPScoreKey, &itr))->value;
                if (cand && cand->lex ) {
                    if (reached || GT(memcmp(gtCmp, cand->lex, gtLen ))) {
                        if (LT(memcmp(cand->lex,ltCmp , ltLen ))) {
                            reached = 1;
                            if ( kh_contains(SIDS, hint, cand->id)) kh_put(SIDS, res, cand->id, &absent);
                        } else {
                            break;
                        }
                    }     
                }
            }
        }
    }
    // RedisModule_ZsetRangeStop(key);
    SpredisUnProtectMap(testLexCont);
    if (hintCont) SpredisUnProtectMap(hintCont);
    RedisModule_ReplyWithLongLong(ctx,kh_size(res));
    // RedisModule_CloseKey(key);
    // RedisModule_CloseKey(store);
    // RedisModule_CloseKey(hintKey);
    return REDISMODULE_OK;
}


int SpredisStoreLexRange_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return SPThreadedWork(ctx, argv, argc, SpredisStoreLexRange_RedisCommandT);
}
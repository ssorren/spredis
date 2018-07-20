#include "../spredis.h"
#include <float.h>

double SPConvertToMeters(double val, const char * unit) {
    if (unit == NULL) return val;
    if (strcasecmp(unit, "km") == 0) return val * 1000;
    if (strcasecmp(unit, "mi") == 0) return val * 1609.344;
    if (strcasecmp(unit, "ft") == 0) return val * 0.3048;
    //assume meters
    return val;
}


void SPStoreRangeInit() {
    // SPRangePool = thpool_init(SP_GEN_TPOOL_SIZE);
}


int SpredisStoreRangeByRadiusField(RedisModuleCtx *ctx,
    double slat, double slon, double radius,
    SPIndexCont *geoCont,
    khash_t(SIDS) *hint,
    khash_t(SIDS) *res,
    int rFieldIndex,
    int coordField,
    const char * units, SPNamespace *ns) {
    SPReadLock(ns->rs->deleteLock);
    SPReadLock(geoCont->lock);

    // SPPtrOrD_t drad;
    double lat, lon, distance;

    kbtree_t(GEOSET) *geoTree = geoCont->index.btree;
    SPScoreSetKey *candKey;
    
    khash_t(SIDS) *members;
    spid_t id;
    // khint_t k;
    int absent;
    // uint16_t pos;
    // SPHashValue *av;
    // khash_t(SORTTRACK) *st = geoCont->st;
    SPFieldData *data, *coordData;
    SPRecordId rid;
    if (hint == NULL) {
        // this is the worst case scenario
        // we'll have to calculate distance for every document
        kbitr_t itr;
        kb_itr_first(GEOSET, geoTree, &itr);
        for (; kb_itr_valid(&itr); kb_itr_next(GEOSET, geoTree, &itr)) { // move on
            candKey = (&kb_itr_key(SPScoreSetKey, &itr));
            if (candKey) {
                SPGeoHashDecode(candKey->value.asUInt, &lat, &lon);
                distance = SPGetDist(slat, slon, lat, lon);
                members = candKey->members->set;

                kh_foreach_key( members , id, {
                    rid.id = id;
                    if (!rid.record->exists) continue;
                    data = &rid.record->fields[rFieldIndex];
                    if (data->iv && data->ilen) {
                        for (uint16_t i = 0; i < data->ilen; ++i)
                        {
                            if (distance <= data->iv[i].asDouble) {
                                kh_put(SIDS, res, id, &absent);
                                break;
                            }
                        }    
                    } else {
                        kh_put(SIDS, res, id, &absent);
                    }
                }); 
            }
        }     
    } else {
        int added;
        kh_foreach_key(hint , id, {
            added = 0;
            rid.id = id;
            if (!rid.record->exists) continue;
            data = &rid.record->fields[rFieldIndex];
            coordData = &rid.record->fields[coordField];
            if (coordData->iv && coordData->ilen && data->iv && data->ilen) {
                for (uint16_t k = 0; k < coordData->ilen; ++k)
                {
                    SPGeoHashDecode(coordData->iv[k].asUInt, &lat, &lon);
                    distance = SPGetDist(slat, slon, lat, lon);
                    for (uint16_t i = 0; i < data->ilen; ++i)
                    {
                        if (distance <= data->iv[i].asDouble) {
                            kh_put(SIDS, res, id, &absent);
                            added = 1;
                            break;
                        }
                    }
                    if (added) break;
                }
            } else if (coordData->iv && coordData->ilen ) {
                // we have coords, but no distance pref, let's assume we're interested in all distances
                kh_put(SIDS, res, id, &absent);
            }
            
        });

    }

    SPReadUnlock(geoCont->lock);
    SPReadUnlock(ns->rs->deleteLock);
    RedisModule_ReplyWithLongLong(ctx, kh_size(res));
    return REDISMODULE_OK;
}

int SpredisStoreRangeByRadius(RedisModuleCtx *ctx,
    double slat, double slon, double radius,
    SPIndexCont *geoCont,
    khash_t(SIDS) *hint,
    khash_t(SIDS) *res,
    const char * units) {

    SPReadLock(geoCont->lock);

    double lat, lon;
    SPGeoHashArea area, bounds;
    SPGeoSearchAreas areas;
    uint64_t start, stop;// = SPGeoHashEncodeForRadius(slat, slon, radius, &ghash);
    SPGetSearchAreas(slat, slon, radius, &areas, &bounds);
    kbtree_t(GEOSET) *geoTree = geoCont->index.btree;
    SPScoreSetKey *l, *u, *use, *candKey;
    
    kbitr_t itr;

    for (int i = 0; i < 9; ++i)
    {
        area = areas.area[i];
        if (!area.hash.bits) continue;
        start = area.hash.bits << (62 - (area.hash.step * 2));
        stop = ++area.hash.bits << (62 - (area.hash.step * 2));
        SPScoreSetKey t = {
            .value.asUInt = start
        };
        // afound = 0;
        kb_intervalp(GEOSET, geoTree, &t, &l, &u);
        use = u == NULL ? l : u;
        kb_itr_getp(GEOSET, geoTree, use, &itr);
        for (; kb_itr_valid(&itr); kb_itr_next(GEOSET, geoTree, &itr)) { // move on
            candKey = (&kb_itr_key(SPScoreSetKey, &itr));
            if (candKey) {
                if (candKey->value.asUInt >= start) {
                    if (candKey->value.asUInt >= stop) break;
                    SPGeoHashDecode(candKey->value.asUInt, &lat, &lon);
                    if (SP_INBOUNDS(lat, lon, bounds) && SPGetDist(slat, slon, lat, lon) <= radius) {
                        SPAddAllToSet(res, candKey, hint);
                    }
                }
            }
        }
    }
    SPReadUnlock(geoCont->lock);
    RedisModule_ReplyWithLongLong(ctx, kh_size(res));
    return REDISMODULE_OK;
}



int SpredisStoreRangeByRadius_RedisCommandT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    

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

    khash_t(SIDS) *res = SPGetTempSet(argv[3]);
    khash_t(SIDS) *hint = SPGetHintSet(argv[4]);
    if (hint != NULL && kh_size(hint) == 0) return RedisModule_ReplyWithLongLong(ctx,0);



    double slat, slon, radius;
    int pres;
    pres = SpredisStringToDouble(argv[5], &slat);
    if (pres != REDISMODULE_OK) return RedisModule_ReplyWithError(ctx, "ERR could not parse latitude");
    pres = SpredisStringToDouble(argv[6], &slon);
    if (pres != REDISMODULE_OK) return RedisModule_ReplyWithError(ctx, "ERR could not parse longitude");


    const char * units = RedisModule_StringPtrLen(argv[8], NULL);
    int rFieldIndex = -1;

    SPReadLock(ns->lock);

    if (argc == 9) {
        pres = SpredisStringToDouble(argv[7], &radius);
        if (pres != REDISMODULE_OK) {
            SPReadUnlock(ns->lock);
            return RedisModule_ReplyWithError(ctx, "ERR could not parse radius");
        }
        radius = SPConvertToMeters(radius, units);
    } else {
        //we're using a field ptr;
        radius = DBL_MAX;
        rFieldIndex = SPFieldIndex(ns, RedisModule_StringPtrLen(argv[9], NULL));

        if (rFieldIndex < 0) {
            SPReadUnlock(ns->lock);
            return RedisModule_ReplyWithError(ctx, "ERR radius field does not exist");
        }
    }

    SPIndexCont *cont = SPIndexForFieldName(ns, argv[2]);

    if (cont == NULL || cont->type != SPTreeIndexType) {
        SPReadUnlock(ns->lock);
        if (cont != NULL) return RedisModule_ReplyWithError(ctx, "wrong index type");
        return RedisModule_ReplyWithLongLong(ctx,0);    
    }

    int retVal;
    if (rFieldIndex < 0) {
        retVal = SpredisStoreRangeByRadius(ctx, slat, slon, radius, cont, hint, res, units);
    } else {
        int coordField = SPFieldIndex(ns, RedisModule_StringPtrLen(argv[2], NULL) );
        retVal = SpredisStoreRangeByRadiusField(ctx, slat, slon, radius, cont, hint, res, rFieldIndex, coordField, units, ns);
    }
    SPReadUnlock(ns->lock);
    return retVal;
}

int SpredisStoreRangeByRadius_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 9 && argc != 10) return RedisModule_WrongArity(ctx);
    return SPThreadedWork(ctx, argv, argc, SpredisStoreRangeByRadius_RedisCommandT);
}

int SpredisStoreRangeByScore_RedisCommandT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // RedisModule_AutoMemory(ctx); /* Use automatic memory management. */
    
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

    khash_t(SIDS) *res = SPGetTempSet(argv[3]);
    khash_t(SIDS) *hint = SPGetHintSet(argv[4]);
    if (hint != NULL && kh_size(hint) == 0) return RedisModule_ReplyWithLongLong(ctx,0);

    SPReadLock(ns->lock);
    SPIndexCont *cont = SPIndexForFieldName(ns, argv[2]);

    if (cont == NULL || cont->type != SPTreeIndexType) {
        SPReadUnlock(ns->lock);
        if (cont != NULL) return RedisModule_ReplyWithError(ctx, "wrong index type");
        return RedisModule_ReplyWithLongLong(ctx,0);    
    }

    kbtree_t(SCORESET) *testScore = cont->index.btree;

    const char * gtCmp = RedisModule_StringPtrLen(argv[5], NULL);
    const char * ltCmp = RedisModule_StringPtrLen(argv[6], NULL);

    int (*GT)(double,double) = SP_SCRGTCMP(gtCmp);
    int (*LT)(double,double) = SP_SCRLTCMP(ltCmp);

    gtCmp = SP_ARG_MINUS_INC_EXC(gtCmp);
    ltCmp = SP_ARG_MINUS_INC_EXC(ltCmp);

    double min;
    double max;
    SpredisStringToDouble(RedisModule_CreateString(ctx, gtCmp, strlen(gtCmp)), &min);
    SpredisStringToDouble(RedisModule_CreateString(ctx, ltCmp, strlen(ltCmp)), &max);

    SPReadLock(cont->lock);
    SPScoreSetKey *l, *u, *cand;
    SPScoreSetKey t = {
        .value.asDouble = min
    };
    if (min > DBL_MIN) {
        kb_intervalp(SCORESET, testScore, &t, &l, &u);    
    } else {
        l = NULL;
    }
    int reached = 0;
    kbitr_t itr;
    if (l != NULL) {
        kb_itr_getp(SCORESET, testScore, l, &itr);
    } else {
        kb_itr_first(SCORESET, testScore, &itr);
    }
    for (; kb_itr_valid(&itr); kb_itr_next(SCORESET, testScore, &itr)) {
        cand = (&kb_itr_key(SPScoreSetKey, &itr));
        if (cand) {
            if (reached || GT(cand->value.asDouble, min)) {
                if (LT(cand->value.asDouble, max)) {
                    reached = 1;
                    SPAddAllToSet(res, cand, hint);
                } else {
                    break;
                }
            }
        }
    }

    SPReadUnlock(cont->lock);
    SPReadUnlock(ns->lock);
    RedisModule_ReplyWithLongLong(ctx,kh_size(res));
    return REDISMODULE_OK;
}

int SpredisStoreRangeByScore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 7) return RedisModule_WrongArity(ctx);
    return SPThreadedWork(ctx, argv, argc, SpredisStoreRangeByScore_RedisCommandT);
}

RedisModuleString * SP_RESOLVE_WILDCARD(RedisModuleCtx *ctx, RedisModuleString *string) {
    // is this a wildcard? look for \xff at the end of the string
    // redis is not converting this character for us unfortunately
    size_t len;

    const char *str = RedisModule_StringPtrLen(string,&len);

    // SpredisLog(ctx, "%s, len=%zu", str, len);
    if(len > 4 && !strcmp(str + len - 4, "\\xff")) {
        // RedisModule_Log(ctx, "notice", "have a wild card%s, len=%zu", str, len);
        char *new_str = RedisModule_Calloc(len - 3, sizeof(char));
        strncpy(new_str, str, len - 4);
        string = RedisModule_CreateStringPrintf(ctx, "%s%c", new_str ,0xff);
        // RedisModule_Log(ctx, "notice", "have a wild card %s", new_str);
        RedisModule_Free(new_str);
    }
    return string;
}

int SpredisStoreLexRange_RedisCommandT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    
    
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

    khash_t(SIDS) *res = SPGetTempSet(argv[3]);
    khash_t(SIDS) *hint = SPGetHintSet(argv[4]);
    if (hint != NULL && kh_size(hint) == 0) return RedisModule_ReplyWithLongLong(ctx,0);

    SPReadLock(ns->lock);
    SPIndexCont *cont = SPIndexForFieldName(ns, argv[2]);
    if (cont == NULL || cont->type != SPTreeIndexType) {
        SPReadUnlock(ns->lock);
        if (cont != NULL) return RedisModule_ReplyWithError(ctx, "wrong index type");
        return RedisModule_ReplyWithLongLong(ctx,0);    
    } 

    SPReadLock(cont->lock);
    
    const unsigned char * gtCmp = (const unsigned char *)RedisModule_StringPtrLen(argv[5], NULL);
    const unsigned char * ltCmp = (const unsigned char *)RedisModule_StringPtrLen(SP_RESOLVE_WILDCARD(ctx, argv[6]), NULL);

    int (*GT)(int) = SP_LEXGTCMP(gtCmp);
    int (*LT)(int) = SP_LEXLTCMP(ltCmp);

    gtCmp = SP_ARG_MINUS_INC_EXC(gtCmp);
    ltCmp = SP_ARG_MINUS_INC_EXC(ltCmp);

    size_t gtLen = strlen((const char *)gtCmp);
    size_t ltLen = strlen((const char *)ltCmp);

    if ( ltLen > 0 && ( (unsigned char)(ltCmp[ ltLen - 1 ]) - 0xff) < 0) {
        // we need to bump the length to handle the wildcard character
        ltLen += 1;
        gtLen += 1;
    } 

    
    kbtree_t(LEXSET) *testLex = cont->index.btree;

    SPScoreSetKey *l, *u, *cand;
    kbitr_t itr;
    SPScoreSetKey t = {
        .value.asUChar = gtCmp
    };
    kb_intervalp(LEXSET, testLex, &t, &l, &u);
    
    int reached = 0;
    if (l != NULL) {
        kb_itr_getp(LEXSET, testLex, l, &itr); // get an iterator pointing to the first    
    } else {
        kb_itr_first(LEXSET, testLex, &itr);
    }
    for (; kb_itr_valid(&itr); kb_itr_next(LEXSET, testLex, &itr)) { // move on
        cand = &kb_itr_key(SPScoreSetKey, &itr);
        if (cand) {
            if (reached || GT(memcmp(gtCmp, cand->value.asUChar, gtLen ))) {
                if (LT(memcmp(cand->value.asUChar,ltCmp , ltLen ))) {
                    reached = 1;
                    SPAddAllToSet(res, cand, hint);
                } else {
                    break;
                }
            }
        }
    }
    // RedisModule_ZsetRangeStop(key);
    SPReadUnlock(cont->lock);
    SPReadUnlock(ns->lock);
    RedisModule_ReplyWithLongLong(ctx,kh_size(res));

    return REDISMODULE_OK;
}


int SpredisStoreLexRange_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 7) return RedisModule_WrongArity(ctx);
    return SPThreadedWork(ctx, argv, argc, SpredisStoreLexRange_RedisCommandT);
}
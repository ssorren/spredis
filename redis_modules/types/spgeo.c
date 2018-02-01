#include "../spredis.h"

const static SPGeoHashRange SPLATBOUNDS = {.max = 85.05112878, .min = -85.05112878};
const static SPGeoHashRange SPLONBOUNDS = {.max = 180.0, .min = -180.0};


#define D_R (M_PI / 180.0)
static inline double spdeg_rad(double ang) { return ang * D_R; }
static inline double sprad_deg(double ang) { return ang / D_R; }
const double SPEARTH_RADIUS_IN_METERS = 6372797.560856;
#define SPGZERO(s) s.bits = s.step = 0;

int SPGeohashBoundingBox(double latitude, double longitude, double radius_meters,
                       SPGeoHashArea *bounds) {
    if (!bounds) return 0;

    bounds->longitude.min = longitude - sprad_deg(radius_meters/SPEARTH_RADIUS_IN_METERS/cos(spdeg_rad(latitude)));
    bounds->longitude.max = longitude + sprad_deg(radius_meters/SPEARTH_RADIUS_IN_METERS/cos(spdeg_rad(latitude)));
    bounds->latitude.min = latitude - sprad_deg(radius_meters/SPEARTH_RADIUS_IN_METERS);
    bounds->latitude.max = latitude + sprad_deg(radius_meters/SPEARTH_RADIUS_IN_METERS);
    return 1;
}

void SPGetSearchAreas(double lat, double lon, double radiusInMeters, SPGeoSearchAreas *areas, SPGeoHashArea *bounds) {
    SPGeoHashBits theHash;
    SPGeoHashNeighbors neighbors;
    SPGeohashBoundingBox(lat, lon, radiusInMeters, bounds);
    SPGeoHashEncodeForRadius(lat, lon, radiusInMeters, &theHash);
    sp_geohash_decode(SPLATBOUNDS, SPLONBOUNDS, theHash, &areas->area[0]);
    sp_geohash_get_neighbors(theHash, &neighbors);

    sp_geohash_decode(SPLATBOUNDS, SPLONBOUNDS, neighbors.north, &areas->area[1]);
    sp_geohash_decode(SPLATBOUNDS, SPLONBOUNDS, neighbors.east, &areas->area[2]);
    sp_geohash_decode(SPLATBOUNDS, SPLONBOUNDS, neighbors.west, &areas->area[3]);
    sp_geohash_decode(SPLATBOUNDS, SPLONBOUNDS, neighbors.south, &areas->area[4]);
    sp_geohash_decode(SPLATBOUNDS, SPLONBOUNDS, neighbors.north_east, &areas->area[5]);
    sp_geohash_decode(SPLATBOUNDS, SPLONBOUNDS, neighbors.south_east, &areas->area[6]);
    sp_geohash_decode(SPLATBOUNDS, SPLONBOUNDS, neighbors.north_west, &areas->area[7]);
    sp_geohash_decode(SPLATBOUNDS, SPLONBOUNDS, neighbors.south_west, &areas->area[8]);

    SPGeoHashArea area = areas->area[0];
    if (area.hash.step > 2) {
        if (area.latitude.min < bounds->latitude.min) {
            SPGZERO(areas->area[4].hash);
            SPGZERO(areas->area[8].hash);
            SPGZERO(areas->area[6].hash);
        }
        if (area.latitude.max > bounds->latitude.max) {
            SPGZERO(areas->area[1].hash);
            SPGZERO(areas->area[5].hash);
            SPGZERO(areas->area[7].hash);
        }
        if (area.longitude.min < bounds->longitude.min) {
            SPGZERO(areas->area[3].hash);
            SPGZERO(areas->area[8].hash);
            SPGZERO(areas->area[7].hash);
        }
        if (area.longitude.max > bounds->longitude.max) {
            SPGZERO(areas->area[2].hash);
            SPGZERO(areas->area[6].hash);
            SPGZERO(areas->area[5].hash);
        }
    }
}

void SPGetSearchArea(SPGeoHashBits theHash, SPGeoHashArea *area) {
    sp_geohash_decode(SPLATBOUNDS, SPLONBOUNDS, theHash, area);
}

const double MERCATOR_MAX = 20037726.37;
const double MERCATOR_MIN = -20037726.37;

uint8_t SPGeoHashEstimateStepsByRadius(double range_meters, double lat) {
    if (range_meters == 0) return 31;
    int step = 1;
    while (range_meters < MERCATOR_MAX) {
        range_meters *= 2;
        step++;
    }
    step -= 2; /* Make sure range is included in most of the base cases. */

    /* Wider range torwards the poles... Note: it is possible to do better
     * than this approximation by computing the distance between meridians
     * at this latitude, but this does the trick for now. */
    if (lat > 66 || lat < -66) {
        step--;
        if (lat > 80 || lat < -80) step--;
    }

    /* Frame to valid range. */
    if (step < 1) step = 1;
    if (step > 31) step = 31;
    return step;
}

uint64_t SPGeoHashEncodeForRadius(double lat, double lon, double radiusInMeters, SPGeoHashBits *theHash) {

    if (radiusInMeters > 5000000) return 0;
    
    uint8_t step = SPGeoHashEstimateStepsByRadius(radiusInMeters, lat);
    // SPGeoHashBits hash;
    sp_geohash_encode(SPLATBOUNDS, SPLONBOUNDS, lat, lon, step, theHash);
    // uint8_t shift = 64 - (step * 2);
    return theHash->bits << (62 - (step * 2));
}


uint64_t SPGeoHashEncode(double lat, double lon) {
    SPGeoHashBits hash;
    // SPGeoHashRange lat_range = {.max = 90, .min = -90};
    // SPGeoHashRange lon_range = {.max = 180.0, .min = -180.0};
    sp_geohash_encode(SPLATBOUNDS, SPLONBOUNDS, lat, lon, 31, &hash);
    return hash.bits;
}

void SPGeoHashDecode(uint64_t score, double *lat, double *lon) {
    
    // GeoCoord coordinate = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
    SPGeoHashArea area;
    // SPGeoHashRange lat_range = {.max = 90, .min = -90};
    // SPGeoHashRange lon_range = {.max = 180.0, .min = -180.0};
    SPGeoHashBits hash = {.bits = score, .step = 31};
    sp_geohash_decode(SPLATBOUNDS, SPLONBOUNDS, hash, &area);
    (*lat) = (area.latitude.max + area.latitude.min) / 2;
    (*lon) = (area.longitude.max + area.longitude.min) / 2;
}

SPGeoScoreCont *SPGeoScoreContInit() {
	SPGeoScoreCont *cont = RedisModule_Calloc(1, sizeof(SPGeoScoreCont));
	cont->set = kh_init(SCORE);
	pthread_rwlock_init ( &cont->mutex,NULL );
	cont->btree = kb_init(GEO, SP_DEFAULT_TREE_SIZE);
	return cont;
}


void SPGeoScoreContDestroy(SPGeoScoreCont *cont) {
	SpredisProtectWriteMap(cont);
    kb_destroy(GEO, cont->btree);
    SPScore *score;
    kh_foreach_value(cont->set, score, {
    	// if (score->lex) RedisModule_Free(score->lex);
    	RedisModule_Free(score);
    });
    kh_destroy(SCORE, cont->set);
    SpredisUnProtectMap(cont);
    pthread_rwlock_destroy(&cont->mutex);
    RedisModule_Free(cont);
}


void SpredisZGeoSetFreeCallback(void *value) {
    if (value == NULL) return;
    SPGeoScoreContDestroy((SPGeoScoreCont *)value);
}

int SPGeoScorePutValue(SPGeoScoreCont *cont, spid_t id, uint16_t pos, double lat, double lon) {
	SpredisProtectWriteMap(cont);
	SPScore *score;
	khint_t k;
	int absent;
	int res = 1;

	SPScoreKey search, *oldKey;
	k = kh_put(SCORE, cont->set, id, &absent);
    if (absent) {
    	score = RedisModule_Calloc(1, sizeof(SPScore));
		score->id = id;
		// score->lat = lat;
		// score->lon = lon;
        score->score = SPGeoHashEncode(lat, lon);

		kh_put_value(cont->set, k, score);

		SPScoreKey key = {.id = score->id, .score = (SPPtrOrD_t)score->score, .value = score};
		kb_putp(GEO, cont->btree, &key);
        
    } else {
    	score = kh_value(cont->set, k);
        score->score = SPGeoHashEncode(lat, lon);

    	// if ( score->lat != lat || score->lon != lon ) {
    		search.id = score->id;
    		search.score = (SPPtrOrD_t)score->score;
    		oldKey = kb_getp(GEO, cont->btree, &search);
    		if (oldKey) {
				kb_delp(GEO, cont->btree, oldKey);
			}
    		// RedisModule_Free(score->lex);
            // char *hash = SPGeoHashEncode(lat, lon, 8);
            // memcpy(&score->score, hash, 8);
            // RedisModule_Free(hash);
    		// score->lex = SPGeoHashEncode(lat, lon, 8);
	  //   	score->lat = lat;
			// score->lon = lon;

	    	SPScoreKey key = {.id = score->id, .score = (SPPtrOrD_t)score->score, .value = score};
	    	kb_putp(GEO, cont->btree, &key);
    	// } else {
    	// 	res = 0;
    	// }
    }
    SpredisUnProtectMap(cont);
	return res;
}


int SPGeoScoreDel(SPGeoScoreCont *cont, spid_t id) {
	SpredisProtectWriteMap(cont);
	SPScore *score;
	SPScoreKey *key = NULL;
	khint_t k;
	int res = 0;
	k = kh_get(SCORE, cont->set, id);
	if (k != kh_end(cont->set)) {
		score = kh_value(cont->set, k);
		SPScoreKey search = {.id=score->id, .score=(SPPtrOrD_t)score->score};
		key = kb_getp(GEO, cont->btree, &search);		
		if (key) {
			kb_delp(GEO, cont->btree, key);
		}
		// if (score->lex) RedisModule_Free(score->lex);
		kh_del_key_value(SCORE, cont->set, k, score, 1);
		res = 1;
	}
	SpredisUnProtectMap(cont);
	return res;
}


void SpredisZGeoSetRDBSave(RedisModuleIO *io, void *ptr) {
    SPGeoScoreCont *dhash = ptr;
    SpredisProtectReadMap(dhash);
    RedisModule_SaveUnsigned(io, kh_size(dhash->set));
    SPScore *s = NULL;
    double lat, lon;
    kh_foreach_value(dhash->set, s, {
        SPGeoHashDecode(s->score, &lat, &lon);
    	RedisModule_SaveUnsigned(io, s->id);
        RedisModule_SaveDouble(io, lat);
        RedisModule_SaveDouble(io, lon);        
    });
    SpredisUnProtectMap(dhash);
}


void SpredisZGeoSetRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value) {
	SPGeoScoreCont *dhash = value;
    SPScore *s;
    double lat, lon;
    kh_foreach_value(dhash->set, s, {

        SPGeoHashDecode(s->score, &lat, &lon);
    	char ress[32];
        sprintf(ress, "%" PRIx64, (unsigned long long)s->id);
        char slat[50];
        sprintf(slat, "%1.17g" ,lat);
        char slon[50];
        sprintf(slon, "%1.17g" ,lon);
        RedisModule_EmitAOF(aof,"spredis.geoadd","sclcc", key, ress, 0, slat, slon);
    });
}

void *SpredisZGeoSetRDBLoad(RedisModuleIO *io, int encver) {
    if (encver != SPREDISDHASH_ENCODING_VERSION) {
        /* We should actually log an error here, or try to implement
           the ability to load older versions of our data structure. */
        return NULL;
    }
    
    SPGeoScoreCont *dhash = SPGeoScoreContInit();
    spid_t valueCount = RedisModule_LoadUnsigned(io);
    for (spid_t i = 0; i < valueCount; ++i)
    {
        spid_t id = RedisModule_LoadUnsigned(io);
        double lat = RedisModule_LoadDouble(io);
        double lon = RedisModule_LoadDouble(io);
        SPGeoScorePutValue(dhash, id, 0, lat, lon);
    }
    return dhash;
}


int SpredisZGeoSetCard_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1], REDISMODULE_READ);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPGEOTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        RedisModule_CloseKey(key);
        return REDISMODULE_OK;
    }

    SPGeoScoreCont *dhash = RedisModule_ModuleTypeGetValue(key);
    SpredisProtectReadMap(dhash);
    RedisModule_ReplyWithLongLong(ctx,kh_size(dhash->set));
    SpredisUnProtectMap(dhash);
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}


int SpredisZGeoSetRem_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_WRITE|REDISMODULE_READ);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPGEOTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx,0);
        RedisModule_CloseKey(key);
        return REDISMODULE_OK;
    }


    SPGeoScoreCont *cont = RedisModule_ModuleTypeGetValue(key);
    spid_t id = TOINTID(argv[2],16);

    RedisModule_ReplyWithLongLong(ctx,SPGeoScoreDel(cont, id));

    if (kh_size(cont->set) == 0) {
        RedisModule_DeleteKey(key);
    }

    RedisModule_CloseKey(key);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

int SpredisZGeoSetAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 6) return RedisModule_WrongArity(ctx);
    int argOffset = 2;
    if ( ((argc - argOffset) % 4) != 0 ) return RedisModule_WrongArity(ctx);
    int keyCount = (argc - argOffset) / 4;
    int setCount = 0;
    int argIndex = argOffset;
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_WRITE);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPGEOTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    double lats[keyCount];
    double lons[keyCount];
    for (int i = 0; i < keyCount; ++i)
    {
        argIndex++;
        argIndex++;
        int scoreRes = RedisModule_StringToDouble(argv[argIndex++], &(lats[i]));
        if (scoreRes != REDISMODULE_OK) {
        	RedisModule_CloseKey(key);
            return RedisModule_ReplyWithError(ctx, "ERR Could not convert lat to double");
        }
        scoreRes = RedisModule_StringToDouble(argv[argIndex++], &(lons[i]));
        if (scoreRes != REDISMODULE_OK) {
        	RedisModule_CloseKey(key);
            return RedisModule_ReplyWithError(ctx, "ERR Could not convert lon to double");
        }
    }

    SPGeoScoreCont *dhash;
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        dhash = SPGeoScoreContInit();
        SpredisSetRedisKeyValueType(key,SPGEOTYPE,dhash);
    } else {
        dhash = RedisModule_ModuleTypeGetValue(key);
    }
    
    argIndex = argOffset;
    for (int i = 0; i < keyCount; ++i)
    {
        spid_t id = strtoll(RedisModule_StringPtrLen(argv[argIndex++],NULL), NULL, 16);//TOINTKEY(argv[2]);
        uint16_t pos = strtol(RedisModule_StringPtrLen(argv[argIndex++],NULL), NULL, 10);
        argIndex++;
        argIndex++;
        setCount += SPGeoScorePutValue(dhash, id, pos , lats[i], lons[i]);	
     
    }
    /* if we've aleady seen this id, just set the score */
    RedisModule_CloseKey(key);
    RedisModule_ReplyWithLongLong(ctx, setCount);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

int SpredisZGeoSetScore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_READ);
    int keyType;

    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPGEOTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithNull(ctx);
        RedisModule_CloseKey(key);
        return REDISMODULE_OK;
    }
    SPGeoScoreCont *cont = RedisModule_ModuleTypeGetValue(key);
    SpredisProtectReadMap(cont);
    SPScore *score;
    spid_t id = TOINTKEY(argv[2]);
    khint_t k = kh_get(SCORE, cont->set, id);
	if (kh_exist(cont->set, k)) {
		score = kh_value(cont->set, k);
		RedisModule_ReplyWithArray(ctx, 2);
        double lat, lon;
        SPGeoHashDecode(score->score, &lat, &lon);
		RedisModule_ReplyWithDouble(ctx, lat);
		RedisModule_ReplyWithDouble(ctx, lon);
	} else {
		RedisModule_ReplyWithNull(ctx);
	}
    RedisModule_CloseKey(key);
    SpredisUnProtectMap(cont);
    return REDISMODULE_OK;
}

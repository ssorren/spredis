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

SPScoreCont *SPGeoScoreContInit() {
    return SPScoreContInit();
}


void SPGeoScoreContDestroy(void *value) {
    SPScoreContDestroy(value);
}


void SpredisZGeoSetFreeCallback(void *value) {
    if (value == NULL) return;
    // SPGeoScoreContDestroy((SPScoreCont *)value);
    SP_TWORK(SPGeoScoreContDestroy, value, {
        //do nothing
    });
}

int SPGeoScorePutValue(SPScoreCont *cont, spid_t id, uint16_t pos, double lat, double lon) {
	SpredisProtectWriteMap(cont);//, "SPGeoScorePutValue");
    SPPtrOrD_t val = (SPPtrOrD_t)SPGeoHashEncode(lat, lon);
    SPAddGeoScoreToSet(cont->btree, cont->st, id, (SPPtrOrD_t)val);
    SpredisUnProtectMap(cont);//, "SPGeoScorePutValue");
	return 1;
}


int SPGeoScoreDel(SPScoreCont *cont, spid_t id, double lat, double lon) {
	SpredisProtectWriteMap(cont);//,"SPGeoScoreDel");
    SPPtrOrD_t val = (SPPtrOrD_t)SPGeoHashEncode(lat, lon);
    SPRemGeoScoreFromSet(cont->btree, cont->st, id, (SPPtrOrD_t)val);
	SpredisUnProtectMap(cont);//,"SPGeoScoreDel");
	return 1;
}


void SpredisZGeoSetRDBSave(RedisModuleIO *io, void *ptr) {
    SpredisZSetRDBSave(io, ptr);
}


void SpredisZGeoSetRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    SPScoreCont *cont = value;
    SPPtrOrD_t val;
    spid_t id;
    double lat, lon;
    sp_scoreset_each(GEOSET, cont->btree, val, id, {
        SPGeoHashDecode(val, &lat, &lon);
        char ress[32];
        sprintf(ress, "%" PRIx64, id);
        char slat[50];
        sprintf(slat, "%1.17g" ,lat);
        char slon[50];
        sprintf(slon, "%1.17g" ,lon);
        RedisModule_EmitAOF(aof,"spredis.geoadd","sclcc", key, ress, 0, slat, slon);
    });

}

void *SpredisZGeoSetRDBLoad(RedisModuleIO *io, int encver) {
    return SpredisZSetRDBLoad(io,encver);
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
        // RedisModule_CloseKey(key);
        return REDISMODULE_OK;
    }

    SPScoreCont *dhash = RedisModule_ModuleTypeGetValue(key);
    SpredisProtectReadMap(dhash);//, "SpredisZGeoSetCard_RedisCommand");
    RedisModule_ReplyWithLongLong(ctx,kb_size(dhash->btree));
    SpredisUnProtectMap(dhash);//, "SpredisZGeoSetCard_RedisCommand");
    // RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}


int SpredisZGeoSetRem_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 5) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_WRITE|REDISMODULE_READ);
    int keyType;
    if (HASH_NOT_EMPTY_AND_WRONGTYPE(key, &keyType, SPGEOTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx,0);
        // RedisModule_CloseKey(key);
        return REDISMODULE_OK;
    }


    SPScoreCont *cont = RedisModule_ModuleTypeGetValue(key);
    spid_t id = TOINTID(argv[2],16);
    double lat, lon;
    RedisModule_StringToDouble(argv[3], &lat);
    RedisModule_StringToDouble(argv[4], &lon);
    RedisModule_ReplyWithLongLong(ctx,SPGeoScoreDel(cont, id, lat, lon));

    if (kb_size(cont->btree) == 0) {
        RedisModule_DeleteKey(key);
    }

    // RedisModule_CloseKey(key);
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
        	// RedisModule_CloseKey(key);
            return RedisModule_ReplyWithError(ctx, "ERR Could not convert lat to double");
        }
        scoreRes = RedisModule_StringToDouble(argv[argIndex++], &(lons[i]));
        if (scoreRes != REDISMODULE_OK) {
        	// RedisModule_CloseKey(key);
            return RedisModule_ReplyWithError(ctx, "ERR Could not convert lon to double");
        }
    }

    SPScoreCont *dhash;
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
    // RedisModule_CloseKey(key);
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
        // RedisModule_CloseKey(key);
        return REDISMODULE_OK;
    }
    SPScoreCont *cont = RedisModule_ModuleTypeGetValue(key);
    SpredisProtectReadMap(cont);//, "SpredisZGeoSetScore_RedisCommand");

		RedisModule_ReplyWithNull(ctx);

    SpredisUnProtectMap(cont);//, "SpredisZGeoSetScore_RedisCommand");
    return REDISMODULE_OK;
}

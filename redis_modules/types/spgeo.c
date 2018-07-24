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


// int SPGeoScorePutValue(SPScoreCont *cont, spid_t id, uint16_t pos, double lat, double lon) {
// 	SpredisProtectWriteMap(cont);//, "SPGeoScorePutValue");
//     SPPtrOrD_t val = {.asUInt = (uint64_t)SPGeoHashEncode(lat, lon)};
//     SPAddGeoScoreToSet(cont->btree, cont->st, id, val);
//     SPRWUnlock(cont);//, "SPGeoScorePutValue");
// 	return 1;
// }


// int SPGeoScoreDel(SPScoreCont *cont, spid_t id, double lat, double lon) {
// 	SpredisProtectWriteMap(cont);//,"SPGeoScoreDel");
//     SPPtrOrD_t val = {.asUInt = (uint64_t)SPGeoHashEncode(lat, lon)};
//     SPRemGeoScoreFromSet(cont->btree, cont->st, id, val);
// 	SPRWUnlock(cont);//,"SPGeoScoreDel");
// 	return 1;
// }


/*
 *Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
 *All rights reserved.
 *
 *Redistribution and use in source and binary forms, with or without
 *modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
 *BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 *THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef SP_GEOHASH_H_
#define SP_GEOHASH_H_

#include <stdint.h>

#if defined(__cplusplus)
extern "C"
{
#endif

#define SP_INBOUNDS(lat, lon, bounds)  (lon >= bounds.longitude.min && lon <= bounds.longitude.max && lat >= bounds.latitude.min && lat <= bounds.latitude.max)
    
    typedef enum
    {
        GEOHASH_NORTH = 0,
        GEOHASH_EAST,
        GEOHASH_WEST,
        GEOHASH_SOUTH,
        GEOHASH_SOUTH_WEST,
        GEOHASH_SOUTH_EAST,
        GEOHASH_NORT_WEST,
        GEOHASH_NORT_EAST
    } SPGeoDirection;

    typedef struct
    {
            uint64_t bits;
            uint8_t step;
    } SPGeoHashBits;

    typedef struct
    {
            double max;
            double min;
    } SPGeoHashRange;

    typedef struct
    {
            SPGeoHashBits hash;
            SPGeoHashRange latitude;
            SPGeoHashRange longitude;
    } SPGeoHashArea;

    typedef struct
    {
            SPGeoHashBits north;
            SPGeoHashBits east;
            SPGeoHashBits west;
            SPGeoHashBits south;
            SPGeoHashBits north_east;
            SPGeoHashBits south_east;
            SPGeoHashBits north_west;
            SPGeoHashBits south_west;
    } SPGeoHashNeighbors;

    /*
     * Fast encode/decode version, more magic in implementation.
     */
    int sp_geohash_encode(SPGeoHashRange lat_range, SPGeoHashRange lon_range, double latitude, double longitude, uint8_t step, SPGeoHashBits* hash);
    int sp_geohash_decode(SPGeoHashRange lat_range, SPGeoHashRange lon_range, SPGeoHashBits hash, SPGeoHashArea* area);

    int sp_geohash_get_neighbors(SPGeoHashBits hash, SPGeoHashNeighbors* neighbors);
    int sp_geohash_get_neighbor(SPGeoHashBits hash, SPGeoDirection direction, SPGeoHashBits* neighbor);

    SPGeoHashBits sp_geohash_next_leftbottom(SPGeoHashBits bits);
    SPGeoHashBits sp_geohash_next_rightbottom(SPGeoHashBits bits);
    SPGeoHashBits sp_geohash_next_lefttop(SPGeoHashBits bits);
    SPGeoHashBits sp_geohash_next_righttop(SPGeoHashBits bits);


#if defined(__cplusplus)
}
#endif
#endif /* GEOHASH_H_ */

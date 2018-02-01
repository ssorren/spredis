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
#include <stddef.h>
#include <stdio.h>
#include <math.h>
#include "geohash.h"

/**
 * Hashing works like this:
 * Divide the world into 4 buckets.  Label each one as such:
 *  -----------------
 *  |       |       |
 *  |       |       |
 *  | 0,1   | 1,1   |
 *  -----------------
 *  |       |       |
 *  |       |       |
 *  | 0,0   | 1,0   |
 *  -----------------
 */

static inline uint64_t sp_interleave64(uint32_t xlo, uint32_t ylo)
{
    static const uint64_t B[] =
        { 0x5555555555555555, 0x3333333333333333, 0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF, 0x0000FFFF0000FFFF };
    static const unsigned int S[] =
        { 1, 2, 4, 8, 16 };

    uint64_t x = xlo; // Interleave lower  bits of x and y, so the bits of x
    uint64_t y = ylo; // are in the even positions and bits from y in the odd; //https://graphics.stanford.edu/~seander/bithacks.html#InterleaveBMN

    // x and y must initially be less than 2**32.

    x = (x | (x << S[4])) & B[4];
    y = (y | (y << S[4])) & B[4];

    x = (x | (x << S[3])) & B[3];
    y = (y | (y << S[3])) & B[3];

    x = (x | (x << S[2])) & B[2];
    y = (y | (y << S[2])) & B[2];

    x = (x | (x << S[1])) & B[1];
    y = (y | (y << S[1])) & B[1];

    x = (x | (x << S[0])) & B[0];
    y = (y | (y << S[0])) & B[0];

    return x | (y << 1);
}

static inline uint64_t sp_deinterleave64(uint64_t interleaved)
{
    static const uint64_t B[] =
        { 0x5555555555555555, 0x3333333333333333, 0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF, 0x0000FFFF0000FFFF,
                0x00000000FFFFFFFF };
    static const unsigned int S[] =
        { 0, 1, 2, 4, 8, 16 };

    uint64_t x = interleaved; ///reverse the interleave process (http://stackoverflow.com/questions/4909263/how-to-efficiently-de-interleave-bits-inverse-morton)
    uint64_t y = interleaved >> 1;

    x = (x | (x >> S[0])) & B[0];
    y = (y | (y >> S[0])) & B[0];

    x = (x | (x >> S[1])) & B[1];
    y = (y | (y >> S[1])) & B[1];

    x = (x | (x >> S[2])) & B[2];
    y = (y | (y >> S[2])) & B[2];

    x = (x | (x >> S[3])) & B[3];
    y = (y | (y >> S[3])) & B[3];

    x = (x | (x >> S[4])) & B[4];
    y = (y | (y >> S[4])) & B[4];

    x = (x | (x >> S[5])) & B[5];
    y = (y | (y >> S[5])) & B[5];

    return x | (y << 32);
}

int sp_geohash_encode(
        SPGeoHashRange lat_range, SPGeoHashRange lon_range, double latitude,
        double longitude, uint8_t step,  SPGeoHashBits* hash)
{
    if (NULL == hash || step > 32 || step == 0)
    {
        return -1;
    }
    hash->bits = 0;
    hash->step = step;
    if   (latitude < lat_range.min || latitude > lat_range.max
      || longitude < lon_range.min || longitude > lon_range.max)
    {
        return -1;
    }

    // The algorithm computes the morton code for the geohash location within
    // the range this can be done MUCH more efficiently using the following code

    //compute the coordinate in the range 0-1
    double lat_offset = (latitude - lat_range.min) / (lat_range.max - lat_range.min);
    double lon_offset = (longitude - lon_range.min) / (lon_range.max - lon_range.min);

    //convert it to fixed point based on the step size
    lat_offset *= (1LL << step);
    lon_offset *= (1LL << step);

    uint32_t ilato = (uint32_t) lat_offset;
    uint32_t ilono = (uint32_t) lon_offset;

    //interleave the bits to create the morton code.  No branching and no bounding
    hash->bits = sp_interleave64(ilato, ilono);
    return 0;
}

int sp_geohash_decode(SPGeoHashRange lat_range, SPGeoHashRange lon_range, SPGeoHashBits hash, SPGeoHashArea* area)
{
    if (NULL == area)
    {
        return -1;
    }
    area->hash = hash;
    uint8_t step = hash.step;
    uint64_t xyhilo = sp_deinterleave64(hash.bits); //decode morton code

    double lat_scale = lat_range.max - lat_range.min;
    double lon_scale = lon_range.max - lon_range.min;

    uint32_t ilato = xyhilo;        //get back the original integer coordinates
    uint32_t ilono = xyhilo >> 32;

    //double lat_offset=ilato;
    //double lon_offset=ilono;
    //lat_offset /= (1<<step);
    //lon_offset /= (1<<step);

    //the ldexp call converts the integer to a double,then divides by 2**step to get the 0-1 coordinate, which is then multiplied times scale and added to the min to get the absolute coordinate
//    area->latitude.min = lat_range.min + ldexp(ilato, -step) * lat_scale;
//    area->latitude.max = lat_range.min + ldexp(ilato + 1, -step) * lat_scale;
//    area->longitude.min = lon_range.min + ldexp(ilono, -step) * lon_scale;
//    area->longitude.max = lon_range.min + ldexp(ilono + 1, -step) * lon_scale;

    /*
     * much faster than 'ldexp'
     */
    area->latitude.min = lat_range.min + (ilato * 1.0 / (1ull << step)) * lat_scale;
    area->latitude.max = lat_range.min + ((ilato + 1) * 1.0 / (1ull << step)) * lat_scale;
    area->longitude.min = lon_range.min + (ilono * 1.0 / (1ull << step)) * lon_scale;
    area->longitude.max = lon_range.min + ((ilono + 1) * 1.0 / (1ull << step)) * lon_scale;

    return 0;
}

static int sp_geohash_move_x(SPGeoHashBits* hash, int8_t d)
{
    if (d == 0)
        return 0;
    uint64_t x = hash->bits & 0xaaaaaaaaaaaaaaaaLL;
    uint64_t y = hash->bits & 0x5555555555555555LL;

    uint64_t zz = 0x5555555555555555LL >> (64 - hash->step * 2);
    if (d > 0)
    {
        x = x + (zz + 1);
    }
    else
    {
        x = x | zz;
        x = x - (zz + 1);
    }
    x &= (0xaaaaaaaaaaaaaaaaLL >> (64 - hash->step * 2));
    hash->bits = (x | y);
    return 0;
}

static int sp_geohash_move_y(SPGeoHashBits* hash, int8_t d)
{
    if (d == 0)
        return 0;
    uint64_t x = hash->bits & 0xaaaaaaaaaaaaaaaaLL;
    uint64_t y = hash->bits & 0x5555555555555555LL;

    uint64_t zz = 0xaaaaaaaaaaaaaaaaLL >> (64 - hash->step * 2);
    if (d > 0)
    {
        y = y + (zz + 1);
    }
    else
    {
        y = y | zz;
        y = y - (zz + 1);
    }
    y &= (0x5555555555555555LL >> (64 - hash->step * 2));
    hash->bits = (x | y);
    return 0;
}

int sp_geohash_get_neighbors(SPGeoHashBits hash, SPGeoHashNeighbors* neighbors)
{
    sp_geohash_get_neighbor(hash, GEOHASH_NORTH, &neighbors->north);
    sp_geohash_get_neighbor(hash, GEOHASH_EAST, &neighbors->east);
    sp_geohash_get_neighbor(hash, GEOHASH_WEST, &neighbors->west);
    sp_geohash_get_neighbor(hash, GEOHASH_SOUTH, &neighbors->south);
    sp_geohash_get_neighbor(hash, GEOHASH_SOUTH_WEST, &neighbors->south_west);
    sp_geohash_get_neighbor(hash, GEOHASH_SOUTH_EAST, &neighbors->south_east);
    sp_geohash_get_neighbor(hash, GEOHASH_NORT_WEST, &neighbors->north_west);
    sp_geohash_get_neighbor(hash, GEOHASH_NORT_EAST, &neighbors->north_east);
    return 0;
}

int sp_geohash_get_neighbor(SPGeoHashBits hash, SPGeoDirection direction, SPGeoHashBits* neighbor)
{
    if (NULL == neighbor)
    {
        return -1;
    }
    *neighbor = hash;
    switch (direction)
    {
        case GEOHASH_NORTH:
        {
            sp_geohash_move_x(neighbor, 0);
            sp_geohash_move_y(neighbor, 1);
            break;
        }
        case GEOHASH_SOUTH:
        {
            sp_geohash_move_x(neighbor, 0);
            sp_geohash_move_y(neighbor, -1);
            break;
        }
        case GEOHASH_EAST:
        {
            sp_geohash_move_x(neighbor, 1);
            sp_geohash_move_y(neighbor, 0);
            break;
        }
        case GEOHASH_WEST:
        {
            sp_geohash_move_x(neighbor, -1);
            sp_geohash_move_y(neighbor, 0);
            break;
        }
        case GEOHASH_SOUTH_WEST:
        {
            sp_geohash_move_x(neighbor, -1);
            sp_geohash_move_y(neighbor, -1);
            break;
        }
        case GEOHASH_SOUTH_EAST:
        {
            sp_geohash_move_x(neighbor, 1);
            sp_geohash_move_y(neighbor, -1);
            break;
        }
        case GEOHASH_NORT_WEST:
        {
            sp_geohash_move_x(neighbor, -1);
            sp_geohash_move_y(neighbor, 1);
            break;
        }
        case GEOHASH_NORT_EAST:
        {
            sp_geohash_move_x(neighbor, 1);
            sp_geohash_move_y(neighbor, 1);
            break;
        }
        default:
        {
            return -1;
        }
    }
    return 0;
}

SPGeoHashBits sp_geohash_next_leftbottom(SPGeoHashBits bits)
{
    SPGeoHashBits newbits = bits;
    newbits.step++;
    newbits.bits <<= 2;
    return newbits;
}
SPGeoHashBits sp_geohash_next_rightbottom(SPGeoHashBits bits)
{
    SPGeoHashBits newbits = bits;
    newbits.step++;
    newbits.bits <<= 2;
    newbits.bits += 2;
    return newbits;
}
SPGeoHashBits sp_geohash_next_lefttop(SPGeoHashBits bits)
{
    SPGeoHashBits newbits = bits;
    newbits.step++;
    newbits.bits <<= 2;
    newbits.bits += 1;
    return newbits;
}

SPGeoHashBits sp_geohash_next_righttop(SPGeoHashBits bits)
{
    SPGeoHashBits newbits = bits;
    newbits.step++;
    newbits.bits <<= 2;
    newbits.bits += 3;
    return newbits;
}

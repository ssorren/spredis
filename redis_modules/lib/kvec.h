/* The MIT License

   Copyright (c) 2008, by Attractive Chaos <attractor@live.co.uk>

   Permission is hereby granted, free of charge, to any person obtaining
   a copy of this software and associated documentation files (the
   "Software"), to deal in the Software without restriction, including
   without limitation the rights to use, copy, modify, merge, publish,
   distribute, sublicense, and/or sell copies of the Software, and to
   permit persons to whom the Software is furnished to do so, subject to
   the following conditions:

   The above copyright notice and this permission notice shall be
   included in all copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
*/

/*
  An example:

#include "kvec.h"
int main() {
	kvec_t(int) array;
	kv_init(array);
	kv_push(int, array, 10); // append
	kv_a(int, array, 20) = 5; // dynamic
	kv_A(array, 20) = 4; // static
	kv_destroy(array);
	return 0;
}
*/

/*
  2008-09-22 (0.1.0):

	* The initial version.

*/

#ifndef AC_KVEC_H
#define AC_KVEC_H

#include <stdlib.h>
#include "redismodule.h"

#define kv_roundup32(x) (--(x), (x)|=(x)>>1, (x)|=(x)>>2, (x)|=(x)>>4, (x)|=(x)>>8, (x)|=(x)>>16, ++(x))

#define kvec_t(type) struct { size_t n, m; type *a; }
#define kv_init(v) ((v).n = (v).m = 0, (v).a = 0)
#define kv_destroy(v) RedisModule_Free((v).a)
#define kv_A(v, i) ((v).a[(i)])
#define kv_pop(v) ((v).a[--(v).n])
#define kv_size(v) ((v).n)
#define kv_max(v) ((v).m)

/* modified to zero fill the empty portion of the array */
#define kv_resize_bad(type, v, s)  do { \
	(v).a = (type*)RedisModule_Realloc((v).a, sizeof(type) * (s)); \
	for (size_t i = ((v).m); i < (s); ++i) \
	{ \
		(v).a[i] = 0; \
	} \
	(v).m = (s); \
} while(0)

#define kv_resize(type, v, s)  ((v).m = (s), (v).a = (type*)RedisModule_Realloc((v).a, sizeof(type) * (v).m))

/* 	begin spredis additions, don't hope to use these stand alone unless you understand sphash.c...
	using an additional 'used' array to determine which indeces have values
	used by sphash.c */
#define kv_set_value(type, used, v, val, pos, do_free)  do { \
	if (pos >= (v.m)) kv_resize(type, v, pos + 1); \
	if (pos >= (used.m)) kv_resize(char, used, pos + 1); \
	if (kv_A(used, pos) == 1 && do_free) RedisModule_Free((kv_A(v, pos)).asChar); \
	kv_A(used, pos) = 1; \
	kv_A(v, pos) = val; \
} while(0)

#define kv_del_value(used, v, pos, do_free, res)  do { \
	if (pos >= (v.m)) break; \
	if (pos >= (used.m)) break; \
	if (kv_A(used, pos) == 1 && do_free) RedisModule_Free((kv_A(v, pos)).asChar); \
	if (kv_A(used, pos) == 1) (*res) += 1; \
	kv_A(used, pos) = 0; \
} while(0)

#define kv_set_used(type, v, val, pos)  do { \
	if (pos >= (v.m)) kv_resize(type, v, pos); \
	kv_A(v, pos) = val; \
} while(0)

#define kv_used_count(v, cnt)  do { \
	(*cnt) = 0; \
	for (uint16_t __itr__ = 0; __itr__ < kv_max(v); ++__itr__) \
	{ \
		if (kv_A(v, __itr__)) ++(*cnt); \
	} \
} while(0)

#define kv_foreach_value(used, v, vvar, pos, code)  do { \
	for (uint16_t __itr__ = 0; __itr__ < kv_max(used); ++__itr__) \
	{ \
		if (kv_A(used, __itr__)) {(*pos) = __itr__; (*vvar) = kv_A(v, __itr__); code } \
	} \
} while(0)

#define kv_foreach_hv_value(hv, vvar, pos, code)  do { \
	for (uint16_t __itr__ = 0; __itr__ < kv_max(hv->used); ++__itr__) \
	{ \
		if (kv_A(hv->used, __itr__)) {(*pos) = __itr__; (*vvar) = kv_A(hv->values, __itr__); code } \
	} \
} while(0)
/* end spredis additions */

#define kv_copy(type, v1, v0) do {							\
		if ((v1).m < (v0).n) kv_resize(type, v1, (v0).n);	\
		(v1).n = (v0).n;									\
		memcpy((v1).a, (v0).a, sizeof(type) * (v0).n);		\
	} while (0)												\

#define kv_push(type, v, x) do {									\
		if ((v).n == (v).m) {										\
			(v).m = (v).m? (v).m<<1 : 2;							\
			(v).a = (type*)RedisModule_Realloc((v).a, sizeof(type) * (v).m);	\
		}															\
		(v).a[(v).n++] = (x);										\
	} while (0)

#define kv_pushp(type, v) (((v).n == (v).m)?							\
						   ((v).m = ((v).m? (v).m<<1 : 2),				\
							(v).a = (type*)RedisModule_Realloc((v).a, sizeof(type) * (v).m), 0)	\
						   : 0), ((v).a + ((v).n++))

#define kv_a(type, v, i) (((v).m <= (size_t)(i)? \
						  ((v).m = (v).n = (i) + 1, kv_roundup32((v).m), \
						   (v).a = (type*)RedisModule_Realloc((v).a, sizeof(type) * (v).m), 0) \
						  : (v).n <= (size_t)(i)? (v).n = (i) + 1 \
						  : 0), (v).a[(i)])

#endif

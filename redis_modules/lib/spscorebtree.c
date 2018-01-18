/*-
 * Copyright 1997-1999, 2001, John-Mark Gurney.
 *           2008-2009, Attractive Chaos <attractor@live.co.uk>
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */


#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "spscorebtree.h"




// #define __spsc_TREE_T(name)
	// typedef struct {
	// 	kbnode_t *root;
	// 	int	off_key, off_ptr, ilen, elen;
	// 	int	n, t;
	// 	int	n_keys, n_nodes;
	// } kbtree_SPScore_t;

// static inline int ScoreComp(SPScore a, SPScore b) {
//     if (a.score < b.score) return -1;
//     if (b.score < a.score) return 1;
//     if (a.id < b.id) return -1;
//     if (b.id < a.id) return 1;
//     return 0;
// }
// // #define __spsc_INIT(name, SPScore)
// 	inline kbtree_SPScore_t *spsc_init(int size)
// 	{
// 		kbtree_SPScore_t *b;
// 		b = (kbtree_SPScore_t*)RedisModule_Calloc(1, sizeof(kbtree_SPScore_t));
// 		b->t = ((size - 4 - sizeof(void*)) / (sizeof(void*) + sizeof(SPScore)) + 1) >> 1;
// 		if (b->t < 2) {
// 			RedisModule_Free(b); return 0;
// 		}
// 		b->n = 2 * b->t - 1;
// 		b->off_ptr = 4 + b->n * sizeof(SPScore);
// 		b->ilen = (4 + sizeof(void*) + b->n * (sizeof(void*) + sizeof(SPScore)) + 3) >> 2 << 2;
// 		b->elen = (b->off_ptr + 3) >> 2 << 2;
// 		b->root = (kbnode_t*)RedisModule_Calloc(1, b->ilen);
// 		++b->n_nodes;
// 		return b;
// 	}

// // #define __spsc_GET_AUX1(name, SPScore, ScoreComp)
	
// 	static inline int __spsc_getp_aux(const kbnode_t * __restrict x, const SPScore * __restrict k, int *r)
// 	{
// 		int tr, *rr, begin = 0, end = x->n;
// 		if (x->n == 0) return -1;
// 		rr = r? r : &tr;
// 		while (begin < end) {
// 			int mid = (begin + end) >> 1;
// 			if (ScoreComp(__spsc_KEY(SPScore, x)[mid], *k) < 0) begin = mid + 1;
// 			else end = mid;
// 		}
// 		if (begin == x->n) { *rr = 1; return x->n - 1; }
// 		if ((*rr = ScoreComp(*k, __spsc_KEY(SPScore, x)[begin])) < 0) --begin;
// 		return begin;
// 	}

// // #define __spsc_GET(name, SPScore)
// 	// static 
// 	inline SPScore *spsc_getp(kbtree_SPScore_t *b, const SPScore * __restrict k)
// 	{
// 		int i, r = 0;
// 		kbnode_t *x = b->root;
// 		while (x) {
// 			i = __spsc_getp_aux(x, k, &r);
// 			if (i >= 0 && r == 0) return &__spsc_KEY(SPScore, x)[i];
// 			if (x->is_internal == 0) return 0;
// 			x = __spsc_PTR(b, x)[i + 1];
// 		}
// 		return 0;
// 	}
// 	// static inline 
// 	inline SPScore *spsc_get(kbtree_SPScore_t *b, const SPScore k)
// 	{
// 		return spsc_getp(b, &k);
// 	}

// // #define __spsc_INTERVAL(name, SPScore)
// 	// static 
// 	inline void spsc_intervalp(kbtree_SPScore_t *b, const SPScore * __restrict k, SPScore **lower, SPScore **upper)
// 	{
// 		int i, r = 0;
// 		kbnode_t *x = b->root;
// 		*lower = *upper = 0;
// 		while (x) {
// 			i = __spsc_getp_aux(x, k, &r);
// 			if (i >= 0 && r == 0) {
// 				*lower = *upper = &__spsc_KEY(SPScore, x)[i];
// 				return;
// 			}
// 			if (i >= 0) *lower = &__spsc_KEY(SPScore, x)[i];
// 			if (i < x->n - 1) *upper = &__spsc_KEY(SPScore, x)[i + 1];
// 			if (x->is_internal == 0) return;
// 			x = __spsc_PTR(b, x)[i + 1];
// 		}
// 	}
// 	// static inline 
// 	inline void spsc_interval(kbtree_SPScore_t *b, const SPScore k, SPScore **lower, SPScore **upper)
// 	{
// 		spsc_intervalp(b, &k, lower, upper);
// 	}

// // #define __spsc_PUT(name, SPScore, ScoreComp)
// 	/* x must be an internal node */
// 	// static 
// 	inline void __spsc_split(kbtree_SPScore_t *b, kbnode_t *x, int i, kbnode_t *y)
// 	{
// 		kbnode_t *z;
// 		z = (kbnode_t*)RedisModule_Calloc(1, y->is_internal? b->ilen : b->elen);
// 		++b->n_nodes;
// 		z->is_internal = y->is_internal;
// 		z->n = b->t - 1;
// 		memcpy(__spsc_KEY(SPScore, z), __spsc_KEY(SPScore, y) + b->t, sizeof(SPScore) * (b->t - 1));
// 		if (y->is_internal) memcpy(__spsc_PTR(b, z), __spsc_PTR(b, y) + b->t, sizeof(void*) * b->t);
// 		y->n = b->t - 1;
// 		memmove(__spsc_PTR(b, x) + i + 2, __spsc_PTR(b, x) + i + 1, sizeof(void*) * (x->n - i));
// 		__spsc_PTR(b, x)[i + 1] = z;
// 		memmove(__spsc_KEY(SPScore, x) + i + 1, __spsc_KEY(SPScore, x) + i, sizeof(SPScore) * (x->n - i));
// 		__spsc_KEY(SPScore, x)[i] = __spsc_KEY(SPScore, y)[b->t - 1];
// 		++x->n;
// 	}
// 	// static 
// 	inline SPScore *__spsc_putp_aux(kbtree_SPScore_t *b, kbnode_t *x, const SPScore * __restrict k)
// 	{
// 		int i = x->n - 1;
// 		SPScore *ret;
// 		if (x->is_internal == 0) {
// 			i = __spsc_getp_aux(x, k, 0);
// 			if (i != x->n - 1)
// 				memmove(__spsc_KEY(SPScore, x) + i + 2, __spsc_KEY(SPScore, x) + i + 1, (x->n - i - 1) * sizeof(SPScore));
// 			ret = &__spsc_KEY(SPScore, x)[i + 1];
// 			*ret = *k;
// 			++x->n;
// 		} else {
// 			i = __spsc_getp_aux(x, k, 0) + 1;
// 			if (__spsc_PTR(b, x)[i]->n == 2 * b->t - 1) {
// 				__spsc_split(b, x, i, __spsc_PTR(b, x)[i]);
// 				if (ScoreComp(*k, __spsc_KEY(SPScore, x)[i]) > 0) ++i;
// 			}
// 			ret = __spsc_putp_aux(b, __spsc_PTR(b, x)[i], k);
// 		}
// 		return ret; 
// 	}
// 	// static 
// 	inline SPScore *spsc_putp(kbtree_SPScore_t *b, const SPScore * __restrict k)
// 	{
// 		kbnode_t *r, *s;
// 		++b->n_keys;
// 		r = b->root;
// 		if (r->n == 2 * b->t - 1) {
// 			++b->n_nodes;
// 			s = (kbnode_t*)RedisModule_Calloc(1, b->ilen);
// 			b->root = s; s->is_internal = 1; s->n = 0;
// 			__spsc_PTR(b, s)[0] = r;
// 			__spsc_split(b, s, 0, r);
// 			r = s;
// 		}
// 		return __spsc_putp_aux(b, r, k);
// 	}
// 	// static inline 
// 	inline void spsc_put(kbtree_SPScore_t *b, const SPScore k)
// 	{
// 		spsc_putp(b, &k);
// 	}


// // #define __spsc_DEL(name, SPScore)
// 	// static 
// 	inline SPScore __spsc_delp_aux(kbtree_SPScore_t *b, kbnode_t *x, const SPScore * __restrict k, int s)
// 	{
// 		int yn, zn, i, r = 0;
// 		kbnode_t *xp, *y, *z;
// 		SPScore kp;
// 		if (x == 0) return *k;
// 		if (s) { /* s can only be 0, 1 or 2 */
// 			r = x->is_internal == 0? 0 : s == 1? 1 : -1;
// 			i = s == 1? x->n - 1 : -1;
// 		} else i = __spsc_getp_aux(x, k, &r);
// 		if (x->is_internal == 0) {
// 			if (s == 2) ++i;
// 			kp = __spsc_KEY(SPScore, x)[i];
// 			memmove(__spsc_KEY(SPScore, x) + i, __spsc_KEY(SPScore, x) + i + 1, (x->n - i - 1) * sizeof(SPScore));
// 			--x->n;
// 			return kp;
// 		}
// 		if (r == 0) {
// 			if ((yn = __spsc_PTR(b, x)[i]->n) >= b->t) {
// 				xp = __spsc_PTR(b, x)[i];
// 				kp = __spsc_KEY(SPScore, x)[i];
// 				__spsc_KEY(SPScore, x)[i] = __spsc_delp_aux(b, xp, 0, 1);
// 				return kp;
// 			} else if ((zn = __spsc_PTR(b, x)[i + 1]->n) >= b->t) {
// 				xp = __spsc_PTR(b, x)[i + 1];
// 				kp = __spsc_KEY(SPScore, x)[i];
// 				__spsc_KEY(SPScore, x)[i] = __spsc_delp_aux(b, xp, 0, 2);
// 				return kp;
// 			} else if (yn == b->t - 1 && zn == b->t - 1) {
// 				y = __spsc_PTR(b, x)[i]; z = __spsc_PTR(b, x)[i + 1];
// 				__spsc_KEY(SPScore, y)[y->n++] = *k;
// 				memmove(__spsc_KEY(SPScore, y) + y->n, __spsc_KEY(SPScore, z), z->n * sizeof(SPScore));
// 				if (y->is_internal) memmove(__spsc_PTR(b, y) + y->n, __spsc_PTR(b, z), (z->n + 1) * sizeof(void*));
// 				y->n += z->n;
// 				memmove(__spsc_KEY(SPScore, x) + i, __spsc_KEY(SPScore, x) + i + 1, (x->n - i - 1) * sizeof(SPScore));
// 				memmove(__spsc_PTR(b, x) + i + 1, __spsc_PTR(b, x) + i + 2, (x->n - i - 1) * sizeof(void*));
// 				--x->n;
// 				RedisModule_Free(z);
// 				return __spsc_delp_aux(b, y, k, s);
// 			}
// 		}
// 		++i;
// 		if ((xp = __spsc_PTR(b, x)[i])->n == b->t - 1) {
// 			if (i > 0 && (y = __spsc_PTR(b, x)[i - 1])->n >= b->t) {
// 				memmove(__spsc_KEY(SPScore, xp) + 1, __spsc_KEY(SPScore, xp), xp->n * sizeof(SPScore));
// 				if (xp->is_internal) memmove(__spsc_PTR(b, xp) + 1, __spsc_PTR(b, xp), (xp->n + 1) * sizeof(void*));
// 				__spsc_KEY(SPScore, xp)[0] = __spsc_KEY(SPScore, x)[i - 1];
// 				__spsc_KEY(SPScore, x)[i - 1] = __spsc_KEY(SPScore, y)[y->n - 1];
// 				if (xp->is_internal) __spsc_PTR(b, xp)[0] = __spsc_PTR(b, y)[y->n];
// 				--y->n; ++xp->n;
// 			} else if (i < x->n && (y = __spsc_PTR(b, x)[i + 1])->n >= b->t) {
// 				__spsc_KEY(SPScore, xp)[xp->n++] = __spsc_KEY(SPScore, x)[i];
// 				__spsc_KEY(SPScore, x)[i] = __spsc_KEY(SPScore, y)[0];
// 				if (xp->is_internal) __spsc_PTR(b, xp)[xp->n] = __spsc_PTR(b, y)[0];
// 				--y->n;
// 				memmove(__spsc_KEY(SPScore, y), __spsc_KEY(SPScore, y) + 1, y->n * sizeof(SPScore));
// 				if (y->is_internal) memmove(__spsc_PTR(b, y), __spsc_PTR(b, y) + 1, (y->n + 1) * sizeof(void*));
// 			} else if (i > 0 && (y = __spsc_PTR(b, x)[i - 1])->n == b->t - 1) {
// 				__spsc_KEY(SPScore, y)[y->n++] = __spsc_KEY(SPScore, x)[i - 1];
// 				memmove(__spsc_KEY(SPScore, y) + y->n, __spsc_KEY(SPScore, xp), xp->n * sizeof(SPScore));
// 				if (y->is_internal) memmove(__spsc_PTR(b, y) + y->n, __spsc_PTR(b, xp), (xp->n + 1) * sizeof(void*));
// 				y->n += xp->n;
// 				memmove(__spsc_KEY(SPScore, x) + i - 1, __spsc_KEY(SPScore, x) + i, (x->n - i) * sizeof(SPScore));
// 				memmove(__spsc_PTR(b, x) + i, __spsc_PTR(b, x) + i + 1, (x->n - i) * sizeof(void*));
// 				--x->n;
// 				RedisModule_Free(xp);
// 				xp = y;
// 			} else if (i < x->n && (y = __spsc_PTR(b, x)[i + 1])->n == b->t - 1) {
// 				__spsc_KEY(SPScore, xp)[xp->n++] = __spsc_KEY(SPScore, x)[i];
// 				memmove(__spsc_KEY(SPScore, xp) + xp->n, __spsc_KEY(SPScore, y), y->n * sizeof(SPScore));
// 				if (xp->is_internal) memmove(__spsc_PTR(b, xp) + xp->n, __spsc_PTR(b, y), (y->n + 1) * sizeof(void*));
// 				xp->n += y->n;
// 				memmove(__spsc_KEY(SPScore, x) + i, __spsc_KEY(SPScore, x) + i + 1, (x->n - i - 1) * sizeof(SPScore));
// 				memmove(__spsc_PTR(b, x) + i + 1, __spsc_PTR(b, x) + i + 2, (x->n - i - 1) * sizeof(void*));
// 				--x->n;
// 				RedisModule_Free(y);
// 			}
// 		}
// 		return __spsc_delp_aux(b, xp, k, s);
// 	}
// 	// static 
// 	inline SPScore spsc_delp(kbtree_SPScore_t *b, const SPScore * __restrict k)
// 	{
// 		kbnode_t *x;
// 		SPScore ret;
// 		ret = __spsc_delp_aux(b, b->root, k, 0);
// 		--b->n_keys;
// 		if (b->root->n == 0 && b->root->is_internal) {
// 			--b->n_nodes;
// 			x = b->root;
// 			b->root = __spsc_PTR(b, x)[0];
// 			RedisModule_Free(x);
// 		}
// 		return ret;
// 	}
// 	// static inline 
// 	inline SPScore spsc_del(kbtree_SPScore_t *b, const SPScore k)
// 	{
// 		return spsc_delp(b, &k);
// 	}

// // #define __spsc_ITR(name, SPScore)
// 	// static inline 
// 	inline void spsc_itr_first(kbtree_SPScore_t *b, kbitr_t *itr)
// 	{
// 		itr->p = 0;
// 		if (b->n_keys == 0) return;
// 		itr->p = itr->stack;
// 		itr->p->x = b->root; itr->p->i = 0;
// 		while (itr->p->x->is_internal && __spsc_PTR(b, itr->p->x)[0] != 0) {
// 			kbnode_t *x = itr->p->x;
// 			++itr->p;
// 			itr->p->x = __spsc_PTR(b, x)[0]; itr->p->i = 0;
// 		}
// 	}
// 	// static 
// 	inline int spsc_itr_get(kbtree_SPScore_t *b, const SPScore * __restrict k, kbitr_t *itr)
// 	{
// 		int i, r = 0;
// 		itr->p = itr->stack;
// 		itr->p->x = b->root; itr->p->i = 0;
// 		while (itr->p->x) {
// 			i = __spsc_getp_aux(itr->p->x, k, &r);
// 			if (i >= 0 && r == 0) return 0;
// 			if (itr->p->x->is_internal == 0) return -1;
// 			itr->p[1].x = __spsc_PTR(b, itr->p->x)[i + 1];
// 			itr->p[1].i = i;
// 			++itr->p;
// 		}
// 		return -1;
// 	}
// 	// static inline 
// 	inline int spsc_itr_next(kbtree_SPScore_t *b, kbitr_t *itr)
// 	{
// 		if (itr->p < itr->stack) return 0;
// 		for (;;) {
// 			++itr->p->i;
// 			while (itr->p->x && itr->p->i <= itr->p->x->n) {
// 				itr->p[1].i = 0;
// 				itr->p[1].x = itr->p->x->is_internal? __spsc_PTR(b, itr->p->x)[itr->p->i] : 0;
// 				++itr->p;
// 			}
// 			--itr->p;
// 			if (itr->p < itr->stack) return 0;
// 			if (itr->p->x && itr->p->i < itr->p->x->n) return 1;
// 		}
// 	}

// #define KBTREE_INIT(name, SPScore, ScoreComp)
	// __spsc_INIT(name, SPScore)
	// __spsc_GET_AUX1(name, SPScore, ScoreComp)
	// __spsc_GET(name, SPScore)
	// __spsc_INTERVAL(name, SPScore)
	// __spsc_PUT(name, SPScore, ScoreComp)
	// __spsc_DEL(name, SPScore)
	// __spsc_ITR(name, SPScore)
	// __spsc_TREE_T(name)





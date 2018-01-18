#ifndef __SP_KBTREE_H
#define __SP_KBTREE_H

#include "redismodule.h"

// #define	__spsc_KEY(type, x)	((type*)((char*)x + 4))
// #define __spsc_PTR(btr, x)	((kbnode_t**)((char*)x + btr->off_ptr))

// #define spsc_MAX_DEPTH 64

// typedef struct _SPScore {
//     uint32_t id;
//     double score;
//     struct _SPScore *next, *prev;
// } SPScore;


// typedef struct {
// 	int32_t is_internal:1, n:31;
// } kbnode_t;

// typedef struct {
// 	kbnode_t *x;
// 	int i;
// } kbpos_t;

// typedef struct {
// 	kbpos_t stack[spsc_MAX_DEPTH], *p;
// } kbitr_t;


// typedef struct {
// 	kbnode_t *root;
// 	int	off_key, off_ptr, ilen, elen;
// 	int	n, t;
// 	int	n_keys, n_nodes;
// } kbtree_SPScore_t;


// #define KB_DEFAULT_SIZE 512
// #define __spsc_destroy(b) do { \
// 		int i, max = 8; \
// 		kbnode_t *x, **top, **stack = 0; \
// 		if (b) { \
// 			top = stack = (kbnode_t**)RedisModule_Calloc(max, sizeof(kbnode_t*)); \
// 			*top++ = (b)->root; \
// 			while (top != stack) { \
// 				x = *--top; \
// 				if (x->is_internal == 0) { RedisModule_Free(x); continue; } \
// 				for (i = 0; i <= x->n; ++i) \
// 					if (__spsc_PTR(b, x)[i]) { \
// 						if (top - stack == max) { \
// 							max <<= 1; \
// 							stack = (kbnode_t**)RedisModule_Realloc(stack, max * sizeof(kbnode_t*)); \
// 							top = stack + (max>>1); \
// 						} \
// 						*top++ = __spsc_PTR(b, x)[i]; \
// 					} \
// 				RedisModule_Free(x); \
// 			} \
// 		} \
// 		RedisModule_Free(b); RedisModule_Free(stack); \
// 	} while (0) \


// #define spsc_itr_key(itr) __spsc_KEY(SPScore, (itr)->p->x)[(itr)->p->i]
// #define spsc_itr_valid(itr) ((itr)->p >= (itr)->stack)
// #define spsc_size(b) ((b)->n_keys)

// extern __inline__ __spsc_delp_aux(kbtree_SPScore_t *b, kbnode_t *x, const SPScore * __restrict k, int s);

// extern inline kbtree_SPScore_t *spsc_init(int size);
// extern inline SPScore *spsc_getp(kbtree_SPScore_t *b, const SPScore * __restrict k);
// extern inline SPScore *spsc_get(kbtree_SPScore_t *b, const SPScore k);
// extern inline SPScore *spsc_putp(kbtree_SPScore_t *b, const SPScore * __restrict k);
// extern inline void spsc_put(kbtree_SPScore_t *b, const SPScore k);
// extern inline SPScore spsc_delp(kbtree_SPScore_t *b, const SPScore * __restrict k);
// extern inline SPScore spsc_del(kbtree_SPScore_t *b, const SPScore k);
// extern inline void spsc_itr_first(kbtree_SPScore_t *b, kbitr_t *itr);
// extern inline int spsc_itr_get(kbtree_SPScore_t *b, const SPScore * __restrict k, kbitr_t *itr);
// extern inline int spsc_itr_next(kbtree_SPScore_t *b, kbitr_t *itr);



#endif

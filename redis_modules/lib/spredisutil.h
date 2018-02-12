#ifndef __SPREDIS_UTIL
#define __SPREDIS_UTIL

#include <math.h>
#include <unistd.h>
// #define R 6371
typedef struct _SPLatLong {
    double lat;
    double lon;
} SPLatLong;
  //we want this code inlined for performance reasons
// #define SP_DISTANCE_INIT(name)
static inline void  SPGeoDist(double alat, double alon, double blat, double blon, double *d)
{
    double dx, dy, dz;
    alon -= blon;
    alon *= 0.017453292519943295, alat *= 0.017453292519943295, blat *= 0.017453292519943295;
    dz = sin(alat) - sin(blat);
    dx = cos(alon) * cos(alat) - cos(blat);
    dy = sin(alon) * cos(alat);
    (*d) = asin(sqrt(dx * dx + dy * dy + dz * dz) / 2) * 2 * 6372797.560856;
}


static inline double  SPGetDist(double alat, double alon, double blat, double blon)
{
    double dx, dy, dz;
    alon -= blon;
    alon *= 0.017453292519943295, alat *= 0.017453292519943295, blat *= 0.017453292519943295;
    dz = sin(alat) - sin(blat);
    dx = cos(alon) * cos(alat) - cos(blat);
    dy = sin(alon) * cos(alat);
    return asin(sqrt(dx * dx + dy * dy + dz * dz) / 2) * 2 * 6372797.560856;
}


#define SpredisProtectWriteMap(map) {while (pthread_rwlock_trywrlock(&map->mutex)) {sleep(1);}}
#define SpredisProtectReadMap(map) pthread_rwlock_rdlock(&map->mutex)
#define SpredisUnProtectMap(map) pthread_rwlock_unlock(&map->mutex)

typedef struct {
    void *left, *right;
    int depth;
} sp_isort_stack_t;


#define SPREDIS_SORT_INIT(type, extraType, CompareLT)					\
					\
static inline void __ks_##type##_insertsort_Spredis(type** s, type** t, extraType *mcd)					\
{					\
    type** i;					\
    type** j;					\
    type* swap_tmp;					\
    for (i = s + 1; i < t; ++i)					\
        for (j = i; j > s && CompareLT(*j, *(j-1), mcd); --j) {					\
            swap_tmp = *j; *j = *(j-1); *(j-1) = swap_tmp;					\
        }					\
}					\
					\
static inline void ks_##type##_combsort_Spredis(size_t n, type** a, extraType *mcd)					\
{					\
    const double shrink_factor = 1.2473309501039786540366528676643;					\
    int do_swap;					\
    size_t gap = n;					\
    type* tmp;					\
    type** i;					\
    type** j;					\
    do {					\
        if (gap > 2) {					\
            gap = (size_t)(gap / shrink_factor);					\
            if (gap == 9 || gap == 10) gap = 11;					\
        }					\
        do_swap = 0;					\
        for (i = a; i < a + n - gap; ++i) {					\
            j = i + gap;					\
            if (CompareLT(*j, *i, mcd)) {					\
                tmp = *i; *i = *j; *j = tmp;					\
                do_swap = 1;					\
            }					\
        }					\
    } while (do_swap || gap > 2);					\
    if (gap != 1) __ks_##type##_insertsort_Spredis(a, a + n, mcd);					\
}					\
					\
static inline void ks_##type##_introsort_Spredis(size_t n, type** a, extraType *mcd)					\
{					\
    int d;					\
    sp_isort_stack_t *top, *stack;					\
    type *rp, *swap_tmp;					\
    type **s,**t,**i, **j, **k;					\
					\
					\
    if (n < 1) return;					\
    else if (n == 2) {					\
        if (CompareLT(a[1], a[0],mcd)) { swap_tmp = a[0]; a[0] = a[1]; a[1] = swap_tmp; }					\
        return;					\
    }					\
    for (d = 2; 1ul<<d < n; ++d);					\
    stack = (sp_isort_stack_t*)RedisModule_Alloc(sizeof(sp_isort_stack_t) * ((sizeof(size_t)*d)+2));					\
    top = stack; s = a; t = a + (n-1); d <<= 1;					\
    while (1) {					\
        if (s < t) {					\
            if (--d == 0) {					\
                ks_##type##_combsort_Spredis(t - s + 1, s, mcd);					\
                t = s;					\
                continue;					\
            }					\
            i = s; j = t; k = i + ((j-i)>>1) + 1;					\
            if (CompareLT(*k, *i, mcd)) {					\
                if (CompareLT(*k, *j, mcd)) k = j;					\
            } else k = CompareLT(*j, *i, mcd)? i : j;					\
            rp = *k;					\
            if (k != t) { swap_tmp = *k; *k = *t; *t = swap_tmp; }					\
            for (;;) {					\
                do ++i; while (CompareLT(*i, rp, mcd));					\
                do --j; while (i <= j && CompareLT(rp, *j, mcd));					\
                if (j <= i) break;					\
                swap_tmp = *i; *i = *j; *j = swap_tmp;					\
            }					\
            swap_tmp = *i; *i = *t; *t = swap_tmp;					\
            if (i-s > t-i) {					\
                if (i-s > 16) { top->left = s; top->right = i-1; top->depth = d; ++top; }					\
                s = t-i > 16? i+1 : t;					\
            } else {					\
                if (t-i > 16) { top->left = i+1; top->right = t; top->depth = d; ++top; }					\
                t = i-s > 16? i-1 : s;					\
            }					\
        } else {					\
            if (top == stack) {					\
                RedisModule_Free(stack);					\
                __ks_##type##_insertsort_Spredis(a, a+n, mcd);					\
                return;					\
            } else { --top; s = (type**)top->left; t = (type**)top->right; d = top->depth; }					\
        }					\
    }					\
}					\
					\
void Spredis##type##Sort(size_t n, type **a, extraType *mcd) {					\
	ks_##type##_introsort_Spredis(n, a, mcd);					\
}					\
					\



#define SP_IS_EXCLUSIVE(arg) ( memcmp(arg, "(", 1) ? 0 : 1 )
#define SP_ARG_MINUS_INC_EXC(arg) ( !memcmp(arg, "(", 1) || !memcmp(arg, "[", 1) ? arg + 1 : arg )

static inline int SpredisLexGT(int a) {
    return a > 0;
}

static inline int SpredisLexGTE(int a) {
    return a == 0;
}

static inline int SpredisLexLT(int a) {
    return a < 0;
}

static inline int SpredisLexLTE(int a) {
    return a <= 0;
}



#define SP_LEXLTCMP(arg) (SP_IS_EXCLUSIVE(arg) ? SpredisLexLT : SpredisLexLTE)
#define SP_LEXGTCMP(arg) (SP_IS_EXCLUSIVE(arg) ? SpredisLexGT : SpredisLexGTE)


static inline int SpredisScoreGT(double a, double b) {
    return (a > b);
}

static inline int SpredisScoreGTE(double a, double b) {
    return (a >= b);
}

static inline int SpredisScoreLT(double a, double b) {
    return (a < b);
}

static inline int SpredisScoreLTE(double a, double b) {
    return (a <= b);
}


#define SP_SCRLTCMP(arg) (SP_IS_EXCLUSIVE(arg) ? SpredisScoreLT : SpredisScoreLTE)
#define SP_SCRGTCMP(arg) (SP_IS_EXCLUSIVE(arg) ? SpredisScoreGT : SpredisScoreGTE)


#endif
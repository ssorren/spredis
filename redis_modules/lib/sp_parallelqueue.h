#ifndef __SPREDIS_PQ
#define __SPREDIS_PQ


#include <pthread.h>
#include "thpool.h"
#include "kvec.h"


typedef struct {
	int size;
	uint8_t  numthreads;
	kvec_t(threadpool) pools;
	pthread_mutex_t qlock;
	pthread_cond_t  poolavail;
} SPJobQ;


SPJobQ *sp_pq_init(int size, uint8_t threads_per_job);
void sp_runjobs(SPJobQ *q, void (**function_p)(void*), void **arg_p, int count);

#endif
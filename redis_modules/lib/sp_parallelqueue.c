#include "sp_parallelqueue.h"
#include "redismodule.h"

SPJobQ *sp_pq_init(int size, uint8_t threads_per_job) {
	SPJobQ *q = RedisModule_Alloc(sizeof(SPJobQ));
	q->size = size;
	q->numthreads = threads_per_job;
	kv_init(q->pools);
	kv_resize(threadpool, q->pools, size);
	for (uint8_t i = 0; i < size; ++i)
	{
		kv_push(threadpool, q->pools, thpool_init(threads_per_job));
	}
	pthread_mutex_init(&q->qlock, NULL);
	pthread_cond_init(&q->poolavail, NULL);
	return q;
}

void sp_runjobs(SPJobQ *q, void (**function_p)(void*), void **arg_p, int count) {

	/* wait for an available tpool */
	pthread_mutex_lock(&q->qlock);
	while (kv_size(q->pools) == 0) {
		pthread_cond_wait(&q->poolavail, &q->qlock);
	}
	/* remove a pool from the stack */
	threadpool p = kv_pop(q->pools);
	pthread_mutex_unlock(&q->qlock);

	/* add the work to the pool */
	for (int i = 0; i < count; ++i)
	{
		thpool_add_work(p, function_p[i], arg_p[i]);
	}
	/* wait for the work to finish */
	thpool_wait(p);

	/* add the pool back to the stack */
	pthread_mutex_lock(&q->qlock);
	kv_push(threadpool, q->pools, p);
	/* notify anyone who was waiting for an empty pool */
	pthread_cond_signal(&q->poolavail);
	pthread_mutex_unlock(&q->qlock);
}
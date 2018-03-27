#include "../spredis.h"
#include "./kvec.h"
#include <pthread.h>

// KV_INIT(SPScoreCont);
#define SORTINTERVAL 30;
#define QUICKSORTINTERVAL 2;

void *SPSortQApplyThread(void *x_void_ptr);

typedef struct {
	pthread_mutex_t lock;
	SPScoreCont *resorting;
	kvec_t(SPScoreCont*) q;
} SPResortCont;

static SPResortCont *SPRESORTQ;

int SpredisSortAppInit() {
	
	SPRESORTQ = RedisModule_Calloc(1, sizeof(SPResortCont));
	kv_init(SPRESORTQ->q);
	pthread_mutex_init(&(SPRESORTQ->lock), NULL);
	pthread_t SPSortQApplyThread_ptr;
	pthread_create(&SPSortQApplyThread_ptr, NULL, SPSortQApplyThread, NULL);
	pthread_detach(SPSortQApplyThread_ptr);
	return 0;
}

#define SP_WAIT_WLOCK(lock) pthread_mutex_lock(&lock)
#define SP_ULOCK(lock) pthread_mutex_unlock(&lock)

void SpredisQSort(SPScoreCont *cont) {
	SP_WAIT_WLOCK(SPRESORTQ->lock);
	size_t cnt = kv_size(SPRESORTQ->q);
	int found = 0;
	while (cnt) {
		if (kv_A(SPRESORTQ->q, --cnt) == cont) {
			found = 1;
		}
	}
	if (!found) {
		kv_push(SPScoreCont*, SPRESORTQ->q, cont);	
	}
	SP_ULOCK(SPRESORTQ->lock);
}

void SpredisDQSort(SPScoreCont *cont) {
	SP_WAIT_WLOCK(SPRESORTQ->lock);
	size_t cnt = kv_size(SPRESORTQ->q);
	if (SPRESORTQ->resorting == cont) SPRESORTQ->resorting = NULL;
	while (cnt--) {
		if (kv_A(SPRESORTQ->q, cnt) == cont) {
			kv_A(SPRESORTQ->q, cnt) = NULL;
		}
	}
	SP_ULOCK(SPRESORTQ->lock);
}

void *SPSortQApplyThread(void *x_void_ptr) {
	int sleepSeconds = SORTINTERVAL;
	while (sleepSeconds) {
		sleep(sleepSeconds);
		SP_WAIT_WLOCK(SPRESORTQ->lock);
		while (kv_size(SPRESORTQ->q)) {
			SPRESORTQ->resorting = kv_pop(SPRESORTQ->q);
			if (SPRESORTQ->resorting == NULL) continue;
			if (pthread_rwlock_trywrlock(&SPRESORTQ->resorting->mutex)) {
				kv_push(SPScoreCont*, SPRESORTQ->q, SPRESORTQ->resorting);
				SPRESORTQ->resorting = NULL;
				// we missed a sort because of a write lock, let's only sleep a couple of seconds so we can catch up
				sleepSeconds = QUICKSORTINTERVAL;
				break;
			};
			double newScore = 0;
		    SPScoreSetKey *skey;
		    kbitr_t itr;
		    // loop through and bump the score by one for each entry
		    kb_itr_first(LEXSET, SPRESORTQ->resorting->btree, &itr);
		    while (kb_itr_valid(&itr)) {
		        skey = &kb_itr_key(SPScoreSetKey, &itr);
		        if (skey != NULL && skey->members != NULL) {
		        	skey->members->score = ++newScore;        	
		        }
				kb_itr_next(LEXSET, SPRESORTQ->resorting->btree, &itr);
		    }
			SpredisUnProtectMap(SPRESORTQ->resorting);
			SPRESORTQ->resorting = NULL;
			// go back to normal speed. wait 10 seconds, let the q build up
			sleepSeconds = SORTINTERVAL;
		}
		SP_ULOCK(SPRESORTQ->lock);
		
	}
	return NULL;
}

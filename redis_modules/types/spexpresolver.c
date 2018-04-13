#include "../spredis.h"
#include <math.h>

void SPResolveGeoScore(double value, void *input, double *score) {
	double *latlon = input;
	double slat,slon;
	SPGeoHashDecode(value, &slat, &slon);
    (*score) = SPGetDist(slat, slon, latlon[0], latlon[1]);
}


SpExpResolverCont* _SpredisInitExpRslvr() {
    SpExpResolverCont *res = RedisModule_Calloc(1, sizeof(SpExpResolverCont));
	pthread_rwlock_init( &res->mutex,NULL );
	return res;
}


void SpredisExpRslvrDBSave(RedisModuleIO *io, void *ptr) {
	//we are not replicating these values
}
void SpredisExpRslvrRewriteFunc(RedisModuleIO *aof, RedisModuleString *key, void *value) {
	//we are not replicating these values
}

void *SpredisExpRslvrRDBLoad(RedisModuleIO *io, int encver) {
	//we are not replicating these values
	return _SpredisInitExpRslvr();
}


void _SpredisDestroyExpRslvr(void *value) {
    SpExpResolverCont *er = value;
    SpredisProtectWriteMap(er, "_SpredisDestroyExpRslvr");
    if (er->keyName) RedisModule_Free(er->keyName);
	if (er->input) RedisModule_Free(er->input);
	SpredisUnProtectMap(er, "_SpredisDestroyExpRslvr");
	pthread_rwlock_destroy(&er->mutex);
	RedisModule_Free(er);
}

void SpredisExpRslvrFreeCallback(void *value) {
	SP_TWORK(_SpredisDestroyExpRslvr, value, {
		//do nothing
	});
    // _SpredisDestroyExpRslvr(value);
}

int SpredisSetGeoResolver_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);
	
	// printf("Createing geo resolver: %d args", argc);
	// for (int i = 0; i < argc; ++i)
	// {
	// 	printf(", %s", RedisModule_StringPtrLen(argv[i], NULL));
	// }
	// printf("\n");

	if (argc < 6 || argc > 7) return RedisModule_WrongArity(ctx);
	RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_WRITE);
	int keyType = RedisModule_KeyType(key);
    if (keyType != REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }
    

    RedisModuleKey *scoreKey = RedisModule_OpenKey(ctx,argv[2],
            REDISMODULE_READ);
	if (HASH_EMPTY_OR_WRONGTYPE(scoreKey, &keyType, SPGEOTYPE) != 0) {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);   
    }

    SPScoreCont *scoreCont = RedisModule_ModuleTypeGetValue(scoreKey);
    SpExpResolverCont* cont = _SpredisInitExpRslvr();
    SpredisSetRedisKeyValueType(key,SPEXPRTYPE,cont);
    cont->set = scoreCont->st;
    cont->type = SPGeoExpType;
    cont->input = RedisModule_Calloc(2, sizeof(double));
    cont->resolve = SPResolveGeoScore;
    double *latlong = (double *)cont->input;
    RedisModule_StringToDouble(argv[4], &latlong[0]);
    RedisModule_StringToDouble(argv[5], &latlong[1]);

    // RedisModule_CloseKey(key);
	RedisModule_ReplyWithLongLong(ctx, 1);
	return REDISMODULE_OK;
}






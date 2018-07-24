#include "./spcomposite.h"

#define SPGeoPartType "GEO"
#define SPDoublePartType "DBL"
#define SPLexPartType "LEX"

int SPCompositeScoreComp(SPCompositeScoreSetKey a,SPCompositeScoreSetKey b) {
	int i = 0, res = 0;
	while (res == 0 && a.compCtx->valueCount > i) {
		res = (a.compCtx->compare[i])(a.value[i], b.value[i]);
		i++;
	}
	return res;
}

int SPGeoPartCompare(SPPtrOrD_t a, SPPtrOrD_t b) {
	return kb_generic_cmp(((a).asUInt), ((b).asUInt));
}

int SPDoublePartCompare(SPPtrOrD_t a, SPPtrOrD_t b) {
	return kb_generic_cmp(((a).asDouble), ((b).asDouble));
}

int SPLexPartCompare(SPPtrOrD_t a, SPPtrOrD_t b) {
	return kb_str_cmp(((a).asChar), ((b).asChar));
}

void SPAddCompScoreToSet(SPIndexCont *cont, spid_t id, SPPtrOrD_t *value)
{
	SPWriteLock(cont->lock);
	SPCompositeScoreSetKey search = {.compCtx = cont->compCtx, .value = value};
    SPCompositeScoreSetKey *compkey = kb_getp(COMPIDX, cont->index.compTree, &search);
    int absent;
    if (compkey == NULL) {
        compkey = kb_putp(COMPIDX, cont->index.compTree, &search);
        compkey->members = RedisModule_Calloc(1, sizeof(SPScoreSetMembers));
    	compkey->members->set = kh_init(SIDS);
    	compkey->compCtx = cont->compCtx;
    } else {
    	RedisModule_Free(value);
    }
    kh_put(SIDS, compkey->members->set, (id), &absent);
    SPWriteUnlock(cont->lock);
}

void SPRemCompScoreFromSet(SPIndexCont *cont, spid_t id, SPPtrOrD_t *value)
{
	SPWriteLock(cont->lock);
	SPCompositeScoreSetKey search = {.compCtx = cont->compCtx, .value = value};
    SPCompositeScoreSetKey *compkey = kb_getp(COMPIDX, cont->index.compTree, &search);
    if (compkey != NULL) {
		khint_t k = kh_get(SIDS, compkey->members->set, id);
    	if (k != kh_end(compkey->members->set)) {
	        kh_del(SIDS, compkey->members->set, k);
        }
        if (kh_size(compkey->members->set) == 0 ) {
        	SPScoreSetMembers *mems = compkey->members;
        	SPPtrOrD_t *evalue = compkey->value;
	        kb_delp(COMPIDX, cont->index.compTree, &search);
            if (mems->set) kh_destroy(SIDS, mems->set);
            RedisModule_Free(mems);
	        RedisModule_Free(evalue);
        }
    }
    SPWriteUnlock(cont->lock);
    RedisModule_Free(value);
}


SPPtrOrD_t SPGetPartValue(const char *ctype, RedisModuleString *arg1, RedisModuleString *arg2) {
	SPPtrOrD_t res;
	// const char *ctype = RedisModule_StringPtrLen(type, NULL);
	double lat, lon;
	if (!strcmp(SPGeoPartType, ctype)) {
		SpredisStringToDouble(arg1, &lat);
		SpredisStringToDouble(arg2, &lon);
		res.asUInt = SPGeoHashEncode(lat, lon);
	} else if (!strcmp(SPDoublePartType, ctype)) {
		SpredisStringToDouble(arg1, &lat);
		res.asDouble = lat;
	} else {
		res.asChar = SPUniqStr(RedisModule_StringPtrLen(arg1, NULL));
	} 
	return res;
}



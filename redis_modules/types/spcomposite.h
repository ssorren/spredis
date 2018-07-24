#ifndef __SPREDIS_COMPOSITE
#define __SPREDIS_COMPOSITE
#include "./spredis.h"

#define SPGeoPart 1
#define SPDoublePart 2
#define SPLexPart 3
#define SPIDPart 4

int SPGeoPartCompare(SPPtrOrD_t a, SPPtrOrD_t b);
int SPDoublePartCompare(SPPtrOrD_t a, SPPtrOrD_t b);
int SPLexPartCompare(SPPtrOrD_t a, SPPtrOrD_t b);

void SPAddCompScoreToSet(SPIndexCont *cont, spid_t id, SPPtrOrD_t *value);
void SPRemCompScoreFromSet(SPIndexCont *cont, spid_t id, SPPtrOrD_t *value);
#endif
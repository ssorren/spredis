#ifndef __SPREDIS_SORTAPP
#define __SPREDIS_SORTAPP

#include "../spredis.h"


int SpredisSortAppInit();
void SpredisQSort(SPScoreCont *cont);
void SpredisDQSort(SPScoreCont *cont);

#endif
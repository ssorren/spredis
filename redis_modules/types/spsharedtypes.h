#ifndef _SP_SHARED_TYPES
#define _SP_SHARED_TYPES

#include <stdlib.h>
#include "../lib/khash.h"
#include "../lib/sp_kbtree.h"
#include "../lib/kvec.h"
#include "../lib/lz4.h"

#define SPScoreSetComp(a,b) (((a).value.asDouble < (b).value.asDouble) ? -1 : ((a).value.asDouble > (b).value.asDouble))
// kb_generic_cmp(((a).value.asDouble), ((b).value.asDouble))
#define SPGeoSetComp(a,b)  (((a).value.asUInt < (b).value.asUInt) ? -1 : ((a).value.asUInt > (b).value.asUInt))
// kb_generic_cmp(((a).value.asUInt), ((b).value.asUInt))
#define SPLexSetComp(a,b)  (strcmp(((a).value.asChar), ((b).value.asChar)))


typedef uint8_t SPHashValueType;
typedef uint8_t SPExpResolverType;
typedef union _SPPtrOrD_t {
    double asDouble;
    int64_t asInt;
    uint64_t asUInt;
    const char *asChar;
    const unsigned char *asUChar;
    int asDumbInt;
} SPPtrOrD_t;

typedef kvec_t(SPPtrOrD_t) SPPtrOrD;

// typedef struct _SPScore {
//     spid_t id;
//     double score;
//     char *lex;
//     struct _SPScore *next, *prev;
// } SPScore;

// typedef struct _SPScoreKey {
// 	spid_t id;
//     SPPtrOrD_t score;
//     void *value;
// } SPScoreKey;


#define SPGeoExpType 0
#define SPMathExpType 1

#define SPHashStringType 0
#define SPHashDoubleType 1

// typedef struct _SPHashValue {
// 	spid_t id;
// 	SPHashValueType type;
// 	kvec_t(char) used;
//     SPPtrOrD values;
//     struct _SPHashValue *next, *prev;
// } SPHashValue;

KHASH_DECLARE_SET(SIDS, spid_t);
// KHASH_DECLARE(SCORE, spid_t, SPScore*);

typedef struct _SPScoreSetMembers {
    SPPtrOrD_t score;
    khash_t(SIDS) *set;
} SPScoreSetMembers;

#include "./spset.h"

// KHASH_DECLARE(SORTTRACK, spid_t, SPScoreSetMembers*);
// KHASH_MAP_INIT_INT64(SORTTRACK, SPScoreSetMembers*);

typedef struct _SPScoreSetKey
{
    SPPtrOrD_t value;
    SPScoreSetMembers *members;
} SPScoreSetKey;

KB_TYPE(COMPIDX);


typedef int (*SPCompositeComp)(SPPtrOrD_t , SPPtrOrD_t);

typedef struct _SPCompositeCompCtx {
    uint8_t valueCount;
    uint8_t *types;
    SPCompositeComp *compare;
} SPCompositeCompCtx;

typedef struct _SPCompositeScoreSetKey
{
    SPPtrOrD_t *value;
    SPCompositeCompCtx *compCtx;
    SPScoreSetMembers *members;
} SPCompositeScoreSetKey;

int SPCompositeScoreComp(SPCompositeScoreSetKey a,SPCompositeScoreSetKey b);

KBTREE_INIT(COMPIDX, SPCompositeScoreSetKey, SPCompositeScoreComp);

typedef struct _SPCompositeScoreCont {
    SPCompositeCompCtx *compCtx;
    SPLock lock;
    kbtree_t(COMPIDX) *btree;
} SPCompositeScoreCont;



// typedef khash_t(SCORE) khash_t(LEX);

// KHASH_DECLARE(HASH, spid_t, SPHashValue*);
// KHASH_MAP_INIT_INT64(HASH, SPHashValue*);
KHASH_SET_INIT_INT64(SIDS);
// KHASH_MAP_INIT_INT64(SCORE, SPScore*);
// KHASH_MAP_INIT_INT64(LEX, SPScore*);


KB_TYPE(SCORESET);
typedef kbtree_t(SCORESET) kbtree_t(LEXSET);
typedef kbtree_t(SCORESET) kbtree_t(GEOSET);


KBTREE_INIT(SCORESET, SPScoreSetKey, SPScoreSetComp);
KBTREE_INIT(LEXSET, SPScoreSetKey, SPLexSetComp);
KBTREE_INIT(GEOSET, SPScoreSetKey, SPGeoSetComp);

/*
    start recordset defs
*/
typedef struct _SPFieldData {
    SPPtrOrD_t *iv, *av; //iv = index value, av = actual value (for facets)
    uint16_t ilen, alen;
} SPFieldData;

typedef struct _SPRawDoc {
    size_t oSize;
    // size_t cSize;
    // char *packed;
    char *unpacked;
} SPRawDoc;


// typedef struct _SPRecordStatus {
//     uint8_t exists:1, unindexed:1; 
// } SPRecordStatus;
typedef struct _SPPackCont SPPackCont;

KHASH_DECLARE(FIELDDATA, const char*, SPFieldData);
KHASH_MAP_INIT_STR(FIELDDATA, SPFieldData);

// typedef struct _SPFieldDataMap {
//     SPFieldData data;
//     UT_hash_handle hh;
// } SPFieldDataMap;

typedef struct _SPRecord {
    uint8_t exists; 
    // uint8_t fc;
    // SPFieldDataMap *fields;
    // khash_t(FIELDDATA) *fields;
    SPFieldData *fields;
    // SPPtrOrD_t *sortValues;
    
    SPRawDoc rawDoc;
    char *sid;
    SPPackCont *pc;

    // kson_t *kson;
} SPRecord;


typedef union _SPRecordId {
    spid_t id;
    SPRecord *record;
} SPRecordId;


typedef struct _SPReindexRequest {
    SPRecordId rid;
    khash_t(FIELDDATA) *adds, *deletes;
} SPReindexRequest;


typedef kvec_t(SPRecordId) SPPRecordIdVec;

struct _SPPackCont {
    SPPRecordIdVec records;
    char *bytes;
    size_t cSize, oSize;
    uint8_t packed;
};


KHASH_DECLARE(MSTDOC, const char*, SPRecordId);
KHASH_MAP_INIT_STR(MSTDOC, SPRecordId);

// KHASH_DECLARE(REVIDMAP, spid_t, SPRecord*);
// KHASH_MAP_INIT_INT64(REVIDMAP, SPRecord*);


/*
    start namespace recordset stuff;
*/

typedef struct _SPFieldPH {
    uint8_t fieldType;
    const char *name;
} SPFieldPH;

typedef struct _SPFieldDef SPFieldDef;
typedef struct _SPNamespace SPNamespace;

typedef int (*SPArgsToValues)(RedisModuleCtx *,int,RedisModuleString**, SPFieldData*, SPFieldDef*,  SPNamespace*);

typedef RedisModuleString** (*SPValuesToArgs)(RedisModuleCtx *,SPRecordId, SPFieldData*, SPFieldDef*, int*);

typedef int (*SPRDBFieldData)(RedisModuleCtx*, RedisModuleIO*, SPRecord*, SPFieldDef*, SPNamespace *, int);
// typedef int (*SPReadFieldDataFromRDB)(RedisModuleCtx*, RedisModuleIO*, SPRecord*, SPFieldDef*, SPNamespace *, int);

typedef void (*SPIndexFieldData)(SPRecordId, SPFieldData*, SPFieldDef*, SPNamespace*);

struct _SPFieldDef {
    uint64_t version;
    kvec_t(SPFieldPH) fieldPlaceHolders;
    // int dataVersion;
    uint16_t fieldOrder;
    const char *name;
    SPArgsToValues argTx;
    SPValuesToArgs rewrite;
    SPRDBFieldData dbWrite;
    SPRDBFieldData dbRead;
    SPIndexFieldData addIndex;
    SPIndexFieldData remIndex;
    uint8_t indexType,
        fieldType,
        index,
        prefix,
        suffix,
        fullText,
        argCount;
};

typedef struct _SPTreeCont {
    SPLock lock;
    uint8_t type;
    kbtree_t(SCORESET) *btree;
} SPTreeCont;

typedef struct _SPCompCont {
    SPLock lock;
    SPCompositeCompCtx *compCtx;
    kbtree_t(COMPIDX) *btree;
} SPCompCont;

KHASH_DECLARE(IDTYPE, const char*, SPScoreSetMembers*);
KHASH_MAP_INIT_STR(IDTYPE, SPScoreSetMembers*);

typedef struct _SPIdTypeCont {
    SPLock lock;
    uint8_t type;
    khash_t(IDTYPE) *hash;
} SPIdTypeCont;



#define SPTreeIndexType 0
#define SPCompIndexType 1
#define SPIdIndexType 2

typedef union _SPIndex {
    kbtree_t(SCORESET) *btree;
    kbtree_t(COMPIDX) *compTree;
    khash_t(IDTYPE) *hash;
} SPIndex;

typedef struct _SPIndexCont {
    SPLock lock;
    SPIndex index;
    size_t records;
    SPCompositeCompCtx *compCtx;
    uint8_t type;
} SPIndexCont;



KHASH_DECLARE(FIELDS, const char*, SPFieldDef*);
KHASH_MAP_INIT_STR(FIELDS, SPFieldDef*);

KHASH_DECLARE(INDECES, const char*, SPIndexCont*);
KHASH_MAP_INIT_STR(INDECES, SPIndexCont*);

KHASH_DECLARE(TMPSETS, const char*, khash_t(SIDS)*);
KHASH_MAP_INIT_STR(TMPSETS, khash_t(SIDS)*);



typedef struct _SPTmpSets {
    SPLock lock;
    khash_t(TMPSETS) *sets;
} SPTmpSets;



typedef kvec_t(SPFieldDef*) SPFieldDefVec;
typedef kvec_t(const char *) SPStrVec;
typedef kvec_t(SPPackCont *) SPPackContVec;


typedef struct _SPDeletedRecord {
    SPRecordId rid;
    long long date;
} SPDeletedRecord;

typedef kvec_t(SPDeletedRecord) SPDeletedRecordVec;

typedef struct _SPRecordSet {
    SPLock lock;
    SPLock deleteLock;
    SPLock lzwLock;
    uint16_t fc;
    uint16_t swapFc;
    uint8_t *types;

    uint8_t *swapTypes;
    khash_t(MSTDOC) *docs;
    // khash_t(SIDS) *deleted;
    SPDeletedRecordVec deleted;
    // LZ4_streamDecode_t* decompressStream;
    // LZ4_stream_t* compressStream;
    // char *unpackBuff;
    // char *lzwDict;
    // int lzwDictSize;
    // char *swap;
    pthread_t deleteThread;
    int delrun;
    SPPackContVec packStack;
} SPRecordSet;

KHASH_DECLARE_SET(STR, const char *);
KHASH_SET_INIT_STR(STR);

struct _SPNamespace {
    // const char *name, *recordSetName;
    SPLock lock;
    SPLock indexLock;
    SPLock strLock;
    char **rewrite;
    int rewriteLen;
    const char *name, *defaultLang;
    uint16_t fieldCount;
    uint16_t compositeCount;
    khash_t(FIELDS) *defs;
    khash_t(INDECES) *indeces;
    
    khash_t(STR) *uniq;
    SPStrVec fields, composites;
    SPRecordSet *rs;

    SPTmpSets temps;
};

typedef union _SPPackContId {
    spid_t id;
    SPPackCont *pc;
} SPPackContId;

typedef kvec_t( SPPtrOrD_t* ) SPPerms;

#endif
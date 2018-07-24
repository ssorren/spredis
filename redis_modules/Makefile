# DEBUG = 1
DEBUGFLAGS = -g -ggdb -O3
ifeq ($(DEBUG), 1)
	DEBUGFLAGS = -g -ggdb -Og
endif

# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
# CPPFLAGS =  -Wall $(DEBUGFLAGS) -fPIC -std=gnu99 -D_GNU_SOURCE
CFLAGS =  -Wall -Wno-unused-function $(DEBUGFLAGS) -fPIC -std=c99 
# CC:=$(shell sh -c 'type $(CC) >/dev/null 2>/dev/null && echo $(CC) || echo gcc')
CC:=$(shell sh -c 'type $(CC) >/dev/null 2>/dev/null && echo $(CC) || echo gcc')

# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	CFLAGS += -D_GNU_SOURCE
	SHOBJ_CFLAGS ?=  -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -shared -Bsymbolic -Bsymbolic-functions
else
	CFLAGS += -mmacosx-version-min=10.6 -D_DARWIN_C_SOURCE
	SHOBJ_CFLAGS ?= -dynamic -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup -macosx_version_min 10.6
endif

# ROOT=$(shell pwd)
# Flags for preprocessor
LDFLAGS = -lc

CFLAGS += -I.
TYPEDIR := types
COMDIR := commands
LIBDIR := lib
MODULE_OBJ = spredis.o
MODULE_SO = spredis.so


OFS = 	$(TYPEDIR)/sptempresult.o $(TYPEDIR)/spset.o $(TYPEDIR)/spscore.o $(TYPEDIR)/splexscore.o $(TYPEDIR)/sphash.o $(TYPEDIR)/spscoreset.o\
		$(TYPEDIR)/spexpresolver.o $(TYPEDIR)/spgeo.o $(TYPEDIR)/spcomposite.o $(TYPEDIR)/spdocument.o $(TYPEDIR)/spnamespace.o $(TYPEDIR)/sprecordset.o\
		$(COMDIR)/spfacet.o $(COMDIR)/spfacetrange.o $(COMDIR)/spmcsort.o $(COMDIR)/sprangestore.o $(COMDIR)/spcomprangestore.o $(COMDIR)/spcursor.o\
		
		
		# $(LIBDIR)/threadpool.o
DEPS = 	$(LIBDIR)/kexpr.o $(LIBDIR)/geohash.o $(LIBDIR)/lz4.o $(LIBDIR)/spsortapplicator.o \
		$(LIBDIR)/thpool.o $(LIBDIR)/sp_parallelqueue.o
export

all: $(MODULE_SO)

$(MODULE_SO): $(MODULE_OBJ) $(OFS) $(DEPS) 
	$(LD) $^ -o $@ $(SHOBJ_LDFLAGS) $(LDFLAGS)

clean:
	$(RM) $(MODULE_OBJ) $(MODULE_SO) $(OFS)

depclean:
	$(RM) $(MODULE_OBJ) $(MODULE_SO) $(OFS) $(DEPS)




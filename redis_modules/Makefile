DEBUGFLAGS = -g -ggdb -O2
ifeq ($(DEBUG), 1)
	DEBUGFLAGS = -g -ggdb -O0
endif

# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
# CPPFLAGS =  -Wall $(DEBUGFLAGS) -fPIC -std=gnu99 -D_GNU_SOURCE
CPPFLAGS =  -Wall $(DEBUGFLAGS) -fPIC -std=c99 
CC:=$(shell sh -c 'type $(CC) >/dev/null 2>/dev/null && echo $(CC) || echo gcc')

# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	# CFLAGS +=  -DHASH_FUNCTION=HASH_FNV
	CPPFLAGS += -D_GNU_SOURCE
	SHOBJ_CFLAGS ?=  -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -shared -Bsymbolic -Bsymbolic-functions
else
	CFLAGS += -mmacosx-version-min=10.6
	CPPFLAGS += -D_DARWIN_C_SOURCE
	SHOBJ_CFLAGS ?= -dynamic -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup -macosx_version_min 10.6
endif

# ROOT=$(shell pwd)
# Flags for preprocessor
LDFLAGS = -lc

CPPFLAGS += -I.
TYPEDIR := types
COMDIR := commands
LIBDIR := lib
MODULE_OBJ = spredis.o
MODULE_SO = spredis.so

DEPS = 	$(TYPEDIR)/spstringtype.o $(TYPEDIR)/spdoubletype.o $(TYPEDIR)/sptempresult.o $(TYPEDIR)/spset.o \
		$(COMDIR)/spfacet.o $(COMDIR)/spmcsort.o \
		$(LIBDIR)/thpool.o

export

all: $(MODULE_SO)

$(MODULE_SO): $(MODULE_OBJ) $(DEPS)
	$(LD) $^ -o $@ $(SHOBJ_LDFLAGS) $(LDFLAGS)

clean:
	$(RM) $(MODULE_OBJ) $(MODULE_SO) $(DEPS)
	$(RM) -f print_version
	$(RM) -rf build
	$(MAKE) -C tests clean



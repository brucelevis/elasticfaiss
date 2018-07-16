#-------------------------------------------------------------------------------
# Copyright (c) 2013-2018, yinqiwen <yinqiwen@gmail.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
#   * Redistributions in binary form must reproduce the above copyright
#     notice, this list of conditions and the following disclaimer in the
#     documentation and/or other materials provided with the distribution.
#   * Neither the name of Redis nor the names of its contributors may be used
#     to endorse or promote products derived from this software without
#     specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
# BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
# THE POSSIBILITY OF SUCH DAMAGE.
#-------------------------------------------------------------------------------
#Makefile
#
# Created on: 2013-3-28
#     Author: yinqiwen

CXX=g++
CC=gcc

ARDB_VERSION=0.10.0

uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

LIB_PATH=$(PWD)/../deps

JEMALLOC_VER=5.1.0
JEMALLOC_PATH=${LIB_PATH}/jemalloc-${JEMALLOC_VER}
JEMALLOC_FILE=${JEMALLOC_PATH}.tar.bz2
JEMALLOC_REMOTE_FILE=https://github.com/jemalloc/jemalloc/releases/download/${JEMALLOC_VER}/jemalloc-${JEMALLOC_VER}.tar.bz2
JEMALLOC_LIBA=${JEMALLOC_PATH}/lib/libjemalloc.a

SNAPPY_VER=1.1.4
SNAPPY_PATH=${LIB_PATH}/snappy-${SNAPPY_VER}
SNAPPY_FILE=${SNAPPY_PATH}.tar.gz
SNAPPY_REMOTE_FILE=https://github.com/google/snappy/releases/download/${SNAPPY_VER}/snappy-${SNAPPY_VER}.tar.gz
SNAPPY_LIBA=${SNAPPY_PATH}/.libs/libsnappy.a
DEP_LIBS=${SNAPPY_LIBA}

LEVELDB_VER=1.20
LEVELDB_PATH=${LIB_PATH}/leveldb-${LEVELDB_VER}
LEVELDB_FILE=${LEVELDB_PATH}.tar.gz
LEVELDB_REMOTE_FILE=https://github.com/google/leveldb/archive/v${LEVELDB_VER}.tar.gz
LEVELDB_LIBA=${LEVELDB_PATH}/out-static/libleveldb.a
LEVELDB_INCLUDE=${LEVELDB_PATH}
LEVELDB_LIBPATH=${LEVELDB_PATH}/out-static
DEP_LIBS+= ${LEVELDB_LIBA}

GFLAGS_VER=2.2.1
GFLAGS_PATH=${LIB_PATH}/gflags-${GFLAGS_VER}
GFLAGS_FILE=${GFLAGS_PATH}.tar.gz
GFLAGS_REMOTE_FILE=https://github.com/gflags/gflags/archive/v${GFLAGS_VER}.tar.gz
GFLAGS_LIBA=${GFLAGS_PATH}/build_dir/lib/libgflags.a
GFLAGS_INCLUDE=${GFLAGS_PATH}/build_dir/include
GFLAGS_LIBPATH=${GFLAGS_PATH}/build_dir/lib
DEP_LIBS+= ${GFLAGS_LIBA}

GLOG_VER=0.3.5
GLOG_PATH=${LIB_PATH}/glog-${GLOG_VER}
GLOG_FILE=${GLOG_PATH}.tar.gz
GLOG_REMOTE_FILE=https://github.com/google/glog/archive/v${GLOG_VER}.tar.gz
GLOG_LIBA=${GLOG_PATH}/.libs/libglog.a
GLOG_INCLUDE=${GLOG_PATH}/src
GLOG_LIBPATH=${GLOG_PATH}/.libs
DEP_LIBS+= ${GLOG_LIBA}


PROTOBUF_VER=3.6.0
PROTOBUF_PATH=${LIB_PATH}/protobuf-${PROTOBUF_VER}
PROTOBUF_FILE=${PROTOBUF_PATH}.tar.gz
PROTOBUF_REMOTE_FILE=https://github.com/google/protobuf/releases/download/v${PROTOBUF_VER}/protobuf-cpp-${PROTOBUF_VER}.tar.gz
PROTOBUF_LIBA=${PROTOBUF_PATH}/src/.libs/libprotobuf.a
PROTOBUF_INCLUDE=${PROTOBUF_PATH}/src
PROTOBUF_LIBPATH=${PROTOBUF_PATH}/src/.libs/
PROTOBUF_COMPILER=${PROTOBUF_PATH}/src/protoc
DEP_LIBS+= ${PROTOBUF_LIBA}
export PATH := ${PROTOBUF_INCLUDE}:$(PATH)


BRPC_VER=master
BRPC_PATH=${LIB_PATH}/brpc-${BRPC_VER}
BRPC_FILE=${BRPC_PATH}.zip
BRPC_REMOTE_FILE=https://github.com/brpc/brpc/archive/${BRPC_VER}.zip
BRPC_LIBA=${BRPC_PATH}/libbrpc.a
BRPC_INCLUDE=${BRPC_PATH}/src
BRPC_INCS=${LEVELDB_INCLUDE} ${GFLAGS_INCLUDE} ${GLOG_INCLUDE} ${PROTOBUF_INCLUDE} /usr/include /usr/local/include 
BRPC_LIBS=${LEVELDB_LIBPATH} ${GFLAGS_LIBPATH} ${GLOG_LIBPATH} ${PROTOBUF_LIBPATH} /usr/lib /usr/local/lib 
DEP_LIBS+= ${BRPC_LIBA}

BRAFT_VER=master
BRAFT_PATH=${LIB_PATH}/braft-${BRAFT_VER}
BRAFT_FILE=${BRAFT_PATH}.zip
BRAFT_REMOTE_FILE=https://github.com/brpc/braft/archive/${BRAFT_VER}.zip
BRAFT_LIBA=${BRAFT_PATH}/build/output/lib/libbraft.a
BRAFT_INCLUDE=${BRAFT_PATH}/src
BRAFT_ENVS=-DLEVELDB_INCLUDE_PATH=${LEVELDB_INCLUDE} -DLEVELDB_LIB=${LEVELDB_LIBA}
BRAFT_ENVS+= -DGFLAGS_INCLUDE_PATH=${GFLAGS_INCLUDE} -DGFLAGS_LIB=${GFLAGS_LIBA} 
BRAFT_ENVS+=  -DGLOG_INCLUDE_PATH=${GLOG_INCLUDE} -DGLOG_LIB=${GLOG_LIBA} 
BRAFT_ENVS+=  -DBRPC_INCLUDE_PATH=${BRPC_PATH}/src -DBRPC_LIB=${BRPC_LIBA} 
BRAFT_ENVS+=  -DBRPC_WITH_GLOG=ON 
DEP_LIBS+= ${BRAFT_LIBA}

FAISS_VER=1.3.0
FAISS_PATH=${LIB_PATH}/faiss-${FAISS_VER}
FAISS_FILE=${FAISS_PATH}.tar.gz
FAISS_REMOTE_FILE=https://github.com/facebookresearch/faiss/archive/v${FAISS_VER}.tar.gz
FAISS_LIBA=${FAISS_PATH}/libfaiss.a
FAISS_INCLUDE=${FAISS_PATH}/
#DEP_LIBS+= ${FAISS_LIBA}


OPTIMIZATION?=-O2
OPT=$(OPTIMIZATION)

CXXFLAGS=-Wall -std=c++11 -g ${OPT}  -fPIC -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS -DBRPC_WITH_GLOG=1 -DGFLAGS_NS=google  -DARDB_VERSION='"${ARDB_VERSION}"'
CCFLAGS=-Wall -std=gnu99 ${OPT} -fPIC -pedantic -g -D__STDC_FORMAT_MACROS -DBRPC_WITH_GLOG=0 -DGFLAGS_NS=google  -DARDB_VERSION='"${ARDB_VERSION}"'
LDFLAGS=-g

SNAPPY_CXXFLAGS=-g $(OPT)
LEVELDB_OPT=-g $(OPT)

LUA_CFLAGS+= $(OPT) -Wall -DLUA_ANSI $(CCFLAGS)
LUA_LDFLAGS+= $(LDFLAGS)

INCS=-I./ -I${GFLAGS_INCLUDE} -I${GLOG_INCLUDE} -I${LEVELDB_INCLUDE} -I${BRPC_INCLUDE} -I${BRAFT_INCLUDE} -I${BRAFT_PATH}/build -I${FAISS_INCLUDE} -I${PROTOBUF_INCLUDE}

ARDB_CC=$(QUIET_CC)$(CC) $(CCFLAGS)
#ARDB_CXX=$(QUIET_CXX)$(CXX) $(CXXFLAGS)
ARDB_CXX=$(CXX) $(CXXFLAGS)
#ARDB_LD=$(QUIET_LINK)$(CXX) $(LDFLAGS)
ARDB_LD=$(CXX) $(LDFLAGS)
ARDB_AR=$(QUIET_LINK) ar rcs

CCCOLOR="\033[34m"
LINKCOLOR="\033[34;1m"
SRCCOLOR="\033[33m"
BINCOLOR="\033[37;1m"
MAKECOLOR="\033[32;1m"
ENDCOLOR="\033[0m"

ifndef V
QUIET_CC = @printf '    %b %b\n' $(CCCOLOR)CC$(ENDCOLOR) $(SRCCOLOR)$@$(ENDCOLOR) 1>&2;
QUIET_CXX = @printf '    %b %b\n' $(CCCOLOR)CXX$(ENDCOLOR) $(SRCCOLOR)$@$(ENDCOLOR) 1>&2;
QUIET_LINK = @printf '    %b %b\n' $(LINKCOLOR)LINK$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR) 1>&2;
QUIET_INSTALL = @printf '    %b %b\n' $(LINKCOLOR)INSTALL$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR) 1>&2;
endif


ifeq ($(MALLOC),libc)
#do nothing
else
MALLOC_LIBA=${JEMALLOC_LIBA}
DEP_LIBS+= ${JEMALLOC_LIBA}
endif

storage_engine?=rocksdb

LIBS= ${DEP_LIBS_PREFIX} ${DEP_LIBS} ${DEP_LIBS_POSFIX} ${MALLOC_LIBA} -lpthread -lssl -lcrypto -ldl -lz 

# Default allocator
ifeq ($(uname_S),Linux)
	MALLOC=jemalloc
	CXXFLAGS+=-DCORO_ASM
	CCFLAGS+=-DCORO_ASM
	LIBS+=-lrt
	CXXFLAGS+=-Werror
	DEP_LIBS_PREFIX=-Wl,--start-group
	DEP_LIBS_POSFIX=-Wl,--end-group
endif

ifeq ($(uname_S),Darwin)
	MALLOC=jemalloc
	BRAFT_ENVS+=  -DCMAKE_CXX_FLAGS="-DNO_CLOCK_GETTIME_IN_MAC=1"
	CXXFLAGS+=-DNO_CLOCK_GETTIME_IN_MAC=1
	INCS+=  -I/usr/local/opt/openssl/include
	DEP_LIBS_PREFIX=-L/usr/local/opt/openssl/lib
	LIBS+= -framework CoreFoundation -framework CoreGraphics -framework CoreData -framework CoreText -framework Security -framework Foundation -Wl,-U,_MallocExtension_ReleaseFreeMemory -Wl,-U,_ProfilerStart -Wl,-U,_ProfilerStop -Wl,-U,_RegisterThriftProtocol
endif

%.o : %.cpp
	${ARDB_CXX} ${INCS} -c $< -o $@
	
%.o : %.cc
	${ARDB_CXX} ${INCS} -c $< -o $@

%.o : %.c
	${ARDB_CC} ${INCS} -c $< -o $@


COMMON_VPATH=counter
COMMON_CPPFILES := $(foreach dir, $(COMMON_VPATH), $(wildcard $(dir)/*.cpp))
COMMON_CFILES := $(foreach dir, $(COMMON_VPATH), $(wildcard $(dir)/*.c))
COMMON_OBJECTS := $(patsubst %.cpp, %.o, $(COMMON_CPPFILES)) $(patsubst %.c, %.o, $(COMMON_CFILES))

CORE_OBJECTS := $(COMMON_OBJECTS)

MASTEROBJ := master/master.o  proto/master.pb.o main.o
CLIENTOBJ := counter/client.o  counter/counter.pb.o

#

#$(CORE_OBJECTS) $(SERVEROBJ) $(TESTOBJ): | $(ZOOKEEPER_PATH) $(STORAGE_ENGINE_PATH)

all:  master 

$(STORAGE_ENGINE_OBJ): $(STORAGE_ENGINE)

$(DIST_LIB): $(STORAGE_ENGINE_OBJ) $(CORE_OBJECTS)
	${ARDB_CXX} -shared -o $@ $^

$(DIST_LIBA):$(STORAGE_ENGINE_OBJ) $(CORE_OBJECTS)
	$(ARDB_AR) $@ $^

lib: $(DEP_LIBS) 

master: ${MASTEROBJ}
	${ARDB_LD} -o tserver $(MASTEROBJ) $(LIBS)

client: ${CLIENTOBJ}
	${ARDB_LD} -o tclient $(CLIENTOBJ) $(LIBS)

.PHONY: jemalloc
jemalloc: $(JEMALLOC_LIBA)
$(JEMALLOC_LIBA): $(JEMALLOC_PATH)
	echo ">>>>> Building JEMALLOC" && \
	cd ${JEMALLOC_PATH} && \
	./configure  && \
	$(MAKE) build_lib_static && \
	echo "<<<<< Done building JEMALLOC"

$(JEMALLOC_PATH): ${JEMALLOC_FILE}
	echo ">>>>> Unpacking JEMALLOC" && \
	cd ${LIB_PATH} && \
	tar jxf ${JEMALLOC_FILE} && \
	echo "<<<<< Done unpacking JEMALLOC"

${JEMALLOC_FILE}:
	echo ">>>>> Dowloading JEMALLOC" && \
	cd ${LIB_PATH} && \
	wget -O ${JEMALLOC_FILE} ${JEMALLOC_REMOTE_FILE} && \
	echo "<<<<< Done dowloading JEMALLOC"

.PHONY: snappy
snappy: $(SNAPPY_LIBA)
$(SNAPPY_LIBA):$(SNAPPY_PATH)
	echo ">>>>> Building SNAPPY" && \
	cd ${SNAPPY_PATH} && \
	./configure && $(MAKE) && \
	echo "<<<<< Done building SNAPPY"

$(SNAPPY_PATH): $(SNAPPY_FILE)
	cd ${LIB_PATH} && \
	tar zxf ${SNAPPY_FILE}

${SNAPPY_FILE}:
	echo ">>>>> Dowloading Snappy" && \
	cd ${LIB_PATH} && \
	wget -O ${SNAPPY_FILE} ${SNAPPY_REMOTE_FILE} && \
	echo "<<<<< Done dowloading Snappy"

.PHONY: leveldb
leveldb: $(LEVELDB_LIBA)
$(LEVELDB_LIBA): $(LEVELDB_PATH)
	echo ">>>>> Building leveldb" && \
	cd ${LEVELDB_PATH} && \
	OPT="$(LEVELDB_OPT)" \
	CXXFLAGS="-I${SNAPPY_PATH} " \
		CFLAGS="-I${SNAPPY_PATH}" \
		LDFLAGS="${SNAPPY_PATH}/.libs" \
		$(MAKE) out-static/libleveldb.a && \
	touch $(LEVELDB_LIBA) && \
	echo "<<<<< Done building leveldb"

$(LEVELDB_PATH): ${LEVELDB_FILE}
	echo ">>>>> Unpacking leveldb" && \
	cd ${LIB_PATH} && \
	tar zxf ${LEVELDB_FILE} && \
	echo "<<<<< Done unpacking leveldb"

${LEVELDB_FILE}:
	echo ">>>>> Dowloading leveldb" && \
	cd ${LIB_PATH} && \
	wget -O ${LEVELDB_FILE} ${LEVELDB_REMOTE_FILE} && \
	echo "<<<<< Done dowloading leveldb"
	
.PHONY: protobuf
protobuf: $(PROTOBUF_LIBA)
$(PROTOBUF_LIBA): $(PROTOBUF_PATH)
	echo ">>>>> Building protobuf" && \
	cd ${PROTOBUF_PATH} && \
	./configure --disable-shared && $(MAKE)  && \
	echo "<<<<< Done building protobuf"

$(PROTOBUF_PATH): ${PROTOBUF_FILE}
	echo ">>>>> Unpacking leveldb" && \
	cd ${LIB_PATH} && \
	tar zxf ${PROTOBUF_FILE} && \
	echo "<<<<< Done unpacking protobuf"

${PROTOBUF_FILE}:
	echo ">>>>> Dowloading protobuf" && \
	cd ${LIB_PATH} && \
	wget -O ${PROTOBUF_FILE} ${PROTOBUF_REMOTE_FILE} && \
	echo "<<<<< Done dowloading protobuf"


.PHONY: gflags
gflags: $(GFLAGS_LIBA)
$(GFLAGS_LIBA):$(GFLAGS_PATH)
	echo ">>>>> Building gflags" && \
	cd ${GFLAGS_PATH} && mkdir build_dir && \
	cd build_dir  && \
	CXXFLAGS="-fPIC" cmake .. &&  $(MAKE) && \
	touch $(GFLAGS_LIBA) && \
	echo "<<<<< Done building gflags"

$(GFLAGS_PATH): $(GFLAGS_FILE)
	cd ${LIB_PATH} && \
	tar zxf ${GFLAGS_FILE}

${GFLAGS_FILE}:
	echo ">>>>> Dowloading gflags" && \
	cd ${LIB_PATH} && \
	wget -O ${GFLAGS_FILE} ${GFLAGS_REMOTE_FILE} && \
	echo "<<<<< Done dowloading gflags"

.PHONY: glog
glog: $(GLOG_LIBA)
$(GLOG_LIBA):$(GLOG_PATH)
	echo ">>>>> Building glog" && \
	cd ${GLOG_PATH} && \
	./configure --with-gflags=${GFLAGS_PATH}/build && $(MAKE)  && \
	touch $(GLOG_LIBA) && \
	echo "<<<<< Done building glog"

$(GLOG_PATH): $(GLOG_FILE)
	cd ${LIB_PATH} && \
	tar zxf ${GLOG_FILE}

${GLOG_FILE}:
	echo ">>>>> Dowloading glog" && \
	cd ${LIB_PATH} && \
	wget -O ${GLOG_FILE} ${GLOG_REMOTE_FILE} && \
	echo "<<<<< Done dowloading glog"
	
.PHONY: brpc
brpc: $(BRPC_LIBA)
$(BRPC_LIBA): ${BRPC_PATH}
	echo ">>>>> Building brpc" && \
	cd ${LIB_PATH} && \
	cd ${BRPC_PATH}  && \
	sh config_brpc.sh --headers="${BRPC_INCS}" --libs="${BRPC_LIBS}"  && \
	$(MAKE) libbrpc.a && \
	echo "<<<<< Done building brpc"

${BRPC_PATH}: ${BRPC_FILE}
	echo ">>>>> Unpacking brpc" && \
	cd ${LIB_PATH} && \
	unzip -o ${BRPC_FILE} && \
	echo "<<<<< Done unpacking brpc"

${BRPC_FILE}:
	echo ">>>>> Dowloading brpc" && \
	cd ${LIB_PATH} && \
	wget -O ${BRPC_FILE} ${BRPC_REMOTE_FILE} && \
	echo "<<<<< Done dowloading brpc"

.PHONY: braft
braft: $(BRAFT_LIBA)
$(BRAFT_LIBA): ${BRAFT_PATH}
	echo ">>>>> Building braft" && \
	cd ${LIB_PATH} && \
	cd ${BRAFT_PATH}  && \
    rm -rf build && mkdir build && cd build  && \
	cmake ${BRAFT_ENVS} .. &&  $(MAKE) braft-static &&  $(MAKE) braft_cli && \
	echo "<<<<< Done building braft"

${BRAFT_PATH}: ${BRAFT_FILE}
	echo ">>>>> Unpacking braft" && \
	cd ${LIB_PATH} && \
	unzip -o ${BRAFT_FILE} && \
	echo "<<<<< Done unpacking braft"

${BRAFT_FILE}:
	echo ">>>>> Dowloading braft" && \
	cd ${LIB_PATH} && \
	wget -O ${BRAFT_FILE} ${BRAFT_REMOTE_FILE} && \
	echo "<<<<< Done dowloading braft"


.PHONY: faiss
faiss: $(FAISS_LIBA)
$(FAISS_LIBA):$(FAISS_PATH)
	echo ">>>>> Building faiss" && \
	cd ${FAISS_PATH} && \
	./configure && $(MAKE) && \
	echo "<<<<< Done building faiss"

$(FAISS_PATH): $(FAISS_FILE)
	cd ${LIB_PATH} && \
	tar zxf ${FAISS_FILE}

${FAISS_FILE}:
	echo ">>>>> Dowloading faiss" && \
	cd ${LIB_PATH} && \
	wget -O ${FAISS_FILE} ${FAISS_REMOTE_FILE} && \
	echo "<<<<< Done dowloading faiss"

.PHONY: deps
deps: jemalloc leveldb gflags glog brpc braft protobuf gen_proto

.PHONY: gen_proto
gen_proto:
	${PROTOBUF_COMPILER} --cpp_out=./ ./proto/*.proto


clean_db_obj:
	rm -f db/engine_factory.o

clean_test:
	rm -f ${TESTOBJ};rm -f ardb-test


noopt:
	$(MAKE) OPT="-O0"


dist:clean all
	rm -rf ardb-${ARDB_VERSION};mkdir -p ardb-${ARDB_VERSION}/bin ardb-${ARDB_VERSION}/conf ardb-${ARDB_VERSION}/logs ardb-${ARDB_VERSION}/data ardb-${ARDB_VERSION}/repl ardb-${ARDB_VERSION}/backup; \
	cp ardb-server ardb-${ARDB_VERSION}/bin; cp ardb-test ardb-${ARDB_VERSION}/bin; cp ardb-repair ardb-${ARDB_VERSION}/bin; cp ../ardb.conf ardb-${ARDB_VERSION}/conf; \
	tar czvf ardb-bin-${ARDB_VERSION}.tar.gz ardb-${ARDB_VERSION}; rm -rf ardb-${ARDB_VERSION};

clean:
	rm -f  ${CORE_OBJECTS} $(SERVEROBJ) ${STORAGE_ENGINE_ALL_OBJ} ${TESTOBJ} ${REPAIR_TOOL_OBJ} ${DIST_LIBA} ${DIST_LIB} \
	       ardb-test  ardb-server ardb-repair

clobber: clean_deps clean

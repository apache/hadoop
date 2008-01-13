# Makefile for C++ Hbase Thrift DemoClient
#
# NOTE: run 'thrift -cpp Hbase.thrift' first

THRIFT_DIR = /usr/local/include/thrift
LIB_DIR = /usr/local/lib

GEN_SRC = ./gen-cpp/Hbase.cpp \
	  ./gen-cpp/Hbase_types.cpp \
	  ./gen-cpp/Hbase_constants.cpp

default: DemoClient

DemoClient: DemoClient.cpp
	g++ -o DemoClient -I${THRIFT_DIR}  -I./gen-cpp -L${LIB_DIR} -lthrift DemoClient.cpp ${GEN_SRC}

clean:
	rm -rf DemoClient

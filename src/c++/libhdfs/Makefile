#
# Copyright 2005 The Apache Software Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Note: This makefile depends on 4 environment variables to funtion correctly:
# a) JAVA_HOME
# b) OS_NAME
# c) OS_ARCH
# d) LIBHDFS_BUILD_DIR
# All these are passed by build.xml.
#

CC = gcc
LD = gcc
CFLAGS =  -g -Wall -O2 -fPIC
LDFLAGS = -L$(JAVA_HOME)/jre/lib/$(OS_ARCH)/server -ljvm -shared -m32 -Wl,-x 
PLATFORM = $(shell echo $$OS_NAME | tr [A-Z] [a-z])
CPPFLAGS = -m32 -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/$(PLATFORM)

LIB_NAME = hdfs
SO_TARGET = $(LIBHDFS_BUILD_DIR)/lib$(LIB_NAME).so.$(SHLIB_VERSION)
SO = $(LIBHDFS_BUILD_DIR)/lib$(LIB_NAME).so

RM = rm -rf
LINK = ln -sf
DOXYGEN = doxygen

CSRC = \
	hdfs.c \
	hdfsJniHelper.c \
	$(NONE)

COBJS = $(addprefix $(LIBHDFS_BUILD_DIR)/,$(patsubst %,%.o,$(basename $(CSRC))))

HDFS_TEST = $(LIBHDFS_BUILD_DIR)/hdfs_test
HDFS_READ_TEST = $(LIBHDFS_BUILD_DIR)/hdfs_read
HDFS_WRITE_TEST = $(LIBHDFS_BUILD_DIR)/hdfs_write

all: $(SO_TARGET) $(HDFS_TEST) $(HDFS_READ_TEST) $(HDFS_WRITE_TEST)

$(SO_TARGET): $(COBJS)
	$(LD) $(LDFLAGS) -o $(SO_TARGET) -Wl,-soname,$(SO_TARGET) $(COBJS) \
	&& $(LINK) $(SO_TARGET) $(SO)
	
$(LIBHDFS_BUILD_DIR)/%.o: %.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

$(HDFS_TEST): hdfs_test.c
	$(CC) $(CPPFLAGS) $< -L$(LIBHDFS_BUILD_DIR) -l$(LIB_NAME) -o $@

$(HDFS_READ_TEST): hdfs_read.c
	$(CC) $(CPPFLAGS) $< -Wl,-rpath,. -L$(LIBHDFS_BUILD_DIR) -l$(LIB_NAME) -o $@ 

$(HDFS_WRITE_TEST): hdfs_write.c
	$(CC) $(CPPFLAGS) $< -Wl,-rpath,. -L$(LIBHDFS_BUILD_DIR) -l$(LIB_NAME) -o $@

clean:
	$(RM) $(LIBHDFS_BUILD_DIR)/* 

doc:
	$(DOXYGEN) docs/Doxyfile

test: $(HDFS_TEST)
	./tests/test-libhdfs.sh	

# vim: sw=4: ts=4: noet


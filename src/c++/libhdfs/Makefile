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
CFLAGS =  -m32 -g -W -fPIC
LDFLAGS = -L$(JAVA_HOME)/jre/lib/$(OS_ARCH)/server -ljvm -m32 -shared -Wl,-x 
PLATFORM = `echo $$OS_NAME | tr [A-Z] [a-z]`
LINC = -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/$(PLATFORM)
HDFS_TEST = hdfs_test
HDFS_READ_TEST = hdfs_read
HDFS_WRITE_TEST = hdfs_write
LIB_NAME = hdfs
SO_TARGET = lib$(LIB_NAME).so.$(SHLIB_VERSION)
SO = lib$(LIB_NAME).so
OBJS = hdfs.o
RM = rm -rf
LINK = ln -sf
DOXYGEN = doxygen

all: $(SO_TARGET) $(HDFS_TEST) $(HDFS_READ_TEST) $(HDFS_WRITE_TEST)

$(SO_TARGET): $(OBJS)
	$(LD) $(LDFLAGS) -o $(SO_TARGET) -Wl,-soname,$(SO_TARGET) $(OBJS) \
	&& $(LINK) $(SO_TARGET) $(SO) \
	&& cp ./$(SO_TARGET) $(LIBHDFS_BUILD_DIR)/ \
	&& $(LINK) $(LIBHDFS_BUILD_DIR)/$(SO_TARGET) $(LIBHDFS_BUILD_DIR)/$(SO)

$(OBJS): hdfs.c
	$(CC) $(CFLAGS) $(LINC) -c hdfs.c

$(HDFS_TEST): hdfs_test.c
	$(CC) $(HDFS_TEST).c $(LINC) -L. -l$(LIB_NAME) -m32 -o $(HDFS_TEST) \
	&& cp $(HDFS_TEST) $(LIBHDFS_BUILD_DIR)/

$(HDFS_READ_TEST): hdfs_read.c
	$(CC) $(HDFS_READ_TEST).c $(LINC) -m32 -Wl,-rpath,. -L. -l$(LIB_NAME) -o $(HDFS_READ_TEST) \
	&& cp $(HDFS_READ_TEST) $(LIBHDFS_BUILD_DIR)/ 

$(HDFS_WRITE_TEST): hdfs_write.c
	$(CC) $(HDFS_WRITE_TEST).c $(LINC) -m32 -Wl,-rpath,. -L. -l$(LIB_NAME) -o $(HDFS_WRITE_TEST) \
	&& cp $(HDFS_WRITE_TEST) $(LIBHDFS_BUILD_DIR)/ 

clean:
	$(RM) *.o *.so* $(HDFS_TEST) $(HDFS_READ_TEST) $(HDFS_WRITE_TEST) \
	$(LIBHDFS_BUILD_DIR)/* ./docs/api 

doc:
	$(DOXYGEN) docs/Doxyfile

test: $(HDFS_TEST)
	./tests/test-libhdfs.sh	

# vim: sw=4: ts=4: noet

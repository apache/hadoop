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
# Note: This makefile depends on 3 environment variables to funtion correctly:
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
PLATFORM = `echo $$OS_NAME | /usr/bin/tr [A-Z] [a-z]`
LINC = -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/$(PLATFORM)
HDFS_TESTS = hdfs_test
HDFS_READ_TEST = hdfs_read
HDFS_WRITE_TEST = hdfs_write
LIB_NAME = hdfs
OBJS = hdfs.o
RM = /bin/rm -f
LINK = ln -sf

all: $(LIB_NAME) $(HDFS_TESTS) $(HDFS_READ_TEST) $(HDFS_WRITE_TEST) 

$(LIB_NAME): $(OBJS)
	$(LD) $(LDFLAGS) -o $(LIBHDFS_BUILD_DIR)/lib$(LIB_NAME).so.$(SHLIB_VERSION) -Wl,-soname,lib$(LIB_NAME).so.$(SHLIB_VERSION) $(OBJS) \
	&& $(LINK) $(LIBHDFS_BUILD_DIR)/lib$(LIB_NAME).so.$(SHLIB_VERSION) $(LIBHDFS_BUILD_DIR)/lib$(LIB_NAME).so

$(OBJS): hdfs.c
	$(CC) $(CFLAGS) $(LINC) -c hdfs.c

$(HDFS_TESTS): hdfs_test.c
	$(CC) $(HDFS_TESTS).c $(LINC) -L$(LIBHDFS_BUILD_DIR) -l$(LIB_NAME) -m32 -o $(LIBHDFS_BUILD_DIR)/$(HDFS_TESTS)

$(HDFS_READ_TEST): hdfs_read.c
	$(CC) $(HDFS_READ_TEST).c $(LINC) -m32 -Wl,-rpath,. -L$(LIBHDFS_BUILD_DIR) -l$(LIB_NAME) -o $(LIBHDFS_BUILD_DIR)/$(HDFS_READ_TEST)

$(HDFS_WRITE_TEST): hdfs_write.c
	$(CC) $(HDFS_WRITE_TEST).c $(LINC) -m32 -Wl,-rpath,. -L$(LIBHDFS_BUILD_DIR) -l$(LIB_NAME) -o $(LIBHDFS_BUILD_DIR)/$(HDFS_WRITE_TEST)

clean:
	$(RM) $(LIBHDFS_BUILD_DIR)/*.o $(LIBHDFS_BUILD_DIR)/*.so* \
	$(LIBHDFS_BUILD_DIR)/$(HDFS_TESTS) $(LIBHDFS_BUILD_DIR)/$(HDFS_READ_TEST) $(LIBHDFS_BUILD_DIR)/$(HDFS_WRITE_TEST)


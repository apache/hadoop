#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
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
# Notes: 
# 1. This makefile is designed to do the actual builds in $(HADOOP_HOME)/build/native/${os.name}-${os-arch}.
# 2. This makefile depends on the following environment variables to function correctly:
#    * HADOOP_NATIVE_SRCDIR 
#    * JAVA_HOME
#    * JVM_DATA_MODEL
#    * OS_NAME
#    * OS_ARCH 
#    All these are setup by build.xml. 
#

# Export $(PLATFORM) to prevent proliferation of sub-shells
export PLATFORM = $(shell echo $$OS_NAME | tr [A-Z] [a-z])

AM_CPPFLAGS = @JNI_CPPFLAGS@ -I$(HADOOP_NATIVE_SRCDIR)/src \
              -Isrc/org/apache/hadoop/io/compress/zlib \
              -Isrc/org/apache/hadoop/io/nativeio \
              -Isrc/org/apache/hadoop/security
AM_LDFLAGS = @JNI_LDFLAGS@ -m$(JVM_DATA_MODEL)
AM_CFLAGS = -g -Wall -fPIC -O2 -m$(JVM_DATA_MODEL)

lib_LTLIBRARIES = libhadoop.la
libhadoop_la_SOURCES = src/org/apache/hadoop/io/compress/zlib/ZlibCompressor.c \
                       src/org/apache/hadoop/io/compress/zlib/ZlibDecompressor.c \
                       src/org/apache/hadoop/security/getGroup.c \
                       src/org/apache/hadoop/security/JniBasedUnixGroupsMapping.c \
                       src/org/apache/hadoop/security/JniBasedUnixGroupsNetgroupMapping.c \
                       src/org/apache/hadoop/io/nativeio/file_descriptor.c \
                       src/org/apache/hadoop/io/nativeio/errno_enum.c \
                       src/org/apache/hadoop/io/nativeio/NativeIO.c
libhadoop_la_LDFLAGS = -version-info 1:0:0
libhadoop_la_LIBADD = -ldl -ljvm

#
#vim: sw=4: ts=4: noet
#

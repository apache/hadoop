# Copyright 2008 The Apache Software Foundation
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

# Makefile for C++ Hbase Thrift DemoClient
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

#!/bin/sh

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

###Define variables###

#This variable defines the name of the jsvc tar file.
#It should be modified if a different version of jsvc is needed.
JSVC_SRC_DIR=commons-daemon-1.0.2-src
JSVC_SRC_TAR_FILE=${JSVC_SRC_DIR}.tar.gz

#This variable defines the link where the jsvc source tar is located.
JSVC_SRC_TAR_LOCATION=http://www.apache.org/dist/commons/daemon/source/${JSVC_SRC_TAR_FILE}

JSVC_SRC_CODE_DIR=src/native/unix
JSVC_EXECUTABLE=jsvc

###Download and untar###

wget --no-check-certificate $JSVC_SRC_TAR_LOCATION
tar zxf $JSVC_SRC_TAR_FILE

###Now build###

cd $JSVC_SRC_DIR/$JSVC_SRC_CODE_DIR
sh support/buildconf.sh
./configure 
make clean
make
cd -
cp $JSVC_SRC_DIR/$JSVC_SRC_CODE_DIR/$JSVC_EXECUTABLE .

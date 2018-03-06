#!/usr/bin/env bash
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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
set -e
mkdir -p build
if [ ! -d "$DIR/build/apache-rat-0.12" ]; then
   wget http://xenia.sote.hu/ftp/mirrors/www.apache.org/creadur/apache-rat-0.12/apache-rat-0.12-bin.tar.gz -O $DIR/build/apache-rat.tar.gz
	cd $DIR/build
	tar zvxf apache-rat.tar.gz
	cd -
fi
java -jar $DIR/build/apache-rat-0.12/apache-rat-0.12.jar $DIR -e public -e apache-rat-0.12 -e .git -e .gitignore
docker build -t apache/hadoop-runner .

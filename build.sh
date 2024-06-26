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

set -eu

mkdir -p build

rat_version=0.16.1

if [ ! -d "$DIR/build/apache-rat-${rat_version}" ]; then
  url="https://dlcdn.apache.org/creadur/apache-rat-${rat_version}/apache-rat-${rat_version}-bin.tar.gz"
  output="$DIR/build/apache-rat.tar.gz"
  if type wget 2> /dev/null; then
    wget -O "$output" "$url"
  elif type curl 2> /dev/null; then
    curl -LSs -o "$output" "$url"
  else
    exit 1
  fi
  cd $DIR/build
  tar zvxf apache-rat.tar.gz
  cd -
fi

java -jar $DIR/build/apache-rat-${rat_version}/apache-rat-${rat_version}.jar $DIR -e .dockerignore -e public -e apache-rat-${rat_version} -e .git -e .gitignore

if [[ $# -ge 1 ]]; then
  for v in "$@"; do
    docker build --progress plain --build-arg "JAVA_VERSION=$v" -t apache/hadoop-runner:jdk${v}-dev .
  done
else
  docker build --progress plain -t apache/hadoop-runner:dev .
fi

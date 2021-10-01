#!/usr/bin/env bash

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

if [ $# -lt 1 ]; then
  echo "ERROR: No platform specified, please specify one"
  exit 1
fi

chmod a+x pkg-resolver/*.sh pkg-resolver/*.py
chmod a+r pkg-resolver/*.json

if [ "$1" == "debian:10" ]; then
  apt-get -q update
  apt-get -q install -y --no-install-recommends python3 \
    python3-pip \
    python3-pkg-resources \
    python3-setuptools \
    python3-wheel
  pip3 install pylint==2.6.0 python-dateutil==2.8.1
else
  # Need to add the code for the rest of the platforms - HADOOP-17920
  echo "ERROR: The given platform $1 is not yet supported or is invalid"
  exit 1
fi

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
  echo "ERROR: Need at least 1 argument, $# were provided"
  exit 1
fi

pkg-resolver/check_platform.py "$1"
if [ $? -eq 1 ]; then
  echo "ERROR: Unsupported platform $1"
  exit 1
fi

default_version="1.72.0"
version_to_install=$default_version
if [ -n "$2" ]; then
  version_to_install="$2"
fi

if [ "$version_to_install" != "1.72.0" ]; then
  echo "WARN: Don't know how to install version $version_to_install, installing the default version $default_version instead"
  version_to_install=$default_version
fi

if [ "$version_to_install" == "1.72.0" ]; then
  # hadolint ignore=DL3003
  mkdir -p /opt/boost-library &&
    curl -L https://sourceforge.net/projects/boost/files/boost/1.72.0/boost_1_72_0.tar.bz2/download >boost_1_72_0.tar.bz2 &&
    mv boost_1_72_0.tar.bz2 /opt/boost-library &&
    cd /opt/boost-library &&
    tar --bzip2 -xf boost_1_72_0.tar.bz2 &&
    cd /opt/boost-library/boost_1_72_0 &&
    ./bootstrap.sh --prefix=/usr/ &&
    ./b2 --without-python install &&
    cd /root &&
    rm -rf /opt/boost-library
else
  echo "ERROR: Don't know how to install version $version_to_install"
  exit 1
fi

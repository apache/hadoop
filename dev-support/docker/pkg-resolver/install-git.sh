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

default_version="2.9.5"
version_to_install=$default_version
if [ -n "$2" ]; then
  version_to_install="$2"
fi

if [ "$version_to_install" != "2.9.5" ]; then
  echo "WARN: Don't know how to install version $version_to_install, installing the default version $default_version instead"
  version_to_install=$default_version
fi

if [ "$version_to_install" == "2.9.5" ]; then
  # hadolint ignore=DL3003
  mkdir -p /tmp/git /opt/git &&
    curl -L -s -S https://mirrors.edge.kernel.org/pub/software/scm/git/git-2.9.5.tar.gz >/tmp/git/git-2.9.5.tar.gz &&
    tar xzf /tmp/git/git-2.9.5.tar.gz --strip-components 1 -C /opt/git &&
    cd /opt/git || exit &&
    make configure &&
    ./configure --prefix=/usr/local &&
    make "-j$(nproc)" &&
    make install &&
    cd /root || exit
else
  echo "ERROR: Don't know how to install version $version_to_install"
  exit 1
fi

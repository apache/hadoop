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

curl -L -s -S \
  https://github.com/hadolint/hadolint/releases/download/v1.11.1/hadolint-Linux-x86_64 \
  -o /bin/hadolint &&
  chmod a+rx /bin/hadolint &&
  shasum -a 512 /bin/hadolint |
  awk '$1!="734e37c1f6619cbbd86b9b249e69c9af8ee1ea87a2b1ff71dccda412e9dac35e63425225a95d71572091a3f0a11e9a04c2fc25d9e91b840530c26af32b9891ca" {exit(1)}'

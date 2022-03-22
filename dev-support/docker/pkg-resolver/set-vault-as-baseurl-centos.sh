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

if [ "$1" == "centos:7" ] || [ "$1" == "centos:8" ]; then
  cd /etc/yum.repos.d/ || exit &&
    sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-* &&
    sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-* &&
    yum update -y &&
    cd /root || exit
else
  echo "ERROR: Setting the archived baseurl is only supported for centos 7 and 8 environments"
  exit 1
fi

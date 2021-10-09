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

# RUN apt-get remove --auto-remove openssl \
RUN mkdir -p /opt/openssl-src \
  && curl -L -s -S \
    http://www.openssl.org/source/openssl-1.0.1g.tar.gz \
    -o /opt/openssl.tar.gz \
  && tar xzf /opt/openssl.tar.gz --strip-components 1 -C /opt/openssl-src \
  && cd /opt/openssl-src \
  && ./config --prefix=/opt/openssl \
  && make \
  && make install_sw \
  && cd /root \
  && rm -rf /opt/openssl-src
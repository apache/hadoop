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

FROM ghcr.io/apache/hadoop-runner:jdk11-u2204
ARG HADOOP_VERSION=3.4.3
ARG BASE_URL=https://dlcdn.apache.org/hadoop/common
ARG TARGETPLATFORM
WORKDIR /opt/hadoop
RUN set -eux; \
    echo "Building for ${TARGETPLATFORM}"; \
    case "${TARGETPLATFORM}" in \
        linux/amd64) HADOOP_ARCH='' ;; \
        linux/arm64) HADOOP_ARCH='-aarch64' ;; \
        *) echo "Unsupported platform: ${TARGETPLATFORM}"; exit 1 ;; \
    esac; \
    export HADOOP_URL="${BASE_URL}/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}${HADOOP_ARCH}.tar.gz"; \
    curl -LSs "$HADOOP_URL" | tar -x -z --strip-components 1 && rm -rf /opt/hadoop/share/doc
ADD log4j.properties /opt/hadoop/etc/hadoop/log4j.properties
RUN sudo chown -R hadoop:users /opt/hadoop/etc/hadoop/*
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop

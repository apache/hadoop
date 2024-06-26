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

ARG JAVA_VERSION=11

# Ubuntu 22.04 LTS
FROM eclipse-temurin:${JAVA_VERSION}-jammy

RUN apt update -q \
    && DEBIAN_FRONTEND=noninteractive apt install -y --no-install-recommends \
      jq \
      krb5-user \
      ncat \
      python3-pip \
      python-is-python3 \
      sudo \
    && apt clean

# Robot Framework for testing
RUN pip install robotframework \
    && rm -fr ~/.cache/pip

#dumb init for proper init handling
RUN set -eux; \
    ARCH="$(arch)"; \
    v=1.2.5 ; \
    url="https://github.com/Yelp/dumb-init/releases/download/v${v}/dumb-init_${v}_${ARCH}"; \
    case "${ARCH}" in \
      x86_64) \
        sha256='e874b55f3279ca41415d290c512a7ba9d08f98041b28ae7c2acb19a545f1c4df'; \
        ;; \
      aarch64) \
        sha256='b7d648f97154a99c539b63c55979cd29f005f88430fb383007fe3458340b795e'; \
        ;; \
      *) echo "Unsupported architecture: ${ARCH}"; exit 1 ;; \
    esac \
    && curl -L ${url} -o dumb-init \
    && echo -n "${sha256} *dumb-init"  | sha256sum -c - \
    && chmod +x dumb-init \
    && mv dumb-init /usr/local/bin/dumb-init

#byteman test for development
RUN curl -Lo /opt/byteman.jar https://repo.maven.apache.org/maven2/org/jboss/byteman/byteman/4.0.23/byteman-4.0.23.jar \
    && chmod o+r /opt/byteman.jar

#async profiler for development profiling
RUN set -eux; \
    ARCH="$(arch)"; \
    v=2.8.3; \
    case "${ARCH}" in \
      x86_64) \
        url="https://github.com/jvm-profiling-tools/async-profiler/releases/download/v${v}/async-profiler-${v}-linux-x64.tar.gz" \
        ;; \
      aarch64) \
        url="https://github.com/jvm-profiling-tools/async-profiler/releases/download/v${v}/async-profiler-${v}-linux-arm64.tar.gz" \
        ;; \
      *) echo "Unsupported architecture: ${ARCH}"; exit 1 ;; \
    esac \
    && curl -L ${url} | tar xvz \
    && mv async-profiler-* /opt/profiler \
    && chmod -R go+rX /opt/profiler

RUN mkdir -p /etc/security/keytabs \
    && chmod -R a+wr /etc/security/keytabs

RUN groupadd --gid 1000 hadoop \
    && useradd --uid 1000 hadoop --gid 100 --home /opt/hadoop \
    && mkdir -p /opt/hadoop \
    && chown hadoop:users /opt/hadoop \
    && echo "hadoop ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

RUN mkdir -p /etc/hadoop \
    && chmod 1777 /etc/hadoop \
    && mkdir -p /var/log/hadoop \
    && chmod 1777 /var/log/hadoop

ENV HADOOP_CONF_DIR=/etc/hadoop
ENV HADOOP_LOG_DIR=/var/log/hadoop
ENV PATH=$PATH:/opt/hadoop/bin

COPY --chown=hadoop --chmod=755 scripts /opt/
COPY --chmod=644 krb5.conf /etc/

WORKDIR /opt/hadoop
USER hadoop
ENTRYPOINT ["/usr/local/bin/dumb-init", "--", "/opt/starter.sh"]

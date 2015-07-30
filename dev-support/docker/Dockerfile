
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

# Dockerfile for installing the necessary dependencies for building Hadoop.
# See BUILDING.txt.

# FROM dockerfile/java:openjdk-7-jdk
# FROM dockerfile/java:oracle-java7
FROM ubuntu:trusty

WORKDIR /root

RUN apt-get install -y software-properties-common
RUN add-apt-repository -y ppa:webupd8team/java
RUN apt-get update

# Auto-accept the Oracle JDK license
RUN echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections

RUN apt-get install -y oracle-java7-installer

# Install dependencies from packages
RUN apt-get update && apt-get install --no-install-recommends -y \
    git curl ant make maven \
    cmake gcc g++ protobuf-compiler \
    build-essential libtool \
    zlib1g-dev pkg-config libssl-dev \
    snappy libsnappy-dev \
    bzip2 libbz2-dev \
    libjansson-dev \
    fuse libfuse-dev \
    libcurl4-openssl-dev \
    python python2.7

# Install Forrest
RUN mkdir -p /usr/local/apache-forrest ; \
    curl -O http://archive.apache.org/dist/forrest/0.8/apache-forrest-0.8.tar.gz ; \
    tar xzf *forrest* --strip-components 1 -C /usr/local/apache-forrest ; \
    echo 'forrest.home=/usr/local/apache-forrest' > build.properties

# Install findbugs
RUN mkdir -p /opt/findbugs && \
    wget http://sourceforge.net/projects/findbugs/files/findbugs/3.0.1/findbugs-noUpdateChecks-3.0.1.tar.gz/download \
         -O /opt/findbugs.tar.gz && \
    tar xzf /opt/findbugs.tar.gz --strip-components 1 -C /opt/findbugs
ENV FINDBUGS_HOME /opt/findbugs

# Install shellcheck
RUN apt-get install -y cabal-install
RUN cabal update && cabal install shellcheck --global

# Fixing the Apache commons / Maven dependency problem under Ubuntu:
# See http://wiki.apache.org/commons/VfsProblems
RUN cd /usr/share/maven/lib && ln -s ../../java/commons-lang.jar .

# Avoid out of memory errors in builds
ENV MAVEN_OPTS -Xms256m -Xmx512m

# Add a welcome message and environment checks.
ADD hadoop_env_checks.sh /root/hadoop_env_checks.sh
RUN chmod 755 /root/hadoop_env_checks.sh
RUN echo '~/hadoop_env_checks.sh' >> /root/.bashrc


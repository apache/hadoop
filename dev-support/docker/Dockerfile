
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

FROM ubuntu:xenial

WORKDIR /root

#####
# Disable suggests/recommends
#####
RUN echo APT::Install-Recommends "0"\; > /etc/apt/apt.conf.d/10disableextras
RUN echo APT::Install-Suggests "0"\; >>  /etc/apt/apt.conf.d/10disableextras

ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_TERSE true

######
# Install common dependencies from packages. Versions here are either
# sufficient or irrelevant.
#
# WARNING: DO NOT PUT JAVA APPS HERE! Otherwise they will install default
# Ubuntu Java.  See Java section below!
######
RUN apt-get -q update && apt-get -q install -y \
    apt-utils \
    build-essential \
    bzip2 \
    curl \
    doxygen \
    fuse \
    g++ \
    gcc \
    git \
    gnupg-agent \
    libbz2-dev \
    libcurl4-openssl-dev \
    libfuse-dev \
    libprotobuf-dev \
    libprotoc-dev \
    libsnappy-dev \
    libssl-dev \
    libtool \
    locales \
    make \
    pinentry-curses \
    pkg-config \
    python \
    python2.7 \
    python-pip \
    python-pkg-resources \
    python-setuptools \
    python-wheel \
    rsync \
    software-properties-common \
    snappy \
    sudo \
    zlib1g-dev

#######
# OpenJDK 8
#######
RUN apt-get -q install -y openjdk-8-jdk

#######
# OpenJDK 9
# w/workaround for
# https://bugs.launchpad.net/ubuntu/+source/openjdk-9/+bug/1593191
#######
RUN apt-get -o Dpkg::Options::="--force-overwrite" \
    -q install -y \
    openjdk-9-jdk-headless

#######
# Set default Java
#######
#
# By default, OpenJDK sets the default Java to the highest version.
# We want the opposite, soooooo....
#
RUN update-java-alternatives --set java-1.8.0-openjdk-amd64
RUN update-alternatives --get-selections | grep -i jdk | \
    while read line; do \
      alternative=$(echo $line | awk '{print $1}'); \
      path=$(echo $line | awk '{print $3}'); \
      newpath=$(echo $path | sed -e 's/java-9/java-8/'); \
      update-alternatives --set $alternative $newpath; \
    done

######
# Install cmake 3.1.0 (3.5.1 ships with Xenial)
######
RUN mkdir -p /opt/cmake && \
    curl -L -s -S \
      https://cmake.org/files/v3.1/cmake-3.1.0-Linux-x86_64.tar.gz \
      -o /opt/cmake.tar.gz && \
    tar xzf /opt/cmake.tar.gz --strip-components 1 -C /opt/cmake
ENV CMAKE_HOME /opt/cmake
ENV PATH "${PATH}:/opt/cmake/bin"

######
# Install Google Protobuf 2.5.0 (2.6.0 ships with Xenial)
######
RUN mkdir -p /opt/protobuf-src && \
    curl -L -s -S \
      https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.gz \
      -o /opt/protobuf.tar.gz && \
    tar xzf /opt/protobuf.tar.gz --strip-components 1 -C /opt/protobuf-src
RUN cd /opt/protobuf-src && ./configure --prefix=/opt/protobuf && make install
ENV PROTOBUF_HOME /opt/protobuf
ENV PATH "${PATH}:/opt/protobuf/bin"

######
# Install Apache Maven 3.3.9 (3.3.9 ships with Xenial)
######
RUN apt-get -q update && apt-get -q install -y maven
ENV MAVEN_HOME /usr

######
# Install findbugs 3.0.1 (3.0.1 ships with Xenial)
# Ant is needed for findbugs
######
RUN apt-get -q update && apt-get -q install -y findbugs ant
ENV FINDBUGS_HOME /usr

####
# Install shellcheck (0.4.6, the latest as of 2017-09-26)
####
RUN add-apt-repository -y ppa:jonathonf/ghc-8.0.2
RUN apt-get -q update && apt-get -q install -y shellcheck

####
# Install bats (0.4.0, the latest as of 2017-09-26, ships with Xenial)
####
RUN apt-get -q update && apt-get -q install -y bats

####
# Install pylint (always want latest)
####
RUN pip2 install pylint

####
# Install dateutil.parser
####
RUN pip2 install python-dateutil

###
# Install node.js for web UI framework (4.2.6 ships with Xenial)
###
RUN apt-get -y install nodejs && \
    ln -s /usr/bin/nodejs /usr/bin/node && \
    apt-get -y install npm && \
    npm install npm@latest -g && \
    npm install -g bower && \
    npm install -g ember-cli

###
# Avoid out of memory errors in builds
###
ENV MAVEN_OPTS -Xms256m -Xmx1g


###
# Everything past this point is either not needed for testing or breaks Yetus.
# So tell Yetus not to read the rest of the file:
# YETUS CUT HERE
###

####
# Install svn & Forrest (for Apache Hadoop website)
###
RUN apt-get -q update && apt-get -q install -y subversion

RUN mkdir -p /opt/apache-forrest && \
    curl -L -s -S \
      https://archive.apache.org/dist/forrest/0.8/apache-forrest-0.8.tar.gz \
      -o /opt/forrest.tar.gz && \
    tar xzf /opt/forrest.tar.gz --strip-components 1 -C /opt/apache-forrest
RUN echo 'forrest.home=/opt/apache-forrest' > build.properties
ENV FORREST_HOME=/opt/apache-forrest

# Add a welcome message and environment checks.
ADD hadoop_env_checks.sh /root/hadoop_env_checks.sh
RUN chmod 755 /root/hadoop_env_checks.sh
RUN echo '~/hadoop_env_checks.sh' >> /root/.bashrc


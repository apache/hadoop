
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


FROM ubuntu:trusty

WORKDIR /root

ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_TERSE true

######
# Install common dependencies from packages
#
# WARNING: DO NOT PUT JAVA APPS HERE! Otherwise they will install default
# Ubuntu Java.  See Java section below!
######
RUN apt-get -q update && apt-get -q install --no-install-recommends -y \
    build-essential \
    bzip2 \
    curl \
    doxygen \
    fuse \
    g++ \
    gcc \
    git \
    gnupg-agent \
    make \
    libbz2-dev \
    libcurl4-openssl-dev \
    libfuse-dev \
    libprotobuf-dev \
    libprotoc-dev \
    libsnappy-dev \
    libssl-dev \
    libtool \
    pinentry-curses \
    pkg-config \
    protobuf-compiler \
    protobuf-c-compiler \
    python \
    python2.7 \
    python2.7-dev \
    python-pip \
    rsync \
    snappy \
    zlib1g-dev

#######
# Oracle Java
#######

RUN echo "dot_style = mega" > "/root/.wgetrc"
RUN echo "quiet = on" >> "/root/.wgetrc"

RUN apt-get -q install --no-install-recommends -y software-properties-common
RUN add-apt-repository -y ppa:webupd8team/java
RUN apt-get -q update

# Auto-accept the Oracle JDK license
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections
RUN apt-get -q install --no-install-recommends -y oracle-java8-installer

####
# Apps that require Java
###
RUN apt-get -q update && apt-get -q install --no-install-recommends -y ant

######
# Install Apache Maven
######
RUN mkdir -p /opt/maven && \
    curl -L -s -S \
         https://www-us.apache.org/dist/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz \
         -o /opt/maven.tar.gz && \
    tar xzf /opt/maven.tar.gz --strip-components 1 -C /opt/maven
ENV MAVEN_HOME /opt/maven
ENV PATH "${PATH}:/opt/maven/bin"

######
# Install cmake
######
RUN mkdir -p /opt/cmake && \
    curl -L -s -S \
         https://cmake.org/files/v3.1/cmake-3.1.0-Linux-x86_64.tar.gz \
         -o /opt/cmake.tar.gz && \
    tar xzf /opt/cmake.tar.gz --strip-components 1 -C /opt/cmake
ENV CMAKE_HOME /opt/cmake
ENV PATH "${PATH}:/opt/cmake/bin"

######
# Install findbugs
######
RUN mkdir -p /opt/findbugs && \
    curl -L -s -S \
         https://sourceforge.net/projects/findbugs/files/findbugs/3.0.1/findbugs-noUpdateChecks-3.0.1.tar.gz/download \
         -o /opt/findbugs.tar.gz && \
    tar xzf /opt/findbugs.tar.gz --strip-components 1 -C /opt/findbugs
ENV FINDBUGS_HOME /opt/findbugs
ENV PATH "${PATH}:/opt/findbugs/bin"

####
# Install shellcheck
####
RUN apt-get -q install -y cabal-install
RUN mkdir /root/.cabal
RUN echo "remote-repo: hackage.fpcomplete.com:http://hackage.fpcomplete.com/" >> /root/.cabal/config
#RUN echo "remote-repo: hackage.haskell.org:http://hackage.haskell.org/" > /root/.cabal/config
RUN echo "remote-repo-cache: /root/.cabal/packages" >> /root/.cabal/config
RUN cabal update
RUN cabal install shellcheck --global

####
# Install bats
####
RUN add-apt-repository -y ppa:duggan/bats
RUN apt-get -q update
RUN apt-get -q install --no-install-recommends -y bats

####
# Install pylint
####
RUN pip install pylint

####
# Install dateutil.parser
####
RUN pip install python-dateutil

###
# Avoid out of memory errors in builds
###
ENV MAVEN_OPTS -Xms256m -Xmx512m

###
# Install node js tools for web UI frameowkr
###
RUN apt-get -y install nodejs && \
    ln -s /usr/bin/nodejs /usr/bin/node && \
    apt-get -y install npm && \
    npm install npm@latest -g && \
    npm install -g bower && \
    npm install -g ember-cli

###
# Everything past this point is either not needed for testing or breaks Yetus.
# So tell Yetus not to read the rest of the file:
# YETUS CUT HERE
###

####
# Install Forrest (for Apache Hadoop website)
###
RUN mkdir -p /usr/local/apache-forrest ; \
    curl -s -S -O http://archive.apache.org/dist/forrest/0.8/apache-forrest-0.8.tar.gz ; \
    tar xzf *forrest* --strip-components 1 -C /usr/local/apache-forrest ; \
    echo 'forrest.home=/usr/local/apache-forrest' > build.properties

# Add a welcome message and environment checks.
ADD hadoop_env_checks.sh /root/hadoop_env_checks.sh
RUN chmod 755 /root/hadoop_env_checks.sh
RUN echo '~/hadoop_env_checks.sh' >> /root/.bashrc


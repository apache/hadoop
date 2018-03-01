
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
######
RUN apt-get -q update && apt-get -q install --no-install-recommends -y \
    ant \
    build-essential \
    bzip2 \
    cmake \
    curl \
    doxygen \
    fuse \
    g++ \
    gcc \
    git \
    gnupg-agent \
    make \
    maven \
    libbz2-dev \
    libcurl4-openssl-dev \
    libfuse-dev \
    libjansson-dev \
    libprotobuf-dev \
    libprotoc-dev \
    libsnappy-dev \
    libssl-dev \
    libtool \
    openjdk-7-jdk \
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

######
# Install Apache Maven
######
RUN mkdir -p /opt/maven && \
    curl -L -s -S \
         http://www-us.apache.org/dist/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz \
         -o /opt/maven.tar.gz && \
    tar xzf /opt/maven.tar.gz --strip-components 1 -C /opt/maven
ENV MAVEN_HOME /opt/maven


#######
# Java OpenJDK
#######

RUN echo "dot_style = mega" > "/root/.wgetrc"
RUN echo "quiet = on" >> "/root/.wgetrc"

RUN apt-get -q install --no-install-recommends -y software-properties-common
RUN add-apt-repository -y ppa:webupd8team/java
RUN apt-get -q update

# Install OpenJDK 7
RUN apt-get -q install --no-install-recommends -y openjdk-7-jdk

######
# Install spotbugs (successor of findbugs)
######
RUN mkdir -p /opt/spotbugs && \
    curl -L -s -S \
         http://repo.maven.apache.org/maven2/com/github/spotbugs/spotbugs/3.1.2/spotbugs-3.1.2.tgz \
         -o /opt/spotbugs.tar.gz && \
    tar xzf /opt/spotbugs.tar.gz --strip-components 1 -C /opt/spotbugs
# Hadoop uses FINDBUGS_HOME to run spotbugs
ENV FINDBUGS_HOME /opt/spotbugs

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
ENV MAVEN_OPTS -Xms256m -Xmx1536m

###
# Install node js tools for web UI frameowkr
###
RUN apt-get -y install nodejs && \
    ln -s /usr/bin/nodejs /usr/bin/node && \
    apt-get -y install npm && \
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


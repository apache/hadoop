
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

######
# Install common dependencies from packages
######
RUN apt-get update && apt-get install --no-install-recommends -y \
    git curl ant make maven \
    cmake gcc g++ \
    protobuf-compiler libprotoc-dev \
    protobuf-c-compiler libprotobuf-dev \
    build-essential libtool \
    zlib1g-dev pkg-config libssl-dev \
    snappy libsnappy-dev \
    bzip2 libbz2-dev \
    libjansson-dev \
    fuse libfuse-dev \
    libcurl4-openssl-dev \
    python python2.7 pylint \
    openjdk-7-jdk doxygen

# Fixing the Apache commons / Maven dependency problem under Ubuntu:
# See http://wiki.apache.org/commons/VfsProblems
RUN cd /usr/share/maven/lib && ln -s ../../java/commons-lang.jar .

#######
# Java OpenJDK
#######

RUN apt-get install -y software-properties-common
RUN add-apt-repository -y ppa:webupd8team/java
RUN apt-get update

# Install OpenJDK 7
RUN apt-get install -y openjdk-7-jdk

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
RUN apt-get install -y cabal-install
RUN cabal update && cabal install shellcheck --global

###
# Avoid out of memory errors in builds
###
ENV MAVEN_OPTS -Xms256m -Xmx1536m

###
# Everything past this point is either not needed for testing or breaks Yetus.
# So tell Yetus not to read the rest of the file:
# YETUS CUT HERE
###

####
# Install Forrest (for Apache Hadoop website)
###
RUN mkdir -p /usr/local/apache-forrest ; \
    curl -O http://archive.apache.org/dist/forrest/0.8/apache-forrest-0.8.tar.gz ; \
    tar xzf *forrest* --strip-components 1 -C /usr/local/apache-forrest ; \
    echo 'forrest.home=/usr/local/apache-forrest' > build.properties

# Add a welcome message and environment checks.
ADD hadoop_env_checks.sh /root/hadoop_env_checks.sh
RUN chmod 755 /root/hadoop_env_checks.sh
RUN echo '~/hadoop_env_checks.sh' >> /root/.bashrc


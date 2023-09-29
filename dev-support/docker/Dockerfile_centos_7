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

FROM centos:7

WORKDIR /root

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

######
# Platform package dependency resolver
######
COPY pkg-resolver pkg-resolver
RUN chmod a+x pkg-resolver/*.sh pkg-resolver/*.py \
    && chmod a+r pkg-resolver/*.json

######
# Install packages from yum
######
# hadolint ignore=DL3008,SC2046
RUN yum update -y \
    && yum groupinstall -y "Development Tools" \
    && yum install -y \
        centos-release-scl \
        python3 \
    && yum install -y $(pkg-resolver/resolve.py centos:7)

# Set GCC 9 as the default C/C++ compiler
RUN echo "source /opt/rh/devtoolset-9/enable" >> /etc/bashrc
SHELL ["/bin/bash", "--login", "-c"]

######
# Set the environment variables needed for CMake
# to find and use GCC 9 for compilation
######
ENV GCC_HOME "/opt/rh/devtoolset-9"
ENV CC "${GCC_HOME}/root/usr/bin/gcc"
ENV CXX "${GCC_HOME}/root/usr/bin/g++"
ENV SHLVL 1
ENV LD_LIBRARY_PATH "${GCC_HOME}/root/usr/lib64:${GCC_HOME}/root/usr/lib:${GCC_HOME}/root/usr/lib64/dyninst:${GCC_HOME}/root/usr/lib/dyninst:${GCC_HOME}/root/usr/lib64:${GCC_HOME}/root/usr/lib:/usr/lib:/usr/lib64"
ENV PCP_DIR "${GCC_HOME}/root"
ENV MANPATH "${GCC_HOME}/root/usr/share/man:"
ENV PATH "${GCC_HOME}/root/usr/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ENV PKG_CONFIG_PATH "${GCC_HOME}/root/usr/lib64/pkgconfig"
ENV INFOPATH "${GCC_HOME}/root/usr/share/info"

# TODO: Set locale

######
# Set env vars required to build Hadoop
######
ENV MAVEN_HOME /opt/maven
ENV PATH "${PATH}:${MAVEN_HOME}/bin"
# JAVA_HOME must be set in Maven >= 3.5.0 (MNG-6003)
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0

#######
# Set env vars for SpotBugs
#######
ENV SPOTBUGS_HOME /opt/spotbugs

#######
# Set env vars for Google Protobuf
#######
ENV PROTOBUF_HOME /opt/protobuf
ENV PATH "${PATH}:/opt/protobuf/bin"

######
# Install packages
######
RUN pkg-resolver/install-maven.sh centos:7
RUN pkg-resolver/install-cmake.sh centos:7
RUN pkg-resolver/install-zstandard.sh centos:7
RUN pkg-resolver/install-yasm.sh centos:7
RUN pkg-resolver/install-protobuf.sh centos:7
RUN pkg-resolver/install-boost.sh centos:7
RUN pkg-resolver/install-spotbugs.sh centos:7
RUN pkg-resolver/install-nodejs.sh centos:7
RUN pkg-resolver/install-git.sh centos:7
RUN pkg-resolver/install-common-pkgs.sh

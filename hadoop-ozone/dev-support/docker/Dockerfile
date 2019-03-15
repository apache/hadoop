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
FROM alpine
RUN apk add --update --no-cache bash alpine-sdk maven grep openjdk8 py-pip rsync procps autoconf automake libtool findutils

#Install real glibc
RUN apk --no-cache add ca-certificates wget && \
    wget -q -O /etc/apk/keys/sgerrand.rsa.pub https://alpine-pkgs.sgerrand.com/sgerrand.rsa.pub && \
    wget https://github.com/sgerrand/alpine-pkg-glibc/releases/download/2.28-r0/glibc-2.28-r0.apk && \
    apk add glibc-2.28-r0.apk

#Install protobuf
RUN mkdir -p /usr/local/src/ && \
    cd /usr/local/src/ && \
    wget https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.gz && \
    tar xvf protobuf-2.5.0.tar.gz && \
    cd protobuf-2.5.0 && \
    ./autogen.sh && \
    ./configure --prefix=/usr && \
    make && \
    make install && \
    protoc --version

#Findbug install
RUN mkdir -p /opt && \
    curl -sL https://sourceforge.net/projects/findbugs/files/findbugs/3.0.1/findbugs-3.0.1.tar.gz/download | tar -xz  && \
     mv findbugs-* /opt/findbugs

#Install apache-ant
RUN mkdir -p /opt && \
    curl -sL 'https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=/ant/binaries/apache-ant-1.10.5-bin.tar.gz' | tar -xz  && \
       mv apache-ant* /opt/ant

#Install docker-compose
RUN pip install docker-compose

#Install pytest==2.8.7
RUN pip install pytest==2.8.7

ENV PATH=$PATH:/opt/findbugs/bin

RUN addgroup -g 1000 default && \
   for i in $(seq 1 2000); do adduser jenkins$i -u $i -G default -h /tmp/ -H -D; done

#This is a very huge local maven cache. Usually the mvn repository is not safe to be 
#shared between builds as concurrent installls are not handled very well
#A simple workaround is to provide all the required 3rd party lib in the docker image
#It will be cached by docker, and any additional dependency can be downloaded, artifacts
#can be installed
USER jenkins1000
RUN cd /tmp && \
   git clone --depth=1 https://gitbox.apache.org/repos/asf/hadoop.git -b trunk && \
   cd /tmp/hadoop && \
   mvn package dependency:go-offline -DskipTests -P hdds -pl :hadoop-ozone-dist -am && \
   rm -rf /tmp/.m2/repository/org/apache/hadoop/*hdds* && \
   rm -rf /tmp/.m2/repository/org/apache/hadoop/*ozone* && \
   find /tmp/.m2/repository -exec chmod o+wx {} \;

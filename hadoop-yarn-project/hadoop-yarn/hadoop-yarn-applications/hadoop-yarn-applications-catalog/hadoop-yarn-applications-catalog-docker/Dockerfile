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

FROM centos:7

RUN yum -y install tomcat lsof krb5-workstation sssd-client curl
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN mkdir -p /opt/apache/solr && \
    curl -SL http://archive.apache.org/dist/lucene/solr/7.7.0/solr-7.7.0.tgz | \
    tar -xzC /opt/apache/solr --strip 1
COPY src/main/scripts/setup-image.sh /setup-image.sh
COPY src/main/resources/samples.xml /tmp/samples.xml
COPY src/main/resources/jaas.config /etc/tomcat/jaas.config.template
COPY src/main/scripts/entrypoint.sh /usr/bin/entrypoint.sh
COPY target/ROOT.war /var/lib/tomcat/webapps/ROOT.war
RUN chmod 755 /setup-image.sh
RUN chmod 755 /usr/bin/entrypoint.sh
RUN /setup-image.sh

EXPOSE 8080
EXPOSE 8983
WORKDIR /

ENTRYPOINT ["/usr/bin/entrypoint.sh" ]

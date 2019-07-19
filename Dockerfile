FROM centos:7 as build

RUN yum -y update && yum clean all

# newer maven
RUN yum -y install --setopt=skip_missing_names_on_install=False centos-release-scl
# cmake3
RUN yum -y install --setopt=skip_missing_names_on_install=False epel-release

RUN yum -y install --setopt=skip_missing_names_on_install=False \
    java-1.8.0-openjdk-devel \
    java-1.8.0-openjdk \
    rh-maven33 \
    protobuf protobuf-compiler \
    patch \
    lzo-devel zlib-devel gcc gcc-c++ make autoconf automake libtool openssl-devel fuse-devel \
    cmake3 \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN ln -s /usr/bin/cmake3 /usr/bin/cmake

RUN mkdir /build
COPY .git /build/.git
COPY hadoop-yarn-project /build/hadoop-yarn-project
COPY hadoop-assemblies /build/hadoop-assemblies
COPY hadoop-project /build/hadoop-project
COPY hadoop-common-project /build/hadoop-common-project
COPY hadoop-cloud-storage-project /build/hadoop-cloud-storage-project
COPY hadoop-project-dist /build/hadoop-project-dist
COPY hadoop-maven-plugins /build/hadoop-maven-plugins
COPY hadoop-dist /build/hadoop-dist
COPY hadoop-minicluster /build/hadoop-minicluster
COPY hadoop-mapreduce-project /build/hadoop-mapreduce-project
COPY hadoop-tools /build/hadoop-tools
COPY hadoop-hdfs-project /build/hadoop-hdfs-project
COPY hadoop-client-modules /build/hadoop-client-modules
COPY hadoop-build-tools /build/hadoop-build-tools
COPY dev-support /build/dev-support
COPY pom.xml /build/pom.xml
COPY LICENSE.txt /build/LICENSE.txt
COPY BUILDING.txt /build/BUILDING.txt
COPY NOTICE.txt /build/NOTICE.txt
COPY README.txt /build/README.txt

ENV CMAKE_C_COMPILER=gcc CMAKE_CXX_COMPILER=g++

# build hadoop
RUN scl enable rh-maven33 'cd /build && mvn -B -e -Dtest=false -DskipTests -Dmaven.javadoc.skip=true clean package -Pdist,native -Dtar'
# Install prometheus-jmx agent
RUN scl enable rh-maven33 'mvn dependency:get -Dartifact=io.prometheus.jmx:jmx_prometheus_javaagent:0.3.1:jar -Ddest=/build/jmx_prometheus_javaagent.jar'
# Get gcs-connector for Hadoop
RUN scl enable rh-maven33 'cd /build && mvn dependency:get -Dartifact=com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.0.0-RC2:jar && mv $HOME/.m2/repository/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.0.0-RC2/gcs-connector-hadoop3-2.0.0-RC2.jar /build/gcs-connector-hadoop3-2.0.0-RC2.jar'

FROM centos:7

# our copy of faq and jq
COPY faq.repo /etc/yum.repos.d/ecnahc515-faq-epel-7.repo

RUN yum install --setopt=skip_missing_names_on_install=False -y \
        epel-release \
    && yum install --setopt=skip_missing_names_on_install=False -y \
        java-1.8.0-openjdk \
        java-1.8.0-openjdk-devel \
        curl \
        less  \
        procps \
        net-tools \
        bind-utils \
        which \
        jq \
        rsync \
        openssl \
        faq \
    && yum clean all \
    && rm -rf /tmp/* /var/tmp/*

ENV JAVA_HOME=/etc/alternatives/jre

ENV HADOOP_VERSION 3.1.1

ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_LOG_DIR=$HADOOP_HOME/logs
ENV HADOOP_CLASSPATH=$HADOOP_HOME/share/hadoop/tools/lib/*
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV PROMETHEUS_JMX_EXPORTER /opt/jmx_exporter/jmx_exporter.jar
ENV PATH=$HADOOP_HOME/bin:$PATH

COPY --from=build /build/hadoop-dist/target/hadoop-$HADOOP_VERSION $HADOOP_HOME
COPY --from=build /build/jmx_prometheus_javaagent.jar $PROMETHEUS_JMX_EXPORTER
COPY --from=build /build/gcs-connector-hadoop3-2.0.0-RC2.jar $HADOOP_HOME/share/hadoop/tools/lib/gcs-connector-hadoop3-2.0.0-RC2.jar

WORKDIR $HADOOP_HOME

# remove unnecessary doc/src files
RUN rm -rf ${HADOOP_HOME}/share/doc \
    && for dir in common hdfs mapreduce tools yarn; do \
         rm -rf ${HADOOP_HOME}/share/hadoop/${dir}/sources; \
       done \
    && rm -rf ${HADOOP_HOME}/share/hadoop/common/jdiff \
    && rm -rf ${HADOOP_HOME}/share/hadoop/mapreduce/lib-examples \
    && rm -rf ${HADOOP_HOME}/share/hadoop/yarn/test \
    && find ${HADOOP_HOME}/share/hadoop -name *test*.jar | xargs rm -rf

RUN ln -s $HADOOP_HOME/etc/hadoop $HADOOP_CONF_DIR
RUN mkdir -p $HADOOP_LOG_DIR

# https://docs.oracle.com/javase/7/docs/technotes/guides/net/properties.html
# Java caches dns results forever, don't cache dns results forever:
RUN sed -i '/networkaddress.cache.ttl/d' $JAVA_HOME/lib/security/java.security
RUN sed -i '/networkaddress.cache.negative.ttl/d' $JAVA_HOME/lib/security/java.security
RUN echo 'networkaddress.cache.ttl=0' >> $JAVA_HOME/lib/security/java.security
RUN echo 'networkaddress.cache.negative.ttl=0' >> $JAVA_HOME/lib/security/java.security

RUN useradd hadoop -m -u 1002 -d $HADOOP_HOME

# imagebuilder expects the directory to be created before VOLUME
RUN mkdir -p /hadoop/dfs/data /hadoop/dfs/name
# to allow running as non-root
RUN chown -R 1002:0 $HADOOP_HOME /hadoop $HADOOP_CONF_DIR && \
    chmod -R 774 $HADOOP_HOME /hadoop $HADOOP_CONF_DIR /etc/passwd

VOLUME /hadoop/dfs/data /hadoop/dfs/name

USER 1002

LABEL io.k8s.display-name="OpenShift Hadoop" \
      io.k8s.description="This is an image used by operator-metering to to install and run HDFS." \
      io.openshift.tags="openshift" \
      maintainer="Chance Zibolski <czibolsk@redhat.com>"


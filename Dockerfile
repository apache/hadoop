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
COPY . /build

ENV CMAKE_C_COMPILER=gcc CMAKE_CXX_COMPILER=g++

WORKDIR /build
RUN scl enable rh-maven33 'mvn -B -e -Dtest=false -DskipTests -Dmaven.javadoc.skip=true clean package -Pdist,native -Dtar'

FROM centos:7

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
    && yum clean all \
    && rm -rf /tmp/* /var/tmp/*

ENV JAVA_HOME=/etc/alternatives/jre

ENV HADOOP_VERSION 3.1.1

ENV HADOOP_CLASSPATH=/opt/hadoop-$HADOOP_VERSION/share/hadoop/tools/lib/*
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV HADOOP_HOME=/opt/hadoop-$HADOOP_VERSION
ENV PATH=$HADOOP_HOME/bin:$PATH

COPY --from=build /build/hadoop-dist/target/hadoop-$HADOOP_VERSION /opt/hadoop-$HADOOP_VERSION
# remove unnecessary doc/src files
RUN rm -rf ${HADOOP_HOME}/share/doc \
    && for dir in common hdfs mapreduce tools yarn; do \
         rm -rf ${HADOOP_HOME}/share/hadoop/${dir}/sources; \
       done \
    && rm -rf ${HADOOP_HOME}/share/hadoop/common/jdiff \
    && rm -rf ${HADOOP_HOME}/share/hadoop/mapreduce/lib-examples \
    && rm -rf ${HADOOP_HOME}/share/hadoop/yarn/test \
    && find ${HADOOP_HOME}/share/hadoop -name *test*.jar | xargs rm -rf

RUN ln -s /opt/hadoop-$HADOOP_VERSION/etc/hadoop /etc/hadoop
RUN ln -s /opt/hadoop-$HADOOP_VERSION /opt/hadoop
RUN mkdir -p /opt/hadoop-$HADOOP_VERSION/logs

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
RUN chown -R 1002:0 /opt /hadoop /hadoop /etc/hadoop && \
    chmod -R 774 /opt /hadoop /etc/hadoop /etc/passwd

VOLUME /hadoop/dfs/data /hadoop/dfs/name

USER 1002

LABEL io.k8s.display-name="OpenShift Hadoop" \
      io.k8s.description="This is an image used by operator-metering to to install and run HDFS." \
      io.openshift.tags="openshift" \
      maintainer="Chance Zibolski <czibolsk@redhat.com>"


#!/bin/bash
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


# Function to probe the exit code of the script commands, 
# and stop in the case of failure with an contextual error 
# message.
run() {
  echo "\$ ${@}"
  "${@}"
  exitCode=$?
  if [[ $exitCode != 0 ]]; then
    echo
    echo "Failed! running ${@} in `pwd`"
    echo
    exit $exitCode
  fi
}

doMD5() {
  MD5CMD="md5sum"
  which $MD5CMD
  if [[ $? != 0 ]]; then
    MD5CMD="md5"
  fi
  run $MD5CMD ${1} > ${1}.md5
}

# If provided, the created release artifacts will be tagged with it 
# (use RC#, i.e: RC0). Do not use a label to create the final release 
# artifact.
RC_LABEL=$1

# Extract Hadoop version from POM
HADOOP_VERSION=`cat pom.xml | grep "<version>" | head -1 | sed 's|^ *<version>||' | sed 's|</version>.*$||'`

# Setup git
GIT=${GIT:-git}

echo
echo "*****************************************************************"
echo
echo "Hadoop version to create release artifacts: ${HADOOP_VERSION}"
echo 
echo "Release Candidate Label: ${RC_LABEL}"
echo
echo "*****************************************************************"
echo

if [[ ! -z ${RC_LABEL} ]]; then
  RC_LABEL="-${RC_LABEL}"
fi

# Get Maven command
if [ -z "$MAVEN_HOME" ]; then
  MVN=mvn
else
  MVN=$MAVEN_HOME/bin/mvn
fi

ARTIFACTS_DIR="target/artifacts"

# git clean to clear any remnants from previous build
run ${GIT} clean -xdf

# mvn clean for sanity
run ${MVN} clean

# Create staging dir for release artifacts
run mkdir -p ${ARTIFACTS_DIR}

# Create RAT report
run ${MVN} apache-rat:check

# Create SRC and BIN tarballs for release,
# Using 'install’ goal instead of 'package' so artifacts are available 
# in the Maven local cache for the site generation
run ${MVN} install -Pdist,src,native -DskipTests -Dtar

# Create site for release
run ${MVN} site site:stage -Pdist -Psrc
run mkdir -p target/staging/hadoop-project/hadoop-project-dist/hadoop-yarn
run mkdir -p target/staging/hadoop-project/hadoop-project-dist/hadoop-mapreduce
run cp ./hadoop-common-project/hadoop-common/src/main/docs/releasenotes.html target/staging/hadoop-project/hadoop-project-dist/hadoop-common/
run cp ./hadoop-common-project/hadoop-common/CHANGES.txt target/staging/hadoop-project/hadoop-project-dist/hadoop-common/
run cp ./hadoop-hdfs-project/hadoop-hdfs/CHANGES.txt target/staging/hadoop-project/hadoop-project-dist/hadoop-hdfs/
run cp ./hadoop-yarn-project/CHANGES.txt target/staging/hadoop-project/hadoop-project-dist/hadoop-yarn/
run cp ./hadoop-mapreduce-project/CHANGES.txt target/staging/hadoop-project/hadoop-project-dist/hadoop-mapreduce/
run mv target/staging/hadoop-project target/r${HADOOP_VERSION}/
run cd target/
run tar czf hadoop-site-${HADOOP_VERSION}.tar.gz r${HADOOP_VERSION}/*
run cd ..

# Stage RAT report
find . -name rat.txt | xargs -I% cat % > ${ARTIFACTS_DIR}/hadoop-${HADOOP_VERSION}${RC_LABEL}-rat.txt

# Stage CHANGES.txt files
run cp ./hadoop-common-project/hadoop-common/CHANGES.txt ${ARTIFACTS_DIR}/CHANGES-COMMON-${HADOOP_VERSION}${RC_LABEL}.txt
run cp ./hadoop-hdfs-project/hadoop-hdfs/CHANGES.txt ${ARTIFACTS_DIR}/CHANGES-HDFS-${HADOOP_VERSION}${RC_LABEL}.txt
run cp ./hadoop-mapreduce-project/CHANGES.txt ${ARTIFACTS_DIR}/CHANGES-MAPREDUCE-${HADOOP_VERSION}${RC_LABEL}.txt
run cp ./hadoop-yarn-project/CHANGES.txt ${ARTIFACTS_DIR}/CHANGES-YARN-${HADOOP_VERSION}${RC_LABEL}.txt

# Prepare and stage BIN tarball
run cd hadoop-dist/target/
run tar -xzf hadoop-${HADOOP_VERSION}.tar.gz
run cp -r ../../target/r${HADOOP_VERSION}/* hadoop-${HADOOP_VERSION}/share/doc/hadoop/
run tar -czf hadoop-${HADOOP_VERSION}.tar.gz hadoop-${HADOOP_VERSION}
run cd ../..
run mv hadoop-dist/target/hadoop-${HADOOP_VERSION}.tar.gz ${ARTIFACTS_DIR}/hadoop-${HADOOP_VERSION}${RC_LABEL}.tar.gz

# Stage SRC tarball
run mv hadoop-dist/target/hadoop-${HADOOP_VERSION}-src.tar.gz ${ARTIFACTS_DIR}/hadoop-${HADOOP_VERSION}${RC_LABEL}-src.tar.gz

# Stage SITE tarball
run mv target/hadoop-site-${HADOOP_VERSION}.tar.gz ${ARTIFACTS_DIR}/hadoop-${HADOOP_VERSION}${RC_LABEL}-site.tar.gz

# MD5 SRC and BIN tarballs
doMD5 ${ARTIFACTS_DIR}/hadoop-${HADOOP_VERSION}${RC_LABEL}.tar.gz
doMD5 ${ARTIFACTS_DIR}/hadoop-${HADOOP_VERSION}${RC_LABEL}-src.tar.gz

run cd ${ARTIFACTS_DIR}
ARTIFACTS_DIR=`pwd`
echo
echo "Congratulations, you have successfully built the release"
echo "artifacts for Apache Hadoop ${HADOOP_VERSION}${RC_LABEL}"
echo
echo "The artifacts for this run are available at ${ARTIFACTS_DIR}:"
run ls -1 ${ARTIFACTS_DIR}
echo 
echo "Remember to sign them before staging them on the open"
echo

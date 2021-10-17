#!/usr/bin/env bash
# Copyright (c) 2021 Cloudera, Inc. All rights reserved.

POM_FILE=hadoop-hdfs-project/pom.xml \
SONAR_PROJECT_KEY=hadoop_hdfs_ut_master \
SONAR_PROJECT_NAME="Hadoop HDFS UT-master" \
cloudera/unit-tests.sh $@

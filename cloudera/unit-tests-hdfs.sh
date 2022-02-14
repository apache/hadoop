#!/usr/bin/env bash
# Copyright (c) 2021 Cloudera, Inc. All rights reserved.

POM_FILE=hadoop-hdfs-project/pom.xml \
SONAR_PROJECT_KEY=hadoop_hdfs_ut_master \
SONAR_PROJECT_NAME="Hadoop HDFS UT-master" \
SONAR_LOGIN="5d60a071a84e127e646f015d43e19d99b2ca4b83" \
cloudera/unit-tests.sh "$@"

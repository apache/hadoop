#!/usr/bin/env bash
# Copyright (c) 2021 Cloudera, Inc. All rights reserved.

POM_FILE=hadoop-common-project/pom.xml \
SONAR_PROJECT_KEY=hadoop_common_ut_master \
SONAR_PROJECT_NAME="Hadoop Common UT-master" \
SONAR_LOGIN="5d931adf967529fa4612dd68ff17f7f022d31be6" \
cloudera/unit-tests.sh $@

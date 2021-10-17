#!/usr/bin/env bash
# Copyright (c) 2021 Cloudera, Inc. All rights reserved.

POM_FILE=hadoop-common-project/pom.xml \
SONAR_PROJECT_KEY=hadoop_common_ut_master \
SONAR_PROJECT_NAME="Hadoop Common UT-master" \
cloudera/unit-tests.sh $@

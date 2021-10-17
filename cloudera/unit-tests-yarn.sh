#!/usr/bin/env bash
# Copyright (c) 2021 Cloudera, Inc. All rights reserved.

POM_FILE=hadoop-yarn-project/pom.xml \
SONAR_PROJECT_KEY=hadoop_yarn_ut_master \
SONAR_PROJECT_NAME="Hadoop YARN UT-master" \
cloudera/unit-tests.sh $@

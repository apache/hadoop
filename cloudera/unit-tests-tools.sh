#!/usr/bin/env bash
# Copyright (c) 2021 Cloudera, Inc. All rights reserved.

POM_FILE=hadoop-tools/pom.xml \
SONAR_PROJECT_KEY=hadoop_tools_ut_master \
SONAR_PROJECT_NAME="Hadoop Tools UT-master" \
cloudera/unit-tests.sh "$@"

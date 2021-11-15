#!/usr/bin/env bash
# Copyright (c) 2021 Cloudera, Inc. All rights reserved.

POM_FILE=hadoop-yarn-project/pom.xml \
SONAR_PROJECT_KEY=hadoop_yarn_ut_master \
SONAR_PROJECT_NAME="Hadoop YARN UT-master" \
SONAR_LOGIN="7a08a1e44bd225e99d1f6b43c67b2ac9c7532039" \
cloudera/unit-tests.sh $@

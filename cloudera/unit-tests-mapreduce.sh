#!/usr/bin/env bash
# Copyright (c) 2021 Cloudera, Inc. All rights reserved.

POM_FILE=hadoop-mapreduce-project/pom.xml \
SONAR_PROJECT_KEY=hadoop_mr_ut_master \
SONAR_PROJECT_NAME="Hadoop MapReduce UT-master" \
cloudera/unit-tests.sh $@

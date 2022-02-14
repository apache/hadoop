#!/usr/bin/env bash
# Copyright (c) 2021 Cloudera, Inc. All rights reserved.

POM_FILE=hadoop-mapreduce-project/pom.xml \
SONAR_PROJECT_KEY=hadoop_mr_ut_master \
SONAR_PROJECT_NAME="Hadoop MapReduce UT-master" \
SONAR_LOGIN="b5c3673f96a5ceb58bf867260469afb64aaaac41" \
cloudera/unit-tests.sh "$@"

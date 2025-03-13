#!/usr/bin/env bash
# Copyright (c) 2021 Cloudera, Inc. All rights reserved.

POM_FILE=hadoop-mapreduce-project/pom.xml \
SONAR_PROJECT_KEY=Hadoop-MapReduce-UT-cdh_main \
SONAR_PROJECT_NAME="Hadoop MapReduce UT-cdh_main" \
SONAR_TOKEN="sqp_60cc1041f49ff508d22606571c2fe2a04f2baa0f" \
cloudera/unit-tests.sh "$@"

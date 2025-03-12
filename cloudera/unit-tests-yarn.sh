#!/usr/bin/env bash
# Copyright (c) 2021 Cloudera, Inc. All rights reserved.

POM_FILE=hadoop-yarn-project/pom.xml \
SONAR_PROJECT_KEY=Hadoop-YARN-UT-cdh_main \
SONAR_PROJECT_NAME="Hadoop YARN UT-cdh_main" \
SONAR_TOKEN="sqp_50dce89c318b11761da0df13b55dec734d9a5a60" \
cloudera/unit-tests.sh --projects '!hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-timelineservice-hbase-tests' "$@"

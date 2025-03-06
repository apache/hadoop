#!/usr/bin/env bash
# Copyright (c) 2021 Cloudera, Inc. All rights reserved.

POM_FILE=hadoop-yarn-project/pom.xml \
SONAR_PROJECT_KEY=Hadoop-YARN-cdh_main \
SONAR_PROJECT_NAME="Hadoop YARN cdh_main" \
SONAR_TOKEN="sqp_bc50a870273f14e119cf704845b5af400e3bbc08" \
cloudera/unit-tests.sh --projects '!hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-timelineservice-hbase-tests' "$@"

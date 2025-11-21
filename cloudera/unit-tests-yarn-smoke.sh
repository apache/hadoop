#!/usr/bin/env bash
# Copyright (c) 2021 Cloudera, Inc. All rights reserved.

set -eux -o pipefail

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
MAVEN_SETTINGS="${MAVEN_SETTINGS:-"${SCRIPT_DIR}/settings.xml"}"

mvn -Dmaven.repo.local=$SCRIPT_DIR -s "$MAVEN_SETTINGS" -B -f "${SCRIPT_DIR}/../pom.xml" clean install -T 16 -DskipTests -DskipShade
mvn -Dmaven.repo.local=$SCRIPT_DIR -s "$MAVEN_SETTINGS" -B -f hadoop-yarn-project/pom.xml test --fail-never --projects 'hadoop-yarn/hadoop-yarn-common, hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common, hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager, hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager, hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-tests'

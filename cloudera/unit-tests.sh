#!/usr/bin/env bash
# Copyright (c) 2021 Cloudera, Inc. All rights reserved.

set -e
set -u
set -o pipefail
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

MAVEN_SETTINGS="${MAVEN_SETTINGS:-"${SCRIPT_DIR}/settings.xml"}"
MAIN_POM="${SCRIPT_DIR}/../pom.xml"
POM_FILE="${POM_FILE:-$MAIN_POM}"
TEST_EXCLUDE_PATTERN="${TEST_EXCLUDE_PATTERN:-$(cat "${SCRIPT_DIR}/test-excludes.txt" | { grep -v "^#" || test $? = 1; } | paste -sd "," -)}"


SONAR_URL="${SONAR_URL:-https://sonarqube.infra.cloudera.com}"
SONAR_LOGIN="${SONAR_LOGIN:-}"
SONAR_TOKEN="${SONAR_TOKEN:-}"
SONAR_PROJECT_KEY="${SONAR_PROJECT_KEY:-}"
SONAR_PROJECT_NAME="${SONAR_PROJECT_NAME:-}"

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

mvn -s "$MAVEN_SETTINGS" -B -e -Pclover -f "${SCRIPT_DIR}/../pom.xml" clean install -DskipTests -DskipShade \
    --projects '!hadoop-client-modules/hadoop-client-check-invariants,!hadoop-client-modules/hadoop-client-check-test-invariants,!hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-timelineservice-hbase-tests'

mvn -s "$MAVEN_SETTINGS" -B -e -Pclover -f "$POM_FILE" test -Dparallel-tests -DtestsThreadCount=8 -Dscale -Dtest.exclude.pattern="$TEST_EXCLUDE_PATTERN" -Dmaven.test.failure.ignore=true -DbatsFailOnError=false "$@"

mvn -s "$MAVEN_SETTINGS" -B -e -Pclover -f "$POM_FILE" clover:aggregate clover:clover

if [ -z ${SKIP_SONAR+x} ]; then
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
    export PATH=$JAVA_HOME/bin:$PATH

    if [ -n "$SONAR_LOGIN" ]; then
        mvn -s "$MAVEN_SETTINGS" -B -e -Pclover -f "$POM_FILE" sonar:sonar -Dsonar.clover.reportPath=./target/clover/clover.xml \
            -Dsonar.host.url="$SONAR_URL" -Dsonar.login="$SONAR_LOGIN" -Dsonar.projectKey="$SONAR_PROJECT_KEY" -Dsonar.projectName="$SONAR_PROJECT_NAME"
    elif [ -n "$SONAR_TOKEN" ]; then
        mvn -s "$MAVEN_SETTINGS" -B -e -Pclover -f "$POM_FILE" sonar:sonar -Dsonar.clover.reportPath=./target/clover/clover.xml \
            -Dsonar.host.url="$SONAR_URL" -Dsonar.token="$SONAR_TOKEN" -Dsonar.projectKey="$SONAR_PROJECT_KEY" -Dsonar.projectName="$SONAR_PROJECT_NAME"
    fi
fi

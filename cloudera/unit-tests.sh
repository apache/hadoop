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
SONAR_PROJECT_KEY="${SONAR_PROJECT_KEY:-}"
SONAR_PROJECT_NAME="${SONAR_PROJECT_NAME:-}"

mvn -s "$MAVEN_SETTINGS" -B -e -Pclover -f "${SCRIPT_DIR}/../pom.xml" clean install -DskipTests -DskipShade \
    --projects '!hadoop-client-modules/hadoop-client-check-invariants,!hadoop-client-modules/hadoop-client-check-test-invariants'

mvn -s "$MAVEN_SETTINGS" -B -e -Pclover -f "$POM_FILE" test -Dparallel-tests -DtestsThreadCount=8 -Dscale -Dtest.exclude.pattern="$TEST_EXCLUDE_PATTERN"

mvn -s "$MAVEN_SETTINGS" -B -e -Pclover -f "$POM_FILE" clover:aggregate clover:clover

if [ -z ${SKIP_SONAR+x} ]; then
    if [ -n "$SONAR_LOGIN" ]; then
        mvn -s "$MAVEN_SETTINGS" -B -e -Pclover -f "$POM_FILE" sonar:sonar -Dsonar.clover.reportPath=./target/clover/clover.xml \
            -Dsonar.host.url="$SONAR_URL" -Dsonar.login="$SONAR_LOGIN" -Dsonar.projectKey="$SONAR_PROJECT_KEY" -Dsonar.projectName="$SONAR_PROJECT_NAME"
    fi
fi

#!/usr/bin/env bash
# Copyright (c) 2021 Cloudera, Inc. All rights reserved.

set -eux -o pipefail
mvn -s cloudera/settings.xml clean install package -T 16 -DskipTests
mvn -s cloudera/settings.xml -B -f "$1/pom.xml" test --fail-never

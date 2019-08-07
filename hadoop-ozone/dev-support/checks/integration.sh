#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../../.." || exit 1

export MAVEN_OPTS="-Xmx4096m"
mvn -B install -f pom.ozone.xml -DskipTests
mvn -B -fn test -f pom.ozone.xml -pl :hadoop-ozone-integration-test,:hadoop-ozone-filesystem,:hadoop-ozone-tools
module_failed_tests=$(find "." -name 'TEST*.xml' -print0 \
    | xargs -0 -n1 "grep" -l -E "<failure|<error"\
    | awk -F/ '{sub("'"TEST-JUNIT_TEST_OUTPUT_DIR"'",""); sub(".xml",""); print $NF}')
if [[ -n "${module_failed_tests}" ]] ; then
    exit 1
fi
exit 0

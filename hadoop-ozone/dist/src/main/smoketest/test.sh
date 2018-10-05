#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

execute_tests(){
  COMPOSE_FILE=$DIR/../compose/$1/docker-compose.yaml
  TESTS=$2
  echo "Executing test ${TESTS[*]} with $COMPOSE_FILE"
  docker-compose -f "$COMPOSE_FILE" down
  docker-compose -f "$COMPOSE_FILE" up -d
  echo "Waiting 30s for cluster start up..."
  sleep 30
  for TEST in "${TESTS[@]}"; do
     set +e
     docker-compose -f "$COMPOSE_FILE" exec datanode python -m robot "smoketest/$TEST"
     set -e
  done
  if [ "$KEEP_RUNNING" = false ]; then
     docker-compose -f "$COMPOSE_FILE" down
  fi
}
RUN_ALL=true
KEEP_RUNNING=false
POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    --env)
    DOCKERENV="$2"
    RUN_ALL=false
    shift # past argument
    shift # past value
    ;;
    --keep)
    KEEP_RUNNING=true
    shift # past argument
    ;;
    --help|-h|-help)
    cat << EOF

 Acceptance test executor for ozone.

 This is a lightweight test executor for ozone.

 You can run it with

     ./test.sh

 Which executes all the tests in all the available environments.

 Or you can run manually one test with

 ./test.sh --keep --env ozone-hdfs basic

     --keep  means that docker cluster won't be stopped after the test (optional)
     --env defines the subdirectory under the compose dir
     The remaining parameters define the test suites under smoketest dir.
     Could be any directory or robot file relative to the smoketest dir.
EOF
    exit 0
    ;;
    *)
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done

if [ "$RUN_ALL" = true ]; then
#
# This is the definition of the ozone acceptance test suite
#
# We select the test suites and execute them on multiple type of clusters
#
   DEFAULT_TESTS=("basic")
   execute_tests ozone "${DEFAULT_TESTS[@]}"
   TESTS=("ozonefs")
   execute_tests ozonefs "${TESTS[@]}"
   TESTS=("s3")
   execute_tests ozones3 "${TESTS[@]}"
else
   execute_tests "$DOCKERENV" "${POSITIONAL[@]}"
fi

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

COMPOSE_ENV_NAME=$(basename "$COMPOSE_DIR")
COMPOSE_FILE=$COMPOSE_DIR/docker-compose.yaml
RESULT_DIR="$COMPOSE_DIR/result"
RESULT_DIR_INSIDE="${OZONE_DIR:-/opt/hadoop}/compose/$(basename "$COMPOSE_ENV_NAME")/result"
SMOKETEST_DIR_INSIDE="${OZONE_DIR:-/opt/hadoop}/smoketest"

#delete previous results
rm -rf "$RESULT_DIR"
mkdir -p "$RESULT_DIR"
#Should be writeable from the docker containers where user is different.
chmod ogu+w "$RESULT_DIR"

## @description wait until 3 datanodes are up (or 30 seconds)
## @param the docker-compose file
wait_for_datanodes(){

  #Reset the timer
  SECONDS=0

  #Don't give it up until 30 seconds
  while [[ $SECONDS -lt 90 ]]; do

     #This line checks the number of HEALTHY datanodes registered in scm over the
     # jmx HTTP servlet
     datanodes=$(docker-compose -f "$1" exec -T scm curl -s 'http://localhost:9876/jmx?qry=Hadoop:service=SCMNodeManager,name=SCMNodeManagerInfo' | jq -r '.beans[0].NodeCount[] | select(.key=="HEALTHY") | .value')
      if [[ "$datanodes" == "3" ]]; then

        #It's up and running. Let's return from the function.
         echo "$datanodes datanodes are up and registered to the scm"
         return
      else

         #Print it only if a number. Could be not a number if scm is not yet started
         if [[ "$datanodes" ]]; then
            echo "$datanodes datanode is up and healthy (until now)"
         fi
      fi

      sleep 2
   done
   echo "WARNING! Datanodes are not started successfully. Please check the docker-compose files"
}

## @description  Starts a docker-compose based test environment
start_docker_env(){
  docker-compose -f "$COMPOSE_FILE" down
  docker-compose -f "$COMPOSE_FILE" up -d --scale datanode=3
  wait_for_datanodes "$COMPOSE_FILE"
  sleep 10
}

## @description  Execute robot tests in a specific container.
## @param        Name of the container in the docker-compose file
## @param        robot test file or directory relative to the smoketest dir
execute_robot_test(){
  CONTAINER="$1"
  TEST="$2"
  TEST_NAME=$(basename "$TEST")
  TEST_NAME=${TEST_NAME%.*}
  set +e
  OUTPUT_NAME="$COMPOSE_ENV_NAME-$TEST_NAME-$CONTAINER"
  docker-compose -f "$COMPOSE_FILE" exec -e  SECURITY_ENABLED="${SECURITY_ENABLED}" -T "$CONTAINER" python -m robot --log NONE -N "$TEST_NAME" --report NONE "${OZONE_ROBOT_OPTS[@]}" --output "$RESULT_DIR_INSIDE/robot-$OUTPUT_NAME.xml" "$SMOKETEST_DIR_INSIDE/$TEST"
  set -e

}

## @description  Stops a docker-compose based test environment (with saving the logs)
stop_docker_env(){
  docker-compose -f "$COMPOSE_FILE" logs > "$RESULT_DIR/docker-$OUTPUT_NAME.log"
  if [ "${KEEP_RUNNING:-false}" = false ]; then
     docker-compose -f "$COMPOSE_FILE" down
  fi
}

## @description  Generate robot framework reports based on the saved results.
generate_report(){
  #Generate the combined output and return with the right exit code (note: robot = execute test, rebot = generate output)
  docker run --rm -v "$DIR/..:${OZONE_DIR:-/opt/hadoop}" apache/ozone-runner rebot -d "$RESULT_DIR_INSIDE" "$RESULT_DIR_INSIDE/robot-*.xml"
}
